"""IPv4 port of origin/topdisc 90b0983 topicindex/{topictable,iptree,ticket}.go.

Times are integer nanoseconds as in Go; wire quotes round up to milliseconds.
The tree deliberately remembers insertion depth, rather than rebuilding a
balanced histogram: historical insertion order is observable in the Go score.
"""
from collections import Counter, deque
from dataclasses import dataclass
import heapq
import ipaddress
import math

SECOND = 10**9
LIFETIME = 900 * SECOND
MAX_WAIT = 86400 * SECOND
WINDOW = 5 * SECOND
GRACE = 10 * SECOND


def duration(seconds):
    """Go secondsToDuration, including the NaN and positive-infinity cases."""
    if not seconds > 0:
        return 0
    return MAX_WAIT if seconds >= 86400 else math.ceil(seconds * SECOND)


def wire_wait(wait):
    # Match waitTimeToMs's floating-point conversion, then waitTimeFromMs.
    return math.ceil(wait / SECOND * 1000) * 10**6


@dataclass(slots=True)
class Bound:
    value: int = 0
    since: int = 0

    def remaining(self, now):
        return max(0, self.value - (now - self.since))

    def bump(self, value, now):
        if value > self.remaining(now):
            self.value, self.since = value, now


class IPNode:
    __slots__ = ('children', 'counter', 'bound')

    def __init__(self):
        self.children = [None, None]
        self.counter = 0
        self.bound = Bound()


class IPTree:
    def __init__(self):
        self.root = IPNode()

    def score_node(self, ip):
        root_count = self.root.counter
        depth_limit = min(32, root_count.bit_length() - 1)
        node = lpm = self.root
        overloaded = 0
        for depth in range(max(0, depth_limit)):
            node = node.children[(ip >> (31 - depth)) & 1]
            if node is None:
                break
            lpm = node
            if node.counter > root_count / (2 ** (depth + 1)):
                overloaded += 1
        # The denominator includes levels beyond the first missing branch.
        return (overloaded / depth_limit if depth_limit > 0 else 0.0), lpm

    def path_floor(self, ip, now):
        node, floor = self.root, 0
        # Go excludes the root and walks *all* existing levels, even if the
        # current effective depth is shallower after removals.
        for depth in range(32):
            node = node.children[(ip >> (31 - depth)) & 1]
            if node is None:
                break
            floor = max(floor, node.bound.remaining(now))
        return floor

    def insert(self, ip):
        depth_limit = min(32, max(0, self.root.counter.bit_length() - 1))
        node = self.root
        node.counter += 1
        for depth in range(depth_limit):
            bit = (ip >> (31 - depth)) & 1
            if node.children[bit] is None:
                node.children[bit] = IPNode()
            node = node.children[bit]
            node.counter += 1
        return depth_limit

    def remove(self, ip, levels):
        node = self.root
        node.counter -= 1
        for depth in range(levels):
            bit = (ip >> (31 - depth)) & 1
            child = node.children[bit]
            if child is None:
                return
            child.counter -= 1
            if child.counter == 0:
                node.children[bit] = None
                return
            node = child


def scored_ip(ip):
    """netutil.IsLAN's IPv4 exclusions (not Python's broader is_private)."""
    return not (ip >> 24 in (10, 127) or ip >> 20 == 0xAC1
                or ip >> 16 in (0xC0A8, 0xA9FE))


@dataclass(frozen=True, slots=True)
class Config:
    capacity: int = 5000
    safety: float = 0.0
    lifetime: int = LIFETIME
    base: float = 0.1
    exponent: int = 10


CONFIGS = {'code': Config(), 'reference': Config(capacity=1000, safety=1e-7)}


@dataclass(frozen=True, slots=True)
class Ticket:
    serial: int
    node: int
    topic: str
    wait: int
    last: int
    first: int

    @property
    def start(self):
        return self.last + self.wait

    def timing(self, now):
        if now < self.start:
            return 'early'
        if now >= self.start + WINDOW + GRACE:
            return 'expired'
        if now >= self.start + WINDOW:
            return 'late'
        return 'on_time'


@dataclass(frozen=True, slots=True)
class Reply:
    # status: admitted, existing, quote. Every request has a reply.
    status: str
    wait: int = 0
    ticket: Ticket | None = None


class Registrar:
    def __init__(self, config=CONFIGS['code'], *, ip_score=True):
        self.config = config
        # Private-address control: like Go's IsLAN exemption, bypass both the
        # score and path floor while still inserting/removing every admitted IP.
        self.ip_score = ip_score
        self.tree = IPTree()
        self.topics = Counter()
        self.topic_bounds = {}
        self.ads = {}
        self.expiries = []
        self.attacker_ads = 0
        self.attacker_topics = Counter()
        self.serial = 0
        self.used = set()
        self.used_queue = deque()
        self.ticket_timings = Counter()
        self.requests = 0
        self.replies = 0

    def add(self, node, topic, ip, now, attacker=False):
        key = (node, topic)
        if key in self.ads or len(self.ads) >= self.config.capacity:
            return False
        levels = self.tree.insert(ip)
        expiry = now + self.config.lifetime
        self.ads[key] = (ip, levels, attacker)
        heapq.heappush(self.expiries, (expiry, node, topic))
        self.topics[topic] += 1
        if attacker:
            self.attacker_ads += 1
            self.attacker_topics[topic] += 1
        return True

    def expire(self, now):
        while self.expiries and self.expiries[0][0] <= now:
            _, node, topic = heapq.heappop(self.expiries)
            ip, levels, attacker = self.ads.pop((node, topic))
            self.topics[topic] -= 1
            if not self.topics[topic]:
                del self.topics[topic]
                self.topic_bounds.pop(topic, None)
            self.tree.remove(ip, levels)
            if attacker:
                self.attacker_ads -= 1
                self.attacker_topics[topic] -= 1
                if not self.attacker_topics[topic]:
                    del self.attacker_topics[topic]

    def components(self, topic, ip, now):
        count = len(self.ads)
        inverse_occupancy = 1 - count / self.config.capacity
        base = (self.config.base * (self.config.lifetime / SECOND)
                / inverse_occupancy ** self.config.exponent
                if inverse_occupancy > 0 else math.inf)
        service = base * (self.topics[topic] / (count + 1))
        score, lpm, floor = 0.0, None, 0
        if self.ip_score and scored_ip(ip):
            score, lpm = self.tree.score_node(ip)
            floor = self.tree.path_floor(ip, now)
        ip_secs = base * score
        topic_floor = self.topic_bounds.get(topic)
        # Comparisons, rather than max(NaN, floor), match Go on a full table.
        charged_service, charged_ip = service, ip_secs
        if topic_floor and topic_floor.remaining(now) / SECOND > charged_service:
            charged_service = topic_floor.remaining(now) / SECOND
        if floor / SECOND > charged_ip:
            charged_ip = floor / SECOND
        needed = charged_service + charged_ip
        if self.config.safety:
            needed += base * self.config.safety
        return duration(needed), service, ip_secs, lpm

    def register(self, node, topic, ip, waited, now, attacker=False):
        if (node, topic) in self.ads:
            return 'existing', 0
        required, service, ip_secs, lpm = self.components(topic, ip, now)
        if waited < required and required - waited > SECOND:
            # Bump raw components only, and only when quoting a waiting ticket.
            if service > 0:
                self.topic_bounds.setdefault(topic, Bound()).bump(duration(service), now)
            if ip_secs > 0 and lpm is not None:
                lpm.bound.bump(duration(ip_secs), now)
            return 'quote', required - waited
        # This check comes *after* wait eligibility in Go. A full table can
        # therefore quote the 24h cap first; eligible requests get next expiry.
        if len(self.ads) >= self.config.capacity:
            return 'quote', self.expiries[0][0] - now
        self.add(node, topic, ip, now, attacker)
        return 'admitted', 0

    def _use(self, ticket, now):
        while self.used_queue and self.used_queue[0][0] <= now:
            _, serial = self.used_queue.popleft()
            self.used.remove(serial)
        if ticket.serial in self.used:
            return False
        if len(self.used_queue) >= 1 << 14:
            _, serial = self.used_queue.popleft()
            self.used.remove(serial)
        self.used.add(ticket.serial)
        self.used_queue.append((now + WINDOW + GRACE, ticket.serial))
        return True

    def request(self, node, topic, ip, now, ticket=None, attacker=False):
        """REGTOPIC handler for a valid, source-matched IPv4 ENR.

        An opaque non-Ticket represents authentication failure. Tickets are
        trusted simulator objects in place of sealed HMAC payloads.
        """
        self.expire(now)
        self.requests += 1
        self.replies += 1
        waited, first = 0, now
        if isinstance(ticket, Ticket) and ticket.node == node and ticket.topic == topic:
            timing = ticket.timing(now)
            self.ticket_timings[timing] += 1
            if timing == 'early':
                return Reply('quote', wire_wait(ticket.start - now), ticket)
            if timing != 'expired' and self._use(ticket, now):
                if timing == 'late':
                    waited = ticket.start - ticket.first
                    first = now - waited
                else:
                    waited, first = now - ticket.first, ticket.first
            elif timing != 'expired':
                self.ticket_timings['used'] += 1
        elif ticket is not None:
            self.ticket_timings['invalid'] += 1
        status, wait = self.register(node, topic, ip, waited, now, attacker)
        if status != 'quote':
            return Reply(status, wire_wait(self.config.lifetime))
        self.serial += 1
        ticket = Ticket(self.serial, node, topic, wait, now, first)
        return Reply(status, wire_wait(wait), ticket)


def retry_wait(reply, total_wait, config):
    """Advertiser's 1s floor and single-quote + cumulative 1.5E budgets."""
    wait = max(SECOND, reply.wait)
    budget = config.lifetime * 3 // 2
    return None if wait > budget or total_wait + wait > budget else wait


def ipv4(address):
    return int(ipaddress.IPv4Address(address))
