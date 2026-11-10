"""topdisc 703d5a2 admission (topictable.go, ticket.go) on the vendored Go
port, with the per-topic view the world model reads."""
import bisect
import heapq
import math

from . import go_registrar as gor
from .go_registrar import SECOND, Config


class GoTable(gor.Registrar):
    """sim-admission port plus topdisc 703d5a2: scaleBase (#170) and the ticket
    sealing key lifecycle of ticket.go. The base class already drops a topic's
    bound with its last ad (#177) and expires ads at the request's now (#173).
    Copied from onur-sim compare/compare.py."""

    REKEY = 100000
    KEY_LIFETIME = 6 * 3600 * gor.SECOND

    def __init__(self, config, *, ip_score=True, topic_score=True):
        super().__init__(config, ip_score=ip_score)
        self.topic_score = topic_score
        # Keys in creation order: first ticket serial and last issuance time.
        self.key_starts, self.key_last, self.key_uses, self.live_key = [], [], 0, 0

    def request(self, node, topic, ip, now, ticket=None, attacker=False):
        self._gc_keys(now)
        if isinstance(ticket, gor.Ticket) and bisect.bisect_right(self.key_starts, ticket.serial) - 1 < self.live_key:
            # v5_udp.go: a ticket whose key is gone fails to unpack, so the
            # request is a new attempt without credit.
            self.ticket_timings['unknown_key'] += 1
            ticket = None
        reply = super().request(node, topic, ip, now, ticket, attacker)
        if reply.ticket is not None and reply.ticket is not ticket:
            self._seal(reply.ticket.serial, now)
        return reply

    def _gc_keys(self, now):
        while self.live_key < len(self.key_last) and self.key_last[self.live_key] <= now - self.KEY_LIFETIME:
            self.live_key += 1

    def _seal(self, serial, now):
        if self.live_key == len(self.key_starts) or self.key_uses >= self.REKEY:
            self.key_starts.append(serial)
            self.key_last.append(now)
            self.key_uses = 1
        else:
            self.key_last[-1] = now
            self.key_uses += 1

    def components(self, topic, ip, now):
        count = len(self.ads)
        inverse_occupancy = 1 - count / self.config.capacity
        base = (self.config.base * (self.config.lifetime / gor.SECOND)
                / inverse_occupancy ** self.config.exponent
                if inverse_occupancy > 0 else math.inf)
        topic_mod = self.topics[topic] / (count + 1) if self.topic_score else 0.0
        service = base * topic_mod if topic_mod > 0 else 0.0
        score, lpm, floor = 0.0, None, 0
        if self.ip_score and gor.scored_ip(ip):
            score, lpm = self.tree.score_node(ip)
            floor = self.tree.path_floor(ip, now)
        ip_secs = base * score if score > 0 else 0.0
        charged_service, charged_ip = service, ip_secs
        topic_floor = self.topic_bounds.get(topic)
        if topic_floor and topic_floor.remaining(now) / gor.SECOND > charged_service:
            charged_service = topic_floor.remaining(now) / gor.SECOND
        if floor / gor.SECOND > charged_ip:
            charged_ip = floor / gor.SECOND
        needed = charged_service + charged_ip + base * self.config.safety
        return gor.duration(needed), service, ip_secs, lpm


class WorldTable(GoTable):
    """GoTable with by_topic: topic -> {advertiser: expiry ns}."""

    def __init__(self, config, *, ip_score=True):
        super().__init__(config, ip_score=ip_score)
        self.by_topic = {}

    @property
    def count(self):
        return len(self.ads)

    def add(self, node, topic, ip, now, attacker=False):
        if not super().add(node, topic, ip, now, attacker):
            return False
        self.by_topic.setdefault(topic, {})[node] = now + self.config.lifetime
        return True

    def expire(self, now):
        while self.expiries and self.expiries[0][0] <= now:
            _, node, topic = heapq.heappop(self.expiries)
            ip, levels, attacker = self.ads.pop((node, topic))
            self.topics[topic] -= 1
            d = self.by_topic[topic]
            del d[node]
            if not self.topics[topic]:
                del self.topics[topic]
                del self.by_topic[topic]
                self.topic_bounds.pop(topic, None)
            self.tree.remove(ip, levels)
            if attacker:
                self.attacker_ads -= 1
                self.attacker_topics[topic] -= 1
                if not self.attacker_topics[topic]:
                    del self.attacker_topics[topic]

    def purge(self, now_s):
        self.expire(round(now_s * SECOND))


def go_config(cfg):
    return Config(capacity=cfg.cache, safety=1e-7, lifetime=round(cfg.ad_lifetime * SECOND))
