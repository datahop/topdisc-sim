"""Sybil advertiser flood: attacker identities that register like honest
advertisers (same walk, same registration table) with the attacker's IDs,
IPs, topics and patience. They never serve queries or enter any DHT table,
aux reply or search table; ids are FLOOD_BASE - j."""
import heapq
import ipaddress
import math
from collections import deque

import numpy as np

from .go_registrar import GRACE, SECOND, WINDOW, scored_ip
from .honest_nets import spread_ip
from .population import DHT_BUCKET_SIZE, DHT_MIN_DIST

FLOOD_BASE = -(1 << 40)
NEAR_BITS = 24


def attack_prefixes(count, layout, rng):
    """count public /24s: spread over IPv4 or inside one /16 (sim-admission
    workload.attack_addresses)."""
    cluster = (int(rng.integers(11, 120)) << 24) | (int(rng.integers(256)) << 16)
    out = []
    while len(out) < count:
        prefix = cluster | (int(rng.integers(256)) << 8) if layout == "clustered" else int(rng.integers(1 << 24)) << 8
        if ipaddress.IPv4Address(prefix + 1).is_global and scored_ip(prefix + 1) and prefix not in out:
            out.append(prefix)
    return out


def flood_ips(sybils, nets, layout, rng):
    if nets == 0:
        return np.array([spread_ip((1 << 31) + j) for j in range(sybils)], dtype=np.int64)
    prefixes = attack_prefixes(nets, layout, rng)
    return np.array([prefixes[j % nets] + 1 + (j // nets) % 254 for j in range(sybils)], dtype=np.int64)


def fake_topic_ids(target_tid, count, near, rng):
    """near: share the top NEAR_BITS of the target's hash, so every registrar
    sits in the same registration bucket for both; else uniform hashes."""
    out = set()
    low = (1 << (64 - NEAR_BITS)) - 1
    while len(out) < count:
        r = int(rng.integers(0, 1 << 63)) * 2 + int(rng.integers(0, 2))
        t = (target_tid & ~low) | (r & low) if near else r
        if t != target_tid:
            out.add(t)
    return sorted(out)


class Flood:
    def __init__(self, mode, sybils, pop, target, seed, topics=300, near=True, nets=4, layout="spread",
                 give_up=False, budget=0.0, burst=1.0, start_window=900.0, k=None):
        self.mode, self.sybils, self.target = mode, sybils, target
        self.nets, self.layout, self.near, self.give_up = nets, layout, near, give_up
        self.start_window = start_window
        rng = np.random.default_rng([seed, 0xF100D])
        self.ip = flood_ips(sybils, nets, layout, rng)
        self.own = [int(rng.integers(0, 1 << 63)) * 2 + int(rng.integers(0, 2)) for _ in range(sybils)]
        k = self.k = len(pop.topic_ids) if k is None else k
        if mode in ("occupy", "occupy-partitioned"):
            fake = fake_topic_ids(int(pop.topic_ids[target]), topics, near, rng)
            pop.topic_ids = np.concatenate([pop.topic_ids, np.array(fake, dtype=np.uint64)])
            self.topics = list(range(k, k + topics))
            if mode == "occupy":
                # every sybil registers every fake topic: S x T chains
                self.chains = [(FLOOD_BASE - j, k + t) for j in range(sybils) for t in range(topics)]
            else:
                # chain c: sybil c mod S registers fake topic c mod T, max(S, T) chains
                n = max(sybils, topics)
                self.chains = [(FLOOD_BASE - c % sybils, k + c % topics) for c in range(n)]
        else:
            self.topics = [target]
            self.chains = [(FLOOD_BASE - j, target) for j in range(sybils)]
        self.starts = rng.random(len(self.chains)) * start_window
        self.sent = np.zeros(pop.n, dtype=np.int64)
        self.seed = seed
        # rate budget: token bucket over all sybils, REGTOPIC/s; 0 = unlimited
        self.budget, self.burst = budget, max(1.0, burst)
        self.tokens, self.last = self.burst, 0.0
        # Parked chains wait for a token. Retries holding a ticket go first, by
        # window start (ticket.last + ticket.wait); fresh attempts get the rest.
        self.parked = {}          # (node, topic) -> park version
        self.retries, self.fresh = [], deque()
        self.pver = 0
        self.wake_at = None
        self.seed_cache = {}
        self.parked_max = 0
        self.admitted = 0

    def index(self, node):
        return FLOOD_BASE - node

    def ip_of(self, node):
        return int(self.ip[FLOOD_BASE - node])

    def regtable(self, depth, adlife, size, standby):
        from .world import RegTable
        return RegTable(depth, adlife, size, standby, timeout=None if self.give_up else math.inf)

    def seeds(self, node, pop):
        """A DHT-shaped table around the sybil's own 64-bit id, no IP limits;
        shared by all topics of the sybil."""
        out = self.seed_cache.get(node)
        if out is None:
            x = self.own[FLOOD_BASE - node]
            rng = np.random.default_rng([self.seed, 0x5EED, FLOOD_BASE - node])
            out = []
            for d in range(DHT_MIN_DIST + 2, 257):
                lo, hi = pop.subtree(x, d)
                out.extend(int(i) for i in pop.sample_bucket(lo, hi, DHT_BUCKET_SIZE, rng, 0))
            lo, hi = pop.subtree_within(x, DHT_MIN_DIST + 1)
            out.extend(int(i) for i in pop.sample_bucket(lo, hi, DHT_BUCKET_SIZE, rng, 0))
            self.seed_cache[node] = out
        return list(out)

    def refill(self, now):
        self.tokens = min(self.burst, self.tokens + (now - self.last) * self.budget)
        self.last = now

    def grant(self, w, node, topic, rt, due):
        """True: the chain may send its due attempt now. False: it is parked
        (or stays parked) until wake hands it a token."""
        chain = (node, topic)
        if chain in self.parked:
            return False
        self.refill(w.now)
        if not self.parked and self.tokens >= 1 - 1e-9:
            self.tokens -= 1
            return True
        self.park(w, chain, rt)
        return False

    def park(self, w, chain, rt, ver=None, wake=True):
        """Queue the chain by its earliest still valid ticket (an expired one
        earns no credit, so it counts as fresh), else as fresh until one of
        its tickets comes due."""
        if ver is None:
            self.pver += 1
            ver = self.parked[chain] = self.pver
        best = rt.best_ticket(self.expired_ns(w.now))
        if best is not None:
            heapq.heappush(self.retries, (best[0], ver, chain))
        else:
            self.fresh.append((ver, chain))
            nt = rt.next_ticket_due()
            if nt is not None and nt > w.now:
                w.at(nt, "rekey", chain, ver)
        self.parked_max = max(self.parked_max, len(self.parked))
        if wake:
            self.schedule(w)

    def rekey(self, w, chain, ver):
        """A chain parked as fresh whose ticket came due moves to the retry
        queue."""
        if self.parked.get(chain) != ver:
            return
        rt = w.reg[chain]
        rt.collect(w.now)
        del self.parked[chain]
        self.park(w, chain, rt)

    @staticmethod
    def expired_ns(now):
        return round(now * SECOND)

    def next_parked(self, w):
        now_ns = self.expired_ns(w.now)
        while self.retries:
            start, ver, chain = heapq.heappop(self.retries)
            if self.parked.get(chain) != ver:
                continue
            if start + WINDOW + GRACE <= now_ns:
                # this ticket expired while queued: requeue by the chain's next one
                rt = w.reg[chain]
                rt.collect(w.now)
                self.park(w, chain, rt, ver, wake=False)
                continue
            return chain
        while self.fresh:
            ver, chain = self.fresh.popleft()
            if self.parked.get(chain) == ver:
                return chain
        return None

    def schedule(self, w):
        if self.wake_at is None and self.parked:
            self.wake_at = w.now + max(0.0, 1 - self.tokens) / self.budget
            w.at(self.wake_at, "flood")

    def wake(self, w):
        self.wake_at = None
        self.refill(w.now)
        while self.parked and self.tokens >= 1 - 1e-9:
            chain = self.next_parked(w)
            if chain is None:
                break
            del self.parked[chain]
            self.tokens -= 1
            if not w.reg_grant(*chain):
                self.tokens += 1
        self.schedule(w)
