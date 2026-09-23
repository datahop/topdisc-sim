"""One topic search, ported from p2p/discover/topicindex/search.go and the
runner in v5_topic.go: the search table, the default and the adaptive query
target, the reply handling, the pass rollover. One query in flight."""
import math
import random

import numpy as np

from .population import bucket_index, logdist

YIELD_WINDOW = 4
MAX_JUMP = 4
PASS_MIN_S = 2.0          # regloopMinTime


class Config:
    def __init__(self, depth=10, bucket_size=16, topic_nodes_limit=16, aux_limit=8,
                 yield_floor=0, aux_radius=0, ad_lifetime=900.0, rtt=0.06):
        self.depth, self.bucket_size = depth, bucket_size
        self.topic_nodes_limit, self.aux_limit = topic_nodes_limit, aux_limit
        self.yield_floor, self.aux_radius = yield_floor, aux_radius
        self.ad_lifetime, self.rtt = ad_lifetime, rtt

    @property
    def adaptive(self):
        return self.yield_floor > 0


class Registrars:
    """The registrar side: which ads a node holds, and its DHT table for aux
    nodes. Counts what every registrar served."""

    def __init__(self, pop, ads, cfg, seed=3):
        self.pop, self.ads, self.cfg = pop, ads, cfg
        self.rng = np.random.default_rng(seed)
        self.queries = np.zeros(pop.n, dtype=np.float64)
        self.ads_served = np.zeros(pop.n, dtype=np.float64)
        self.aux_served = np.zeros(pop.n, dtype=np.float64)
        self._tables = {}

    def table(self, i):
        t = self._tables.get(i)
        if t is None:
            idx = self.pop.dht_table(i)
            t = (idx, self.pop.ids[idx])
            if len(self._tables) > 200000:
                self._tables.clear()
            self._tables[i] = t
        return t

    def charge(self, registrars, weight):
        np.add.at(self.queries, np.asarray(registrars, dtype=np.int64), weight)

    def serve(self, registrar, topic, tid, dists, weight):
        """TOPICQUERY: up to topic_nodes_limit random ads of the topic, and one
        node per requested distance from the registrar's table, the first
        aux_limit distances only (collectTopicAuxNodes caps the list)."""
        self.queries[registrar] += weight
        held = self.ads.ads_at(registrar, topic)
        if len(held) > self.cfg.topic_nodes_limit:
            results = self.rng.choice(held, size=self.cfg.topic_nodes_limit, replace=False)
        else:
            results = held
        self.ads_served[registrar] += len(results) * weight
        aux = []
        if dists:
            want = dists[:self.cfg.aux_limit]
            idx, ids = self.table(registrar)
            x = np.bitwise_xor(ids, np.uint64(tid))
            # log-distance of every table entry to the topic, 256-bit terms
            bl = np.zeros(len(x), dtype=np.int64)
            nz = x != 0
            bl[nz] = np.floor(np.log2(x[nz].astype(np.float64))).astype(np.int64) + 1
            # float rounding at the top bit: fix exact powers
            d = 256 - 64 + bl
            for want_d in want:
                m = np.nonzero(d == want_d)[0]
                if len(m):
                    aux.append(int(idx[self.rng.choice(m)]))
        self.aux_served[registrar] += len(aux) * weight
        return results, aux


class Search:
    def __init__(self, cfg, pop, tid, topic, self_idx, rng, asked, active):
        self.cfg, self.pop, self.tid, self.topic, self.me = cfg, pop, tid, topic, self_idx
        self.rng = rng
        self.new = [set() for _ in range(cfg.depth)]
        self.asked_b = [set() for _ in range(cfg.depth)]
        self.num_requests = [0] * cfg.depth
        self.yield_ = [[] for _ in range(cfg.depth)]
        self.origin = {}
        self.asked = asked           # shared filter: idx -> expiry time
        self.active = active
        self.spare = set()
        self.spare_used = False
        self.result_seen = set()
        self.results = []

    def bucket(self, i):
        return bucket_index(int(self.tid), int(self.pop.ids[i]), self.cfg.depth)

    def count(self, b):
        return len(self.new[b]) + len(self.asked_b[b])

    def add_nodes(self, src, nodes, now):
        check = set()
        for i in nodes:
            if i == self.me:
                continue
            b = self.bucket(i)
            if self.cfg.adaptive and self.asked.get(i, -1.0) > now:
                self.spare.add(i)
                continue
            if i in self.new[b] or i in self.asked_b[b] or self.count(b) >= self.cfg.bucket_size:
                continue
            if src is not None:
                if b in check:
                    continue
                check.add(b)
            self.new[b].add(i)
            self.origin.setdefault(i, src is not None)

    def remove_node(self, i):
        b = self.bucket(i)
        self.new[b].discard(i)
        self.asked_b[b].discard(i)

    def query_target(self):
        if self.cfg.adaptive:
            return self.adaptive_target()
        withnew = []
        for i in range(self.cfg.depth):
            if self.new[i]:
                withnew.append(i)
                if self.num_requests[i] == 0:
                    break
        if withnew:
            b = self.rng.choice(withnew)
            return self.rng.choice(tuple(self.new[b]))
        return None

    def adaptive_target(self):
        for d in range(self.cfg.depth):
            for i in (self.active - d, self.active + d):
                if 0 <= i < self.cfg.depth and self.new[i]:
                    return self.rng.choice(tuple(self.new[i]))
        if not self.spare_used and self.spare:
            self.spare_used = True
            i = self.rng.choice(tuple(self.spare))
            self.spare.discard(i)
            return i
        return None

    def buckets_with_free_space(self):
        free = lambda i: self.count(i) < self.cfg.bucket_size
        dists = []
        if self.cfg.adaptive:
            r = max(1, self.cfg.aux_radius)
            for i in range(max(0, self.active - r), min(self.cfg.depth - 1, self.active + r) + 1):
                if free(i):
                    dists.append(256 - i)
            if self.cfg.aux_radius > 0:
                return dists
        for i in range(self.cfg.depth):
            if free(i) and (256 - i) not in dists:
                dists.append(256 - i)
        return dists

    def observe(self, bi, ads):
        y = self.yield_[bi]
        y.append(ads)
        if len(y) > YIELD_WINDOW:
            del y[0]
        if len(y) < 2:
            return
        s = sorted(y)
        lower, upper = s[(len(s) - 1) // 2], s[len(s) // 2]
        if lower >= self.cfg.topic_nodes_limit:
            self.active = max(0, bi - 1)
        elif upper >= self.cfg.yield_floor:
            self.active = bi
        elif upper == 0:
            self.active = min(self.cfg.depth - 1, bi + MAX_JUMP)
        else:
            jump = int(math.ceil(math.log2(self.cfg.yield_floor / upper)))
            self.active = min(self.cfg.depth - 1, bi + min(jump, MAX_JUMP))

    def add_query_results(self, frm, results, now):
        b = self.bucket(frm)
        self.new[b].discard(frm)
        self.asked_b[b].add(frm)
        self.num_requests[b] += 1
        if self.cfg.adaptive:
            self.observe(b, len(results))
            self.asked[frm] = now + self.cfg.ad_lifetime
            self.remove_node(frm)
        new = 0
        for r in results:
            r = int(r)
            if r == self.me or r in self.result_seen:
                continue
            self.result_seen.add(r)
            self.results.append(r)
            new += 1
        return new

    def is_done(self):
        if self.results:
            return False
        if self.cfg.adaptive:
            if not self.new[self.active] and len(self.yield_[self.active]) >= 2:
                return True
        if any(self.new):
            return False
        if self.cfg.adaptive and not self.spare_used and self.spare:
            return False
        if self.cfg.adaptive and self.active < self.cfg.depth - 1:
            self.active += 1
        return True


class Searcher:
    """One node searching one topic: passes until the time or query budget
    runs out. Records found registrants against queries and time."""

    def __init__(self, cfg, pop, regs, topic, self_idx, seed):
        self.cfg, self.pop, self.regs, self.topic, self.me = cfg, pop, regs, topic, self_idx
        self.tid = int(pop.topic_ids[topic])
        self.rng = random.Random(seed)
        self.seed_nodes = [int(i) for i in pop.dht_table(self_idx)]
        self.asked = {}
        self.active = 0
        self.found = set()
        self.queries = 0
        self.passes = 0
        self.trace = []          # (queries, time, found) at every new result
        self.bucket_queries = [0] * cfg.depth
        self.active_trace = []   # (time, active) on change
        self.pass_hits = []      # per pass: (start time, end time, [registrars queried])
        self.extrapolated = 0.0  # queries charged beyond the cap by extrapolation

    def run(self, horizon, max_queries, weight=1.0, target=0):
        """target > 0: stop once that many distinct registrants were found, as a
        connection-driven searcher does when its outbound slots are full."""
        now = 0.0
        cfg = self.cfg
        while now < horizon and self.queries < max_queries and not (target and len(self.found) >= target):
            pass_start = now
            if cfg.adaptive:
                for k in [k for k, exp in self.asked.items() if exp <= now]:
                    del self.asked[k]
            st = Search(cfg, self.pop, self.tid, self.topic, self.me, self.rng, self.asked, self.active)
            st.add_nodes(None, self.seed_nodes, now)
            self.passes += 1
            hits = []
            self.pass_hits.append([now, now, hits])
            last_active = st.active
            self.active_trace.append((now, st.active))
            while now < horizon and self.queries < max_queries and not (target and len(self.found) >= target):
                # results are consumed at once (lookup-only searcher)
                for r in st.results:
                    if r not in self.found:
                        self.found.add(r)
                        self.trace.append((self.queries, now, len(self.found)))
                st.results.clear()
                if st.is_done():
                    break
                dst = st.query_target()
                if dst is None:
                    break
                dists = st.buckets_with_free_space()
                b = st.bucket(dst)
                self.bucket_queries[b] += 1
                self.queries += 1
                results, aux = self.regs.serve(dst, self.topic, self.tid, dists, weight)
                hits.append(dst)
                now += cfg.rtt
                st.add_nodes(dst, aux, now)
                st.add_query_results(dst, results, now)
                if st.active != last_active:
                    last_active = st.active
                    self.active_trace.append((now, st.active))
            self.active = st.active
            now = max(now, pass_start + PASS_MIN_S)
            self.pass_hits[-1][1] = now
            if len(self.pass_hits) > 12:
                del self.pass_hits[0]
        self.end_time = now
        if self.queries >= max_queries and now < horizon and not (target and len(self.found) >= target):
            # The search would have gone on re-walking until the horizon: charge
            # the registrars of the last passes for the remaining time at the
            # rate those passes ran, instead of simulating every query.
            recent = [p for p in self.pass_hits[:-1] if p[2]]
            if recent:
                span = recent[-1][1] - recent[0][0]
                nq = sum(len(p[2]) for p in recent)
                if span > 0 and nq > 0:
                    factor = (horizon - now) / span
                    for p in recent:
                        self.regs.charge(p[2], weight * factor)
                    self.extrapolated = nq * factor
                    self.end_time = horizon
        return self
