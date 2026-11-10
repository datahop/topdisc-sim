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
        # searchBucketIPLimit: one node per /24 in a bucket. pop.net is the
        # shared /24 of a node, or -1 when the node has a /24 of its own.
        self.net = getattr(pop, "net", None)
        self.nets_b = [set() for _ in range(cfg.depth)]

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
            if self.net is not None:
                k = int(self.net[i])
                if k >= 0:
                    if k in self.nets_b[b]:
                        continue
                    self.nets_b[b].add(k)
            self.new[b].add(i)
            self.origin.setdefault(i, src is not None)

    def remove_node(self, i):
        b = self.bucket(i)
        if self.net is not None and (i in self.new[b] or i in self.asked_b[b]):
            self.nets_b[b].discard(int(self.net[i]))
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

    def add_query_results(self, frm, results, now, ads=None):
        """ads is the size of the reply before the result filter; the adaptive
        rule samples it, as Search.AddReply does."""
        b = self.bucket(frm)
        self.new[b].discard(frm)
        self.asked_b[b].add(frm)
        self.num_requests[b] += 1
        if self.cfg.adaptive:
            self.observe(b, len(results) if ads is None else ads)
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


class ForkConfig:
    """The search values of the fork: topicindex/search.go, searchfilter.go,
    common.go, v5_topic.go and the v5 response timeout."""

    def __init__(self, search_depth=18, bucket_size=8, ip_limit=1, yield_floor=8, yield_window=4,
                 max_jump=4, aux_radius=1, pass_queries=32, topic_nodes_limit=16, ad_lifetime=900.0,
                 mix_registrars=5, mix_wait=5.0, pass_backoff=60.0, resp_timeout=0.7, unread_limit=16,
                 filter_limit=50000):
        self.search_depth, self.bucket_size, self.ip_limit = search_depth, bucket_size, ip_limit
        self.yield_floor, self.yield_window, self.max_jump = yield_floor, yield_window, max_jump
        self.aux_radius, self.pass_queries = aux_radius, pass_queries
        self.topic_nodes_limit, self.ad_lifetime = topic_nodes_limit, ad_lifetime
        self.mix_registrars, self.mix_wait = mix_registrars, mix_wait
        self.pass_backoff, self.resp_timeout = pass_backoff, resp_timeout
        self.filter_limit = filter_limit         # searchFilterLimit
        # searchUnreadLimit: no query while this many results wait unread. The
        # model's consumer reads every result at once, so it never binds.
        self.unread_limit = unread_limit


class AskedSet:
    """askedset.go: node -> [first, last] ask; kept an ad lifetime from the
    first ask, a later ask only moves last. Expired entries are swept at most
    once a second, so one may outlive the ttl by up to a second."""

    def __init__(self, ttl):
        self.ttl, self.at = ttl, {}
        self.expired = 0.0

    def has(self, i, now):
        self.expire(now)
        return i in self.at

    def add(self, i, now):
        self.expire(now)
        e = self.at.get(i)
        if e is not None:
            e[1] = now
        else:
            self.at[i] = [now, now]

    def last_asked(self, i, now):
        self.expire(now)
        e = self.at.get(i)
        return e[1] if e is not None else 0.0

    def expire(self, now):
        if now - self.expired < 1.0 and self.at:
            return
        self.expired = now
        for i in [i for i, e in self.at.items() if now - e[0] >= self.ttl]:
            del self.at[i]


class ForkSearch:
    """One pass of topicindex.Search (search.go): its own table of
    search_depth buckets, decoupled from the registration depth."""

    def __init__(self, fc, pop, tid, self_idx, rng, asked, failed, mint=None, ledger=None, net_of=None, m2=False, m2_nets=2, admission_ledger=None):
        self.fc, self.pop, self.tid, self.me, self.rng = fc, pop, tid, self_idx, rng
        self.asked, self.failed = asked, failed
        self.mint, self.ledger, self.net_lookup = mint, ledger, net_of
        self.m2, self.m2_nets = m2, m2_nets
        self.admission_ledger = admission_ledger
        D = fc.search_depth
        self.new = [set() for _ in range(D)]
        self.yield_ = [[] for _ in range(D)]
        self.yield_nets = [[] for _ in range(D)]
        self.done = [0] * D
        self.failed_b = [0] * D
        self.ips = [dict() for _ in range(D)]     # /24 -> nodes of the bucket on it
        self.net = getattr(pop, "net", None)
        self.active = self.start = 0
        self.spare = set()
        self.spare_used = self.placed = False
        self.replies = self.ads = 0
        self.asked_now = set()
        self.buffer = []
        self.result_seen = set()

    def set_active(self, i):
        self.active = max(0, min(i, self.fc.search_depth - 1))
        self.start = self.active

    def bucket(self, i):
        if self.mint is not None and i in self.mint:
            return self.mint[i][0]
        return bucket_index(self.tid, int(self.pop.ids[i]), self.fc.search_depth)

    def net_of(self, i):
        if self.mint is not None and i in self.mint:
            return self.mint[i][1]
        if self.net_lookup is not None:
            return self.net_lookup(i)
        return int(self.net[i]) if self.net is not None else -1

    def active_failed(self):
        a = self.active
        return not self.placed and self.done[a] == 0 and self.failed_b[a] > 0

    def active_done(self):
        a = self.active
        return self.replies >= self.fc.pass_queries or (not self.new[a] and len(self.yield_[a]) >= 2)

    def is_done(self):
        if self.buffer:
            return False
        if self.active_done():
            return True
        if any(self.new):
            return False
        return self.spare_used or not self.spare

    def next_pass_bucket(self):
        last = self.fc.search_depth - 1
        nxt = self.active
        if not self.active_done() and not self.placed and not self.active_failed():
            nxt = min(self.active + 1, last)
        if self.ads == 0 and nxt == last and self.start == last:
            return 0
        return nxt

    def buckets_with_free_space(self):
        r, D = self.fc.aux_radius, self.fc.search_depth
        return [256 - i for i in range(max(0, self.active - r), min(D - 1, self.active + r) + 1)
                if len(self.new[i]) < self.fc.bucket_size]

    def add_nodes(self, src, nodes, now):
        check = set()
        for i in nodes:
            if i == self.me or self.failed.has(i, now):
                continue
            if self.ledger is not None and not self.ledger.allows(self.net_of(i), now):
                continue
            if self.admission_ledger is not None and not self.admission_ledger.allows(self.net_of(i), i, now):
                continue
            b = self.bucket(i)
            if i in self.asked_now:
                continue
            if self.asked.has(i, now):
                self.spare.add(i)
                if self.admission_ledger is not None:
                    self.admission_ledger.record(self.net_of(i), i, now)
                continue
            nb = self.new[b]
            if i in nb or len(nb) >= self.fc.bucket_size:
                continue
            if src is not None:
                if b in check:
                    continue
                check.add(b)
            k = self.net_of(i)
            if k >= 0:
                c = self.ips[b].get(k, 0)
                if c >= self.fc.ip_limit:
                    continue
                self.ips[b][k] = c + 1
            nb.add(i)
            if self.admission_ledger is not None:
                self.admission_ledger.record(k, i, now)

    def remove_node(self, i, keep_ip):
        """keep_ip: the node's /24 stays counted in its bucket for the pass."""
        b = self.bucket(i)
        if i in self.new[b]:
            k = self.net_of(i)
            if not keep_ip and k >= 0:
                self.ips[b][k] -= 1
            self.new[b].discard(i)

    def handle_error(self, i, now):
        self.replies += 1
        self.asked_now.add(i)
        self.failed.add(i, now)
        self.spare.discard(i)
        self.failed_b[self.bucket(i)] += 1
        self.remove_node(i, False)

    def query_target(self, now):
        if self.active_done():
            return None
        D = self.fc.search_depth
        for d in range(D):
            for i in (self.active - d, self.active + d):
                if 0 <= i < D and self.new[i]:
                    return self.rng.choice(tuple(self.new[i]))
        if not self.spare_used and self.spare:
            pick = min(self.spare, key=lambda j: (self.asked.last_asked(j, now), j))
            self.spare_used = True
            self.spare.discard(pick)
            return pick
        return None

    def observe(self, bi, ads, net=-1):
        fc = self.fc
        y, nets = self.yield_[bi], self.yield_nets[bi]
        y.append(ads)
        nets.append(net)
        if len(y) > fc.yield_window:
            del y[0]
            del nets[0]
        if len(y) < 2:
            return
        s = sorted(y)
        lower, upper = s[(len(s) - 1) // 2], s[len(s) // 2]
        last = fc.search_depth - 1
        if lower >= fc.topic_nodes_limit:
            nxt = max(0, bi - 1)
        elif upper >= fc.yield_floor:
            nxt = bi
        elif upper == 0:
            nxt = min(last, bi + fc.max_jump)
        else:
            nxt = min(last, bi + min(int(math.ceil(math.log2(fc.yield_floor / upper))), fc.max_jump))
        # M2 samples retain the fork's four-reply window. A public reply
        # may steer only with enough distinct public /24s in that window.
        # LAN-origin replies are exempt from this extra gate.
        if self.m2 and net != -1 and len({n for n in nets if n != -1}) < self.m2_nets:
            return
        # a helper farther out than the active bucket can only pull the
        # search out, one closer in can only push it in
        a, r = self.active, fc.aux_radius
        if not (a - r <= bi <= a + r or (bi < a and nxt <= bi) or (bi > a and nxt > bi)):
            return
        self.active, self.placed = nxt, True

    def add_reply(self, i, results, ads, sent, now, partial=False):
        """AddReply, or AddPartialReply: a reply cut short by an error is a
        density sample only when it reaches the floor on its own."""
        self.replies += 1
        self.ads += ads
        self.asked_now.add(i)
        self.spare.discard(i)
        b = self.bucket(i)
        self.done[b] += 1
        if not partial or sent >= self.fc.yield_floor:
            self.observe(b, sent, self.net_of(i))
        self.asked.add(i, now)
        self.remove_node(i, True)
        self.add_results(results)

    def add_results(self, nodes):
        for r in nodes:
            if r != self.me and r not in self.result_seen:
                self.result_seen.add(r)
                self.buffer.append(r)
