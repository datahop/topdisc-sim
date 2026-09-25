"""Event-driven TopDisc model: every node registers its topic through the
registration walk (ported from topicindex/registration.go and v5_topic.go),
serves as registrar with the admission rule of topictable.go, and searches
its topic with the searcher of search.py, all on one model clock.

    python3 -m sim.searchsim.world --nodes 10000 --topics 100 \
        --model scenarios/models/crawl-2026-09-18.json --floor 4 --target 16 --out runs/sim-w10k
"""
import argparse
import heapq
import json
import math
import os
import random
import time

import numpy as np

from .population import Population, shares_from_model, zipf_shares, bucket_index
from .search import Config, Search, PASS_MIN_S

STANDBY, WAITING, REGISTERED = 0, 1, 2
OCCUPANCY_EXP, WAIT_BASE_MOD, WAIT_FLOOR = 10, 0.1, 1.0
REG_BUCKET_SIZE, REG_STANDBY = 5, 20


class Attempt:
    __slots__ = ("state", "next", "first_issued", "total_wait", "filled", "ver", "reqs")

    def __init__(self):
        self.state, self.next, self.first_issued, self.total_wait, self.filled, self.ver, self.reqs = STANDBY, 0.0, None, 0.0, None, 0, 0


class RegTable:
    """Registration table of one advertiser for one topic."""
    __slots__ = ("depth", "buckets", "counts", "heap", "ever", "adlife", "timeout")

    def __init__(self, depth, adlife):
        self.depth = depth
        self.buckets = [dict() for _ in range(depth)]
        self.counts = [[0, 0, 0] for _ in range(depth)]
        self.heap = []
        self.ever = set()
        self.adlife, self.timeout = adlife, 1.5 * adlife

    def node_count(self):
        return sum(len(b) for b in self.buckets)

    def add_nodes(self, w, tid, src, nodes):
        src_att = None
        if src is not None:
            src_att = self.buckets[bucket_index(tid, int(w.pop.ids[src]), self.depth)].get(src)
        for n in nodes:
            bi = bucket_index(tid, int(w.pop.ids[n]), self.depth)
            b = self.buckets[bi]
            if n in b:
                continue
            if src_att is not None:
                if src_att.filled is None:
                    src_att.filled = set()
                if bi in src_att.filled:
                    continue
            if self.counts[bi][STANDBY] >= REG_STANDBY:
                continue
            b[n] = Attempt()
            self.counts[bi][STANDBY] += 1
            if src_att is not None:
                src_att.filled.add(bi)
            self.refill(bi, w.now)

    def set_state(self, att, bi, state):
        self.counts[bi][att.state] -= 1
        self.counts[bi][state] += 1
        att.state = state

    def push(self, att, bi, reg):
        att.ver += 1
        heapq.heappush(self.heap, (att.next, bi, reg, att.ver))

    def refill(self, bi, now):
        c = self.counts[bi]
        if c[WAITING] + c[REGISTERED] >= REG_BUCKET_SIZE:
            return
        for reg, att in self.buckets[bi].items():
            if att.state == STANDBY:
                self.set_state(att, bi, WAITING)
                att.next = now
                self.push(att, bi, reg)
                return

    def remove(self, bi, reg):
        att = self.buckets[bi].pop(reg)
        self.counts[bi][att.state] -= 1
        att.ver = -1

    def next_time(self):
        while self.heap:
            t, bi, reg, ver = self.heap[0]
            att = self.buckets[bi].get(reg)
            if att is None or att.ver != ver:
                heapq.heappop(self.heap)
                continue
            return t
        return None

    def update(self, now):
        """Expire registered attempts whose ad ran out; return the next
        attempt (bucket, registrar) due to send, or None."""
        while self.heap:
            t, bi, reg, ver = self.heap[0]
            att = self.buckets[bi].get(reg)
            if att is None or att.ver != ver:
                heapq.heappop(self.heap)
                continue
            if t > now:
                return None
            if att.state == REGISTERED:
                heapq.heappop(self.heap)
                self.remove(bi, reg)
                self.refill(bi, now)
                continue
            heapq.heappop(self.heap)      # StartRequest
            att.ver += 1
            att.reqs += 1
            return bi, reg
        return None

    def free_dists(self):
        return [256 - i for i in range(self.depth) if self.counts[i][STANDBY] < REG_STANDBY]

    def on_ticket(self, bi, reg, wait, now):
        att = self.buckets[bi][reg]
        if wait > self.timeout:
            self.remove(bi, reg); self.refill(bi, now); return
        att.total_wait += wait
        if att.total_wait > self.timeout:
            self.remove(bi, reg); self.refill(bi, now); return
        if att.first_issued is None:
            att.first_issued = now
        att.next = now + wait
        self.push(att, bi, reg)

    def on_registered(self, bi, reg, ttl, now):
        att = self.buckets[bi][reg]
        self.set_state(att, bi, REGISTERED)
        self.ever.add(reg)
        att.next = now + min(ttl, self.adlife)
        att.first_issued = None
        att.total_wait = 0.0
        self.push(att, bi, reg)
        self.refill(bi, now)


class TopicTable:
    """One registrar's ad cache and admission state."""
    __slots__ = ("ads", "count", "lb", "heap")

    def __init__(self):
        self.ads, self.count, self.lb, self.heap = {}, 0, {}, []

    def purge(self, now):
        while self.heap and self.heap[0][0] <= now:
            exp, t, adv = heapq.heappop(self.heap)
            d = self.ads.get(t)
            if d is not None and d.get(adv) == exp:
                del d[adv]
                self.count -= 1
                if not d:
                    del self.ads[t]

    def register(self, adv, t, waited, now, cfg):
        self.purge(now)
        d = self.ads.get(t)
        if d is not None and adv in d:
            return 0.0
        occupancy = 1.0 - self.count / cfg.cache
        base = WAIT_BASE_MOD * cfg.ad_lifetime / (occupancy ** OCCUPANCY_EXP) if occupancy > 0 else float("inf")
        service = base * (len(d) if d else 0) / (self.count + 1)
        lbv, lbs = self.lb.get(t, (0.0, 0.0))
        rem = max(0.0, lbv - (now - lbs))
        required = max(service, rem) + 0.0     # IP component: 0 (LAN or single address per /24)
        if waited < required:
            remaining = required - waited
            if remaining > WAIT_FLOOR:
                if service > rem:
                    self.lb[t] = (service, now)
                return remaining
        if self.count >= cfg.cache:
            return max(WAIT_FLOOR, self.heap[0][0] - now) if self.heap else WAIT_FLOOR
        exp = now + cfg.ad_lifetime
        if d is None:
            d = self.ads[t] = {}
        d[adv] = exp
        self.count += 1
        heapq.heappush(self.heap, (exp, t, adv))
        return 0.0


class WorldConfig(Config):
    def __init__(self, cache=5000, **kw):
        super().__init__(**kw)
        self.cache = cache


class World:
    def __init__(self, pop, cfg, rtt, seed, backoff=0.0, sybil=None, idle_replies=0, min_sources=0, min_buckets=0):
        self.pop, self.cfg, self.rtt = pop, cfg, rtt
        self.backoff = backoff
        self.idle_replies = idle_replies
        self.min_sources = min_sources
        self.min_buckets = min_buckets
        self.first_found = {}
        # sybil[i]: node i is an attacker. A sybil registrar answers every query
        # with sybil ads of the topic only and hands out sybils as aux nodes;
        # sybils also register their own ids, so honest registrars carry them
        # in proportion. Sybil searchers are not counted.
        self.sybil = sybil if sybil is not None else np.zeros(pop.n, dtype=bool)
        self.sybil_by_topic = {}
        self.now = 0.0
        self.events = []
        self.seq = 0
        self.rng = random.Random(seed)
        n = pop.n
        self.ttab = [TopicTable() for _ in range(n)]
        self.reg = {}            # (node, topic) -> RegTable
        self.reg_start = {}
        self.first_admit = {}
        self.quotes = []
        self.reg_rcv = np.zeros(n, dtype=np.int64)
        self.tq_rcv = np.zeros(n, dtype=np.int64)
        self.ads_served = np.zeros(n, dtype=np.int64)
        self.reg_sent = np.zeros(n, dtype=np.int64)
        self.q_sent = np.zeros(n, dtype=np.int64)
        self._tables = {}
        self.searchers = {}

    # --- infrastructure --------------------------------------------------
    def at(self, t, kind, *args):
        self.seq += 1
        heapq.heappush(self.events, (t, self.seq, kind, args))

    def table(self, i):
        t = self._tables.get(i)
        if t is None:
            idx = self.pop.dht_table(i)
            t = (idx, self.pop.ids[idx])
            self._tables[i] = t
        return t

    def aux(self, registrar, tid, dists):
        if not dists:
            return []
        want = dists[:self.cfg.aux_limit]
        idx, ids = self.table(registrar)
        x = np.bitwise_xor(ids, np.uint64(tid))
        bl = np.zeros(len(x), dtype=np.int64); nz = x != 0
        bl[nz] = np.floor(np.log2(x[nz].astype(np.float64))).astype(np.int64) + 1
        d = 256 - 64 + bl
        out = []
        for wd in want:
            m = np.nonzero(d == wd)[0]
            if len(m):
                out.append(int(idx[self.rng.randrange(len(m))]))
        return out

    # --- registration ----------------------------------------------------
    def start_registration(self, node, topic):
        self.reg_start[(node, topic)] = self.now
        self.at(self.now, "reg", node, topic)

    def reg_tick(self, node, topic):
        key = (node, topic)
        tid = int(self.pop.topic_ids[topic])
        rt = self.reg.get(key)
        if rt is None or rt.node_count() == 0:
            if rt is None:
                rt = self.reg[key] = RegTable(self.cfg.depth, self.cfg.ad_lifetime)
            seeds = [int(i) for i in self.pop.dht_table(node)]
            self.rng.shuffle(seeds)
            rt.add_nodes(self, tid, None, [s for s in seeds if s != node])
            if rt.node_count() == 0:
                self.at(self.now + PASS_MIN_S, "reg", node, topic); return
        due = rt.update(self.now)
        if due is None:
            nt = rt.next_time()
            self.at(nt if nt is not None else self.now + PASS_MIN_S, "reg", node, topic)
            return
        bi, registrar = due
        att = rt.buckets[bi][registrar]
        waited = (self.now - att.first_issued) if att.first_issued is not None else 0.0
        dists = rt.free_dists()
        # registrar side, at arrival
        self.reg_rcv[registrar] += 1
        self.reg_sent[node] += 1
        aux = self.aux(registrar, tid, dists)
        res = self.ttab[registrar].register(node, topic, waited, self.now, self.cfg)
        if res > 0:
            self.quotes.append(res)
        self.at(self.now + self.rtt, "regresp", node, topic, bi, registrar, res, aux)

    def reg_resp(self, node, topic, bi, registrar, res, aux):
        key = (node, topic); rt = self.reg[key]
        tid = int(self.pop.topic_ids[topic])
        if registrar not in rt.buckets[bi]:
            self.at(self.now, "reg", node, topic); return
        if aux:
            rt.add_nodes(self, tid, registrar, [a for a in aux if a != node])
        if res > 0:
            rt.on_ticket(bi, registrar, res, self.now)
        else:
            rt.on_registered(bi, registrar, self.cfg.ad_lifetime, self.now)
            if key not in self.first_admit:
                self.first_admit[key] = self.now - self.reg_start[key]
        nt = rt.next_time()
        self.at(max(self.now, nt) if nt is not None else self.now + PASS_MIN_S, "reg", node, topic)

    def note_found(self, r, topic, now):
        key = (r, topic)
        if key not in self.first_found:
            self.first_found[key] = now

    # --- search ------------------------------------------------------------
    def start_search(self, node, topic, target, stop_at):
        s = SearcherState(self, node, topic, target, stop_at)
        self.searchers[node] = s
        self.at(self.now, "sstep", node)

    def sybil_aux(self, tid, dists):
        out = []
        for wd in dists[:self.cfg.aux_limit]:
            b = 256 - wd
            lo, hi = (self.pop.subtree(tid, wd) if b < self.cfg.depth - 1 else self.pop.subtree_within(tid, wd))
            if hi > lo:
                cand = lo + np.nonzero(self.sybil[lo:hi])[0]
                if len(cand):
                    out.append(int(cand[self.rng.randrange(len(cand))]))
        return out

    def serve_query(self, registrar, topic, tid, dists):
        self.tq_rcv[registrar] += 1
        if self.sybil[registrar]:
            pool = self.sybil_by_topic.get(topic)
            if pool is None:
                pool = self.sybil_by_topic[topic] = [int(i) for i in np.nonzero(self.sybil & (self.pop.topics == topic) & self.pop.registrant)[0]]
            results = self.rng.sample(pool, self.cfg.topic_nodes_limit) if len(pool) > self.cfg.topic_nodes_limit else list(pool)
            self.ads_served[registrar] += len(results)
            return results, self.sybil_aux(tid, dists)
        tt = self.ttab[registrar]
        tt.purge(self.now)
        d = tt.ads.get(topic)
        if d:
            held = list(d.keys())
            results = self.rng.sample(held, self.cfg.topic_nodes_limit) if len(held) > self.cfg.topic_nodes_limit else held
        else:
            results = []
        self.ads_served[registrar] += len(results)
        return results, self.aux(registrar, tid, dists)

    def run(self, until, progress=None):
        n = 0
        while self.events and self.events[0][0] <= until:
            t, _, kind, args = heapq.heappop(self.events)
            self.now = t
            if kind == "reg":
                self.reg_tick(*args)
            elif kind == "regresp":
                self.reg_resp(*args)
            elif kind == "sstep":
                self.searchers[args[0]].step()
            elif kind == "sreply":
                self.searchers[args[0]].reply(*args[1:])
            elif kind == "start_reg":
                self.start_registration(*args)
            elif kind == "start_search":
                self.start_search(*args)
            n += 1
            if progress and n % 2000000 == 0:
                progress(self.now, n)
        self.now = until
        return n


class SearcherState:
    def __init__(self, w, node, topic, target, stop_at):
        self.w, self.node, self.topic, self.target, self.stop_at = w, node, topic, target, stop_at
        self.tid = int(w.pop.topic_ids[topic])
        self.rng = random.Random(node * 7919 + 1)
        self.seeds = [int(i) for i in w.pop.dht_table(node) if int(i) != node]
        self.asked, self.active = {}, 0
        self.found, self.trace = set(), []
        self.queries, self.passes = 0, 0
        self.t0 = w.now
        self.st = None
        self.pass_start = w.now
        self.active_trace = []
        self.done = False
        self.idle_passes = 0
        self.new_in_pass = 0
        self.idle_replies = 0
        self.per_source = {}      # registrar -> results accepted from it
        self.sources = set()
        self.result_src = {}      # result -> registrar that returned it first
        self.src_buckets = set()  # search buckets of the registrars whose results were accepted

    def pass_gap(self):
        """Minimum gap before the next pass: 2 s, doubling after every pass
        that found nothing new, up to the configured back-off cap."""
        if self.new_in_pass > 0 or self.w.backoff <= 0:
            self.idle_passes = 0
            return PASS_MIN_S
        self.idle_passes += 1
        return min(self.w.backoff, PASS_MIN_S * (2 ** self.idle_passes))

    def new_pass(self):
        w, cfg = self.w, self.w.cfg
        self.new_in_pass = 0
        if cfg.adaptive:
            for k in [k for k, e in self.asked.items() if e <= w.now]:
                del self.asked[k]
        self.st = Search(cfg, w.pop, self.tid, self.topic, self.node, self.rng, self.asked, self.active)
        self.st.add_nodes(None, self.seeds, w.now)
        self.passes += 1
        self.pass_start = w.now
        self.idle_replies = 0
        self.active_trace.append((w.now - self.t0, self.st.active))

    def step(self):
        w = self.w
        if self.done or w.now >= self.stop_at or (self.target and len(self.found) >= self.target and (not w.min_sources or len(self.sources) >= w.min_sources) and (not w.min_buckets or len(self.src_buckets) >= w.min_buckets)):
            self.done = True; self.end = w.now; return
        if self.st is None:
            self.new_pass()
        st = self.st
        for r in st.results:
            if r not in self.found:
                src = self.result_src.get(r)
                if w.min_sources and self.target and src is not None:
                    cap = -(-self.target // w.min_sources)
                    if self.per_source.get(src, 0) >= cap:
                        continue
                    self.per_source[src] = self.per_source.get(src, 0) + 1
                    self.sources.add(src)
                if src is not None and w.min_buckets:
                    self.src_buckets.add(bucket_index(self.tid, int(w.pop.ids[src]), w.cfg.depth))
                self.found.add(r)
                self.new_in_pass += 1
                self.trace.append((self.queries, w.now - self.t0, len(self.found)))
                w.note_found(r, self.topic, w.now)
        st.results.clear()
        if self.target and len(self.found) >= self.target and (not w.min_sources or len(self.sources) >= w.min_sources) and (not w.min_buckets or len(self.src_buckets) >= w.min_buckets):
            self.done = True; self.end = w.now; return
        # end the pass early after --idle-replies replies in a row brought nothing
        # new, once the searcher holds any result: the walk is not finding ads
        if w.idle_replies and self.found and self.idle_replies >= w.idle_replies:
            st.results.clear()
            self.active = st.active; self.st = None
            w.at(max(w.now, self.pass_start + self.pass_gap()), "sstep", self.node)
            return
        if st.is_done() or (dst := st.query_target()) is None:
            self.active = st.active
            self.st = None
            w.at(max(w.now, self.pass_start + self.pass_gap()), "sstep", self.node)
            return
        dists = st.buckets_with_free_space()
        self.queries += 1
        w.q_sent[self.node] += 1
        results, aux = w.serve_query(dst, self.topic, self.tid, dists)
        w.at(w.now + w.rtt, "sreply", self.node, dst, results, aux)

    def reply(self, dst, results, aux):
        w, st = self.w, self.st
        if st is None:
            w.at(w.now, "sstep", self.node); return
        st.add_nodes(dst, aux, w.now)
        before = st.active
        for r in results:
            self.result_src.setdefault(int(r), dst)
        new = st.add_query_results(dst, results, w.now)
        self.idle_replies = 0 if any(int(r) not in self.found for r in results) else self.idle_replies + 1
        if st.active != before:
            self.active_trace.append((w.now - self.t0, st.active))
        w.at(w.now, "sstep", self.node)


CLASSES = (("1000+", 1000, 10**9), ("100-999", 100, 999), ("10-99", 10, 99), ("<10", 0, 9))


def size_class(n):
    for name, lo, hi in CLASSES:
        if lo <= n <= hi:
            return name
    return "<10"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=10000)
    ap.add_argument("--topics", type=int, default=100)
    ap.add_argument("--model", default="")
    ap.add_argument("--zipf", type=float, default=1.0)
    ap.add_argument("--register-frac", type=float, default=0.5)
    ap.add_argument("--all-register", action="store_true")
    ap.add_argument("--floor", type=int, default=0)
    ap.add_argument("--aux-radius", type=int, default=0)
    ap.add_argument("--depth", type=int, default=10)
    ap.add_argument("--bucket-size", type=int, default=16)
    ap.add_argument("--cache", type=int, default=5000)
    ap.add_argument("--topic-nodes-limit", type=int, default=16)
    ap.add_argument("--aux-limit", type=int, default=8)
    ap.add_argument("--ad-lifetime", type=float, default=900)
    ap.add_argument("--rtt", type=float, default=0.06)
    ap.add_argument("--start-window", type=float, default=900, help="node starts spread over this (s)")
    ap.add_argument("--register-wait", type=float, default=1350, help="search phase starts this long after the window (s)")
    ap.add_argument("--search-time", type=float, default=3600)
    ap.add_argument("--search-stagger", type=float, default=-1, help="seconds between searcher starts; default spreads the starts over a quarter of the search phase, at most 0.1 s apart")
    ap.add_argument("--target", type=int, default=0, help="conn-driven stop at this many results; 0 = continuous")
    ap.add_argument("--search-frac", type=float, default=1.0, help="fraction of nodes that search (sampled)")
    ap.add_argument("--pass-backoff", type=float, default=0, help="cap (s) of the exponential gap between passes that found nothing new; 0 = fixed 2 s")
    ap.add_argument("--min-sources", type=int, default=0, help="connection-driven: accept at most target/min_sources results from one registrar and stop only with results from this many registrars; 0 = off")
    ap.add_argument("--min-buckets", type=int, default=0, help="connection-driven: stop only with accepted results from registrars in this many distinct distance buckets; 0 = off")
    ap.add_argument("--idle-replies", type=int, default=0, help="end a pass after this many consecutive replies with nothing new; 0 = walk the whole table")
    ap.add_argument("--late-frac", type=float, default=0, help="fraction of registrants that start during the search phase instead of the start window")
    ap.add_argument("--sybil-frac", type=float, default=0, help="fraction of nodes that are sybils: they register, answer queries with sybil ads only and refer sybils")
    ap.add_argument("--bootnodes", type=int, default=0)
    ap.add_argument("--no-spawn-bias", action="store_true", help="uniform DHT tables instead of tables filled from the nodes alive at start")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    t0 = time.time()
    pop = Population(a.nodes, seed=a.seed, bootnodes=a.bootnodes, spawn_bias=not a.no_spawn_bias)
    shares = shares_from_model(a.model, a.topics) if a.model else zipf_shares(a.topics, a.zipf)
    pop.assign_topics(shares, a.register_frac, a.all_register)
    cfg = WorldConfig(cache=a.cache, depth=a.depth, bucket_size=a.bucket_size, topic_nodes_limit=a.topic_nodes_limit,
                      aux_limit=a.aux_limit, yield_floor=a.floor, aux_radius=a.aux_radius, ad_lifetime=a.ad_lifetime, rtt=a.rtt)
    sybil = np.random.default_rng(a.seed + 9).random(pop.n) < a.sybil_frac
    w = World(pop, cfg, a.rtt, a.seed, backoff=a.pass_backoff, sybil=sybil, idle_replies=a.idle_replies, min_sources=a.min_sources, min_buckets=a.min_buckets)
    rng = np.random.default_rng(a.seed + 5)
    starts = rng.random(pop.n) * a.start_window
    k = len(shares)
    search_at = a.start_window + a.register_wait
    late = rng.random(pop.n) < a.late_frac
    for i in range(pop.n):
        if pop.registrant[i]:
            t = search_at + rng.random() * a.search_time * 0.5 if late[i] else float(starts[i])
            w.at(float(t), "start_reg", i, int(pop.topics[i]))
    searchers = [i for i in range(pop.n) if rng.random() < a.search_frac and not sybil[i]]
    if a.search_stagger < 0:
        a.search_stagger = min(0.1, 0.25 * a.search_time / max(1, len(searchers)))
    for j, i in enumerate(searchers):
        w.at(search_at + j * a.search_stagger, "start_search", i, int(pop.topics[i]), a.target, search_at + a.search_time)
    end = search_at + a.search_time
    print(f"{pop.n} nodes, {k} topics, {int(pop.registrant.sum())} registrants, {len(searchers)} searchers; floor {a.floor}; run to {end:.0f} s", flush=True)
    last = [time.time()]
    def prog(now, n):
        print(f"  t={now:.0f}s events={n/1e6:.1f}M wall={time.time()-t0:.0f}s", flush=True)
    nev = w.run(end, prog)
    print(f"done: {nev/1e6:.1f}M events in {time.time()-t0:.0f}s", flush=True)

    # --- results
    for tt in w.ttab:
        tt.purge(w.now)
    held = np.array([tt.count for tt in w.ttab])
    fanout = {}
    for r, tt in enumerate(w.ttab):
        for t, d in tt.ads.items():
            for adv in d:
                fanout[(adv, t)] = fanout.get((adv, t), 0) + 1
    sizes = np.bincount(pop.topics[pop.registrant], minlength=k)
    members = {t: set(int(i) for i in pop.topic_members(t)) for t in range(k)}
    rows = []
    for i, s in w.searchers.items():
        reach = members[s.topic] - {i}
        found = len(s.found & reach)
        def at(frac):
            need = frac * len(reach)
            for q, tm, n in s.trace:
                if n >= need:
                    return tm
            return None
        nsyb = sum(1 for r in s.found if sybil[r])
        rows.append(dict(sybil_found=nsyb, sybil_share=(nsyb / len(s.found)) if s.found else None, eclipsed=bool(s.found) and nsyb == len(s.found),
                         searcher=i, topic=s.topic, size=int(sizes[s.topic]), cls=size_class(int(sizes[s.topic])), queries=s.queries,
                         passes=s.passes, found=found, reach=len(reach), recall=found / len(reach) if reach else 1.0,
                         t50=at(0.5), t90=at(0.9), t99=at(0.99), settled=s.active_trace[-1][1] if s.active_trace else 0,
                         done=s.done, dur=(getattr(s, "end", w.now) - s.t0)))
    by_class = {}
    for name, _, _ in CLASSES:
        rs = [r for r in rows if r["cls"] == name]
        if not rs:
            continue
        med = lambda key: (float(np.median([r[key] for r in rs if r[key] is not None])) if any(r[key] is not None for r in rs) else None)
        shares = [r["sybil_share"] for r in rs if r["sybil_share"] is not None]
        by_class[name] = dict(sybil_share_median=(float(np.median(shares)) if shares else None), sybil_share_mean=(float(np.mean(shares)) if shares else None),
                              eclipsed=float(np.mean([r["eclipsed"] for r in rs])),
                              topics=len({r["topic"] for r in rs}), searchers=len(rs), recall_median=med("recall"),
                              full=float(np.mean([r["recall"] >= 0.999 for r in rs])), queries_median=med("queries"),
                              t50=med("t50"), t90=med("t90"), t99=med("t99"), settled_median=med("settled"))
    adm = np.array(list(w.first_admit.values())) if w.first_admit else np.array([np.nan])
    # late joiners: time from first admission to first discovery by any searcher, by topic class
    late_disc = {}
    for i in range(pop.n):
        if late[i] and pop.registrant[i]:
            key = (i, int(pop.topics[i]))
            if key in w.first_admit:
                adm_at = w.reg_start[key] + w.first_admit[key]
                cls = size_class(int(sizes[key[1]]))
                late_disc.setdefault(cls, []).append((w.first_found[key] - adm_at) if key in w.first_found else None)
    late_summary = {c: dict(n=len(v), found=sum(x is not None for x in v), delay_median=(float(np.median([x for x in v if x is not None])) if any(x is not None for x in v) else None),
                            delay_p90=(float(np.percentile([x for x in v if x is not None], 90)) if any(x is not None for x in v) else None)) for c, v in late_disc.items()}
    fo = np.array(list(fanout.values())) if fanout else np.array([0])
    q = np.array(w.quotes) if w.quotes else np.array([0.0])
    summary = dict(config=vars(a), events=nev, elapsed=time.time() - t0, sybil_frac=float(sybil.mean()),
                   registrar_queries=dict(median=float(np.median(w.tq_rcv)), p99=float(np.percentile(w.tq_rcv, 99)), max=int(w.tq_rcv.max())),
                   registrar_regtopic=dict(median=float(np.median(w.reg_rcv)), p99=float(np.percentile(w.reg_rcv, 99)), max=int(w.reg_rcv.max())),
                   ads_held=dict(median=float(np.median(held)), p99=float(np.percentile(held, 99)), max=int(held.max()), total=int(held.sum())),
                   fanout=dict(median=float(np.median(fo)), p10=float(np.percentile(fo, 10)), registered=len(fanout), registrants=int(pop.registrant.sum())),
                   admission_s=dict(median=float(np.nanmedian(adm)), p90=float(np.nanpercentile(adm, 90)), n=len(w.first_admit)),
                   quoted_wait_s=dict(median=float(np.median(q)), p90=float(np.percentile(q, 90)), n=len(w.quotes)),
                   regtopic_sent_per_registrant=dict(median=float(np.median(w.reg_sent[pop.registrant])), max=int(w.reg_sent.max())),
                   by_class=by_class, late_joiners=late_summary,
                   queries_sent=dict(total=int(w.q_sent.sum()), per_searcher_median=float(np.median(w.q_sent[list(w.searchers)])) if w.searchers else 0.0))
    json.dump(summary, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    json.dump(rows, open(os.path.join(a.out, "searches.json"), "w"))
    np.save(os.path.join(a.out, "tq_rcv.npy"), w.tq_rcv); np.save(os.path.join(a.out, "reg_rcv.npy"), w.reg_rcv)
    np.save(os.path.join(a.out, "spawn.npy"), pop.spawn); np.save(os.path.join(a.out, "held.npy"), held); np.save(os.path.join(a.out, "ids.npy"), pop.ids); np.save(os.path.join(a.out, "topic_ids.npy"), pop.topic_ids)
    print(json.dumps({k2: v for k2, v in summary.items() if k2 != "config"}, indent=1))


if __name__ == "__main__":
    main()
