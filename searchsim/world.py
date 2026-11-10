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
from array import array
from collections import Counter, OrderedDict

import numpy as np

from .population import Population, shares_from_model, zipf_shares, bucket_index, logdist, DHT_MIN_DIST
from .search import Config, Search, PASS_MIN_S, ForkConfig, ForkSearch, AskedSet
from .attack import Attacker, SubnetLedger, AdmissionLedger
from .admission import WorldTable, go_config
from .go_registrar import GRACE, SECOND, WINDOW
from .honest_nets import ipv4_of_labels, honest_times, honest_reached

STANDBY, WAITING, REGISTERED = 0, 1, 2
OCCUPANCY_EXP, WAIT_BASE_MOD, WAIT_FLOOR = 10, 0.1, 1.0
GRACE_NS = WINDOW + GRACE     # a ticket presented this long after its start has expired
REG_BUCKET_SIZE, REG_STANDBY = 5, 20
TABLE_IP_LIMIT, BUCKET_IP_LIMIT = 10, 2     # discv5 table: nodes per /24 in the table and in a bucket


class Attempt:
    __slots__ = ("state", "next", "ticket", "total_wait", "filled", "ver", "reqs")

    def __init__(self):
        self.state, self.next, self.ticket, self.total_wait, self.filled, self.ver, self.reqs = STANDBY, 0.0, None, 0.0, None, 0, 0


class RegTable:
    """Registration table of one advertiser for one topic."""
    __slots__ = ("depth", "buckets", "counts", "heap", "ready", "rr", "adlife", "timeout", "size", "standby", "banned")

    def __init__(self, depth, adlife, size=REG_BUCKET_SIZE, standby=REG_STANDBY, timeout=None):
        self.depth = depth
        self.buckets = [dict() for _ in range(depth)]
        self.counts = [[0, 0, 0] for _ in range(depth)]
        self.heap = []
        self.ready = {}           # bucket -> {registrar: attempt} waiting and due
        self.rr = depth - 1       # bucket of the last started attempt (rrBucket)
        self.adlife = adlife
        self.timeout = 1.5 * adlife if timeout is None else timeout
        self.size, self.standby = size, standby
        # registration.go overloaded set: registrar -> end of its ad-lifetime ban
        self.banned = {}

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
            if self.banned and self.banned.get(n, -1.0) > w.now:
                continue
            if src_att is not None:
                if src_att.filled is None:
                    src_att.filled = set()
                if bi in src_att.filled:
                    continue
            if self.counts[bi][STANDBY] >= self.standby:
                continue
            if w.net is not None:
                k = w.net[n]
                if k >= 0 and any(w.net[m] == k for m in b):
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
        if c[WAITING] + c[REGISTERED] >= self.size:
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
        r = self.ready.get(bi)
        if r is not None and r.pop(reg, None) is not None and not r:
            del self.ready[bi]

    def next_time(self):
        if self.ready:
            return min(att.next for r in self.ready.values() for att in r.values())
        while self.heap:
            t, bi, reg, ver = self.heap[0]
            att = self.buckets[bi].get(reg)
            if att is None or att.ver != ver:
                heapq.heappop(self.heap)
                continue
            return t
        return None

    def collect(self, now):
        """Expire registered attempts whose ad ran out; move due waiting
        attempts to their bucket's ready set."""
        heap = self.heap
        while heap and heap[0][0] <= now:
            _, bi, reg, ver = heapq.heappop(heap)
            att = self.buckets[bi].get(reg)
            if att is None or att.ver != ver:
                continue
            if att.state == REGISTERED:
                self.remove(bi, reg)
                self.refill(bi, now)
            else:
                self.ready.setdefault(bi, {})[reg] = att

    def pick(self, now_ns=None):
        """Registration.Update: buckets in turn from the one after the last
        started attempt; within a bucket ticket holders first, then the
        earliest due. With now_ns (a budgeted attacker, who owes Go no send
        order) the earliest still valid ticket in any bucket goes first, else
        the attempt due longest."""
        if not self.ready:
            return None
        if now_ns is not None:
            best = self.best_ticket(now_ns)
            if best is not None:
                return best[1], best[2]
            return min((att.next, b, reg) for b, r in self.ready.items() for reg, att in r.items())[1:]
        for i in range(1, self.depth + 1):
            b = (self.rr + i) % self.depth
            r = self.ready.get(b)
            if r:
                return b, min(r, key=lambda k: (r[k].ticket is None, r[k].next))
        return None

    def best_ticket(self, now_ns):
        """(start, bucket, registrar) of the ready attempt whose ticket is
        still valid at now_ns and starts first, or None."""
        best = None
        for b, r in self.ready.items():
            for reg, att in r.items():
                t = att.ticket
                if t is not None and t.start + GRACE_NS > now_ns and (best is None or t.start < best[0]):
                    best = (t.start, b, reg)
        return best

    def next_ticket_due(self):
        """Earliest heap time of a waiting attempt that holds a ticket."""
        out = None
        for t, bi, reg, ver in self.heap:
            att = self.buckets[bi].get(reg)
            if att is not None and att.ver == ver and att.state == WAITING and att.ticket is not None and (out is None or t < out):
                out = t
        return out

    def start(self, bi, reg):
        """StartRequest."""
        r = self.ready[bi]
        att = r.pop(reg)
        if not r:
            del self.ready[bi]
        att.ver += 1
        att.reqs += 1
        self.rr = bi

    def update(self, now):
        """Return the next attempt (bucket, registrar) due to send, started,
        or None."""
        self.collect(now)
        due = self.pick()
        if due is not None:
            self.start(*due)
        return due

    def free_dists(self):
        return [256 - i for i in range(self.depth) if self.counts[i][STANDBY] < self.standby]

    def drop(self, bi, reg, now):
        if self.banned.get(reg, -1.0) <= now:
            self.banned[reg] = now + self.adlife
        self.remove(bi, reg)
        self.refill(bi, now)

    def on_ticket(self, bi, reg, wait, now, ticket):
        """HandleTicketResponse; returns the give-up reason or None."""
        att = self.buckets[bi][reg]
        wait = max(WAIT_FLOOR, wait)
        if wait > self.timeout:
            self.drop(bi, reg, now); return "single"
        att.total_wait += wait
        if att.total_wait > self.timeout:
            self.drop(bi, reg, now); return "cumulative"
        att.ticket = ticket
        att.next = now + wait
        self.push(att, bi, reg)
        return None

    def on_registered(self, bi, reg, ttl, now):
        att = self.buckets[bi][reg]
        self.set_state(att, bi, REGISTERED)
        att.next = now + min(ttl, self.adlife)
        att.ticket = None
        att.total_wait = 0.0
        self.push(att, bi, reg)
        self.refill(bi, now)


class WorldConfig(Config):
    def __init__(self, cache=5000, **kw):
        super().__init__(**kw)
        self.cache = cache


class World:
    def __init__(self, pop, cfg, rtt, seed, backoff=0.0, sybil=None, idle_replies=0, min_sources=0, min_buckets=0,
                 registrar_limit=0, sybil_fresh=False, sybil_nets=0, mix_registrars=0, fork=None, silent=None, attack="none", attack_nets=0, w1=False, w1_limit=1, m2=False, m2_nets=2, m3=False, w1p=False, w1p_limit=1, honest_nets=None,
                 reg_bucket_size=REG_BUCKET_SIZE, reg_standby=REG_STANDBY, ip_score=True, flood=None, target=None):
        if rtt <= 0:
            raise ValueError("rtt must be positive: a retry at the ticket's start would be an early echo")
        self.pop, self.cfg, self.rtt = pop, cfg, rtt
        self.reg_bucket_size, self.reg_standby = reg_bucket_size, reg_standby
        # flood: sybil advertisers (flood.Flood) with ids below zero; they
        # register only, never serve or enter any table.
        self.flood = flood
        # fork: a ForkConfig runs every search as the fork's topicSearch does
        # (ForkSearcherState); None keeps the original searcher.
        self.fork = fork
        self.attack, self.attack_nets = attack, attack_nets
        self.attacker = Attacker(attack, attack_nets, fork, seed) if attack != "none" else None
        self.mint = self.attacker.mint if self.attacker is not None else None
        self.w1, self.w1_limit = w1, w1_limit
        self.w1p, self.w1p_limit = w1p, w1p_limit
        self.m2, self.m2_nets, self.m3 = m2, m2_nets, m3
        # silent[i]: node i never answers a TOPICQUERY; the query fails after
        # the response timeout (fork searcher only).
        self.silent = silent
        # mix_registrars: result mixer. The new results of each registrar wait
        # in a queue of their own. While this many registrars have results
        # waiting, the search hands out one result from each in turn. The end
        # of a pass hands out what is left, also one registrar at a time. 0 = off.
        self.mix_registrars = mix_registrars
        # sybil_nets: the sybils share this many /24 prefixes, and the IP limits
        # of the Go code apply to them: discv5 table (10 per table, 2 per
        # bucket), search and registration table (1 per bucket), admission IP
        # score. 0 = every sybil has a /24 of its own, no limit binds.
        self.net = None
        if honest_nets is not None:
            self.net = np.asarray(honest_nets, dtype=np.int64)
            if self.net.shape != (pop.n,) or np.any(self.net < 0):
                raise ValueError("honest subnet assignment must give every node a public /24")
        elif sybil_nets > 0 and sybil is not None and sybil.any():
            self.net = np.full(pop.n, -1, dtype=np.int64)
            sy = np.nonzero(sybil)[0]
            self.net[sy] = np.random.default_rng(seed + 11).integers(0, sybil_nets, size=len(sy))
        pop.net = self.net
        self.ips = ipv4_of_labels(self.net, pop.n, seed)
        # registrar_limit: SearchFilter.Take/Release of the Go code. A registrar
        # contributes this many new results per ad lifetime; the rest are held
        # until the end of the pass. 0 = off.
        self.registrar_limit = registrar_limit
        # sybil_fresh: a sybil registrar answers with 16 new sybil ids on every
        # query (ids below zero) in place of the sybils that belong to the topic.
        self.sybil_fresh = sybil_fresh
        self.fresh_id = 0
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
        self.ttab = [WorldTable(go_config(cfg), ip_score=ip_score) for _ in range(n)]
        self.reg = {}            # (node, topic) -> RegTable
        self.reg_start = {}
        self.first_admit = {}
        k = flood.k if flood is not None else (len(pop.topic_ids) if pop.topic_ids is not None else 0)
        self.k = k
        # quoted waits (s) of honest and attacker requests
        self.quotes, self.quotes_att = array("d"), array("d")
        self.giveups = Counter()        # (honest|sybil, single|cumulative)
        self.adv_timings = {"honest": Counter(), "sybil": Counter()}
        self.phase = {}                 # "start"/"end" -> counter snapshot at the search phase boundaries
        self.reg_stats = {key: np.zeros(k, dtype=np.int64) for key in ("sent", "admitted", "existing", "giveups")}
        self.target = target
        self.target_queries = np.zeros(n, dtype=np.int64)
        self.served = {}                # topic -> [queries, results, sybil results, sum target share, sum cache share, queries with topic ads]
        self.samples, self.sample_rows = [], []
        self.sample_period = 0.0
        self.last_rcv = (np.zeros(n, dtype=np.int64), np.zeros(n, dtype=np.int64), np.zeros(n, dtype=np.int64))
        self.reg_rcv = np.zeros(n, dtype=np.int64)
        self.tq_rcv = np.zeros(n, dtype=np.int64)
        self.ads_served = np.zeros(n, dtype=np.int64)
        self.reg_sent = np.zeros(n, dtype=np.int64)
        self.q_sent = np.zeros(n, dtype=np.int64)
        self._tables = {}
        self.searchers = {}

    def is_sybil(self, i):
        return i < 0 or bool(self.sybil[i])

    def ip_of(self, i):
        return self.flood.ip_of(i) if i < 0 else int(self.ips[i])

    def new_regtable(self, node):
        if node < 0:
            return self.flood.regtable(self.cfg.depth, self.cfg.ad_lifetime, self.reg_bucket_size, self.reg_standby)
        return RegTable(self.cfg.depth, self.cfg.ad_lifetime, self.reg_bucket_size, self.reg_standby)

    def seeds_for(self, node):
        if node < 0:
            return self.flood.seeds(node, self.pop)
        return [int(i) for i in self.dht(node)]

    def registrar_net(self, i):
        return self.mint[i][1] if self.mint is not None and i in self.mint else self.public_net(i)

    def public_net(self, i):
        # With no explicit IP array each honest node has a distinct public
        # /24. Explicit -1 entries retain the fork's LAN exemption.
        return int(self.net[i]) if self.net is not None else self.attack_nets + i

    # --- infrastructure --------------------------------------------------
    def at(self, t, kind, *args):
        self.seq += 1
        heapq.heappush(self.events, (t, self.seq, kind, args))

    def dht(self, i):
        """The discv5 table of node i with the IP limits applied. An entry
        over the limit is replaced by a node of the same bucket that has a /24
        of its own, when a few draws find one."""
        idx = self.pop.dht_table(i)
        if self.net is None:
            return idx
        nets = self.net[idx]
        if not (nets >= 0).any():
            return idx
        x = int(self.pop.ids[i])
        rng = random.Random(i * 31 + 7)
        out = list(int(j) for j in idx)
        have = set(out)
        tab, buck = {}, {}
        order = [j for j in range(len(out)) if nets[j] >= 0]
        rng.shuffle(order)
        for j in order:
            k = int(nets[j])
            d = max(logdist(x, int(self.pop.ids[out[j]])), DHT_MIN_DIST + 1)
            if tab.get(k, 0) < TABLE_IP_LIMIT and buck.get((d, k), 0) < BUCKET_IP_LIMIT:
                tab[k] = tab.get(k, 0) + 1
                buck[(d, k)] = buck.get((d, k), 0) + 1
                continue
            lo, hi = self.pop.subtree(x, d) if d > DHT_MIN_DIST + 1 else self.pop.subtree_within(x, d)
            new = -1
            for _ in range(8 if hi > lo else 0):
                c = lo + rng.randrange(hi - lo)
                if c != i and c not in have and self.net[c] < 0:
                    new = c
                    break
            have.discard(out[j])
            out[j] = new
            if new >= 0:
                have.add(new)
        return np.array([j for j in out if j >= 0], dtype=np.int64)

    def table(self, i):
        t = self._tables.get(i)
        if t is None:
            idx = self.dht(i)
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
                out.append(int(idx[m[self.rng.randrange(len(m))]]))
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
                rt = self.reg[key] = self.new_regtable(node)
            seeds = self.seeds_for(node)
            self.rng.shuffle(seeds)
            rt.add_nodes(self, tid, None, [s for s in seeds if s != node])
            if rt.node_count() == 0:
                self.at(self.now + PASS_MIN_S, "reg", node, topic); return
        rt.collect(self.now)
        due = rt.pick(self.flood.expired_ns(self.now) if node < 0 and self.flood.budget else None)
        if due is None:
            nt = rt.next_time()
            self.at(nt if nt is not None else self.now + PASS_MIN_S, "reg", node, topic)
            return
        if node < 0 and self.flood.budget and not self.flood.grant(self, node, topic, rt, due):
            return      # parked until the attacker's rate budget has a token
        rt.start(*due)
        self.send_reg(node, topic, *due)

    def reg_grant(self, node, topic):
        """A parked sybil chain got a token: send its best due attempt now."""
        rt = self.reg[(node, topic)]
        rt.collect(self.now)
        due = rt.pick(self.flood.expired_ns(self.now))
        if due is None:
            nt = rt.next_time()
            self.at(nt if nt is not None else self.now + PASS_MIN_S, "reg", node, topic)
            return False
        rt.start(*due)
        self.send_reg(node, topic, *due)
        return True

    def send_reg(self, node, topic, bi, registrar):
        rt = self.reg[(node, topic)]
        att = rt.buckets[bi][registrar]
        dists = rt.free_dists()
        tid = int(self.pop.topic_ids[topic])
        # registrar side, at arrival
        self.reg_rcv[registrar] += 1
        if node >= 0:
            self.reg_sent[node] += 1
            if topic < self.k:
                self.reg_stats["sent"][topic] += 1
        else:
            self.flood.sent[registrar] += 1
        aux = self.aux(registrar, tid, dists)
        now_ns = round(self.now * SECOND)
        if att.ticket is not None:
            # the advertiser's view of its ticket's timing (the registrars' sum mixes both classes)
            self.adv_timings["sybil" if node < 0 else "honest"][att.ticket.timing(now_ns)] += 1
        reply = self.ttab[registrar].request(node, topic, self.ip_of(node), now_ns, att.ticket,
                                             attacker=self.is_sybil(node))
        if reply.status == "quote":
            (self.quotes_att if node < 0 else self.quotes).append(reply.wait / SECOND)
        self.at(self.now + self.rtt, "regresp", node, topic, bi, registrar, reply, aux)

    def reg_resp(self, node, topic, bi, registrar, reply, aux):
        key = (node, topic); rt = self.reg[key]
        tid = int(self.pop.topic_ids[topic])
        if registrar not in rt.buckets[bi]:
            self.at(self.now, "reg", node, topic); return
        if aux:
            rt.add_nodes(self, tid, registrar, [a for a in aux if a != node])
        honest = node >= 0 and topic < self.k
        if reply.status == "quote":
            why = rt.on_ticket(bi, registrar, reply.wait / SECOND, self.now, reply.ticket)
            if why is not None:
                self.giveups[("sybil" if node < 0 else "honest", why)] += 1
                if honest:
                    self.reg_stats["giveups"][topic] += 1
        else:
            rt.on_registered(bi, registrar, reply.wait / SECOND, self.now)
            if honest:
                self.reg_stats[reply.status][topic] += 1
            elif node < 0 and reply.status == "admitted":
                self.flood.admitted += 1
            if key not in self.first_admit:
                self.first_admit[key] = self.now - self.reg_start[key]
        nt = rt.next_time()
        self.at(max(self.now, nt) if nt is not None else self.now + PASS_MIN_S, "reg", node, topic)

    def schedule_phases(self, search_at, end, period):
        """Samples on the grid search_at + j * period; counter snapshots at
        the search phase's start and end."""
        self.sample_period = period
        self.at(search_at % period or period, "sample")
        self.at(search_at, "phase", "start")
        self.at(end, "phase", "end")

    def snapshot(self, name):
        sent = self.flood.sent if self.flood is not None else np.zeros(self.pop.n, dtype=np.int64)
        self.phase[name] = dict(t=self.now, reg_rcv=self.reg_rcv.copy(), flood_sent=sent.copy(),
                                sent=self.reg_stats["sent"].copy())

    def sample(self):
        """Per-registrar snapshot every sample_period seconds."""
        tgt = self.target
        sent = self.flood.sent if self.flood is not None else self.last_rcv[2]
        ads = np.empty(self.pop.n, dtype=np.int32); att = np.empty_like(ads)
        tads = np.empty_like(ads); tatt = np.empty_like(ads)
        live = set()
        for r, tt in enumerate(self.ttab):
            tt.purge(self.now)
            ads[r], att[r] = len(tt.ads), tt.attacker_ads
            d = tt.by_topic.get(tgt)
            tads[r] = len(d) if d else 0
            tatt[r] = tt.attacker_topics[tgt] if d else 0
            if d:
                live.update(a for a in d if a >= 0)
        rr, tq, fs = self.last_rcv
        row = (ads, att, tads, tatt, (self.reg_rcv - rr).astype(np.int32), (self.tq_rcv - tq).astype(np.int32),
               (sent - fs).astype(np.int32))
        self.last_rcv = (self.reg_rcv.copy(), self.tq_rcv.copy(), sent.copy())
        self.samples.append(row)
        self.sample_rows.append(dict(t=self.now, honest_ads=int(ads.sum() - att.sum()), sybil_ads=int(att.sum()),
                                     sybil_requests=int(row[6].sum()), honest_requests=int(row[4].sum() - row[6].sum()),
                                     target_honest_live=len(live)))
        self.at(self.now + self.sample_period, "sample")

    def note_found(self, r, topic, now):
        key = (r, topic)
        if key not in self.first_found:
            self.first_found[key] = now

    # --- search ------------------------------------------------------------
    def start_search(self, node, topic, target, stop_at):
        s = (ForkSearcherState if self.fork is not None else SearcherState)(self, node, topic, target, stop_at)
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
        if self.sybil[registrar] and self.sybil_fresh:
            results = list(range(self.fresh_id - 1, self.fresh_id - 1 - self.cfg.topic_nodes_limit, -1))
            self.fresh_id -= self.cfg.topic_nodes_limit
            self.ads_served[registrar] += len(results)
            return results, self.sybil_aux(tid, dists)
        if self.sybil[registrar]:
            pool = self.sybil_by_topic.get(topic)
            if pool is None:
                pool = self.sybil_by_topic[topic] = [int(i) for i in np.nonzero(self.sybil & (self.pop.topics == topic) & self.pop.registrant)[0]]
            results = self.rng.sample(pool, self.cfg.topic_nodes_limit) if len(pool) > self.cfg.topic_nodes_limit else list(pool)
            self.ads_served[registrar] += len(results)
            return results, self.sybil_aux(tid, dists)
        tt = self.ttab[registrar]
        tt.purge(self.now)
        d = tt.by_topic.get(topic)
        if d:
            held = list(d.keys())
            results = self.rng.sample(held, self.cfg.topic_nodes_limit) if len(held) > self.cfg.topic_nodes_limit else held
        else:
            results = []
        self.ads_served[registrar] += len(results)
        if topic == self.target:
            self.target_queries[registrar] += 1
        sv = self.served.get(topic)
        if sv is None:
            sv = self.served[topic] = [0, 0, 0, 0.0, 0.0, 0]
        sv[0] += 1
        sv[1] += len(results)
        sv[2] += sum(1 for r in results if self.is_sybil(r))
        if d:
            sv[3] += tt.attacker_topics[topic] / len(d)
            sv[5] += 1
        if tt.ads:
            sv[4] += tt.attacker_ads / len(tt.ads)
        return results, self.aux(registrar, tid, dists)

    def fork_query(self, dst, topic, tid, dists, searcher=None):
        """A TOPICQUERY as the fork's searcher sees it: (delay, results, aux,
        failed). The response timer restarts on every packet, so a reply cut
        short fails resp_timeout after its last packet; a reply arrives whole
        after one rtt unless that is past resp_timeout, and a silent node
        fails after resp_timeout."""
        if self.attacker is not None and dst < 0:
            return self.attacker.query(searcher, dst, dists)
        if self.silent is not None and self.silent[dst]:
            return self.fork.resp_timeout, [], [], True
        results, aux = self.serve_query(dst, topic, tid, dists)
        if self.rtt > self.fork.resp_timeout:
            return self.fork.resp_timeout, [], [], True
        return self.rtt, results, aux, False

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
            elif kind == "flood":
                self.flood.wake(self)
            elif kind == "rekey":
                self.flood.rekey(self, *args)
            elif kind == "phase":
                self.snapshot(*args)
            elif kind == "sample":
                self.sample()
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
        self.seeds = [int(i) for i in w.dht(node) if int(i) != node]
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
        self.sybil_queries = 0
        self.order = []           # (result, registrar it was taken from, time, queries), in delivery order
        self.reg_count = {}       # registrar -> [results taken, expiry]
        self.held, self.held_src = [], {}
        self.mixq, self.mix_in = {}, {}   # registrar -> results waiting; result -> its registrar
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

    def deliver(self, results):
        w = self.w
        for r in results:
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
                self.order.append((r, self.held_src.get(r, src), w.now - self.t0, self.queries))
                if r >= 0:
                    w.note_found(r, self.topic, w.now)

    def mix_rows(self, flush):
        w = self.w
        while True:
            ready = [d for d, q in self.mixq.items() if q]
            if not ready or (len(ready) < w.mix_registrars and not flush):
                break
            row = []
            for d in ready:
                r = self.mixq[d].pop(0)
                del self.mix_in[r]
                self.result_src[r] = d
                row.append(r)
            self.deliver(row)
        if flush:
            self.mixq = {}

    def take(self, dst, results):
        """SearchFilter.Take."""
        w = self.w
        results = [int(r) for r in results]
        if w.mix_registrars:
            q = self.mixq.setdefault(dst, [])
            for r in results:
                if r in self.found or r == self.node or r in self.mix_in:
                    continue
                self.mix_in[r] = dst
                q.append(r)
            self.mix_rows(False)
            return []
        if not w.registrar_limit:
            return results
        rc = self.reg_count.get(dst)
        if rc is None or rc[1] <= w.now:
            rc = self.reg_count[dst] = [0, w.now + w.cfg.ad_lifetime]
        out = []
        for r in results:
            if r in self.found or r == self.node:
                continue
            if rc[0] >= w.registrar_limit:
                if r not in self.held_src:
                    self.held_src[r] = dst
                    self.held.append(r)
                continue
            rc[0] += 1
            self.held_src.pop(r, None)
            self.result_src[r] = dst
            out.append(r)
        return out

    def release(self):
        """SearchFilter.Release, at the end of a pass: the held results are
        handed out in arrival order."""
        if self.w.mix_registrars:
            self.mix_rows(True)
            return
        if not self.held:
            return
        rel = [r for r in self.held if r in self.held_src and r not in self.found]
        self.deliver(rel)
        self.held, self.held_src = [], {}

    def step(self):
        w = self.w
        if self.done or w.now >= self.stop_at or (self.target and len(self.found) >= self.target and (not w.min_sources or len(self.sources) >= w.min_sources) and (not w.min_buckets or len(self.src_buckets) >= w.min_buckets)):
            self.done = True; self.end = w.now; return
        if self.st is None:
            self.new_pass()
        st = self.st
        self.deliver(st.results)
        st.results.clear()
        if self.target and len(self.found) >= self.target and (not w.min_sources or len(self.sources) >= w.min_sources) and (not w.min_buckets or len(self.src_buckets) >= w.min_buckets):
            self.done = True; self.end = w.now; return
        # end the pass early after --idle-replies replies in a row brought nothing
        # new, once the searcher holds any result: the walk is not finding ads
        if w.idle_replies and self.found and self.idle_replies >= w.idle_replies:
            st.results.clear()
            self.release()
            self.active = st.active; self.st = None
            w.at(max(w.now, self.pass_start + self.pass_gap()), "sstep", self.node)
            return
        if st.is_done() or (dst := st.query_target()) is None:
            self.release()
            self.active = st.active
            self.st = None
            w.at(max(w.now, self.pass_start + self.pass_gap()), "sstep", self.node)
            return
        dists = st.buckets_with_free_space()
        self.queries += 1
        w.q_sent[self.node] += 1
        if w.sybil[dst]:
            self.sybil_queries += 1
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
        fresh = any(int(r) not in self.found for r in results)
        new = st.add_query_results(dst, self.take(dst, results), w.now, ads=len(results))
        self.idle_replies = 0 if fresh else self.idle_replies + 1
        if st.active != before:
            self.active_trace.append((w.now - self.t0, st.active))
        w.at(w.now, "sstep", self.node)


class SearchFilter:
    """topicindex/searchfilter.go. A result handed out is not handed out
    again for an ad lifetime. Within a pass the results wait in one queue per
    registrar whose reply held at least one of them, and go out one registrar
    at a time once mix_registrars registrars replied or the first waited
    mix_wait; mix_wait is only checked when a reply or failure comes in."""

    def __init__(self, me, ttl, registrars, wait, limit=50000, m3=False, net_of=None):
        self.me, self.ttl, self.mix_registrars, self.mix_wait = me, ttl, registrars, wait
        self.limit = limit
        self.m3, self.net_of = m3, net_of
        self.seen = OrderedDict()   # id -> expiry, oldest add first (seen + order of the Go code)
        self.reset()

    def reset(self):
        self.queues = []            # [registrar, nodes, queued_at], in arrival order
        self.by_reg = {}
        self.mix_nets = set()
        self.handed = {}            # node -> registrar it went out from, this pass
        self.first_queued, self.waiting = 0.0, False

    def expire(self, now):
        while self.seen and next(iter(self.seen.values())) <= now:
            self.seen.popitem(last=False)

    def is_seen(self, n, now):
        self.expire(now)
        return n in self.seen

    def add(self, n, now):
        """A re-add refreshes the expiry and moves the entry to the back; a
        new entry evicts the oldest at searchFilterLimit."""
        self.expire(now)
        if n in self.seen:
            self.seen[n] = now + self.ttl
            self.seen.move_to_end(n)
            return
        while len(self.seen) >= self.limit:
            self.seen.popitem(last=False)
        self.seen[n] = now + self.ttl

    def take(self, registrar, results, now):
        q = self.by_reg.get(registrar)
        for n in results:
            if n == self.me or self.is_seen(n, now) or n in self.handed:
                continue
            if q is None:
                q = self.by_reg[registrar] = [registrar, [], now]
                self.queues.append(q)
                if self.m3:
                    net = self.net_of(registrar)
                    # Each LAN registrar counts independently, exempt from
                    # public-/24 grouping. Empty replies never create q.
                    self.mix_nets.add(("lan", registrar) if net == -1 else ("net", net))
            if not q[1]:
                q[2] = now
            if not self.waiting:
                self.first_queued, self.waiting = now, True
            q[1].append(n)
        return self.mix(False, now)

    def release(self, now):
        out = self.mix(True, now)
        handed = self.handed
        self.reset()
        return out, handed

    def mix(self, everything, now):
        waited = lambda since: now - since >= self.mix_wait
        sources = len(self.mix_nets) if self.m3 else len(self.queues)
        if not everything and sources < self.mix_registrars and not (self.waiting and waited(self.first_queued)):
            return []
        out = []
        while True:
            rnd = [q for q in self.queues if q[1]]
            if not rnd or (not everything and len(rnd) == 1 and not waited(rnd[0][2])):
                return out
            for q in rnd:
                n = q[1].pop(0)
                if n in self.handed:
                    continue
                self.handed[n] = q[0]
                out.append(n)


class ForkSearcherState(SearcherState):
    """topicSearch of v5_topic.go (runLoop, run, nextPass, passGap): a new
    ForkSearch every pass started at the carried active bucket and seeded
    with the shuffled DHT table, one query in flight, asked and failed sets
    and the result filter shared across passes. The consumer reads every
    result at once."""

    def __init__(self, w, node, topic, target, stop_at):
        super().__init__(w, node, topic, target, stop_at)
        fc = self.fc = w.fork
        self.asked_set, self.failed_set = AskedSet(fc.ad_lifetime), AskedSet(fc.ad_lifetime)
        self.filter = SearchFilter(node, fc.ad_lifetime, fc.mix_registrars, fc.mix_wait, fc.filter_limit,
                                   m3=w.m3, net_of=w.registrar_net)
        self.retried = False
        self.nresults = 0
        self.handed = {}
        self.failures = 0
        self.first_honest = None
        self.passlog = []         # per pass: (start, end, queries, failures, end reason, start bucket)
        self.pass_q = self.pass_f = self.pass_sybil_q = 0
        self.ledger = SubnetLedger(fc.ad_lifetime, w.w1_limit) if w.w1 else None
        self.admission_ledger = AdmissionLedger(fc.ad_lifetime, w.w1p_limit) if w.w1p else None
        self.attack_pinned = False
        self.study_passes = []
        self.first_mix = None

    def note_mix(self, results, flush=False):
        if results and self.first_mix is None:
            self.first_mix = dict(time=self.w.now - self.t0, results=len(results),
                                  sybil=sum(self.w.is_sybil(r) for r in results), flush=flush)

    def new_pass(self):
        w = self.w
        st = self.st = ForkSearch(self.fc, w.pop, self.tid, self.node, self.rng, self.asked_set, self.failed_set,
                                       mint=w.mint, ledger=self.ledger, net_of=w.public_net if w.w1 or w.w1p or w.m2 else None,
                                       m2=w.m2, m2_nets=w.m2_nets, admission_ledger=self.admission_ledger)
        st.set_active(self.active)
        seeds = w.attacker.seeds(self) if w.attacker is not None else list(self.seeds)
        self.rng.shuffle(seeds)
        st.add_nodes(None, seeds, w.now)
        self.passes += 1
        self.pass_start = w.now
        self.nresults = 0
        self.pass_q = self.pass_f = self.pass_sybil_q = 0
        if not self.active_trace or self.active_trace[-1][1] != st.active:
            self.active_trace.append((w.now - self.t0, st.active))

    def drain(self):
        """The consumer reads the buffered results: each one enters the
        result filter."""
        st, w = self.st, self.w
        for r in st.buffer:
            self.nresults += 1
            self.filter.add(r, w.now)
            src = self.handed.get(r, self.filter.handed.get(r))
            if src is not None:
                self.result_src[r] = src
            if r >= 0 and not self.w.sybil[r] and self.first_honest is None:
                self.first_honest = w.now - self.t0
            self.deliver([r])
        st.buffer.clear()

    def reached(self):
        return self.target and len(self.found) >= self.target

    def step(self):
        w = self.w
        if self.done or w.now >= self.stop_at or self.reached():
            self.done = True; self.end = w.now; return
        if self.st is None:
            self.new_pass()
        st = self.st
        self.drain()
        if self.reached():
            self.done = True; self.end = w.now; return
        dst = None if st.is_done() else st.query_target(w.now)
        if dst is None:
            held, self.handed = self.filter.release(w.now)
            self.note_mix(held, flush=True)
            st.add_results(held)
            self.drain()
            self.handed = {}
            self.end_pass()
            return
        dists = st.buckets_with_free_space()
        self.queries += 1
        self.pass_q += 1
        w.q_sent[self.node] += 1
        if w.is_sybil(dst):
            self.sybil_queries += 1
            self.pass_sybil_q += 1
        if self.ledger is not None:
            self.ledger.record(st.net_of(dst), w.now)
        delay, results, aux, failed = w.fork_query(dst, self.topic, self.tid, dists, self)
        w.at(w.now + delay, "sreply", self.node, dst, [int(r) for r in results], aux, failed)

    def reply(self, dst, results, aux, failed):
        w, st = self.w, self.st
        before = st.active
        if failed:
            self.failures += 1
            self.pass_f += 1
        taken = self.filter.take(dst, results, w.now)
        self.note_mix(taken)
        if failed and not results and not aux:
            st.handle_error(dst, w.now)
            st.add_results(taken)
        else:
            st.add_reply(dst, taken, len(results), len(results), w.now, partial=failed)
            st.add_nodes(dst, aux, w.now)
        if st.active != before:
            self.active_trace.append((w.now - self.t0, st.active))
        w.at(w.now, "sstep", self.node)

    def end_pass(self):
        """nextPass and passGap of v5_topic.go."""
        w, st, fc = self.w, self.st, self.fc
        reason = "cap" if st.replies >= fc.pass_queries else ("active" if st.active_done() else "dry")
        nxt = st.next_pass_bucket()
        retry = nxt == self.active and st.active_failed()
        if retry and self.retried:
            st.set_active(nxt + 1)
            nxt, retry = st.active, False
        self.retried = retry
        moved = nxt != self.active and st.ads > 0
        self.passlog.append((self.pass_start - self.t0, w.now - self.t0, self.pass_q, self.pass_f, reason, self.active))
        self.study_passes.append(dict(start=self.pass_start - self.t0, end=w.now - self.t0,
                                      queries=self.pass_q, sybil_queries=self.pass_sybil_q,
                                      honest_queries=self.pass_q - self.pass_sybil_q,
                                      timeouts=self.pass_f, complete=True, start_bucket=self.active,
                                      end_bucket=st.active))
        self.active = nxt
        if self.nresults > 0 or moved:
            self.idle_passes = 0
            gap = PASS_MIN_S
        else:
            self.idle_passes += 1
            gap = fc.pass_backoff if self.idle_passes >= 30 else min(fc.pass_backoff, PASS_MIN_S * (2 ** self.idle_passes))
        self.st = None
        w.at(max(w.now, self.pass_start + gap), "sstep", self.node)


CLASSES = (("1000+", 1000, 10**9), ("100-999", 100, 999), ("10-99", 10, 99), ("<10", 0, 9))


def size_class(n):
    for name, lo, hi in CLASSES:
        if lo <= n <= hi:
            return name
    return "<10"


def fork_row(s, dur, depth):
    """Per-search pass and active-bucket figures of a ForkSearcherState."""
    dwell = [0.0] * depth
    tr = s.active_trace + [(dur, None)]
    for (t, b), (t2, _) in zip(tr, tr[1:]):
        dwell[b] += max(0.0, min(t2, dur) - t)
    tot = sum(dwell) or 1.0
    passes = s.passlog
    return dict(first_honest=s.first_honest, failures=s.failures,
                q_per_pass=(s.queries / s.passes) if s.passes else None,
                pass_q=[p[2] for p in passes], pass_len=[round(p[1] - p[0], 3) for p in passes],
                pass_end={k: sum(1 for p in passes if p[4] == k) for k in ("cap", "active", "dry")},
                active_mean=sum(i * d for i, d in enumerate(dwell)) / tot, active_max=max(b for _, b in s.active_trace) if s.active_trace else 0,
                active_changes=max(0, len(s.active_trace) - 1), dwell=[round(d / tot, 4) for d in dwell])


def fork_class(rs, depth):
    def stat(vals):
        vals = [v for v in vals if v is not None]
        if not vals:
            return None
        return dict(median=float(np.median(vals)), mean=float(np.mean(vals)), p10=float(np.percentile(vals, 10)), p90=float(np.percentile(vals, 90)))
    f = [r["fork"] for r in rs]
    ends = {k: sum(x["pass_end"][k] for x in f) for k in ("cap", "active", "dry")}
    npass = sum(ends.values()) or 1
    settled = {}
    for r in rs:
        settled[r["settled"]] = settled.get(r["settled"], 0) + 1
    return dict(searchers=len(rs), t_first=stat([r["first1"]["t"] if r["first1"] else None for r in rs]),
                t_first_honest=stat([x["first_honest"] for x in f]),
                t50=stat([r["t50"] for r in rs]), t90=stat([r["t90"] for r in rs]),
                reached50=float(np.mean([r["t50"] is not None for r in rs])), reached90=float(np.mean([r["t90"] is not None for r in rs])),
                recall=stat([r["recall"] for r in rs]), queries=stat([r["queries"] for r in rs]), passes=stat([r["passes"] for r in rs]),
                q_per_pass=stat([x["q_per_pass"] for x in f]),
                pass_queries=stat([q for x in f for q in x["pass_q"]]), pass_len_s=stat([q for x in f for q in x["pass_len"]]),
                pass_end={k: v / npass for k, v in ends.items()}, failures=int(sum(x["failures"] for x in f)),
                settled={int(k): v for k, v in sorted(settled.items())}, active_mean=stat([x["active_mean"] for x in f]),
                active_max=stat([x["active_max"] for x in f]), active_changes=stat([x["active_changes"] for x in f]),
                dwell=[round(float(np.mean([x["dwell"][i] for x in f])), 4) for i in range(depth)])


SAMPLE_FIELDS = ("ads", "attacker_ads", "target_ads", "target_attacker_ads", "regtopic", "topicquery", "sybil_regtopic")
HOT_SET = 20


def dist(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return dict(median=None, p90=None, n=0)
    return dict(median=float(np.median(vals)), p90=float(np.percentile(vals, 90)), n=len(vals))


def wait_tail(quotes):
    q = np.frombuffer(quotes, dtype=np.float64) if len(quotes) else np.zeros(0)
    if not len(q):
        return dict(p99=None, max=None, ge_1350=0, ge_86400=0)
    return dict(p99=float(np.percentile(q, 99)), max=float(q.max()), ge_1350=int((q >= 1350).sum()), ge_86400=int((q >= 86400).sum()))


def median_all(vals):
    """Median with never-reached (None) counted as +inf; None if that median
    is never reached."""
    if not vals:
        return None
    m = float(np.median([math.inf if v is None else v for v in vals]))
    return m if math.isfinite(m) else None


def search_extras(rs):
    """Honest-only completion times and sybil exposure of a group of searches.
    honest_tNN medians are conditional on reaching the fraction; reachedNN
    is the share that did, honest_tNN_all counts the rest as never."""
    med = lambda key: (float(np.median([r[key] for r in rs if r[key] is not None])) if any(r[key] is not None for r in rs) else None)
    mean = lambda key: (float(np.mean([r[key] for r in rs if r[key] is not None])) if any(r[key] is not None for r in rs) else None)
    out = dict(honest_t50=med("honest_t50"), honest_t90=med("honest_t90"), honest_t99=med("honest_t99"),
               first16_sybil_mean=mean("first16_sybil"), eclipsed16=float(np.mean([r["eclipsed16"] for r in rs])),
               eclipsed_mean=float(np.mean([r["eclipsed"] for r in rs])),
               without_honest=float(np.mean([r["honest_found"] == 0 for r in rs])))
    for f in ("50", "90", "99"):
        vals = [r["honest_t" + f] for r in rs]
        out["honest_reached" + f] = sum(v is not None for v in vals)
        out["honest_reached" + f + "_frac"] = out["honest_reached" + f] / len(rs) if rs else None
        out["honest_t" + f + "_all"] = median_all(vals)
    return out


def share(num, den):
    return float(num / den) if den else None


def per_advertiser(pop, fanout, adv_buckets):
    """Live registrations and distinct registration buckets of every honest
    registrant, zero for one with no live ad."""
    regs = np.flatnonzero(pop.registrant)
    keys = [(int(i), int(pop.topics[i])) for i in regs]
    fo = np.array([fanout.get(key, 0) for key in keys], dtype=np.int64)
    nb = np.array([len(adv_buckets.get(key, ())) for key in keys], dtype=np.int64)
    return fo, nb


def hot_stats(w, idx, sp, ph, cache):
    """Search-phase averages over the sampled snapshots and REGTOPIC rates
    over the exact phase window for the registrars idx."""
    if not sp:
        return dict(registrars=[int(r) for r in idx])
    col = lambda f: np.stack([w.samples[j][f][idx] for j in sp]).astype(np.float64)
    ads, att, tads, tatt = col(0), col(1), col(2), col(3)
    out = dict(registrars=[int(r) for r in idx], ads=float(ads.mean()), fill=float(ads.mean() / cache),
               target_ads=float(tads.mean()), honest_target_ads=float((tads - tatt).mean()),
               target_sybil_share=share(tatt.sum(), tads.sum()), cache_sybil_share=share(att.sum(), ads.sum()))
    if ph is not None:
        rcv, syb, dur = ph
        out.update(honest_regtopic_rate=float((rcv[idx].sum() - syb[idx].sum()) / dur), sybil_regtopic_rate=float(syb[idx].sum() / dur))
    return out


def capacity_metrics(w, a, rows, held, sizes, fanout, adv_buckets, bucket_ads, search_at, end):
    """Capacity-study metrics: give-ups, bucket spread, served sybil share,
    sampled time averages, hot sets, REGTOPIC rates and the flood."""
    k, target, flood, pop = w.k, w.target, w.flood, w.pop
    out = {}
    out["giveups"] = {cls: {why: int(w.giveups[(cls, why)]) for why in ("single", "cumulative")} for cls in ("honest", "sybil")}
    out["giveups"]["honest_per_advertiser"] = share(sum(out["giveups"]["honest"].values()), int(pop.registrant.sum()))
    fo, nb = per_advertiser(pop, fanout, adv_buckets)
    if not len(nb):
        fo = nb = np.zeros(1)
    out["fanout_buckets"] = dict(distinct_median=float(np.median(nb)), distinct_p10=float(np.percentile(nb, 10)),
                                 without_live_ad=int((fo == 0).sum()), live_advertisers=int((fo > 0).sum()),
                                 by_class={c: dict(ads=int(v.sum()), share=[round(float(x) / v.sum(), 5) if v.sum() else 0.0 for x in v])
                                           for c, v in bucket_ads.items() if v.sum()})
    out["served"] = {str(t): dict(queries=sv[0], results=sv[1], sybil_results=sv[2], sybil_share_served=share(sv[2], sv[1]),
                                  target_share_mean=share(sv[3], sv[5]), cache_share_mean=share(sv[4], sv[0]))
                     for t, sv in sorted(w.served.items()) if t < k}
    rs = w.reg_stats
    def regrow(mask):
        sent, admitted = int(rs["sent"][mask].sum()), int(rs["admitted"][mask].sum())
        return dict(sent=sent, admitted=admitted, existing=int(rs["existing"][mask].sum()),
                    giveups=int(rs["giveups"][mask].sum()), attempts_per_admission=share(sent, admitted))
    out["reg_stats"] = dict(all=regrow(np.ones(k, dtype=bool)), target=regrow(np.arange(k) == target),
                            by_class={c: regrow(np.array([size_class(int(x)) == c for x in sizes])) for c, _, _ in CLASSES
                                      if any(size_class(int(x)) == c for x in sizes)})
    tt_sum = Counter()
    for tt in w.ttab:
        tt_sum.update(tt.ticket_timings)
    out["ticket_timings"] = {key: int(v) for key, v in sorted(tt_sum.items())}
    out["ticket_timings_advertiser"] = {cls: {key: int(v) for key, v in sorted(c.items())} for cls, c in w.adv_timings.items()}
    # REGTOPIC rates over the exact search phase, from the boundary snapshots
    ph = None
    if "start" in w.phase and "end" in w.phase:
        p0, p1 = w.phase["start"], w.phase["end"]
        dur = p1["t"] - p0["t"]
        rcv, syb = p1["reg_rcv"] - p0["reg_rcv"], p1["flood_sent"] - p0["flood_sent"]
        tsent = p1["sent"] - p0["sent"]
        ph = (rcv, syb, dur)
        out["regtopic_rate"] = dict(window_s=dur, honest=float((rcv.sum() - syb.sum()) / dur), sybil=float(syb.sum() / dur),
                                    honest_target=float(tsent[target] / dur))
    else:
        out["regtopic_rate"] = dict(window_s=0.0, honest=None, sybil=None, honest_target=None)
    # samples at search_at + j * period, j >= 1, up to the end
    sp = [j for j, r in enumerate(w.sample_rows) if search_at + 1e-9 < r["t"] <= end + 1e-9]
    smp = dict(period=w.sample_period, n=len(w.sample_rows), search_phase_n=len(sp))
    hot = np.argsort(-w.target_queries, kind="stable")[:HOT_SET]
    if sp:
        ads = np.stack([w.samples[j][0] for j in sp]).astype(np.float64)
        att = np.stack([w.samples[j][1] for j in sp]).astype(np.float64)
        tads = np.stack([w.samples[j][2] for j in sp]).astype(np.float64)
        tatt = np.stack([w.samples[j][3] for j in sp]).astype(np.float64)
        avg_ads = ads.mean(axis=0)
        g = lambda key: float(np.mean([w.sample_rows[j][key] for j in sp]))
        smp["search_phase"] = dict(honest_ads=g("honest_ads"), sybil_ads=g("sybil_ads"),
                                   sybil_ad_share=share(att.sum(), ads.sum()), target_honest_live=g("target_honest_live"),
                                   honest_target_ads=float((tads - tatt).sum(axis=1).mean()),
                                   target_sybil_share=share(tatt.sum(), tads.sum()),
                                   ads_held=dict(median=float(np.median(avg_ads)), p99=float(np.percentile(avg_ads, 99)), max=float(avg_ads.max())))
        hot_load = np.argsort(-avg_ads, kind="stable")[:HOT_SET]
        hot_target = np.argsort(-tads.mean(axis=0), kind="stable")[:HOT_SET]
    else:
        hot_load = hot_target = np.array([], dtype=np.int64)
    smp["hot_set"] = dict(hot_stats(w, hot, sp, ph, a.cache), target_queries=[int(w.target_queries[r]) for r in hot])
    smp["hot_load"] = hot_stats(w, hot_load, sp, ph, a.cache)
    smp["hot_target"] = hot_stats(w, hot_target, sp, ph, a.cache)
    if ph is not None:
        for name in ("hot_set", "hot_load", "hot_target"):
            out["regtopic_rate"][name + "_honest"] = smp[name].get("honest_regtopic_rate")
            out["regtopic_rate"][name + "_sybil"] = smp[name].get("sybil_regtopic_rate")
    end_t = [w.ttab[int(r)] for r in hot]
    tgt_d = [tt.by_topic.get(target, {}) for tt in end_t]
    smp["hot_set"]["end"] = dict(ads=float(np.mean([len(tt.ads) for tt in end_t])) if end_t else None,
                                 target_sybil_share=share(sum(tt.attacker_topics[target] for tt in end_t), sum(len(d) for d in tgt_d)),
                                 cache_sybil_share=share(sum(tt.attacker_ads for tt in end_t), sum(len(tt.ads) for tt in end_t)))
    live = set()
    for tt in w.ttab:
        live.update(adv for adv in tt.by_topic.get(target, ()) if adv >= 0)
    smp["target_honest_live_end"] = len(live)
    smp["target_honest_advertisers"] = int(sizes[target])
    out["samples"] = smp
    trs = [r for r in rows if r["topic"] == target]
    med = lambda key: (float(np.median([r[key] for r in trs if r[key] is not None])) if any(r[key] is not None for r in trs) else None)
    shares = [r["sybil_share"] for r in trs if r["sybil_share"] is not None]
    out["target_search"] = dict(topic=target, searchers=len(trs), recall_median=med("recall"),
                                sybil_share_mean=float(np.mean(shares)) if shares else None,
                                **(search_extras(trs) if trs else {}))
    if flood is not None:
        sent = int(flood.sent.sum())
        hon_t = int(w.reg_stats["sent"][target])
        out["flood"] = dict(mode=flood.mode, target=target, sybils=flood.sybils, chains=len(flood.chains),
                            topics=len(flood.topics), nets=flood.nets, layout=flood.layout, near=flood.near,
                            give_up=flood.give_up, budget=flood.budget, burst=flood.burst,
                            requests=sent, rate=sent / end, sybil_admissions=flood.admitted,
                            ads_end=int(sum(tt.attacker_ads for tt in w.ttab)),
                            ratio_to_honest_target=share(sent, hon_t), honest_target_requests=hon_t,
                            parked_max=flood.parked_max, rate_search_phase=out["regtopic_rate"]["sybil"])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=10000)
    ap.add_argument("--topics", type=int, default=100)
    ap.add_argument("--model", default="")
    ap.add_argument("--zipf", type=float, default=1.0)
    ap.add_argument("--register-frac", type=float, default=1.0, help="fraction of nodes that register their topic; every node searches its topic")
    ap.add_argument("--all-register", action="store_true", help="kept for old command lines; every node registers by default")
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
    ap.add_argument("--sybil-nets", type=int, default=0, help="the sybils share this many /24 prefixes and the IP limits of the tables apply; 0 = every sybil has its own /24")
    ap.add_argument("--sybil-fresh", action="store_true", help="a sybil registrar answers every query with new sybil ids")
    ap.add_argument("--mix-registrars", type=int, default=0, help="result mixer: hand out results one registrar at a time, and only while this many registrars have results waiting; the end of a pass hands out the rest; 0 = off")
    ap.add_argument("--registrar-limit", type=int, default=0, help="results a search takes from one registrar per ad lifetime, the rest are held until the pass ends; 0 = off")
    ap.add_argument("--fork-faithful", action="store_true", help="search as the fork does (ForkSearcherState) with its values: search depth 18, bucket 8, floor 8, aux radius 1, 32 queries a pass, mix 5 registrars or 5 s, back-off 60 s, timeout 0.7 s; registration and ads unchanged")
    ap.add_argument("--fork-mix-registrars", type=int, default=None, help="with --fork-faithful: number of registrars the result mixer waits for (fork default 5)")
    ap.add_argument("--search-bucket-ip-limit", type=int, default=1, help="fork search: maximum registrars per public /24 in each bucket (default 1); LAN exempt")
    ap.add_argument("--attack", choices=("none", "A1", "A2", "A3", "A4", "A5", "A6"), default="none")
    ap.add_argument("--attack-nets", type=int, default=0)
    ap.add_argument("--w1", action="store_true", help="block /24 re-admission after w1-limit queries per ad lifetime")
    ap.add_argument("--w1-limit", type=int, default=1)
    ap.add_argument("--w1p", action="store_true", help="cap distinct admitted IDs per /24 while preserving same-ID re-admission")
    ap.add_argument("--w1p-limit", type=int, default=1)
    ap.add_argument("--honest-nets", default="", help="assign public /24s from crawl[:path], conditional on topic/chain")
    ap.add_argument("--m2", action="store_true", help="require diverse public /24s in the yield window before steering")
    ap.add_argument("--m2-nets", type=int, default=2)
    ap.add_argument("--m3", action="store_true", help="count distinct public /24s toward the mix release threshold")
    ap.add_argument("--silent-frac", type=float, default=0, help="fork searcher: fraction of nodes that never answer a topic query, which then fails after the response timeout")
    ap.add_argument("--bootnodes", type=int, default=0)
    ap.add_argument("--no-spawn-bias", action="store_true", help="uniform DHT tables instead of tables filled from the nodes alive at start")
    ap.add_argument("--reg-bucket-size", type=int, default=REG_BUCKET_SIZE, help="active registrars per registration bucket (Go RegBucketSize is 10)")
    ap.add_argument("--reg-standby", type=int, default=REG_STANDBY, help="standby registrars per registration bucket")
    ap.add_argument("--sample-period", type=float, default=60, help="seconds between per-registrar samples")
    ap.add_argument("--no-ip-score", action="store_true", help="admission IP score off: the tree is still fed, the score is 0")
    ap.add_argument("--flood", choices=("none", "target", "occupy", "occupy-partitioned"), default="none",
                    help="sybil advertisers flood the target topic, or occupy caches with fake topics: every sybil "
                         "registers all of them (occupy, S x T chains) or one each (occupy-partitioned, max(S, T) chains)")
    ap.add_argument("--flood-ratio", type=float, default=1.0, help="sybil ids per honest advertiser of the target topic")
    ap.add_argument("--flood-ids", type=int, default=0, help="number of sybil ids; overrides --flood-ratio")
    ap.add_argument("--flood-target", type=int, default=-1, help="target topic; default the largest")
    ap.add_argument("--flood-topics", type=int, default=300, help="fake topics of --flood occupy")
    ap.add_argument("--flood-near", dest="flood_spread", action="store_false", default=False, help="fake topic hashes share the target's top 24 bits (default)")
    ap.add_argument("--flood-spread", dest="flood_spread", action="store_true", default=False, help="fake topic hashes are uniform")
    ap.add_argument("--flood-nets", type=int, default=4, help="sybil public /24s; 0 = a distinct address per sybil")
    ap.add_argument("--flood-layout", choices=("spread", "clustered"), default="spread")
    ap.add_argument("--flood-give-up", action="store_true", help="sybils drop a registrar over the 1.5 E budget, as honest nodes do; default they wait out every quote")
    ap.add_argument("--flood-budget", type=float, default=0, help="sybil REGTOPIC/s over all sybils; 0 = unlimited")
    ap.add_argument("--flood-burst", type=float, default=1, help="token bucket size of --flood-budget")
    ap.add_argument("--flood-start-window", type=float, default=-1, help="sybil starts spread over this (s); default --start-window")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    fork = None
    if a.rtt <= 0:
        ap.error("--rtt must be positive")
    if a.reg_bucket_size < 1 or a.reg_standby < 0 or a.sample_period <= 0:
        ap.error("--reg-bucket-size and --sample-period must be positive")
    if a.flood != "none":
        if not a.fork_faithful or not a.honest_nets:
            ap.error("--flood needs --fork-faithful and --honest-nets")
        if a.attack != "none" or a.sybil_frac or a.sybil_nets or a.sybil_fresh:
            ap.error("--flood cannot combine --attack or legacy --sybil-* flags")
        if a.flood_nets < 0 or a.flood_ratio < 0 or a.flood_ids < 0 or a.flood_topics < 1 or a.flood_budget < 0:
            ap.error("--flood-* values must not be negative")
    elif a.flood_budget or a.flood_give_up or a.flood_ids or a.flood_target >= 0:
        ap.error("--flood-budget, --flood-give-up, --flood-ids and --flood-target need --flood")
    if a.search_bucket_ip_limit < 1:
        ap.error("--search-bucket-ip-limit must be positive")
    if a.search_bucket_ip_limit != 1 and not a.fork_faithful:
        ap.error("--search-bucket-ip-limit needs --fork-faithful")
    if a.fork_mix_registrars is not None and not a.fork_faithful:
        ap.error("--fork-mix-registrars needs --fork-faithful")
    if a.fork_mix_registrars is not None and a.fork_mix_registrars < 1:
        ap.error("--fork-mix-registrars must be positive")
    if a.fork_faithful:
        for k in ("registrar_limit", "min_sources", "min_buckets", "idle_replies"):
            if getattr(a, k):
                ap.error("--%s is not part of the fork's search" % k.replace("_", "-"))
        fork_kw = {} if a.fork_mix_registrars is None else {"mix_registrars": a.fork_mix_registrars}
        fork = ForkConfig(ip_limit=a.search_bucket_ip_limit, topic_nodes_limit=a.topic_nodes_limit, ad_lifetime=a.ad_lifetime, **fork_kw)
        for k, v in (("floor", fork.yield_floor), ("aux_radius", fork.aux_radius), ("bucket_size", fork.bucket_size),
                     ("mix_registrars", fork.mix_registrars), ("pass_backoff", fork.pass_backoff)):
            if getattr(a, k) not in (ap.get_default(k), v):
                ap.error("--fork-faithful sets --%s %s" % (k.replace("_", "-"), v))
            setattr(a, k, v)
    elif a.silent_frac:
        ap.error("--silent-frac needs --fork-faithful")
    if (a.attack != "none" or a.w1 or a.w1p or a.m2 or a.m3) and fork is None:
        ap.error("--attack, --w1, --w1p, --m2 and --m3 need --fork-faithful")
    if a.attack != "none" and a.attack_nets < 1:
        ap.error("--attack requires --attack-nets >= 1")
    if a.attack == "none" and a.attack_nets:
        ap.error("--attack-nets requires an attack")
    if a.m2_nets < 1:
        ap.error("--m2-nets must be positive")
    if a.w1p_limit < 1:
        ap.error("--w1p-limit must be positive")
    if a.w1_limit < 1:
        ap.error("--w1-limit must be positive")
    if a.attack != "none" and (a.sybil_frac or a.sybil_nets or a.sybil_fresh):
        ap.error("attack runs isolate the search channel; do not combine legacy --sybil-* flags")
    if a.honest_nets and (fork is None or a.sybil_frac or a.sybil_nets or a.sybil_fresh):
        ap.error("--honest-nets needs --fork-faithful and cannot combine legacy --sybil-* flags")
    os.makedirs(a.out, exist_ok=True)
    t0 = time.time()
    pop = Population(a.nodes, seed=a.seed, bootnodes=a.bootnodes, spawn_bias=not a.no_spawn_bias)
    shares = shares_from_model(a.model, a.topics) if a.model else zipf_shares(a.topics, a.zipf)
    pop.assign_topics(shares, a.register_frac, a.all_register)
    honest_nets = assignment = None
    if a.honest_nets:
        from .honest_nets import load_crawl, topic_chains, assign_nets
        data = load_crawl(a.honest_nets)
        chains, mapping_rule = topic_chains(a.model, len(shares), data)
        honest_nets, assignment = assign_nets(pop, data, chains, a.seed,
                                            whole_groups=mapping_rule.startswith("population-rank"))
        assignment["mapping_rule"] = mapping_rule
        json.dump(assignment, open(os.path.join(a.out, "honest-net-assignment.json"), "w"), indent=1)
        np.save(os.path.join(a.out, "honest_nets.npy"), honest_nets)
    cfg = WorldConfig(cache=a.cache, depth=a.depth, bucket_size=a.bucket_size, topic_nodes_limit=a.topic_nodes_limit,
                      aux_limit=a.aux_limit, yield_floor=a.floor, aux_radius=a.aux_radius, ad_lifetime=a.ad_lifetime, rtt=a.rtt)
    sizes0 = np.bincount(pop.topics[pop.registrant], minlength=len(shares))
    target = a.flood_target if a.flood_target >= 0 else int(np.argmax(sizes0))
    flood = None
    if a.flood != "none":
        from .flood import Flood
        if not 0 <= target < len(shares):
            ap.error("--flood-target out of range")
        nsyb = a.flood_ids or int(round(a.flood_ratio * int(sizes0[target])))
        if nsyb < 1:
            ap.error("--flood needs at least one sybil id")
        flood = Flood(a.flood, nsyb, pop, target, a.seed, topics=a.flood_topics, near=not a.flood_spread,
                      nets=a.flood_nets, layout=a.flood_layout, give_up=a.flood_give_up, budget=a.flood_budget,
                      burst=a.flood_burst, start_window=a.flood_start_window if a.flood_start_window >= 0 else a.start_window,
                      k=len(shares))
    sybil = np.random.default_rng(a.seed + 9).random(pop.n) < a.sybil_frac
    w = World(pop, cfg, a.rtt, a.seed, backoff=a.pass_backoff, sybil=sybil, idle_replies=a.idle_replies, min_sources=a.min_sources, min_buckets=a.min_buckets,
              registrar_limit=a.registrar_limit, sybil_fresh=a.sybil_fresh, sybil_nets=a.sybil_nets, mix_registrars=a.mix_registrars,
              fork=fork, silent=(np.random.default_rng(a.seed + 13).random(pop.n) < a.silent_frac) if a.silent_frac else None,
              attack=a.attack, attack_nets=a.attack_nets, w1=a.w1, w1_limit=a.w1_limit, w1p=a.w1p, w1p_limit=a.w1p_limit,
              m2=a.m2, m2_nets=a.m2_nets, m3=a.m3, honest_nets=honest_nets,
              reg_bucket_size=a.reg_bucket_size, reg_standby=a.reg_standby, ip_score=not a.no_ip_score,
              flood=flood, target=target)
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
    if flood is not None:
        for (node, topic), t in zip(flood.chains, flood.starts):
            w.at(float(t), "start_reg", node, topic)
    end = search_at + a.search_time
    w.schedule_phases(search_at, end, a.sample_period)
    print(f"{pop.n} nodes, {k} topics, {int(pop.registrant.sum())} registrants, {len(searchers)} searchers; floor {a.floor}; run to {end:.0f} s", flush=True)
    if fork is not None:
        print("fork-faithful search: " + json.dumps(vars(fork)), flush=True)
    last = [time.time()]
    def prog(now, n):
        print(f"  t={now:.0f}s events={n/1e6:.1f}M wall={time.time()-t0:.0f}s", flush=True)
    nev = w.run(end, prog)
    print(f"done: {nev/1e6:.1f}M events in {time.time()-t0:.0f}s", flush=True)

    # --- results
    for tt in w.ttab:
        tt.purge(w.now)
    held = np.array([tt.count for tt in w.ttab])
    sizes = np.bincount(pop.topics[pop.registrant], minlength=k)
    fanout, adv_buckets = {}, {}
    bucket_ads = {name: np.zeros(cfg.depth, dtype=np.int64) for name, _, _ in CLASSES}
    for r, tt in enumerate(w.ttab):
        for t, d in tt.by_topic.items():
            if t >= k:
                continue
            bi = bucket_index(int(pop.topic_ids[t]), int(pop.ids[r]), cfg.depth)
            nh = 0
            for adv in d:
                if adv < 0:
                    continue
                nh += 1
                fanout[(adv, t)] = fanout.get((adv, t), 0) + 1
                adv_buckets.setdefault((adv, t), set()).add(bi)
            bucket_ads[size_class(int(sizes[t]))][bi] += nh
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
        nsyb = sum(1 for r in s.found if r < 0 or sybil[r])
        first = {}
        for T in (1, 16, 50):
            o = s.order[:T]
            if len(o) < T:
                first[T] = None
                continue
            per = {}
            for r, src, tm, q in o:
                per[src] = per.get(src, 0) + 1
            first[T] = dict(t=o[-1][2], q=o[-1][3], sybil=sum(1 for r, _, _, _ in o if r < 0 or sybil[r]) / T,
                            top=max(per.values()) / T, sources=len(per))
        times = honest_times(w, s)
        rows.append(dict(first1=first[1], first16=first[16], first50=first[50], results=len(s.found), sybil_queries=s.sybil_queries, sybil_found=nsyb, sybil_share=(nsyb / len(s.found)) if s.found else None, eclipsed=bool(s.found) and nsyb == len(s.found),
                         searcher=i, topic=s.topic, size=int(sizes[s.topic]), cls=size_class(int(sizes[s.topic])), queries=s.queries,
                         passes=s.passes, found=found, reach=len(reach), recall=found / len(reach) if reach else 1.0,
                         t50=at(0.5), t90=at(0.9), t99=at(0.99), settled=s.active_trace[-1][1] if s.active_trace else 0,
                         done=s.done, dur=(getattr(s, "end", w.now) - s.t0),
                         honest_t50=honest_reached(times, len(reach), 0.5), honest_t90=honest_reached(times, len(reach), 0.9),
                         honest_t99=honest_reached(times, len(reach), 0.99), honest_found=len(times),
                         first16_sybil=first[16]["sybil"] if first[16] else None,
                         eclipsed16=bool(first[16]) and first[16]["sybil"] == 1.0))
        if fork is not None:
            rows[-1]["fork"] = fork_row(s, rows[-1]["dur"], fork.search_depth)
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
                              t50=med("t50"), t90=med("t90"), t99=med("t99"), settled_median=med("settled"),
                              **search_extras(rs))
    honest_admit = {key: v for key, v in w.first_admit.items() if key[0] >= 0 and key[1] < k}
    adm = np.array(list(honest_admit.values())) if honest_admit else np.array([np.nan])
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
    fo = per_advertiser(pop, fanout, adv_buckets)[0]
    fo = fo if len(fo) else np.array([0])
    q = np.frombuffer(w.quotes, dtype=np.float64) if w.quotes else np.array([0.0])
    hon_ads = syb_ads = 0
    for r, tt in enumerate(w.ttab):
        if not sybil[r]:
            syb_ads += tt.attacker_ads
            hon_ads += len(tt.ads) - tt.attacker_ads
    # Omit inactive new options so attack-none output retains part 1's schema.
    config = {k: v for k, v in vars(a).items()
              if (a.fork_faithful or k not in ("fork_faithful", "silent_frac"))
              and (a.attack != "none" or a.w1 or a.w1p or a.m2 or a.m3 or a.honest_nets or k not in
                   ("attack", "attack_nets", "w1", "w1_limit", "m2", "m2_nets", "m3"))
              and (a.w1p or k not in ("w1p", "w1p_limit"))
              and (a.honest_nets or k != "honest_nets")
              and (a.search_bucket_ip_limit != 1 or k != "search_bucket_ip_limit")
              and (a.fork_mix_registrars is not None or k != "fork_mix_registrars")
              and (a.flood != "none" or not k.startswith("flood"))}
    summary = dict(config=config, events=nev, elapsed=time.time() - t0, sybil_frac=float(sybil.mean()),
                   sybil_ad_share=(syb_ads / (syb_ads + hon_ads)) if syb_ads + hon_ads else 0.0,
                   registrar_queries=dict(median=float(np.median(w.tq_rcv)), p99=float(np.percentile(w.tq_rcv, 99)), max=int(w.tq_rcv.max())),
                   registrar_regtopic=dict(median=float(np.median(w.reg_rcv)), p99=float(np.percentile(w.reg_rcv, 99)), max=int(w.reg_rcv.max())),
                   ads_held=dict(median=float(np.median(held)), p99=float(np.percentile(held, 99)), max=int(held.max()), total=int(held.sum())),
                   fanout=dict(median=float(np.median(fo)), p10=float(np.percentile(fo, 10)), registered=len(fanout), registrants=int(pop.registrant.sum())),
                   admission_s=dict(median=float(np.nanmedian(adm)), p90=float(np.nanpercentile(adm, 90)), n=len(honest_admit),
                                    admitted_frac=share(len(honest_admit), int(pop.registrant.sum())),
                                    median_all=median_all([honest_admit.get((int(i), int(pop.topics[i]))) for i in np.flatnonzero(pop.registrant)]),
                                    target=dict(dist([v for (adv, t), v in honest_admit.items() if t == target]),
                                                admitted_frac=share(sum(1 for (adv, t) in honest_admit if t == target), int(sizes[target]))),
                                    sybil=dist([v for (adv, t), v in w.first_admit.items() if adv < 0])),
                   quoted_wait_s=dict(median=float(np.median(q)), p90=float(np.percentile(q, 90)), n=len(w.quotes),
                                      **wait_tail(w.quotes), attacker=dict(dist(w.quotes_att), **wait_tail(w.quotes_att))),
                   regtopic_sent_per_registrant=dict(median=float(np.median(w.reg_sent[pop.registrant])), max=int(w.reg_sent.max())),
                   by_class=by_class, late_joiners=late_summary,
                   queries_sent=dict(total=int(w.q_sent.sum()), per_searcher_median=float(np.median(w.q_sent[list(w.searchers)])) if w.searchers else 0.0))
    summary.update(capacity_metrics(w, a, rows, held, sizes, fanout, adv_buckets, bucket_ads, search_at, end))
    if fork is not None:
        summary["fork_search"] = dict(config=vars(fork), by_class={c: fork_class([r for r in rows if r["cls"] == c], fork.search_depth)
                                                                  for c, _, _ in CLASSES if any(r["cls"] == c for r in rows)})
    if fork is not None:
        from .metrics import study_row, study_summary
        study_rows = [study_row(s, getattr(s, "end", w.now) - s.t0, fork.search_depth)
                      for s in w.searchers.values()]
        for row, metrics in zip(rows, study_rows):
            metrics["cls"] = row["cls"]
        json.dump(study_rows, open(os.path.join(a.out, "search_metrics.json"), "w"))
        study = study_summary(study_rows, a.attack, a.attack_nets, a.w1, a.m2, a.m3, w1p=a.w1p)
        study["by_class"] = {cls: study_summary([r for r in study_rows if r["cls"] == cls],
                                                a.attack, a.attack_nets, a.w1, a.m2, a.m3, w1p=a.w1p)
                             for cls, _, _ in CLASSES if any(r["cls"] == cls for r in study_rows)}
        json.dump(study, open(os.path.join(a.out, "metrics.json"), "w"), indent=1)
        if assignment is not None:
            from .honest_nets import search_observations
            json.dump(search_observations(w, rows, study_rows, assignment),
                      open(os.path.join(a.out, "honest-net-searches.json"), "w"))
        if a.attack != "none" or a.w1 or a.w1p or a.m2 or a.m3:
            for row, metrics in zip(rows, study_rows):
                row["study"] = metrics
            summary["study"] = study
    json.dump(summary, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    json.dump(rows, open(os.path.join(a.out, "searches.json"), "w"))
    np.save(os.path.join(a.out, "tq_rcv.npy"), w.tq_rcv); np.save(os.path.join(a.out, "reg_rcv.npy"), w.reg_rcv)
    if w.samples:
        np.savez_compressed(os.path.join(a.out, "samples.npz"), t=np.array([r["t"] for r in w.sample_rows]),
                            **{name: np.stack([row[j] for row in w.samples]) for j, name in enumerate(SAMPLE_FIELDS)},
                            **{"global_" + key: np.array([r[key] for r in w.sample_rows]) for key in w.sample_rows[0] if key != "t"})
    np.save(os.path.join(a.out, "spawn.npy"), pop.spawn); np.save(os.path.join(a.out, "held.npy"), held); np.save(os.path.join(a.out, "ids.npy"), pop.ids); np.save(os.path.join(a.out, "topic_ids.npy"), pop.topic_ids)
    print(json.dumps({k2: v for k2, v in summary.items() if k2 != "config"}, indent=1))


if __name__ == "__main__":
    main()
