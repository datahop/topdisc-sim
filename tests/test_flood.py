"""Sybil advertiser flood: registration only, IP pools, rate budget, metrics."""
import heapq
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from searchsim import world as world_mod  # noqa: E402
from searchsim.flood import FLOOD_BASE, Flood, fake_topic_ids, flood_ips  # noqa: E402
from searchsim.go_registrar import scored_ip  # noqa: E402
from searchsim.honest_nets import honest_reached, honest_times  # noqa: E402
from searchsim.population import Population, bucket_index, zipf_shares  # noqa: E402
from searchsim.world import RegTable, World, WorldConfig, search_extras, share  # noqa: E402

HERE = Path(__file__).resolve().parent
CRAWL = Path.home() / "workspace/datahop/runs/2026-09-18-crawl24h-aws/derived/gap10"
MODEL = HERE.parent / "models/crawl-2026-09-18.json"
needs_crawl = pytest.mark.skipif(not CRAWL.exists(), reason="crawl data absent")


class NoNegative(np.ndarray):
    """Fails on a negative scalar index, which numpy would silently wrap."""

    def __getitem__(self, i):
        assert not (isinstance(i, (int, np.integer)) and i < 0), i
        return super().__getitem__(i)

    def __setitem__(self, i, v):
        assert not (isinstance(i, (int, np.integer)) and i < 0), i
        super().__setitem__(i, v)


@pytest.fixture(scope="module")
def flood_run(tmp_path_factory):
    """F1: a 400-node flood world with every entry point to tables, aux and
    the search guarded against sybil ids."""
    out = tmp_path_factory.mktemp("flood")
    mp = pytest.MonkeyPatch()
    seen = dict(world=None, aux=0, query=0, add=0)
    orig_init, orig_aux, orig_query, orig_add = World.__init__, World.aux, World.fork_query, RegTable.add_nodes

    def init(self, *a, **kw):
        orig_init(self, *a, **kw)
        seen["world"] = self
        self.sybil = self.sybil.view(NoNegative)
        self.reg_sent = self.reg_sent.view(NoNegative)
        self.pop.topics = self.pop.topics.view(NoNegative)

    def aux(self, registrar, tid, dists):
        assert registrar >= 0
        res = orig_aux(self, registrar, tid, dists)
        assert all(n >= 0 for n in res)
        seen["aux"] += 1
        return res

    def fork_query(self, dst, *a, **kw):
        assert dst >= 0
        seen["query"] += 1
        return orig_query(self, dst, *a, **kw)

    def add_nodes(self, w, tid, src, nodes):
        assert (src is None or src >= 0) and all(n >= 0 for n in nodes)
        seen["add"] += 1
        return orig_add(self, w, tid, src, nodes)

    mp.setattr(World, "__init__", init)
    mp.setattr(World, "aux", aux)
    mp.setattr(World, "fork_query", fork_query)
    mp.setattr(RegTable, "add_nodes", add_nodes)
    mp.setattr(sys, "argv", ["world", "--nodes", "400", "--topics", "3", "--model", str(MODEL), "--fork-faithful",
                             "--honest-nets", "crawl", "--depth", "18", "--reg-bucket-size", "10", "--start-window", "100",
                             "--register-wait", "300", "--search-time", "120", "--search-frac", "0.2", "--seed", "2",
                             "--flood", "target", "--flood-ratio", "1", "--flood-nets", "4", "--out", str(out)])
    try:
        world_mod.main()
    finally:
        mp.undo()
    return seen, json.loads((out / "summary.json").read_text()), out


@needs_crawl
def test_f1_sybils_register_never_serve_or_enter_tables(flood_run):
    seen, s, _ = flood_run
    w = seen["world"]
    assert seen["aux"] and seen["query"] and seen["add"]
    assert any(adv < 0 for tt in w.ttab for (adv, _) in tt.ads)
    assert all(i >= 0 for i in w._tables)
    assert all(n >= 0 for s_ in w.searchers.values() for n in s_.seeds)
    assert int(w.reg_sent.sum()) == int(w.reg_rcv.sum() - w.flood.sent.sum())
    assert s["flood"]["requests"] == int(w.flood.sent.sum()) > 0
    assert sum(tt.requests for tt in w.ttab) == sum(tt.replies for tt in w.ttab) == int(w.reg_rcv.sum())


def test_f2_near_fake_topics_share_the_target_buckets():
    pop = Population(2000, seed=1)
    pop.assign_topics(zipf_shares(2, 1.0), 1.0, True)
    target = int(pop.topic_ids[0])
    rng = np.random.default_rng(1)
    near, spread = fake_topic_ids(target, 50, True, rng), fake_topic_ids(target, 50, False, rng)
    ids = [int(x) for x in pop.ids]
    for f in near:
        assert all(bucket_index(f, x, 18) == bucket_index(target, x, 18) for x in ids)
    assert sum(all(bucket_index(f, x, 18) == bucket_index(target, x, 18) for x in ids) for f in spread) < 5


def test_f3_ip_pools():
    rng = np.random.default_rng(3)
    ips = flood_ips(1100, 4, "spread", rng)
    assert len({int(ip) >> 8 for ip in ips}) == 4
    assert [int(ip) & 0xff for ip in ips[:8]] == [1, 1, 1, 1, 2, 2, 2, 2]
    assert int(ips[4 * 254]) & 0xff == 1          # hosts cycle 1..254
    clustered = flood_ips(100, 8, "clustered", np.random.default_rng(3))
    assert len({int(ip) >> 8 for ip in clustered}) == 8 and len({int(ip) >> 16 for ip in clustered}) == 1
    diverse = flood_ips(1000, 0, "spread", np.random.default_rng(3))
    assert len(set(diverse.tolist())) == 1000 and all(scored_ip(int(ip)) for ip in diverse)


def flood_world(n=400, sybils=40, budget=2.0, cache=5000):
    pop = Population(n, seed=1)
    pop.assign_topics(zipf_shares(1, 1.0), 1.0, True)
    fl = Flood("target", sybils, pop, 0, 1, budget=budget, start_window=0.0)
    cfg = WorldConfig(cache=cache, depth=18, ad_lifetime=900, rtt=0.06)
    return World(pop, cfg, 0.06, 1, reg_bucket_size=10, flood=fl, target=0), fl


def record_sends(w):
    sends = []
    orig = w.send_reg
    def spy(node, topic, bi, reg):
        if node < 0:
            sends.append((w.now, w.reg[(node, topic)].buckets[bi][reg].ticket is not None))
        orig(node, topic, bi, reg)
    w.send_reg = spy
    return sends


def test_f4_token_bucket_paces_sends():
    w, fl = flood_world(budget=2.0)
    fl.seeds = lambda node, pop: [10 + (FLOOD_BASE - node)]
    w.aux = lambda *a: []
    sends = record_sends(w)
    for node, topic in fl.chains:
        w.at(0.0, "start_reg", node, topic)
    w.run(100)
    assert [t for t, _ in sends] == pytest.approx([0.5 * i for i in range(40)])
    assert fl.admitted == 40 and fl.parked_max == 39


def test_f4_retries_first_and_late_expired_timings():
    # 40 sybils on one registrar quoting ~45 s. At 0.5/s the first round of
    # fresh sends takes 80 s; retries holding a ticket jump that queue.
    w, fl = flood_world(budget=0.5)
    r = 5
    w.ttab[r].add(7, 0, w.ip_of(7), 0)
    fl.seeds = lambda node, pop: [r]
    w.aux = lambda *a: []
    sends = record_sends(w)
    for node, topic in fl.chains:
        w.at(0.0, "start_reg", node, topic)
    w.run(400)
    first_retry = min(t for t, tk in sends if tk)
    fresh_before = sum(1 for t, tk in sends if not tk and t <= first_retry)
    # the first ticket is served about 45 s in while fresh chains still wait
    assert first_retry < 50 and fresh_before < 40
    assert sum(1 for t, tk in sends if not tk and t < 100) < 40
    tt = w.ttab[r]
    assert tt.requests == tt.replies == int(fl.sent.sum()) == int(w.reg_rcv.sum())
    assert fl.sent.sum() <= 0.5 * 400 + 1
    timings = w.adv_timings["sybil"]
    assert timings["on_time"] > 0 and timings["early"] == 0
    # 40 tickets every ~45 s need 0.9/s: at 0.5/s some still go late or expire
    assert timings["late"] + timings["expired"] > 0
    assert sum(timings.values()) == sum(tt.ticket_timings[k] for k in ("on_time", "late", "expired", "early"))


def test_f10_budgeted_rho4_keeps_its_tickets():
    def world(sybils, budget):
        pop = Population(300, seed=4)
        pop.assign_topics(zipf_shares(1, 1.0), 1.0, True)
        fl = Flood("target", sybils, pop, 0, 4, budget=budget, start_window=60.0) if sybils else None
        w = World(pop, WorldConfig(cache=1000, depth=18, ad_lifetime=900, rtt=0.06), 0.06, 1, reg_bucket_size=10,
                  flood=fl, target=0)
        for i in range(pop.n):
            w.at(float(i % 60), "start_reg", i, 0)
        if fl is not None:
            for (node, topic), t in zip(fl.chains, fl.starts):
                w.at(float(t), "start_reg", node, topic)
        w.schedule_phases(300.0, 900.0, 60.0)
        w.run(900.0)
        return w
    honest = world(0, 0)
    p0, p1 = honest.phase["start"], honest.phase["end"]
    rate = (p1["sent"][0] - p0["sent"][0]) / (p1["t"] - p0["t"])
    w = world(4 * 300, 2.5 * rate)
    t = w.adv_timings["sybil"]
    presented = sum(t.values())
    assert w.flood.parked_max > 100 and presented > 1000
    # FIFO parking expired about 70 % of presented tickets here; the queue
    # of 6b7b67a 32 % with 1,685 admissions, because a parked fresh chain was
    # never promoted and a token went to a fresh attempt while the chain held
    # a valid ticket in another bucket
    assert t["expired"] / presented < 0.01
    assert w.flood.admitted > 2 * 1685


def test_f11_parked_fresh_chain_promoted_when_its_ticket_comes_due():
    """A chain parked behind an older fresh chain gets the next token once its
    own ticket is due, ahead of that fresh chain."""
    from searchsim.go_registrar import SECOND, Ticket
    from searchsim.world import WAITING, Attempt
    w, fl = flood_world(sybils=2, budget=0.1)
    w.aux = lambda *a: []
    a_node, b_node = fl.chains[0][0], fl.chains[1][0]
    regs = [10, 11, 12]
    def table(node, attempts):
        rt = w.reg[(node, 0)] = w.new_regtable(node)
        w.reg_start[(node, 0)] = 0.0
        tid = int(w.pop.topic_ids[0])
        for reg, nxt, ticket in attempts:
            bi = bucket_index(tid, int(w.pop.ids[reg]), 18)
            att = rt.buckets[bi][reg] = Attempt()
            rt.counts[bi][WAITING] += 1
            att.state, att.next, att.ticket = WAITING, nxt, ticket
            rt.push(att, bi, reg)
        return rt
    table(a_node, [(regs[0], 0.0, None)])
    ticket = Ticket(1, b_node, 0, SECOND, 0, 0)             # window opens at 1 s, closes at 16 s
    table(b_node, [(regs[1], 0.0, None), (regs[2], 1.06, ticket)])
    sends = record_sends(w)
    fl.tokens = 0.0
    w.reg_tick(a_node, 0)                                   # parks fresh first
    w.reg_tick(b_node, 0)                                   # parks fresh behind it, ticket due at 1.06
    w.run(10.5)
    assert sends == [(10.0, True)]                          # the first token (t = 10) serves b's ticket
    assert w.adv_timings["sybil"]["late"] == 1 and w.adv_timings["sybil"]["expired"] == 0


def test_f5_query_weighted_target_share():
    w, _ = flood_world()
    r = 3
    tt = w.ttab[r]
    for adv in (10, 11, 12):
        tt.add(adv, 0, w.ip_of(adv), 0)
    tt.add(FLOOD_BASE, 0, w.ip_of(FLOOD_BASE), 0, attacker=True)
    tid = int(w.pop.topic_ids[0])
    for _ in range(2):
        res, _ = w.serve_query(r, 0, tid, [])
        assert len(res) == 4
    sv = w.served[0]
    assert sv[:3] == [2, 8, 2]
    assert share(sv[3], sv[5]) == 0.25 and share(sv[2], sv[1]) == 2 / 8 and share(sv[4], sv[0]) == 0.25
    assert w.target_queries[r] == 2


def test_f6_samples_match_live_attacker_ads():
    w, fl = flood_world(budget=0.0)
    w.sample_period = 60.0
    for node, topic in fl.chains:
        w.at(0.0, "start_reg", node, topic)
    for i in range(w.pop.n):
        w.at(float(i % 50), "start_reg", i, 0)
    w.at(60.0, "sample")
    w.run(120.0)
    assert len(w.samples) == 2
    ads, att = w.samples[-1][0], w.samples[-1][1]
    for tt in w.ttab:
        tt.purge(120.0)
    assert np.array_equal(att, [tt.attacker_ads for tt in w.ttab]) and att.sum() > 0
    assert np.array_equal(ads, [len(tt.ads) for tt in w.ttab])
    assert w.sample_rows[-1]["sybil_requests"] == int(w.samples[-1][6].sum())


def test_f7_eclipsed16_and_honest_times():
    pop = SimpleNamespace(topics=np.array([0, 0, 0, 1]))
    s = SimpleNamespace(node=0, topic=0, order=[(-5, 1, 0.1, 1), (1, 1, 0.2, 2), (3, 1, 0.3, 3), (2, 1, 1.0, 4)])
    times = honest_times(SimpleNamespace(pop=pop), s)
    assert times == [0.2, 1.0]
    assert honest_reached(times, 2, 0.5) == 0.2 and honest_reached(times, 2, 0.9) == 1.0
    assert honest_reached(times, 3, 0.9) is None
    rows = [dict(honest_t50=1.0, honest_t90=None, honest_t99=None, first16_sybil=1.0, eclipsed16=True, eclipsed=False, honest_found=0),
            dict(honest_t50=3.0, honest_t90=4.0, honest_t99=None, first16_sybil=0.5, eclipsed16=False, eclipsed=False, honest_found=5)]
    ex = search_extras(rows)
    assert ex["honest_t50"] == 2.0 and ex["honest_t90"] == 4.0 and ex["honest_t99"] is None
    assert ex["eclipsed16"] == 0.5 and ex["first16_sybil_mean"] == 0.75 and ex["without_honest"] == 0.5


@needs_crawl
def test_f8_results_code_counts_honest_only(flood_run):
    seen, s, _ = flood_run
    w = seen["world"]
    honest_adm = sum(1 for (adv, t) in w.first_admit if adv >= 0)
    assert s["admission_s"]["n"] == honest_adm and s["admission_s"]["sybil"]["n"] == len(w.first_admit) - honest_adm
    assert s["fanout"]["registered"] <= s["fanout"]["registrants"]
    att = sum(tt.attacker_ads for tt in w.ttab)
    assert s["sybil_ad_share"] == pytest.approx(att / sum(len(tt.ads) for tt in w.ttab)) and att > 0
    assert s["flood"]["ads_end"] == att
    for key in ("samples", "giveups", "fanout_buckets", "served", "regtopic_rate", "ticket_timings", "flood"):
        assert key in s


class Stop(Exception):
    pass


def cli(monkeypatch, tmp_path, flags, capture=None):
    def fake(pop, cfg, rtt, seed, **kw):
        if capture is not None:
            capture.update(kw)
        raise Stop
    monkeypatch.setattr(world_mod, "World", fake)
    monkeypatch.setattr(sys, "argv", ["world", "--nodes", "8", "--topics", "1", "--out", str(tmp_path)] + flags)
    world_mod.main()


@pytest.mark.parametrize("flags", [
    ["--flood", "target"],
    ["--fork-faithful", "--flood", "target"],
    ["--fork-faithful", "--honest-nets", "crawl", "--flood", "target", "--attack", "A1", "--attack-nets", "1"],
    ["--fork-faithful", "--honest-nets", "crawl", "--flood", "target", "--sybil-frac", "0.1"],
    ["--flood-budget", "5"],
    ["--flood-give-up"],
    ["--rtt", "0"],
])
def test_f9_cli_rejects(monkeypatch, tmp_path, flags):
    with pytest.raises(SystemExit):
        cli(monkeypatch, tmp_path, flags)


def test_f9_reg_bucket_size_reaches_regtable(monkeypatch, tmp_path):
    kw = {}
    with pytest.raises(Stop):
        cli(monkeypatch, tmp_path, ["--fork-faithful", "--reg-bucket-size", "10", "--no-ip-score"], kw)
    assert kw["reg_bucket_size"] == 10 and kw["ip_score"] is False and kw["flood"] is None
    pop = Population(50, seed=1)
    pop.assign_topics(zipf_shares(1, 1.0), 1.0, True)
    w = World(pop, WorldConfig(depth=18), 0.06, 1, reg_bucket_size=10, ip_score=False)
    assert w.new_regtable(0).size == 10 and not w.ttab[0].ip_score
    with pytest.raises(ValueError):
        World(pop, WorldConfig(depth=18), 0.0, 1)
