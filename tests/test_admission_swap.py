"""The vendored Go admission port and the world model's use of it."""
import hashlib
import json
import math
import os
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from searchsim import go_registrar as gor  # noqa: E402
from searchsim.admission import GoTable, WorldTable, go_config  # noqa: E402
from searchsim.go_registrar import (  # noqa: E402
    Bound, Config, IPTree, LIFETIME, MAX_WAIT, Registrar, Reply, SECOND, Ticket, duration, ipv4, scored_ip, wire_wait,
)
from searchsim.honest_nets import HONEST_NET_BASE, ipv4_of_labels  # noqa: E402
from searchsim.population import Population, bucket_index, zipf_shares  # noqa: E402
from searchsim.world import REGISTERED, World, WorldConfig  # noqa: E402

HERE = Path(__file__).resolve().parent
UPSTREAM = Path.home() / "workspace/datahop/sim-admission/admissionsim/registrar.py"
FIXTURE = json.loads((HERE / "fixtures/admission-go.json").read_text())
CRAWL = Path.home() / "workspace/datahop/runs/2026-09-18-crawl24h-aws/derived/gap10"
MODEL = HERE.parent / "models/crawl-2026-09-18.json"


@pytest.mark.skipif(not UPSTREAM.exists(), reason="sim-admission checkout absent")
def test_vendored_port_is_byte_identical():
    mine = (HERE.parent / "searchsim/go_registrar.py").read_bytes()
    assert hashlib.sha256(mine).hexdigest() == hashlib.sha256(UPSTREAM.read_bytes()).hexdigest()


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda c: c["name"])
def test_actual_go_wait_values_vendored(case):
    # Fixture from Go 90b0983: G = 0, so the base Registrar with safety 0.
    reg = Registrar(Config(safety=0.0))
    for i in range(case["count"]):
        ip = ((i + 1) * 2654435761) & 0xffffffff
        if case["style"] == "repeated":
            ip = ipv4("11.22.33.0") + 1 + i % 254
        assert reg.add(i, "target" if i < case["topic_count"] else "other", ip, 0)
    if case["bound_ns"]:
        reg.topic_bounds["target"] = Bound(case["bound_ns"], 0)
    wait, service, _, _ = reg.components("target", ipv4("11.22.33.200"), case["now_ns"])
    assert reg.tree.score_node(ipv4("11.22.33.200"))[0] == case["ip_score"]
    assert service == pytest.approx(case["service_s"], rel=2e-15)
    assert wait == case["wait_ns"]


def test_duration_nan_inf_cap():
    assert duration(math.nan) == duration(-1) == duration(0) == 0
    assert duration(math.inf) == duration(86400) == MAX_WAIT
    assert duration(1.00000000001) == SECOND + 1


def test_tree_depth_and_historical_removal():
    tree, ip = IPTree(), ipv4("11.22.33.1")
    levels = [tree.insert(ip) for _ in range(8)]
    assert levels == [0, 0, 1, 1, 2, 2, 2, 2]
    assert tree.score_node(ip)[0] == 2 / 3
    tree.remove(ip, levels[0]); tree.remove(ip, levels[1])
    assert tree.score_node(ip)[0] == 1
    for d in levels[2:]:
        tree.remove(ip, d)
    assert tree.root.counter == 0 and tree.root.children == [None, None]


def test_ancestor_ip_floor():
    reg, ip = Registrar(), ipv4("11.22.33.1")
    for node in range(4):
        reg.add(node, "old", ip, 0)
    _, ancestor = reg.tree.score_node(ip)
    ancestor.bound.bump(100 * SECOND, 0)
    for node in range(4, 20):
        reg.add(node, "old", ip, 0)
    assert reg.tree.path_floor(ip, 7 * SECOND) == 93 * SECOND


def test_bounds_bump_decay_only_on_quotes():
    b = Bound(120 * SECOND, 0)
    b.bump(100 * SECOND, 7 * SECOND)
    assert b.remaining(7 * SECOND) == 113 * SECOND
    reg = GoTable(Config(safety=1e-7))
    reg.request(1, "t", ipv4("11.22.33.1"), 0)
    assert not reg.topic_bounds
    reg.request(2, "t", ipv4("11.22.33.2"), 0)
    assert reg.topic_bounds["t"].value > 0


def test_one_second_slack_inclusive():
    reg, ip = Registrar(), ipv4("11.22.33.1")
    reg.add(1, "t", ip, 0)
    reg.topic_bounds["t"] = Bound(100 * SECOND, 0)
    assert reg.register(2, "t", ip, 99 * SECOND, 0)[0] == "admitted"
    reg2 = Registrar()
    reg2.add(1, "t", ip, 0)
    reg2.topic_bounds["t"] = Bound(100 * SECOND, 0)
    assert reg2.register(2, "t", ip, 99 * SECOND - 1, 0)[0] == "quote"


def test_goTable_hand_values_and_full_table():
    assert GoTable(Config(capacity=1000, safety=1e-7)).components("new", ipv4("8.8.8.8"), 0)[0] == 9000
    reg = GoTable(Config(capacity=16, safety=1e-7))
    for node in range(16):
        reg.add(node, "t", ((node + 1) * 2654435761) & 0xffffffff, 0)
    wait, service, ip_secs, lpm = reg.components("new", ipv4("8.8.8.8"), 0)
    assert (wait, service, ip_secs) == (MAX_WAIT, 0.0, 0.0)
    # scaleBase quotes the 24 h cap first; an eligible request gets the next expiry
    assert reg.register(100, "t", ipv4("8.8.8.8"), 0, 0) == ("quote", MAX_WAIT)
    assert reg.register(100, "t", ipv4("8.8.8.8"), MAX_WAIT, 0) == ("quote", LIFETIME)


def test_topic_bound_dropped_with_last_ad():
    reg = WorldTable(Config(safety=1e-7))
    reg.add(1, "t", ipv4("11.22.33.1"), 0)
    reg.request(2, "t", ipv4("11.22.33.2"), 0)
    assert "t" in reg.topic_bounds
    reg.expire(LIFETIME)
    assert not reg.topic_bounds and not reg.by_topic


def test_expired_pruned_at_request_time():
    reg = WorldTable(Config(capacity=1, safety=1e-7))
    reg.add(1, "t", ipv4("11.22.33.1"), 0)
    assert reg.request(2, "u", ipv4("12.0.0.1"), LIFETIME).status == "admitted"
    assert set(reg.ads) == {(2, "u")} and reg.by_topic == {"u": {2: 2 * LIFETIME}}


def test_world_table_views_under_shared_expiry():
    reg = WorldTable(Config(safety=1e-7))
    for node in range(5):
        reg.add(node, node % 2, ipv4("11.22.33.1") + node, 7, attacker=node == 4)
    reg.add(9, 1, ipv4("11.22.33.9"), 8)
    assert reg.count == 6 and {t: len(d) for t, d in reg.by_topic.items()} == {0: 3, 1: 3}
    reg.purge((LIFETIME + 7) / SECOND)
    assert reg.count == 1 and reg.by_topic == {1: {9: LIFETIME + 8}} and reg.attacker_ads == 0
    assert sum(len(d) for d in reg.by_topic.values()) == len(reg.ads) == reg.tree.root.counter


def small_world(n=300, **kw):
    pop = Population(n, seed=1)
    pop.assign_topics(zipf_shares(1, 1.0), 1.0, True)
    cfg = WorldConfig(cache=5000, depth=18, ad_lifetime=900, rtt=0.06)
    return World(pop, cfg, 0.06, 1, reg_bucket_size=10, **kw)


def test_reg_tick_credits_like_go():
    w = small_world()
    tid = int(w.pop.topic_ids[0])
    a, r = 0, 1
    tt = w.ttab[r]
    tt.add(7, 0, w.ip_of(7), 0)
    calls = []
    orig = tt.request
    def spy(node, topic, ip, now, ticket=None, attacker=False):
        reply = orig(node, topic, ip, now, ticket, attacker)
        calls.append((now, ticket, reply))
        return reply
    tt.request = spy
    w.aux = lambda *args: []
    rt = w.reg[(a, 0)] = w.new_regtable(a)
    rt.add_nodes(w, tid, None, [r])
    w.reg_start[(a, 0)] = 0.0
    w.at(0.0, "reg", a, 0)
    w.run(400)
    (t0, tk0, q), (t1, tk1, ok) = calls
    assert tk0 is None and q.status == "quote" and q.ticket.first == t0 == 0
    assert t1 - q.ticket.start == pytest.approx(0.06 * SECOND, abs=SECOND // 1000)
    assert tk1 is q.ticket and ok.status == "admitted"
    assert tt.ticket_timings == Counter({"on_time": 1})
    bi = bucket_index(tid, int(w.pop.ids[r]), 18)
    assert rt.buckets[bi][r].state == REGISTERED


def test_full_table_quote_drops_and_bans_honest_attacker_waits():
    w = small_world()
    tid = int(w.pop.topic_ids[0])
    r = 1
    bi = bucket_index(tid, int(w.pop.ids[r]), 18)
    rt = w.new_regtable(0)
    rt.add_nodes(w, tid, None, [r])
    tk = Ticket(1, 0, 0, MAX_WAIT, 0, 0)
    assert rt.on_ticket(bi, r, 86400.0, 0.0, tk) == "single"
    assert r not in rt.buckets[bi] and rt.banned[r] == 900
    rt.add_nodes(w, tid, None, [r])
    assert r not in rt.buckets[bi]
    w.now = 901.0
    rt.add_nodes(w, tid, None, [r])
    assert r in rt.buckets[bi]
    from searchsim.world import RegTable
    att_rt = RegTable(18, 900, 10, 20, timeout=math.inf)
    w.now = 0.0
    att_rt.add_nodes(w, tid, None, [r])
    assert att_rt.on_ticket(bi, r, 86400.0, 5.0, tk) is None
    assert att_rt.buckets[bi][r].next == 5.0 + 86400


def test_on_ticket_clamps_to_one_second_and_cumulative_budget():
    w = small_world()
    tid = int(w.pop.topic_ids[0])
    r = 1
    bi = bucket_index(tid, int(w.pop.ids[r]), 18)
    rt = w.new_regtable(0)
    rt.add_nodes(w, tid, None, [r])
    assert rt.on_ticket(bi, r, 0.3, 10.0, None) is None
    assert rt.buckets[bi][r].next == 11.0 and rt.buckets[bi][r].total_wait == 1.0
    assert rt.on_ticket(bi, r, 1349.0, 11.0, None) is None
    assert rt.on_ticket(bi, r, 1.0, 12.0, None) == "cumulative"


def test_existing_reply_takes_registered_path():
    w = small_world()
    tid = int(w.pop.topic_ids[0])
    r = 1
    bi = bucket_index(tid, int(w.pop.ids[r]), 18)
    rt = w.reg[(0, 0)] = w.new_regtable(0)
    rt.add_nodes(w, tid, None, [r])
    w.reg_start[(0, 0)] = 0.0
    w.reg_resp(0, 0, bi, r, Reply("existing", wire_wait(LIFETIME)), [])
    assert rt.buckets[bi][r].state == REGISTERED and rt.buckets[bi][r].next == 900
    assert w.reg_stats["existing"][0] == 1


def test_ipv4_of_labels():
    p1, p2 = ipv4("34.12.200.0") >> 8, ipv4("52.1.2.0") >> 8
    labels = np.array([HONEST_NET_BASE + p1] * 300 + [HONEST_NET_BASE + p2] + [HONEST_NET_BASE + (1 << 24) + p1] * 3
                      + [HONEST_NET_BASE + (2 << 24) + p1, -1, 5, 5], dtype=np.int64)
    ips = ipv4_of_labels(labels, len(labels), 1)
    assert all(int(ip) >> 8 == p1 for ip in ips[:300]) and int(ips[300]) >> 8 == p2
    hosts = [int(ip) & 0xff for ip in ips[:300]]
    assert hosts[:254] == list(range(1, 255)) and hosts[254:] == list(range(1, 47))
    copy1, copy2 = int(ips[301]) >> 8, int(ips[304]) >> 8
    assert copy1 != p1 and copy2 != p1 and copy1 != copy2
    assert copy1 >> 12 == copy2 >> 12 == p1 >> 12
    assert [int(ip) & 0xff for ip in ips[301:304]] == [1, 2, 3]
    assert scored_ip(int(ips[305])) and int(ips[306]) >> 8 == int(ips[307]) >> 8 and scored_ip(int(ips[306]))
    spread = ipv4_of_labels(None, 5000, 1)
    assert len(set(spread.tolist())) == 5000 and all(scored_ip(int(ip)) for ip in spread)


def test_honest_world_tickets_on_time():
    w = small_world(n=300)
    for i in range(w.pop.n):
        w.at(float(i), "start_reg", i, 0)
    w.run(2000)
    tt = Counter()
    for t in w.ttab:
        tt.update(t.ticket_timings)
    assert tt["on_time"] > 0
    assert tt["early"] == tt["late"] == tt["expired"] == tt["unknown_key"] == 0
    assert sum(t.requests for t in w.ttab) == sum(t.replies for t in w.ttab) == int(w.reg_rcv.sum()) == int(w.reg_sent.sum())


def run_main(monkeypatch, out, extra):
    from searchsim import world
    argv = ["world", "--nodes", "300", "--topics", "3", "--model", str(MODEL), "--fork-faithful", "--honest-nets", "crawl",
            "--depth", "18", "--reg-bucket-size", "10", "--start-window", "100", "--register-wait", "200",
            "--search-time", "100", "--search-frac", "0.2", "--seed", "3", "--out", str(out)] + extra
    monkeypatch.setattr(sys, "argv", argv)
    world.main()
    s = json.loads((out / "summary.json").read_text())
    s.pop("elapsed")
    s["config"].pop("out")
    return s


@pytest.mark.skipif(not CRAWL.exists(), reason="crawl data absent")
def test_flood_none_equals_flags_absent(monkeypatch, tmp_path):
    a = run_main(monkeypatch, tmp_path / "a", [])
    b = run_main(monkeypatch, tmp_path / "b", ["--flood", "none"])
    assert a == b
    assert not any(k.startswith("flood") for k in a["config"]) and "flood" not in a
