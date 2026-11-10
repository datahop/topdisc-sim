"""Registration.Update order, survivor-free time metrics, per-registrant
fanout and phase-aligned windows."""
import os
import sys
from types import SimpleNamespace

import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from searchsim.flood import Flood  # noqa: E402
from searchsim.go_registrar import Ticket  # noqa: E402
from searchsim.population import Population, bucket_index, zipf_shares  # noqa: E402
from searchsim.world import WAITING, Attempt, RegTable, World, WorldConfig, median_all, per_advertiser, search_extras  # noqa: E402


def waiting(rt, bi, reg, nxt, ticket=None):
    att = Attempt()
    rt.buckets[bi][reg] = att
    rt.counts[bi][WAITING] += 1
    att.state, att.next, att.ticket = WAITING, nxt, ticket
    rt.push(att, bi, reg)
    return att


def test_update_ticket_holders_first_then_round_robin():
    rt = RegTable(18, 900, 10, 20)
    tk = Ticket(1, 0, 0, 10, 0, 0)
    waiting(rt, 3, 100, 1.0)            # fresh, earliest
    waiting(rt, 3, 101, 5.0, tk)        # ticket holder, later
    waiting(rt, 3, 102, 4.0, tk)        # ticket holder, earlier
    waiting(rt, 7, 200, 0.5)
    waiting(rt, 12, 300, 9.0)           # not yet due at t = 6
    assert rt.update(6.0) == (3, 102)   # rr starts after bucket 17: bucket 3 first, its earliest ticket holder
    assert rt.update(6.0) == (7, 200)   # then the next bucket with a due attempt
    assert rt.update(6.0) == (3, 101)   # round robin wraps back to bucket 3
    assert rt.update(6.0) == (3, 100)
    assert rt.update(6.0) is None and rt.next_time() == 9.0
    assert rt.update(9.0) == (12, 300) and rt.rr == 12


def test_remove_clears_ready_entry():
    rt = RegTable(18, 900, 10, 20)
    waiting(rt, 2, 5, 0.0)
    rt.collect(1.0)
    assert rt.ready == {2: {5: rt.buckets[2][5]}}
    rt.remove(2, 5)
    assert rt.ready == {} and rt.pick() is None


def test_mixed_success_times_are_not_survivor_medians():
    rows = [dict(honest_t50=1.0, honest_t90=10.0, honest_t99=None, first16_sybil=0.0, eclipsed16=False, eclipsed=False, honest_found=9),
            dict(honest_t50=2.0, honest_t90=None, honest_t99=None, first16_sybil=0.0, eclipsed16=False, eclipsed=False, honest_found=3),
            dict(honest_t50=None, honest_t90=None, honest_t99=None, first16_sybil=None, eclipsed16=False, eclipsed=False, honest_found=0)]
    ex = search_extras(rows)
    assert ex["honest_t90"] == 10.0                       # conditional on reaching 90 %
    assert ex["honest_reached90"] == 1 and ex["honest_reached90_frac"] == 1 / 3
    assert ex["honest_t90_all"] is None                   # the median searcher never reached 90 %
    assert ex["honest_t50_all"] == 2.0 and ex["honest_reached50_frac"] == 2 / 3
    assert median_all([5.0, None]) is None and median_all([1.0, 3.0, None]) == 3.0 and median_all([]) is None


def test_fanout_counts_every_registrant():
    pop = SimpleNamespace(registrant=np.array([True, True, True, False]), topics=np.array([0, 0, 1, 1]))
    fanout = {(1, 0): 3}
    adv_buckets = {(1, 0): {4, 5}}
    fo, nb = per_advertiser(pop, fanout, adv_buckets)
    assert fo.tolist() == [0, 3, 0] and nb.tolist() == [0, 2, 0]
    assert float(np.median(nb)) == 0.0


def small_world(flood_sybils=0, cache=5000, n=300):
    pop = Population(n, seed=2)
    pop.assign_topics(zipf_shares(1, 1.0), 1.0, True)
    fl = Flood("target", flood_sybils, pop, 0, 2, start_window=60.0) if flood_sybils else None
    w = World(pop, WorldConfig(cache=cache, depth=18, ad_lifetime=900, rtt=0.06), 0.06, 1, reg_bucket_size=10,
              flood=fl, target=0)
    for i in range(pop.n):
        w.at(float(i % 60), "start_reg", i, 0)
    if fl is not None:
        for (node, topic), t in zip(fl.chains, fl.starts):
            w.at(float(t), "start_reg", node, topic)
    return w


def test_phase_windows_with_unaligned_search_start():
    w = small_world()
    w.schedule_phases(130.0, 250.0, 60.0)
    w.run(250.0)
    assert [r["t"] for r in w.sample_rows] == [10.0, 70.0, 130.0, 190.0, 250.0]
    p0, p1 = w.phase["start"], w.phase["end"]
    assert (p0["t"], p1["t"]) == (130.0, 250.0)
    inside = [row for row, r in zip(w.samples, w.sample_rows) if 130.0 < r["t"] <= 250.0]
    assert sum(int(row[4].sum()) for row in inside) == int((p1["reg_rcv"] - p0["reg_rcv"]).sum()) > 0


def test_honest_tickets_on_time_at_c1000_under_flood():
    w = small_world(flood_sybils=600, cache=1000)
    w.run(900.0)
    h = w.adv_timings["honest"]
    assert h["on_time"] > 0 and h["late"] == h["expired"] == h["early"] == 0
    assert w.adv_timings["sybil"]["on_time"] > 0
