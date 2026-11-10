"""Attacker and W1 guards against the fork's unchanged search mechanics."""
import random

import pytest

from test_fork_search import Pop, reply, search, world_fork
from searchsim.attack import Attacker, SubnetLedger
from searchsim.metrics import study_row, study_summary
from searchsim.population import DHT_MIN_DIST, bucket_index, logdist
from searchsim.search import AskedSet, ForkConfig, ForkSearch
from searchsim.world import ForkSearcherState, SearchFilter


def attacker_state(strategy='A3', nets=2, w1=False):
    w = world_fork(n=200)
    w.attack, w.attack_nets = strategy, nets
    w.attacker = Attacker(strategy, nets, w.fork, 1)
    w.mint = w.attacker.mint
    w.w1 = w1
    s = ForkSearcherState(w, 5, 0, 0, 10000)
    return w, s


def test_a3_reaches_deepest_and_pins_with_two_subnets():
    # Conditional steering guard: two controlled registrars per bucket are
    # reachable. The 30k stop-gate, not this fixture, tests the DHT foothold.
    w, s = attacker_state()
    s.new_pass()
    st = s.st
    st.new = [set() for _ in range(18)]
    st.ips = [{} for _ in range(18)]
    nodes = [w.attacker.node(s.tid, b, net) for b in range(18) for net in range(2)]
    st.add_nodes(None, nodes, 0)
    states = []
    for q in range(32):
        dst = st.query_target(w.now)
        if dst is None:
            break
        delay, ads, aux, failed = w.fork_query(dst, 0, s.tid, st.buckets_with_free_space(), s)
        assert not failed
        w.now += delay
        st.add_reply(dst, [], len(ads), len(ads), w.now)
        st.add_nodes(dst, aux, w.now)
        states.append(st.active)
    assert 17 in states and states[-1] == 17 and len(states) <= 32
    assert s.attack_pinned
    assert all(b == 17 for b in states[states.index(17):])


def test_minted_refill_pass_stops_at_exactly_32():
    w, s = attacker_state('A5')
    s.new_pass()
    st = s.st
    st.new = [set() for _ in range(18)]
    st.ips = [{} for _ in range(18)]
    # Partial replies below the yield floor do not terminate an empty active
    # bucket early; new public /24s refill it until the hard cap.
    for q in range(32):
        n = w.attacker.node(s.tid, 0, 1000 + q)
        st.add_nodes(None, [n], w.now)
        assert st.query_target(w.now) is not None
        st.add_reply(n, [], 0, 0, w.now, partial=True)
    assert st.replies == 32 and st.query_target(w.now) is None and st.is_done()


@pytest.mark.parametrize('nets', [1, 2, 4, 8, 16])
def test_owned_subnet_limit(nets):
    w, s = attacker_state(nets=nets)
    s.new_pass()
    st = s.st
    st.new[17].clear()
    st.ips[17].clear()
    nodes = [w.attacker.node(s.tid, 17, j % nets) for j in range(80)]
    st.add_nodes(None, nodes, 0)
    assert len(st.new[17]) == min(nets, 8)


def test_far_zero_reply_does_not_push_in():
    p = Pop()
    st = search(p, active=8)
    reply(st, p, 1, 0)
    reply(st, p, 1, 0)
    assert st.active == 8


def test_a6_wait_release_only_sybil_queues_when_no_honest_ads_wait():
    w, s = attacker_state('A6')
    n = w.attacker.node(s.tid, 0, 0)
    delay, ads, _, failed = w.fork_query(n, 0, s.tid, [256, 255], s)
    assert not failed and delay == pytest.approx(0.75)
    f = SearchFilter(5, 900, 5, 5)
    assert f.take(n, ads, delay) == []
    # Three empty honest replies do not count toward the five registrars.
    for t in (1., 3., 5.):
        assert f.take(1, [], t) == []
    released = f.take(2, [], delay + 5)
    assert released == ads and all(r < 0 for r in released)


def test_a6_wait_does_not_exclude_queued_honest_ads():
    # The plan's unconditional sybil-only claim is false: the mixer includes
    # every waiting queue, even with fewer than five honest registrars.
    f = SearchFilter(5, 900, 5, 5)
    f.take(-1, [-2, -4], 0)
    f.take(1, [9], 1)
    assert 9 in f.take(2, [], 5)


def test_answer_keeps_subnet_error_frees_it():
    w, s = attacker_state('A1')
    s.new_pass()
    for error in (False, True):
        st = ForkSearch(w.fork, w.pop, s.tid, 5, random.Random(1), AskedSet(900), AskedSet(900), mint=w.mint)
        a, b = [w.attacker.node(s.tid, 0, 0) for _ in range(2)]
        st.add_nodes(None, [a], 0)
        if error:
            st.handle_error(a, 0)
        else:
            st.add_reply(a, [], 16, 16, 0)
        st.add_nodes(a, [b], 0)
        assert (b in st.new[0]) == error


def test_a5_timeout_with_referrals_is_partial_and_keeps_subnet():
    w, s = attacker_state('A5')
    s.new_pass()
    s.st.new[0].clear()
    s.st.ips[0].clear()
    n = w.attacker.node(s.tid, 0, 0)
    s.st.add_nodes(None, [n], 0)
    delay, ads, aux, failed = w.fork_query(n, 0, s.tid, [256], s)
    assert delay == pytest.approx(0.76) and failed and not ads
    assert len(aux) == 16 and len({w.mint[i][1] for i in aux}) == 16
    assert any(w.mint[i][0] != 0 for i in aux)  # unrequested distances
    w.now += delay
    s.reply(n, ads, aux, failed)
    assert s.failures == s.pass_f == 1
    assert s.asked_set.has(n, w.now) and not s.failed_set.has(n, w.now)
    assert s.st.ips[0][0] == 1 and not s.st.yield_[0]


def test_w1_blocks_fresh_id_reentry_across_passes_until_ttl():
    w, s = attacker_state('A1', w1=True)
    s.new_pass()
    n = w.attacker.node(s.tid, 17, 0)
    s.ledger.record(0, 0)
    for now, admitted in ((1, False), (899.999, False), (900, True)):
        st = ForkSearch(w.fork, w.pop, s.tid, 5, random.Random(1), s.asked_set, s.failed_set,
                        mint=w.mint, ledger=s.ledger)
        st.add_nodes(None, [n], now)
        assert (n in st.new[17]) == admitted


def test_w1_limit_and_lan_exemption():
    ledger = SubnetLedger(limit=2)
    ledger.record(7, 0)
    assert ledger.allows(7, 1)
    ledger.record(7, 1)
    assert not ledger.allows(7, 899)
    assert not ledger.allows(7, 899.5)
    assert ledger.allows(7, 900)
    for _ in range(10):
        ledger.record(-1, 0)
    assert ledger.allows(-1, 1) and -1 not in ledger.queries


def test_mint_namespace_geometry_and_fresh_pass_seeds():
    w, s = attacker_state('A1')
    before = set(w.attacker.seeds(s))
    # Geometry is retained only for the live pass; numeric IDs never repeat.
    old_geometry = {n: w.attacker.ids[n] for n in before if n < 0}
    old_mint = {n: w.mint[n] for n in before if n < 0}
    after = set(w.attacker.seeds(s))
    assert not set(old_geometry) & set(w.mint)
    for n, node_id in old_geometry.items():
        assert bucket_index(s.tid, node_id, 18) == old_mint[n][0]
    all_geometry = old_geometry | w.attacker.ids
    all_mint = old_mint | w.mint
    assert not ({n for n in before if n < 0} & {n for n in after if n < 0})
    for seeds in (before, after):
        subnets, bucket_subnets, buckets = {}, {}, {}
        for n in seeds:
            node_id = all_geometry[n] if n < 0 else int(w.pop.ids[n])
            b = max(logdist(int(w.pop.ids[s.node]), node_id), DHT_MIN_DIST + 1)
            buckets[b] = buckets.get(b, 0) + 1
            if n < 0:
                topic_b, net = all_mint[n]
                assert bucket_index(s.tid, node_id, 18) == topic_b
                subnets[net] = subnets.get(net, 0) + 1
                bucket_subnets[b, net] = bucket_subnets.get((b, net), 0) + 1
        assert max(buckets.values()) <= 16
        assert max(subnets.values()) <= 10
        assert max(bucket_subnets.values()) <= 2
    assert not set(w.attacker.advertisements(100)) & set(w.mint)
    assert not w.sybil.any()  # no sybil registration channel


def test_metrics_window_boundaries_and_censoring():
    w, s = attacker_state()
    s.order = [(-2, -1, 899.9, 1), (9, 1, 900., 2)]
    s.active_trace = [(0., 0), (800., 17)]
    s.study_passes = [dict(start=0, end=1, queries=2, sybil_queries=2, honest_queries=0,
                          timeouts=1, complete=True, start_bucket=0, end_bucket=17)]
    row = study_row(s, 1800, 18)
    assert row['windows'][0]['sybil_share'] == 1
    assert row['windows'][1]['honest_found'] == 1
    assert row['deepest_dwell_s'] == 1000
    assert not row['first']['16']['complete']
    assert row['passes_with_zero_honest_queries'] == 1
    summary = study_summary([row], 'A3', 2, False)
    assert summary['searchers_without_honest_result'] == 1
    assert summary['metrics']['first16_sybil_share']['mean'] == .5


def test_w1_records_queries_and_rejects_later_pass_seeds():
    w, s = attacker_state('A1', w1=True)
    w.searchers[s.node] = s
    w.at(0, 'sstep', s.node)
    w.run(10)
    assert s.ledger.queries
    assert any(net < 2 for net in s.ledger.queries)
    st = ForkSearch(w.fork, w.pop, s.tid, 5, random.Random(1), s.asked_set, s.failed_set,
                    mint=w.mint, ledger=s.ledger)
    nodes = [w.attacker.node(s.tid, 17, net) for net in range(2)]
    st.add_nodes(None, nodes, w.now)
    for node in nodes:
        net = w.mint[node][1]
        assert (node in st.new[17]) == s.ledger.allows(net, w.now)


@pytest.mark.parametrize('ads,active,want', [(0, 4, 8), (11, 4, 4), (16, 4, 3)])
def test_m2_single_subnet_cannot_steer_but_two_can(ads, active, want):
    w, s = attacker_state('A3', nets=2)
    st = ForkSearch(w.fork, w.pop, s.tid, 5, random.Random(1), AskedSet(900), AskedSet(900),
                    mint=w.mint, m2=True)
    st.set_active(active)
    for _ in range(2):
        n = w.attacker.node(s.tid, 4, 0)
        st.add_reply(n, [], ads, ads, 0)
    assert st.active == active and not st.placed
    assert st.yield_[4] == [ads, ads]
    n = w.attacker.node(s.tid, 4, 1)
    st.add_reply(n, [], ads, ads, 1)
    assert st.active == want and st.placed


def test_m2_nets_limit_window_and_lan_exemption():
    w, s = attacker_state()
    st = ForkSearch(w.fork, w.pop, s.tid, 5, random.Random(1), AskedSet(900), AskedSet(900),
                    mint=w.mint, m2=True, m2_nets=3)
    st.set_active(4)
    for net in (0, 1, 1, 1, 1, 2):
        st.observe(4, 0, net)
    assert st.active == 4 and st.yield_nets[4] == [1, 1, 1, 2]
    st.observe(4, 0, 0)
    assert st.active == 8
    st.set_active(4)
    st.observe(4, 0, -1)
    assert st.active == 8  # a LAN-origin sample is exempt


def test_m2_uses_honest_public_subnets_without_w1():
    w = world_fork(n=200)
    w.m2 = True
    s = ForkSearcherState(w, 5, 0, 0, 1000)
    s.new_pass()
    assert s.st.net_of(1) != s.st.net_of(2)
    assert s.ledger is None


def test_m3_same_subnet_does_not_reach_count_threshold_wait_unchanged():
    f = SearchFilter(99, 900, 5, 5, m3=True, net_of=lambda _: 7)
    for registrar in range(5):
        assert f.take(registrar, [registrar], registrar * .1) == []
    assert len(f.queues) == 5 and len(f.mix_nets) == 1
    assert f.take(6, [], 4.99) == []
    assert f.take(6, [], 5) == list(range(5))


def test_m3_distinct_subnets_empty_replies_lan_and_release_reset():
    nets = {0: 7, 1: 7, 2: 8, 3: -1, 4: -1, 5: 9, 6: 10}
    f = SearchFilter(99, 900, 5, 5, m3=True, net_of=nets.__getitem__)
    for registrar in range(5):
        assert f.take(registrar, [registrar], 0) == []
    assert len(f.mix_nets) == 4  # same public /24 merged; LANs independent
    assert f.take(5, [], 0) == [] and len(f.mix_nets) == 4
    assert f.take(6, [6], 0) == [0, 1, 2, 3, 4, 6]
    f.release(0)
    assert not f.mix_nets
    assert f.take(0, [10], 1) == []
    assert f.release(1)[0] == [10]  # pass-end flush still hands out all



def test_a5_unowned_public_referrals_are_silent_not_extra_responding_subnets():
    w, s = attacker_state('A5')
    source = w.attacker.node(s.tid, 0, 0)
    _, _, aux, _ = w.fork_query(source, 0, s.tid, [256], s)
    for junk in aux:
        assert w.mint[junk][1] >= w.attack_nets
        delay, ads, more, failed = w.fork_query(junk, 0, s.tid, [256], s)
        assert delay == .7 and not ads and not more and failed


def test_w1p_rejects_fresh_id_from_full_subnet_across_buckets_and_passes():
    from searchsim.attack import AdmissionLedger
    pop = Pop()
    first, fresh = pop.add(0, 7), pop.add(1, 7)
    ledger = AdmissionLedger()
    s = search(pop)
    s.admission_ledger = ledger
    s.add_nodes(None, [first], 0)
    assert first in s.new[0]
    later = search(pop)
    later.admission_ledger = ledger
    later.add_nodes(None, [fresh], 1)
    assert fresh not in later.new[1]
    assert set(ledger.admitted[7]) == {first}


def test_w1p_readmits_same_id_and_preserves_asked_spares_without_refresh():
    from searchsim.attack import AdmissionLedger
    pop = Pop()
    node = pop.add(0, 7)
    ledger = AdmissionLedger()
    for now, asked in ((0, False), (100, False), (800, True)):
        s = search(pop)
        s.admission_ledger = ledger
        if asked:
            s.asked.add(node, now)
        s.add_nodes(None, [node], now)
        assert node in (s.spare if asked else s.new[0])
    assert ledger.admitted[7][node] == 0


def test_w1p_entry_expires_after_one_ad_lifetime():
    from searchsim.attack import AdmissionLedger
    ledger = AdmissionLedger()
    ledger.record(7, 11, 0)
    assert not ledger.allows(7, 12, 899)
    assert ledger.allows(7, 12, 900)
    assert 7 not in ledger.admitted


def test_w1p_lan_is_exempt():
    from searchsim.attack import AdmissionLedger
    ledger = AdmissionLedger()
    for node in range(20):
        assert ledger.allows(-1, node, 0)
        ledger.record(-1, node, 0)
    assert not ledger.admitted


def test_w1p_limit_per_entry_expiry_and_askedset_sweep_cadence():
    from searchsim.attack import AdmissionLedger
    ledger = AdmissionLedger(limit=2)
    ledger.record(7, 11, 0)
    ledger.record(7, 12, 10)
    ledger.record(7, 11, 800)  # Same ID does not refresh its first admission.
    assert not ledger.allows(7, 13, 899.5)
    assert not ledger.allows(7, 13, 900)  # AskedSet also defers a sweep <1s.
    assert ledger.allows(7, 13, 900.5)
    assert set(ledger.admitted[7]) == {12}
    ledger.record(7, 13, 900.5)
    assert not ledger.allows(7, 14, 909)
    assert ledger.allows(7, 14, 910)


def test_w1p_rejected_table_candidate_does_not_reserve_subnet():
    from searchsim.attack import AdmissionLedger
    pop = Pop()
    nodes = [pop.add(0, n) for n in range(9)]
    ledger = AdmissionLedger()
    s = search(pop)
    s.admission_ledger = ledger
    s.add_nodes(None, nodes, 0)
    assert nodes[-1] not in s.new[0] and 8 not in ledger.admitted
    later = search(pop)
    later.admission_ledger = ledger
    later.add_nodes(None, [nodes[-1]], 1)
    assert nodes[-1] in later.new[0]


def test_w1p_is_shared_by_fork_searcher_passes():
    w = world_fork(n=200)
    w.w1p = True
    s = ForkSearcherState(w, 5, 0, 0, 10000)
    s.new_pass()
    assert s.admission_ledger.admitted and s.ledger is None
    first = {net: set(ids) for net, ids in s.admission_ledger.admitted.items()}
    s.new_pass()
    assert all(set(s.admission_ledger.admitted[net]) == ids for net, ids in first.items())
    assert s.st.admission_ledger is s.admission_ledger
