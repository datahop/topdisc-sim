"""The fork-faithful searcher against the mechanics the fork's own tests pin
down (topicindex/search_test.go, searchfilter_test.go, v5_topic.go)."""
import os
import random
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from searchsim.population import Population, zipf_shares  # noqa: E402
from searchsim.search import AskedSet, ForkConfig, ForkSearch  # noqa: E402
from searchsim.world import ForkSearcherState, SearchFilter, World, WorldConfig  # noqa: E402

TID = 0x5A5A5A5A5A5A5A5A


class Pop:
    """Hand-placed nodes: ids[i] in a chosen search bucket, net[i] its /24."""

    def __init__(self):
        self.ids, self.nets, self.rng = [], [], random.Random(1)
        self.freeze()

    def add(self, bucket, net=-1):
        x = (1 << (63 - bucket)) | self.rng.getrandbits(63 - bucket) if bucket < 64 else 0
        self.ids.append(TID ^ x)
        self.nets.append(net)
        self.freeze()
        return len(self.ids) - 1

    def freeze(self):
        self.pop_ids = np.array(self.ids, dtype=np.uint64)
        self.net = np.array(self.nets, dtype=np.int64)


def search(pop, fc=None, active=0):
    class P:
        pass
    p = P()
    p.ids, p.net = pop.pop_ids, pop.net
    fc = fc or ForkConfig()
    s = ForkSearch(fc, p, TID, -1, random.Random(2), AskedSet(fc.ad_lifetime), AskedSet(fc.ad_lifetime))
    s.set_active(active)
    return s


def refresh(s, pop):
    s.pop.ids, s.pop.net, s.net = pop.pop_ids, pop.net, pop.net


def reply(s, pop, bucket, ads, now=0.0):
    i = pop.add(bucket)
    refresh(s, pop)
    s.add_nodes(None, [i], now)
    s.add_reply(i, [], ads, ads, now)
    return i


def test_search_depth_and_clamp():
    pop = Pop()
    a, b = pop.add(17), pop.add(30)
    s = search(pop)
    assert s.bucket(a) == 17 and s.bucket(b) == 17
    assert len(s.new) == 18


def test_far_helper_pulls_out():
    # TestSearchAdaptiveFarHelperPullsOut
    for ads, want in (((2, 2), 12), ((16, 16), 8), ((8, 9), 9)):
        pop = Pop()
        s = search(pop, active=12)
        for n in ads:
            reply(s, pop, 9, n)
        assert s.active == want, (ads, s.active, want)


def test_helper_keeps_bucket():
    # TestSearchAdaptiveHelperKeepsBucket: a far 0-reply cannot push the search in
    pop = Pop()
    s = search(pop, active=8)
    reply(s, pop, 1, 0)
    reply(s, pop, 1, 0)
    assert s.active == 8
    reply(s, pop, 7, 16)
    reply(s, pop, 7, 16)
    assert s.active == 6


def test_pass_budget():
    # TestSearchAdaptivePassBudget: 32 queries whatever their outcome
    for outcome in ("full", "partial", "failed"):
        pop = Pop()
        nodes = [pop.add(3, net=k) for k in range(40)]
        s = search(pop, active=3)
        s.add_nodes(None, nodes[:2], 0.0)
        for q in range(32):
            n = s.query_target(0.0)
            assert n is not None, (outcome, q)
            if outcome == "full":
                s.add_reply(n, [], 16, 16, 0.0)
            elif outcome == "partial":
                s.add_reply(n, [], 0, 7, 0.0, partial=True)
            else:
                s.handle_error(n, 0.0)
            s.add_nodes(n, [nodes[2 + q]], 0.0)
        assert s.query_target(0.0) is None and s.is_done()
        assert s.next_pass_bucket() == s.active


def test_partial_reply_samples_only_at_floor():
    pop = Pop()
    s = search(pop, active=5)
    for sent in (7, 7):
        i = pop.add(5); refresh(s, pop); s.add_nodes(None, [i], 0.0)
        s.add_reply(i, [], sent, sent, 0.0, partial=True)
    assert s.yield_[5] == [] and not s.placed
    for sent in (8, 8):
        i = pop.add(5); refresh(s, pop); s.add_nodes(None, [i], 0.0)
        s.add_reply(i, [], sent, sent, 0.0, partial=True)
    assert s.yield_[5] == [8, 8] and s.placed and s.active == 5


def test_answer_keeps_subnet_failure_frees_it():
    # TestSearchAdaptiveRepliedNodeKeepsSubnet, and the failure path
    pop = Pop()
    a, same, other = pop.add(0, net=7), pop.add(0, net=7), pop.add(0, net=9)
    s = search(pop)
    s.add_nodes(None, [a], 0.0)
    s.add_reply(a, [], 0, 0, 0.0)
    s.add_nodes(a, [same], 0.0)
    assert same not in s.new[0]
    s.add_nodes(a, [other], 0.0)
    assert other in s.new[0]
    pop2 = Pop()
    a, same = pop2.add(0, net=7), pop2.add(0, net=7)
    s = search(pop2)
    s.add_nodes(None, [a], 0.0)
    s.handle_error(a, 0.0)
    s.add_nodes(a, [same], 0.0)
    assert same in s.new[0]
    assert s.failed.has(a, 899.0) and not s.failed.has(a, 900.0)


def test_failures_keep_bucket():
    # TestSearchAdaptiveFailuresKeepBucket
    pop = Pop()
    nodes = [pop.add(3) for _ in range(2)] + [pop.add(b) for b in (2, 4, 5)]
    s = search(pop, active=3)
    s.add_nodes(None, nodes, 0.0)
    for _ in range(2):
        n = s.query_target(0.0)
        assert s.bucket(n) == 3
        s.handle_error(n, 0.0)
    while (n := s.query_target(0.0)) is not None:
        s.handle_error(n, 0.0)
    assert s.is_done() and s.next_pass_bucket() == 3


def test_mix_wait():
    # TestSearchFilterMixWait
    f = SearchFilter(-1, 900.0, 5, 5.0)
    assert f.take("A", [1, 2], 0.0) == []
    assert f.take("B", [3, 4], 4.0) == []
    assert f.take("C", [], 5.0) == [1, 3, 2, 4]


def test_mix_continuous():
    # TestSearchFilterMixContinuous
    f = SearchFilter(-1, 900.0, 2, 5.0)
    f.take("A", [1], 0.0)
    assert f.take("B", [2], 0.0) == [1, 2]
    assert f.take("S", [10, 11, 12, 13], 0.0) == []
    assert f.take("D", [20, 21], 1.0) == [10, 20, 11, 21]
    assert f.take("E", [], 1.0) == []
    assert f.take("E", [], 6.0) == [12, 13]


def test_filter_ttl():
    f = SearchFilter(-1, 900.0, 1, 5.0)
    f.add(1, 0.0)
    assert f.take("A", [1], 899.0) == []
    assert f.release(899.0)[0] == []
    f.take("A", [1], 900.0)
    assert f.release(900.0)[0] == [1]


def test_filter_limit_evicts_oldest():
    # searchFilterLimit: at the limit a new entry evicts the oldest one; a
    # re-added entry moves to the back.
    f = SearchFilter(-1, 900.0, 1, 5.0, limit=3)
    for t, n in enumerate((1, 2, 3)):
        f.add(n, float(t))
    f.add(1, 3.0)
    f.add(4, 4.0)
    assert sorted(f.seen) == [1, 3, 4]
    assert not f.is_seen(2, 5.0) and f.is_seen(1, 5.0)
    big = SearchFilter(-1, 900.0, 1, 5.0)
    for n in range(50005):
        big.add(n, 1.0)
    assert len(big.seen) == 50000 and 4 not in big.seen and 5 in big.seen


def test_filter_readd_refreshes_expiry():
    # a re-add moves the expiry: Go keeps the entry until the later one
    f = SearchFilter(-1, 900.0, 1, 5.0)
    f.add(1, 0.0)
    f.add(1, 1.0)
    assert f.is_seen(1, 900.0) and not f.is_seen(1, 901.0)
    f = SearchFilter(-1, 900.0, 1, 5.0)
    f.add(1, 0.0)
    f.add(1, 1.0)
    f.add(2, 2.0)
    assert f.is_seen(2, 901.0) and not f.is_seen(1, 901.0)


def test_filter_readd_same_time_moves_to_back():
    # limit 3, adds 1,2,3,1,4 at one timestamp: the re-added 1 is the newest,
    # so 4 evicts 2, as list.MoveToBack does in Go
    f = SearchFilter(-1, 900.0, 1, 5.0, limit=3)
    for n in (1, 2, 3, 1, 4):
        f.add(n, 0.0)
    assert list(f.seen) == [3, 1, 4]


def test_asked_set_sweeps_once_a_second():
    # askedset.go expire: at most one sweep a second, so an entry may outlive
    # the ttl by up to a second.
    a = AskedSet(900.0)
    a.add(7, 0.5)
    assert a.has(7, 900.4)          # sweep at 900.4: 899.9 < ttl, kept
    assert a.has(7, 900.5)          # ttl reached, but no sweep within the second
    assert a.last_asked(7, 900.6) == 0.5
    assert not a.has(7, 901.4)      # next sweep drops it
    b = AskedSet(900.0)
    b.add(7, 0.5)
    assert not b.has(7, 900.5)      # first lookup after the add sweeps: ttl reached


def test_spare_longest_unasked_first():
    pop = Pop()
    a, b = pop.add(0), pop.add(0)
    s = search(pop)
    s.asked.add(a, 10.0)
    s.asked.add(b, 5.0)
    s.add_nodes(None, [a, b], 20.0)
    assert s.query_target(20.0) == b and s.query_target(20.0) is None


def world_fork(silent_all=False, n=3000, fc=None):
    pop = Population(n, seed=1)
    pop.assign_topics(zipf_shares(1, 1.0), 1.0, True)
    cfg = WorldConfig(cache=5000, depth=10, bucket_size=8, topic_nodes_limit=16, aux_limit=8,
                      yield_floor=8, aux_radius=1, ad_lifetime=900, rtt=0.06)
    silent = np.ones(pop.n, dtype=bool) if silent_all else None
    return World(pop, cfg, 0.06, 1, backoff=60, mix_registrars=5, fork=fc or ForkConfig(), silent=silent)


def test_all_failed_bucket_retried_once_then_stepped_in():
    # nextPass of v5_topic.go: a bucket whose queries all failed is kept for
    # one retry pass, then the search steps in by one.
    w = world_fork(silent_all=True)
    w.at(0.0, "start_search", 5, int(w.pop.topics[5]), 0, 1e9)
    starts = []
    while w.events and len(starts) < 7:
        w.run(w.events[0][0])
        s = w.searchers.get(5)
        if s is not None and s.st is not None and s.st.replies == 0 and (not starts or starts[-1][0] != s.passes):
            starts.append((s.passes, s.st.active))
    s = w.searchers[5]
    assert isinstance(s, ForkSearcherState)
    seq = [a for _, a in starts]
    assert seq[:4] == [0, 0, 1, 1], starts
    assert all(not (seq[j] == seq[j + 1] == seq[j + 2]) for j in range(len(seq) - 2)), starts
    assert all(p[2] <= 32 for p in s.passlog)
    # every failure takes the 0.7 s timeout and the result filter saw nothing
    assert s.queries - s.failures <= 1 and not s.found   # one may be in flight


def test_slow_reply_times_out():
    # every reply is held to the response timeout, not just silent nodes'
    w = world_fork(n=2000)
    w.rtt = 1.0
    w.at(0.0, "start_search", 5, int(w.pop.topics[5]), 0, 60.0)
    w.run(60.0)
    s = w.searchers[5]
    assert s.queries > 30 and s.queries - s.failures <= 1 and not s.found
    w = world_fork(n=2000)
    w.rtt = 0.7
    w.at(0.0, "start_search", 5, int(w.pop.topics[5]), 0, 60.0)
    w.run(60.0)
    assert w.searchers[5].failures == 0


def test_honest_search_finds_ads():
    w = world_fork(n=2000)
    for i in range(w.pop.n):
        w.at(random.Random(i).random() * 300, "start_reg", i, int(w.pop.topics[i]))
    for j, i in enumerate((3, 50, 700)):
        w.at(1200.0 + j, "start_search", i, int(w.pop.topics[i]), 0, 1800.0)
    w.run(1800.0)
    for i in (3, 50, 700):
        s = w.searchers[i]
        assert len(s.found) > 100
        assert all(p[2] <= 32 for p in s.passlog)
        assert s.failures == 0


if __name__ == "__main__":
    for k, v in list(globals().items()):
        if k.startswith("test_"):
            v()
            print("ok", k)


@pytest.mark.parametrize("flags, expected", [([], 1), (["--search-bucket-ip-limit", "1"], 1), (["--search-bucket-ip-limit", "2"], 2)])
def test_cli_search_bucket_ip_limit(monkeypatch, tmp_path, flags, expected):
    from searchsim import world
    class StopBeforeSimulation(Exception):
        pass
    def capture(pop, cfg, rtt, seed, **kwargs):
        assert kwargs["fork"].ip_limit == expected
        assert kwargs["fork"].ad_lifetime == 900
        raise StopBeforeSimulation
    monkeypatch.setattr(world, "World", capture)
    monkeypatch.setattr(sys, "argv", ["world", "--nodes", "8", "--topics", "1", "--fork-faithful", "--out", str(tmp_path)] + flags)
    with pytest.raises(StopBeforeSimulation):
        world.main()


def test_search_bucket_ip_limit_two_enforces_two_and_preserves_other_buckets():
    pop = Pop()
    a, b, c = [pop.add(3, net=7) for _ in range(3)]
    d = pop.add(4, net=7)
    lan = [pop.add(3) for _ in range(3)]
    s = search(pop, ForkConfig(ip_limit=2), active=3)
    s.add_nodes(None, [a, b, c, d] + lan, 0)
    assert a in s.new[3] and b in s.new[3] and c not in s.new[3]
    assert d in s.new[4] and set(lan) <= s.new[3]
    s.remove_node(a, keep_ip=True)
    s.add_nodes(None, [c], 0)
    assert c not in s.new[3]  # answered /24 slot stays used for this pass
    s.remove_node(b, keep_ip=False)
    s.add_nodes(None, [c], 0)
    assert c in s.new[3]  # a failure frees only its slot
