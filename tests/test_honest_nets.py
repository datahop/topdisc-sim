import csv
import json
from collections import Counter
from types import SimpleNamespace

import numpy as np
import pytest

from searchsim.honest_nets import HONEST_NET_BASE, assign_nets, load_crawl, topic_chains, search_observations
from searchsim.population import Population, DHT_MIN_DIST, logdist
from searchsim.search import AskedSet, ForkConfig, ForkSearch
from searchsim.world import World, WorldConfig
import random


@pytest.fixture
def crawl(tmp_path):
    rows = []
    sessions = []
    for chain, prefixes in (("A", [1, 1, 1, 1, 2, 3]), ("B", [1, 4, 4, 5])):
        for i, prefix in enumerate(prefixes):
            node = f"{chain}{i}"
            rows.append(dict(id=node, chain=chain, ip=f"8.8.{prefix}.{i + 1}"))
            sessions.append(dict(id=node, chain=chain, start=2, end="9" if i == 0 else ""))
    rows += [dict(id="v6", chain="A", ip="2001:4860::1"), dict(id="dead", chain="A", ip="1.1.1.1")]
    sessions += [dict(id="v6", chain="A", start=0, end=""), dict(id="missing", chain="A", start=0, end=""),
                 dict(id="A0", chain="B", start=0, end=1)]
    for name, fields, values in (("nodes.csv", ["id", "chain", "ip"], rows),
                                  ("sessions.csv", ["id", "chain", "start", "end"], sessions)):
        with (tmp_path / name).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(values)
    return tmp_path


def test_crawl_isolated_ipv4_alive_union_latest_chain_and_prefixes(crawl):
    data = load_crawl("crawl:" + str(crawl))
    assert data["reader_isolated"]
    assert data["ipv4_nodes"] == 10 and data["distinct_nets"] == 5
    assert [(x["chain"], x["nodes"], x["distinct_nets"]) for x in data["chains"]] == [("A", 6, 3), ("B", 4, 3)]
    assert data["shared_node_fraction"] == .7
    assert set(data["source_sha256"]) == {"sessions.csv", "nodes.csv"}


def test_exact_chain_multisets_and_cross_topic_subnet_identity(crawl):
    data = load_crawl("crawl:" + str(crawl))
    pop = SimpleNamespace(n=10, topics=np.array([0] * 6 + [1] * 4))
    nets, mapping = assign_nets(pop, data, ["A", "B"], 1)
    for topic, chain in enumerate(data["chains"]):
        expected = Counter({HONEST_NET_BASE + prefix: count for prefix, count in chain["prefixes"]})
        assert Counter(nets[pop.topics == topic]) == expected
    assert len(set(nets[:6]) & set(nets[6:])) == 1
    assert mapping["assigned"]["distinct_nets"] == 5 and np.all(nets >= HONEST_NET_BASE)


def test_scaling_rounding_reproducibility_and_protocol_rng_untouched(crawl):
    data = load_crawl("crawl:" + str(crawl))
    pop = Population(23, seed=1)
    pop.assign_topics([.6, .4], all_register=True)
    state = json.dumps(pop.rng.bit_generator.state)
    a, meta = assign_nets(pop, data, ["A", "B"], 1)
    b, again = assign_nets(pop, data, ["A", "B"], 1)
    assert np.array_equal(a, b) and meta == again
    assert json.dumps(pop.rng.bit_generator.state) == state
    assert len(a) == 23 and sum(t["nodes"] for t in meta["topics"]) == 23
    for topic in meta["topics"]:
        assert sum(n for _, n in topic["groups"]) == topic["nodes"]


def test_named_model_mapping_and_unnamed_chain_rank(crawl, tmp_path):
    data = load_crawl("crawl:" + str(crawl))
    model = tmp_path / "model.json"
    model.write_text(json.dumps(dict(topics=[dict(chain="B", share=.4), dict(chain="A", share=.6)])))
    assert topic_chains(str(model), 2, data)[0] == ["B", "A"]
    assert topic_chains("", 2, data)[0] == ["A", "B"]
    with pytest.raises(ValueError):
        topic_chains("", 3, data)


def test_unnamed_topic_whole_groups_preserve_concentration_when_scaled(crawl):
    data = load_crawl("crawl:" + str(crawl))
    pop = SimpleNamespace(n=600, topics=np.zeros(600, dtype=int))
    nets, meta = assign_nets(pop, data, ["A"], 1, whole_groups=True)
    assert meta["topics"][0]["distinct_net_ratio"] == .5
    assert sorted(Counter(nets).values()) == [1] * 200 + [4] * 100


def test_honest_public_nets_enforce_frozen_dht_and_search_ip_limits(crawl):
    data = load_crawl("crawl:" + str(crawl))
    pop = Population(200, seed=1)
    pop.assign_topics([1], all_register=True)
    nets, _ = assign_nets(pop, data, ["A"], 1)
    world = World(pop, WorldConfig(), .06, 1, fork=ForkConfig(), attack="A3", attack_nets=2, honest_nets=nets)
    assert world.net is pop.net and np.array_equal(world.net, nets)
    table = world.dht(0)
    assert max(Counter(world.net[table]).values(), default=0) <= 10
    buckets = Counter((max(logdist(pop.ids[0], pop.ids[i]), DHT_MIN_DIST + 1), int(world.net[i])) for i in table)
    assert max(buckets.values(), default=0) <= 2
    st = ForkSearch(world.fork, pop, int(pop.topic_ids[0]), 0, random.Random(1), AskedSet(900), AskedSet(900))
    st.add_nodes(None, list(range(1, pop.n)), 0)
    for bucket in st.new:
        assert len({world.public_net(i) for i in bucket}) == len(bucket)
    assert world.public_net(1) not in (0, 1)


def test_honest_latency_observation_excludes_sybil_and_self():
    s = SimpleNamespace(node=0, topic=0, order=[(-2, 1, .1, 1), (0, 1, .2, 2), (1, 1, 1., 3), (2, 1, 2., 4)])
    world = SimpleNamespace(searchers={0: s}, pop=SimpleNamespace(topics=np.array([0, 0, 0])))
    row = dict(searcher=0, reach=2, recall=1., t50=.1, t90=.2)
    study = dict(pass_queries=4., pass_sybil_query_share=.25, sybil_query_share=.25, passes_with_zero_honest_queries=0)
    result = search_observations(world, [row], [study], dict(topics=[dict(chain="A", distinct_net_ratio=.05)]))[0]
    assert result["t50"] == 1 and result["t90"] == 2 and result["ratio_bin"] == "<10%"
