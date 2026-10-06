"""A registrar returns aux nodes at the distances the request names."""
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from searchsim.population import Population, zipf_shares  # noqa: E402
from searchsim.world import World, WorldConfig  # noqa: E402


def dist(a, b):
    return 256 - 64 + (int(a) ^ int(b)).bit_length()


def test_aux_nodes_at_wanted_distance():
    pop = Population(5000, seed=1)
    pop.assign_topics(zipf_shares(1, 1.0), 0.5, True)
    cfg = WorldConfig(cache=5000, depth=10, bucket_size=16, topic_nodes_limit=16, aux_limit=8,
                      yield_floor=4, aux_radius=0, ad_lifetime=900, rtt=0.06)
    w = World(pop, cfg, 0.06, 1)
    tid = 0x5A5A5A5A5A5A5A5A
    want = [256, 255, 254, 253, 252, 251, 250, 249]
    rng = random.Random(7)
    checked = 0
    for _ in range(200):
        r = rng.randrange(pop.n)
        _, ids = w.table(r)
        have = {dist(i, tid) for i in ids}
        served = [d for d in want if d in have]
        out = w.aux(r, tid, want)
        assert len(out) == len(served)
        for d, n in zip(served, out):
            assert dist(pop.ids[n], tid) == d
            checked += 1
    assert checked > 500


if __name__ == "__main__":
    test_aux_nodes_at_wanted_distance()
    print("ok")
