"""Population, DHT tables and ad placement for the search-load simulator.

Node IDs are 64-bit integers kept sorted, so every "nodes at log-distance d
from X" set, and every subtree of the ID space, is one contiguous slice of
the array: a random node in it is a bisect plus a draw. Nothing is stored
per node except what a search touches.
"""
import hashlib
import json
import math

import numpy as np

BITS = 64
DHT_BUCKETS = 17        # go-ethereum: nBuckets = 256/15
DHT_BUCKET_SIZE = 16
DHT_MIN_DIST = 256 - DHT_BUCKETS   # log-distances at or below this share bucket 0


def logdist(a, b):
    """discv5 LogDist for 64-bit ids, in 256-bit terms (0..256)."""
    x = int(a) ^ int(b)
    return 256 - BITS + x.bit_length() if x else 0


class Population:
    def __init__(self, n, seed=1, bootnodes=0, spawn_bias=True):
        self.n = n
        self.bootnodes = bootnodes
        self.spawn_bias = spawn_bias
        self.rng = np.random.default_rng(seed)
        ids = self.rng.integers(0, 2**63, size=n, dtype=np.int64).astype(np.uint64) * np.uint64(2)
        ids |= self.rng.integers(0, 2, size=n, dtype=np.uint64)
        ids = np.unique(ids)
        while len(ids) < n:
            more = self.rng.integers(0, 2**63, size=n - len(ids), dtype=np.int64).astype(np.uint64) * np.uint64(2)
            ids = np.unique(np.concatenate([ids, more]))
        self.ids = np.sort(ids[:n])
        # spawn order: the node at sorted position p started as the spawn[p]-th node.
        # discv5 tables fill from the nodes that exist when a node starts and never
        # evict a live entry, so early nodes end up in far more tables (measured on
        # the 10k runs: load falls 20x from the first ten nodes to the last half).
        self.spawn = self.rng.permutation(n)
        # table-membership weight by spawn order, fitted on the 10k runs (load falls
        # 2x per spawn-index quintile, 20x end to end): w = (spawn + 20) ** -0.5
        self.weight = (self.spawn.astype(np.float64) + 20.0) ** -0.5
        self.cw = np.concatenate([[0.0], np.cumsum(self.weight)])
        self.topics = None      # int32 per node, -1 = none
        self.registrant = None  # bool per node
        self.topic_ids = None   # uint64 per topic

    # --- subtrees -------------------------------------------------------
    def _only_self(self, x):
        i = int(np.searchsorted(self.ids, np.uint64(x)))
        return (i, i + 1) if i < self.n and self.ids[i] == np.uint64(x) else (i, i)

    def _range(self, lo_int, size):
        hi_int = lo_int + size
        lo = int(np.searchsorted(self.ids, np.uint64(lo_int)))
        hi = self.n if hi_int >= 2**BITS else int(np.searchsorted(self.ids, np.uint64(hi_int)))
        return lo, hi

    def subtree(self, x, d):
        """Indices [lo, hi) of nodes at log-distance exactly d from x (256-bit
        terms; d = 256 is the half of the space whose top bit differs)."""
        if d <= 256 - BITS:
            return self._only_self(x)
        shift = d - (256 - BITS) - 1          # the differing bit
        x = int(x)
        prefix = (x >> (shift + 1)) << (shift + 1)
        flipped = prefix | ((1 ^ ((x >> shift) & 1)) << shift)
        return self._range(flipped, 1 << shift)

    def subtree_within(self, x, d):
        """Indices of nodes at log-distance <= d from x: the subtree sharing
        the top 256-d bits with x."""
        if d <= 256 - BITS:
            return self._only_self(x)
        shift = d - (256 - BITS)
        x = int(x)
        prefix = (x >> shift) << shift
        return self._range(prefix, 1 << shift)

    def sample_bucket(self, lo, hi, k, rng, s, exclude=None):
        """k members of a DHT bucket over the node range [lo, hi), drawn with
        probability proportional to the node's table-membership weight (early
        spawners first), by bisecting the prefix sums of the weights."""
        m = hi - lo
        if m <= 0:
            return np.empty(0, dtype=np.int64)
        if not self.spawn_bias:
            return self.sample_range(lo, hi, k, rng, exclude)
        if m <= k:
            out = np.arange(lo, hi)
            return out[out != exclude] if exclude is not None else out
        a, b = self.cw[lo], self.cw[hi]
        picks = set()
        tries = 0
        while len(picks) < k and tries < 20 * k:
            u = a + rng.random() * (b - a)
            c = int(np.searchsorted(self.cw, u, side="right")) - 1
            c = min(max(c, lo), hi - 1)
            tries += 1
            if c != exclude:
                picks.add(c)
        return np.fromiter(picks, dtype=np.int64)

    def sample_range(self, lo, hi, k, rng, exclude=None):
        m = hi - lo
        if m <= 0:
            return np.empty(0, dtype=np.int64)
        if exclude is not None and lo <= exclude < hi:
            pool = np.concatenate([np.arange(lo, exclude), np.arange(exclude + 1, hi)])
            if len(pool) <= k:
                return pool
            return rng.choice(pool, size=k, replace=False)
        if m <= k:
            return np.arange(lo, hi)
        return lo + rng.choice(m, size=k, replace=False)

    # --- DHT table ------------------------------------------------------
    def dht_table(self, i):
        """The discv5 table of node i: up to 16 random nodes per log-distance
        bucket (17 buckets, the closest one for every distance at or below
        240). Deterministic per node, as a settled table would be."""
        x = int(self.ids[i])
        rng = np.random.default_rng(hash((int(x), 0xd47)) & 0xffffffff)
        out = []
        s = int(self.spawn[i])
        for d in range(DHT_MIN_DIST + 2, 257):        # 241..256, one bucket each
            lo, hi = self.subtree(x, d)
            out.append(self.sample_bucket(lo, hi, DHT_BUCKET_SIZE, rng, s))
        lo, hi = self.subtree_within(x, DHT_MIN_DIST + 1)  # <= 240: bucket 0
        out.append(self.sample_bucket(lo, hi, DHT_BUCKET_SIZE, rng, s, exclude=i))
        table = np.concatenate(out)
        if self.bootnodes and i > 0:
            # The nodes a node bootstrapped from stay in its table: simnet gives
            # node i up to `bootnodes` random earlier nodes, so the earliest nodes
            # sit in most tables; a real network has its bootnodes and long-lived
            # peers the same way. They take the place of random entries in their
            # buckets, keeping bucket sizes as they are.
            boots = rng.choice(i, size=min(self.bootnodes, i), replace=False)
            keep = np.ones(len(table), dtype=bool)
            tb = np.array([logdist(x, int(self.ids[j])) for j in table])
            for j in boots:
                d = logdist(x, int(self.ids[j]))
                same = np.nonzero(keep & (tb == d) & (table != j))[0]
                if len(same) >= DHT_BUCKET_SIZE:
                    keep[same[rng.integers(len(same))]] = False
            table = np.concatenate([table[keep], boots[~np.isin(boots, table)]])
        return table

    # --- topics ---------------------------------------------------------
    def assign_topics(self, shares, register_frac=1.0, all_register=False):
        k = len(shares)
        cdf = np.cumsum(shares) / np.sum(shares)
        draws = self.rng.random(self.n)
        self.topics = np.searchsorted(cdf, draws).astype(np.int32)
        self.topics = np.minimum(self.topics, k - 1)
        if all_register:
            self.registrant = np.ones(self.n, dtype=bool)
        else:
            self.registrant = self.rng.random(self.n) < register_frac
        self.topic_ids = np.array([int.from_bytes(hashlib.sha256(b"topic-%d" % t).digest()[:8], "big")
                                   for t in range(k)], dtype=np.uint64)
        return self.topics

    def topic_members(self, t):
        return np.nonzero((self.topics == t) & self.registrant)[0]


def shares_from_model(path, k):
    m = json.load(open(path))
    topics = m["topics"]
    if k <= 0 or k > len(topics):
        k = len(topics)
    out = [t["share"] for t in topics[:k - 1]]
    out.append(max(0.0, 1 - sum(out)))
    return out


def zipf_shares(k, s):
    w = np.array([1 / (i + 1) ** s for i in range(k)])
    return list(w / w.sum())


# --- search-table geometry --------------------------------------------------
def bucket_index(topic_id, node_id, depth):
    """Search/registration bucket of node for topic: 0 the farthest half,
    depth-1 the closest neighbourhood."""
    d = 256 - logdist(topic_id, node_id)
    return min(d, depth - 1)


class Ads:
    """Steady-state ad placement: every registrant holds `copies` ads in each
    of `depth` rings around its topic's hash, on registrars drawn uniformly
    within the ring (the registration walk's mixing is assumed complete),
    subject to the registrar's cache capacity."""

    def __init__(self, pop, depth=10, copies=5, cache=5000, seed=2):
        self.pop = pop
        self.depth, self.copies, self.cache = depth, copies, cache
        rng = np.random.default_rng(seed)
        n = pop.n
        held = np.zeros(n, dtype=np.int32)
        # per registrar: dict topic -> list of advertiser indices (built via arrays)
        reg_idx, reg_topic, reg_adv = [], [], []
        for t in range(len(pop.topic_ids)):
            members = pop.topic_members(t)
            if len(members) == 0:
                continue
            tid = int(pop.topic_ids[t])
            for b in range(depth):
                lo, hi = self.ring(tid, b)
                if hi - lo == 0:
                    continue
                want = len(members) * copies
                cand = lo + rng.integers(0, hi - lo, size=want)
                # capacity: registrars over the cache drop the ad (advertiser retries elsewhere,
                # modelled as: dropped)
                cnt = np.bincount(cand, minlength=n)
                over = held[cand] + cnt[cand] > cache
                cand = cand[~over]
                adv = np.repeat(members, copies)[~over]
                np.add.at(held, cand, 1)
                reg_idx.append(cand); reg_topic.append(np.full(len(cand), t, dtype=np.int32)); reg_adv.append(adv)
        self.reg = np.concatenate(reg_idx); self.topic = np.concatenate(reg_topic); self.adv = np.concatenate(reg_adv)
        order = np.lexsort((self.topic, self.reg))
        self.reg, self.topic, self.adv = self.reg[order], self.topic[order], self.adv[order]
        # index: (registrar, topic) -> slice
        key = self.reg.astype(np.int64) * (len(pop.topic_ids) + 1) + self.topic
        self.key = key
        self.held = held

    def ring(self, tid, b):
        """Node index range of search bucket b around topic id tid."""
        if b < self.depth - 1:
            d = 256 - b
            return self.pop.subtree(tid, d)
        return self.pop.subtree_within(tid, 256 - (self.depth - 1))

    def ads_at(self, registrar, t):
        k = int(registrar) * (len(self.pop.topic_ids) + 1) + t
        lo = np.searchsorted(self.key, k); hi = np.searchsorted(self.key, k + 1)
        return self.adv[lo:hi]
