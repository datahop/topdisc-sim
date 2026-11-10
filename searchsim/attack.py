"""Synthetic registrars for the isolated, discrete-event search experiment.

Registrar indices are negative odd integers; fake advertiser indices are
negative even integers. A minted registrar has a real, ground 64-bit ID,
so its topic bucket and its DHT bucket obey the same XOR geometry as honest
nodes. No synthetic node participates in registration.
"""
import random
from collections import deque

from .population import DHT_BUCKET_SIZE, DHT_MIN_DIST, logdist


class SubnetLedger:
    """W1: a sliding ad-lifetime window of query times for each public /24.

    Rejected admissions do not refresh the window. LAN (-1) is exempt.
    Nodes already admitted may still be queried. The ledger gates subsequent
    admission, across all buckets and passes, rather than cancelling queries.
    """

    def __init__(self, ttl=900.0, limit=1):
        self.ttl, self.limit, self.queries = ttl, limit, {}

    def allows(self, net, now):
        if net == -1:
            return True
        q = self.queries.get(net)
        if q is None:
            return True
        while q and q[0] + self.ttl <= now:
            q.popleft()
        if not q:
            del self.queries[net]
            return True
        return len(q) < self.limit

    def record(self, net, now):
        if net != -1:
            self.allows(net, now)
            self.queries.setdefault(net, deque()).append(now)


class AdmissionLedger:
    """W1 prime: distinct admitted IDs per public /24, across passes.

    Each ID expires from its first admission, without refresh on re-admission.
    Match AskedSet's at-most-once-per-second expiry sweep. LAN is exempt.
    Checking a rejected candidate never reserves a slot. Event times are
    monotonic, so an expiry queue avoids scanning every subnet at each sweep.
    """

    def __init__(self, ttl=900.0, limit=1):
        self.ttl, self.limit = ttl, limit
        self.admitted = {}
        self.expiry = deque()
        self.expired = 0.0

    def expire(self, now):
        if now - self.expired < 1.0 and self.admitted:
            return
        self.expired = now
        while self.expiry and now - self.expiry[0][0] >= self.ttl:
            _, net, node = self.expiry.popleft()
            ids = self.admitted[net]
            del ids[node]
            if not ids:
                del self.admitted[net]

    def allows(self, net, node, now):
        if net == -1:
            return True
        self.expire(now)
        ids = self.admitted.get(net, {})
        return node in ids or len(ids) < self.limit

    def record(self, net, node, now):
        if net != -1:
            self.expire(now)
            ids = self.admitted.setdefault(net, {})
            if node not in ids:
                ids[node] = now
                self.expiry.append((now, net, node))


class Attacker:
    def __init__(self, strategy, nets, fc, seed):
        self.strategy, self.nets, self.fc = strategy, nets, fc
        self.rng = random.Random(seed + 1701)
        self.mint = {}             # registrar index -> (topic bucket, /24)
        self.ids = {}              # registrar index -> actual ground node ID
        self.serial = self.ads = self.referral_net = 0
        self.round_robin = {}
        self.by_searcher = {}

    def node(self, tid, bucket, net, owner=None):
        self.serial += 1
        i = -(2 * self.serial - 1)
        shift = 63 - bucket
        node_id = tid ^ ((1 << shift) | self.rng.getrandbits(shift))
        self.mint[i] = (bucket, net)
        self.ids[i] = node_id
        if owner is not None:
            self.by_searcher.setdefault(owner.node, []).append(i)
        return i

    def advertisements(self, count):
        out = [-2 * (self.ads + j + 1) for j in range(count)]
        self.ads += count
        return out

    def seeds(self, searcher):
        """Merge fresh attacker candidates with the searcher's honest DHT.

        One candidate per owned /24 and topic bucket is offered each pass.
        A shuffled merge admits at most 16 entries per DHT bucket, 2 per
        /24 per DHT bucket and 10 per /24 per table. This is an explicit
        attacker foothold model; it does not simulate DHT poisoning.
        """
        # Only the current pass can refer to these ground-ID records. Old
        # asked/filter entries retain their numeric IDs, which never repeat;
        # observations retain numeric source IDs without consulting geometry.
        for i in self.by_searcher.pop(searcher.node, []):
            del self.mint[i]
            del self.ids[i]
        tid, pop = searcher.tid, searcher.w.pop
        candidates = [self.node(tid, b, net, owner=searcher) for b in range(self.fc.search_depth)
                      for net in range(self.nets)]
        merged = list(searcher.seeds) + candidates
        self.rng.shuffle(merged)
        counts, subnets, bucket_subnets, out = {}, {}, {}, []
        own = int(pop.ids[searcher.node])
        for i in merged:
            node_id = self.ids[i] if i < 0 else int(pop.ids[i])
            net = self.mint[i][1] if i < 0 else searcher.w.public_net(i)
            b = max(logdist(own, node_id), DHT_MIN_DIST + 1)
            if counts.get(b, 0) >= DHT_BUCKET_SIZE:
                continue
            if net >= 0 and (subnets.get(net, 0) >= 10 or bucket_subnets.get((b, net), 0) >= 2):
                continue
            counts[b] = counts.get(b, 0) + 1
            if net >= 0:
                subnets[net] = subnets.get(net, 0) + 1
                bucket_subnets[b, net] = bucket_subnets.get((b, net), 0) + 1
            out.append(i)
        return out

    def infer_active(self, dists):
        # The requested band is truncated at the edges and can have holes
        # when buckets are full. Do not inspect the searcher's hidden active.
        buckets = sorted(256 - d for d in dists)
        if not buckets:
            return None
        if len(buckets) >= 3:
            return (buckets[0] + buckets[-1]) // 2
        if buckets[0] == 0:
            return 0
        if buckets[-1] == self.fc.search_depth - 1:
            return self.fc.search_depth - 1
        return (buckets[0] + buckets[-1]) // 2

    def query(self, searcher, dst, dists):
        fc, strategy = self.fc, self.strategy
        if strategy == 'A5' and self.mint[dst][1] >= self.nets:
            # Arbitrary public addresses are valid referral records, not
            # extra source subnets controlled by the k-subnet attacker.
            return fc.resp_timeout, [], [], True
        inferred = self.infer_active(dists)
        if strategy == 'A3' and inferred == fc.search_depth - 1:
            searcher.attack_pinned = True
        count = 11 if strategy == 'A2' or (strategy == 'A3' and searcher.attack_pinned) else 16
        if strategy == 'A3' and not searcher.attack_pinned or strategy == 'A5':
            count = 0
        buckets = [256 - d for d in dists[:searcher.w.cfg.aux_limit]]
        if strategy == 'A3':
            # Malicious ENRs need not match requested distances. Respect the
            # wire's 16-record aux cap and receiver's one-per-bucket rule.
            start = inferred if inferred is not None else self.mint[dst][0]
            buckets = list(range(start, fc.search_depth))[:fc.topic_nodes_limit]
        elif strategy == 'A5':
            # Unrequested distances are accepted, but only one node from a
            # given bucket per response can enter the receiver's table.
            start = inferred if inferred is not None else self.mint[dst][0]
            buckets = [(start + j) % fc.search_depth for j in range(fc.topic_nodes_limit)]
        aux = []
        for b in buckets:
            if strategy == 'A5':
                self.referral_net += 1
                net = self.nets + searcher.w.pop.n + self.referral_net
            else:
                turn = self.round_robin.get(b, 0)
                net = turn % self.nets
                self.round_robin[b] = turn + 1
            aux.append(self.node(searcher.tid, b, net, owner=searcher))
        delay, failed = searcher.w.rtt, strategy == 'A5'
        if strategy == 'A5':
            # One aux packet arrives at RTT; a missing final packet times
            # out 700 ms later. This is a responsive partial reply in geth.
            delay += fc.resp_timeout
        elif strategy == 'A6':
            # Two packets separated by 0.69 s (<700 ms). topicSearch sees
            # them together only when the complete query returns.
            delay += fc.resp_timeout - 0.01
        return delay, self.advertisements(count), aux, failed
