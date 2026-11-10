"""Crawl-derived population input; does not change protocol decisions.

Raw crawl CSVs are read only by this file's isolated ``python -I`` entry point.
Prefix equality is preserved across chains. Model labels reserve a separate
positive namespace from attacker-owned and fabricated-referral subnet labels.
"""
import csv
import hashlib
import ipaddress
import json
import math
import subprocess
import sys
from collections import Counter
from pathlib import Path

import numpy as np

DEFAULT_CRAWL = Path.home() / "workspace/datahop/runs/2026-09-18-crawl24h-aws/derived/gap10"
HONEST_NET_BASE = 1 << 40


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        while block := f.read(1024 * 1024):
            h.update(block)
    return h.hexdigest()


def read_crawl(path):
    """One latest-chain label per ID observed alive at any time in the crawl."""
    path = Path(path).expanduser().resolve()
    latest = {}
    with (path / "sessions.csv").open(newline="") as f:
        for row in csv.DictReader(f):
            stamp = float(row["start"])
            if row["id"] not in latest or stamp >= latest[row["id"]][0]:
                latest[row["id"]] = (stamp, row["chain"])
    addresses = {}
    with (path / "nodes.csv").open(newline="") as f:
        for row in csv.DictReader(f):
            if row["id"] not in latest:
                continue
            try:
                address = ipaddress.ip_address(row["ip"])
            except ValueError:
                continue
            if address.version == 4:
                addresses[row["id"]] = int(address) >> 8
    chains = {}
    total = Counter(addresses.values())
    for node, prefix in addresses.items():
        chains.setdefault(latest[node][1], Counter())[prefix] += 1
    if not total:
        raise ValueError("crawl has no alive IPv4 nodes")
    entries = [dict(chain=chain, nodes=sum(counts.values()), distinct_nets=len(counts),
                    prefixes=[[prefix, count] for prefix, count in sorted(counts.items())])
               for chain, counts in chains.items()]
    entries.sort(key=lambda row: (-row["nodes"], row["chain"]))
    return dict(path=str(path), alive_definition="ID has any session; latest session start selects chain",
                reader_isolated=bool(sys.flags.isolated),
                ipv4_nodes=sum(total.values()), distinct_nets=len(total),
                shared_node_fraction=sum(n for n in total.values() if n > 1) / sum(total.values()),
                above50_node_fraction=sum(n for n in total.values() if n > 50) / sum(total.values()),
                source_sha256={name: digest(path / name) for name in ("sessions.csv", "nodes.csv")},
                chains=entries)


def load_crawl(spec):
    if spec != "crawl" and not spec.startswith("crawl:"):
        raise ValueError("--honest-nets must be crawl or crawl:path")
    path = spec.partition(":")[2] or str(DEFAULT_CRAWL)
    result = subprocess.run([sys.executable, "-I", str(Path(__file__).resolve()), str(Path(path).expanduser())],
                            check=True, capture_output=True, text=True)
    data = json.loads(result.stdout)
    if not data["reader_isolated"]:
        raise ValueError("crawl reader must run with Python -I")
    return data


def topic_chains(model, count, data):
    """Use named model topics; unnamed topics map to chains by population rank."""
    known = {row["chain"] for row in data["chains"]}
    if model:
        topics = json.loads(Path(model).read_text())["topics"][:count]
        names = [row.get("chain", row.get("name")) for row in topics]
        if len(names) == count and all(name in known for name in names):
            return names, "explicit model topic -> crawl chain"
    if count > len(data["chains"]):
        raise ValueError("more unnamed simulation topics than crawl chains; supply an explicit chain model")
    return [row["chain"] for row in data["chains"][:count]], "population-rank topic -> crawl chain"


def assign_nets(pop, data, chains, seed, whole_groups=False):
    """Scale each chain's complete /24 groups with largest-remainder rounding.

    Sampling individual nodes with replacement would lose many singleton /24s
    even at scale 1. Whole-group count apportionment instead preserves the exact
    multiset at scale 1 and avoids that concentration bias. Seeded ties and node
    permutation use an independent RNG, leaving population/protocol RNGs intact.
    """
    by_chain = {row["chain"]: row for row in data["chains"]}
    nets = np.full(pop.n, -1, dtype=np.int64)
    topics = []
    for topic, chain in enumerate(chains):
        source = by_chain[chain]
        members = np.flatnonzero(pop.topics == topic)
        prefixes = np.array([p for p, _ in source["prefixes"]], dtype=np.int64)
        weights = np.array([n for _, n in source["prefixes"]], dtype=np.int64)
        rng = np.random.default_rng([seed, 0x24, topic])
        if whole_groups:
            # Unnamed topics have no real-chain identity. Resample complete
            # group templates, using a new subnet-copy namespace each cycle;
            # truncate only the last group to meet the exact population size.
            pieces, remaining, cycle = [], len(members), 0
            while remaining:
                for j in rng.permutation(len(prefixes)):
                    take = min(int(weights[j]), remaining)
                    pieces.append(np.full(take, HONEST_NET_BASE + cycle * (1 << 24) + prefixes[j], dtype=np.int64))
                    remaining -= take
                    if not remaining:
                        break
                cycle += 1
            assigned = np.concatenate(pieces) if pieces else np.empty(0, dtype=np.int64)
        else:
            # Integer arithmetic makes apportionment exact and reproducible.
            numerators = weights * len(members)
            counts = numerators // source["nodes"]
            remainders = numerators % source["nodes"]
            tie = rng.permutation(len(prefixes))
            order = tie[np.argsort(-remainders[tie], kind="stable")]
            counts[order[:len(members) - int(counts.sum())]] += 1
            assigned = np.repeat(prefixes + HONEST_NET_BASE, counts)
        rng.shuffle(assigned)
        nets[members] = assigned
        encoded, actual_counts = np.unique(assigned - HONEST_NET_BASE, return_counts=True)
        distinct = len(encoded)
        ratio = distinct / len(members) if len(members) else None
        topics.append(dict(topic=topic, chain=chain, nodes=len(members), distinct_nets=distinct,
                           distinct_net_ratio=ratio, source_nodes=source["nodes"],
                           source_distinct_nets=source["distinct_nets"],
                           source_distinct_net_ratio=source["distinct_nets"] / source["nodes"],
                           groups=[[int(p), int(n)] for p, n in zip(encoded, actual_counts)]))
    if np.any(nets < 0):
        raise ValueError("every honest node must have a mapped public /24")
    _, sizes = np.unique(nets, return_counts=True)
    method = "whole-/24-group templates, shuffled cycles and truncated final group" if whole_groups else "chain-conditional fixed-prefix largest-remainder scaling"
    return nets, dict(method=method + "; independent seeded shuffle",
                      group_encoding="net = namespace_base + copy * 2^24 + source IPv4 /24 prefix; named-chain mapping uses copy 0",
                      namespace_base=HONEST_NET_BASE, seed=seed, source=data, topics=topics,
                      assigned=dict(nodes=pop.n, distinct_nets=len(sizes),
                                    shared_node_fraction=float(sizes[sizes > 1].sum() / pop.n),
                                    above50_node_fraction=float(sizes[sizes > 50].sum() / pop.n)),
                      assignment_sha256=hashlib.sha256(nets.tobytes()).hexdigest())


def spread_ip(i):
    """A distinct scored IPv4 address per index (Knuth multiplicative hash)."""
    from .go_registrar import scored_ip
    ip = ((i + 1) * 2654435761) & 0xffffffff
    while not scored_ip(ip):
        ip = ((ip + 1) * 2654435761) & 0xffffffff
    return ip


def ipv4_of_labels(net, n, seed):
    """IPv4 address per node from its /24 label. A crawl label keeps its real
    /24; copy c > 0 of a template XORs a seeded 12-bit mask into the low bits
    of the prefix, so the top 12 bits (all the IP tree reads at <= 5000 ads)
    stay the crawl's. Hosts number 1..254 by node order within a label.
    Without a label a node gets a spread_ip address; a legacy small label
    gets a hashed /24 shared by its nodes."""
    ips = np.zeros(n, dtype=np.int64)
    if net is None:
        net = np.full(n, -1, dtype=np.int64)
    net = np.asarray(net, dtype=np.int64)
    masks = np.concatenate([[0], np.random.default_rng([seed, 0x1b]).permutation(np.arange(1, 1 << 12))])
    rank = {}
    for i in range(n):
        label = int(net[i])
        if label < 0:
            ips[i] = spread_ip(i)
            continue
        host = 1 + rank.get(label, 0) % 254
        rank[label] = rank.get(label, 0) + 1
        if label >= HONEST_NET_BASE:
            copy, prefix = divmod(label - HONEST_NET_BASE, 1 << 24)
            prefix ^= int(masks[copy % len(masks)])
        else:
            prefix = spread_ip(label + n) >> 8
        ips[i] = (prefix << 8) | host
    return ips


def honest_times(world, s):
    """Delivery times of the honest members of the searcher's topic."""
    pop = world.pop
    return [time for node, _, time, _ in s.order
            if node >= 0 and node != s.node and int(pop.topics[node]) == s.topic]


def honest_reached(times, reach, fraction):
    needed = math.ceil(fraction * reach)
    return 0.0 if not needed else times[needed - 1] if len(times) >= needed else None


def search_observations(world, rows, study_rows, assignment):
    """Honest-only recall times, including in attack runs; observation only."""
    out = []
    for row, study in zip(rows, study_rows):
        s = world.searchers[row["searcher"]]
        reach = row["reach"]
        times = honest_times(world, s)
        reached = lambda fraction: honest_reached(times, reach, fraction)
        topic = assignment["topics"][s.topic]
        ratio = topic["distinct_net_ratio"]
        out.append(dict(searcher=s.node, topic=s.topic, chain=topic["chain"], reach=reach,
                        distinct_net_ratio=ratio, ratio_bin="<10%" if ratio < .1 else "10–50%" if ratio <= .5 else ">50%",
                        recall=row["recall"], t50=reached(.5), t90=reached(.9),
                        legacy_t50=row["t50"], legacy_t90=row["t90"],
                        queries_per_pass=study["pass_queries"],
                        pass_sybil_query_share=study["pass_sybil_query_share"],
                        sybil_query_share=study["sybil_query_share"],
                        zero_honest_passes=study["passes_with_zero_honest_queries"]))
    return out


if __name__ == "__main__":
    print(json.dumps(read_crawl(sys.argv[1])))
