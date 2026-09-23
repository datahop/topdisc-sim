"""Search-load simulator: where topic queries land, for populations simnet
cannot afford.

    python3 -m sim.searchsim.run --nodes 100000 --topics 100 \
        --model scenarios/models/crawl-2026-09-18.json --floor 4 --out runs/sim-100k

A sample of searchers per topic runs the real searcher logic against the
steady-state ad placement; registrar load is scaled by the topic's searcher
count, so the per-registrar distribution is exact up to sampling noise.
"""
import argparse
import json
import os
import time

import numpy as np

from .population import Population, Ads, shares_from_model, zipf_shares, bucket_index
from .search import Config, Registrars, Searcher

CLASSES = (("1000+", 1000, 10**9), ("100-999", 100, 999), ("10-99", 10, 99), ("<10", 0, 9))


def size_class(n):
    for name, lo, hi in CLASSES:
        if lo <= n <= hi:
            return name
    return "<10"


def write_figures(a, q, near, per_search, regs):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    label = f"{a.nodes} nodes, {a.topics} topics, floor {a.floor}" + (f", aux radius {a.aux_radius}" if a.aux_radius else "")
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.4), constrained_layout=True)
    ax = axes[0]
    qs = np.sort(q[q > 0]); ax.plot(qs, np.arange(1, len(qs) + 1) / len(qs))
    ax.set_xscale("log"); ax.set_xlabel("queries served per registrar"); ax.set_ylabel("CDF over registrars"); ax.grid(alpha=.3)
    ax.set_title(f"median {np.median(q):.0f}, p99 {np.percentile(q, 99):.0f}, max {q.max():.0f}", fontsize=9)
    ax = axes[1]
    xs = np.arange(a.depth); means = [q[near == b].mean() if (near == b).any() else 0 for b in xs]; maxs = [q[near == b].max() if (near == b).any() else 0 for b in xs]
    ax.bar(xs - 0.2, means, 0.4, label="mean"); ax.bar(xs + 0.2, maxs, 0.4, label="max"); ax.set_yscale("log")
    ax.set_xlabel("registrar's bucket to its nearest topic (0 far, %d closest)" % (a.depth - 1)); ax.set_ylabel("queries served"); ax.legend(); ax.grid(alpha=.3)
    ax = axes[2]
    for name, _, _ in CLASSES:
        rows = [r for r in per_search if r["cls"] == name and r["reach"] > 0]
        if not rows:
            continue
        grid = np.unique(np.concatenate([[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096], [a.max_queries]]))
        # median share found by query count, from traces recomputed: use q50/q90/q99 points
        pts = [(np.median([r["q50"] for r in rows if r["q50"]] or [np.nan]), 0.5), (np.median([r["q90"] for r in rows if r["q90"]] or [np.nan]), 0.9), (np.median([r["q99"] for r in rows if r["q99"]] or [np.nan]), 0.99)]
        ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", label=f"{name} ({len({r['topic'] for r in rows})} topics)")
    ax.set_xscale("log"); ax.set_xlabel("queries (median searcher) to reach 50 / 90 / 99 % of the topic"); ax.set_ylabel("share of registrants found"); ax.grid(alpha=.3); ax.legend(fontsize=8)
    fig.suptitle(label)
    fig.savefig(os.path.join(a.out, "load.png"), dpi=110)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=10000)
    ap.add_argument("--topics", type=int, default=100)
    ap.add_argument("--model", default="", help="crawl model JSON for topic shares; else Zipf")
    ap.add_argument("--zipf", type=float, default=1.0)
    ap.add_argument("--register-frac", type=float, default=0.5)
    ap.add_argument("--all-register", action="store_true")
    ap.add_argument("--floor", type=int, default=0, help="search_yield_floor; 0 = default searcher")
    ap.add_argument("--aux-radius", type=int, default=0)
    ap.add_argument("--depth", type=int, default=10)
    ap.add_argument("--bucket-size", type=int, default=16)
    ap.add_argument("--copies", type=int, default=5)
    ap.add_argument("--cache", type=int, default=5000)
    ap.add_argument("--topic-nodes-limit", type=int, default=16)
    ap.add_argument("--aux-limit", type=int, default=8)
    ap.add_argument("--rtt", type=float, default=0.06, help="seconds per query")
    ap.add_argument("--ad-lifetime", type=float, default=900)
    ap.add_argument("--horizon", type=float, default=3600, help="model seconds per search")
    ap.add_argument("--max-queries", type=int, default=20000, help="cap per search")
    ap.add_argument("--searchers", type=int, default=20, help="sampled searchers per topic")
    ap.add_argument("--target", type=int, default=0, help="stop a search at this many distinct registrants (conn-driven, e.g. 16); 0 = continuous")
    ap.add_argument("--bootnodes", type=int, default=20, help="earlier nodes kept in every node's DHT table, as simnet bootstraps (0 = pure Kademlia tables)")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    t0 = time.time()
    pop = Population(a.nodes, seed=a.seed, bootnodes=a.bootnodes)
    shares = shares_from_model(a.model, a.topics) if a.model else zipf_shares(a.topics, a.zipf)
    pop.assign_topics(shares, a.register_frac, a.all_register)
    cfg = Config(a.depth, a.bucket_size, a.topic_nodes_limit, a.aux_limit, a.floor, a.aux_radius, a.ad_lifetime, a.rtt)
    ads = Ads(pop, a.depth, a.copies, a.cache, seed=a.seed + 1)
    regs = Registrars(pop, ads, cfg, seed=a.seed + 2)
    k = len(shares)
    sizes = np.bincount(pop.topics[pop.registrant], minlength=k)
    searchers_of = [np.nonzero(pop.topics == t)[0] for t in range(k)]
    print(f"{a.nodes} nodes, {k} topics, {int(pop.registrant.sum())} registrants, {len(ads.reg)} ads placed "
          f"({ads.held.max()} max per registrar); floor {a.floor}; setup {time.time()-t0:.1f}s", flush=True)

    rng = np.random.default_rng(a.seed + 3)
    per_search = []
    t1 = time.time()
    for t in range(k):
        cand = searchers_of[t]
        if len(cand) == 0:
            continue
        pick = rng.choice(cand, size=min(a.searchers, len(cand)), replace=False)
        weight = len(cand) / len(pick)
        members = set(int(i) for i in pop.topic_members(t))
        for s in pick:
            sr = Searcher(cfg, pop, regs, t, int(s), seed=int(s) * 7 + a.seed).run(a.horizon, a.max_queries, weight, a.target)
            reach = members - {int(s)}
            found = len(sr.found & reach)
            # queries to reach a share of the topic
            def q_at(frac):
                need = frac * len(reach)
                for q, tm, n in sr.trace:
                    if n >= need:
                        return q, tm
                return None, None
            q50, t50 = q_at(0.5); q90, t90 = q_at(0.9); q99, t99 = q_at(0.99)
            settled = sr.active_trace[-1][1] if sr.active_trace else 0
            per_search.append(dict(topic=t, size=int(sizes[t]), cls=size_class(int(sizes[t])), searcher=int(s),
                                   queries=sr.queries, passes=sr.passes, found=found, reach=len(reach),
                                   recall=found / len(reach) if reach else 1.0,
                                   q50=q50, q90=q90, q99=q99, t50=t50, t90=t90, t99=t99,
                                   settled=settled, end=sr.end_time, weight=weight, extrapolated=sr.extrapolated,
                                   bucket_queries=sr.bucket_queries))
        if t % 10 == 0:
            print(f"  topic {t} ({sizes[t]} members): {len(pick)} searchers, {time.time()-t1:.0f}s", flush=True)

    # registrar load
    q = regs.queries
    by_class = {}
    for name, _, _ in CLASSES:
        rows = [r for r in per_search if r["cls"] == name]
        if not rows:
            continue
        med = lambda key: float(np.median([r[key] for r in rows if r[key] is not None])) if any(r[key] is not None for r in rows) else None
        by_class[name] = dict(topics=len({r["topic"] for r in rows}), searches=len(rows),
                              recall_median=med("recall"), full=float(np.mean([r["recall"] >= 0.999 for r in rows])),
                              queries_median=med("queries"), passes_median=med("passes"),
                              q50=med("q50"), q90=med("q90"), q99=med("q99"), t50=med("t50"), t90=med("t90"), t99=med("t99"),
                              settled_median=med("settled"))
    # each registrar's bucket relative to its nearest topic hash (depth-1 = closest ring)
    near = np.zeros(pop.n, dtype=np.int32)
    ids_f = pop.ids
    for t in range(k):
        x = np.bitwise_xor(ids_f, pop.topic_ids[t])
        bl = np.zeros(pop.n, dtype=np.int64); nz = x != 0
        bl[nz] = np.floor(np.log2(x[nz].astype(np.float64))).astype(np.int64) + 1
        b = np.minimum(256 - (256 - 64 + bl), a.depth - 1)
        near = np.maximum(near, b.astype(np.int32))
    load_by_bucket = {int(b): dict(nodes=int((near == b).sum()), queries_mean=float(q[near == b].mean()) if (near == b).any() else 0.0,
                                   queries_max=float(q[near == b].max()) if (near == b).any() else 0.0) for b in range(a.depth)}
    write_figures(a, q, near, per_search, regs)
    summary = dict(config=vars(a), topics=k, registrants=int(pop.registrant.sum()),
                   registrar_queries=dict(median=float(np.median(q)), p99=float(np.percentile(q, 99)), max=float(q.max()),
                                          mean=float(q.mean()), nonzero=int((q > 0).sum())),
                   ads_served=dict(median=float(np.median(regs.ads_served)), p99=float(np.percentile(regs.ads_served, 99)), max=float(regs.ads_served.max())),
                   by_class=by_class, load_by_nearest_bucket=load_by_bucket, elapsed=time.time() - t0)
    json.dump(summary, open(os.path.join(a.out, "summary.json"), "w"), indent=1)
    json.dump(per_search, open(os.path.join(a.out, "searches.json"), "w"))
    np.save(os.path.join(a.out, "registrar_queries.npy"), q)
    np.save(os.path.join(a.out, "ids.npy"), pop.ids)
    np.save(os.path.join(a.out, "topics.npy"), pop.topics)
    np.save(os.path.join(a.out, "topic_ids.npy"), pop.topic_ids)
    print(json.dumps({k2: v for k2, v in summary.items() if k2 != "config"}, indent=1))


if __name__ == "__main__":
    main()
