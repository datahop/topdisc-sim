"""Cross-seed tables of capacity runs: mean (min–max) over seeds, one column
per cache capacity.

    python -m searchsim.aggregate runs/capacity/h-* runs/capacity/t-* --out RESULTS.md
"""
import argparse
import json
import math
import statistics
import sys
from pathlib import Path

IGNORED = ("seed", "out", "elapsed", "cache")
CLASSES = ("1000+", "100-999", "10-99", "<10")
SECTIONS = {"none": "Q1 honest cost", "target": "Q2 target-topic flood", "occupy": "Q3 cache occupation, every sybil on all T topics",
            "occupy-partitioned": "Q3 cache occupation, one fake topic per sybil"}


def flatten(d, prefix=""):
    out = {}
    for k, v in d.items():
        key = prefix + str(k)
        if isinstance(v, dict):
            out.update(flatten(v, key + "."))
        elif isinstance(v, list) and all(isinstance(x, (int, float)) for x in v):
            for i, x in enumerate(v):
                out[f"{key}.{i}"] = x
        elif isinstance(v, (int, float)) and not isinstance(v, bool) or v is None:
            out[key] = v
        elif isinstance(v, bool):
            out[key] = float(v)
    return out


def load(run_dir):
    s = json.loads((Path(run_dir) / "summary.json").read_text())
    flat = flatten({k: v for k, v in s.items() if k != "config"})
    target = s.get("target_search", {}).get("topic")
    if target is not None:
        for k, v in flatten(s.get("served", {}).get(str(target), {})).items():
            flat["served_target." + k] = v
    qw = s.get("quoted_wait_s", {})
    if qw.get("n"):
        flat["quoted_wait_s.share_ge_1350"] = qw.get("ge_1350", 0) / qw["n"]
        flat["quoted_wait_s.share_ge_86400"] = qw.get("ge_86400", 0) / qw["n"]
    fl = s.get("flood")
    if fl and fl.get("sybil_admissions"):
        flat["flood.requests_per_admission"] = fl["requests"] / fl["sybil_admissions"]
    config = dict(s["config"])
    meta_path = Path(run_dir) / "matrix.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    if config.get("flood_budget"):
        flat["config.flood_budget"] = config["flood_budget"]
        if meta.get("budget_policy"):
            # calibrated per capacity: group by the rule, keep each R as a row
            config["flood_budget"] = "policy: " + meta["budget_policy"]
    config["_baseline"] = meta.get("baseline")
    return config, flat


def fmt(values, kind):
    """mean (min–max) over seeds; seeds with no value (never reached, or
    the metric absent) are counted, not dropped silently."""
    n = len(values)
    values = [v for v in values if v is not None and not math.isnan(v)]
    if not values:
        return "n/a"
    mean, lo, hi = statistics.mean(values), min(values), max(values)
    if kind == "pct":
        cell = f"{100 * mean:.1f}% ({100 * lo:.1f}–{100 * hi:.1f})"
    elif kind == "sec":
        cell = f"{mean:,.1f} ({lo:,.1f}–{hi:,.1f})" if hi < 100 else f"{mean:,.0f} ({lo:,.0f}–{hi:,.0f})"
    else:
        cell = f"{mean:,.2f} ({lo:,.2f}–{hi:,.2f})"
    return cell if len(values) == n else f"{cell} [{n - len(values)} of {n} seeds n/a]"


def per_class(label, metric, kind):
    return [(f"{label}, {c}", f"by_class.{c}.{metric}", kind) for c in CLASSES]


def hot(name, label):
    return [
        (f"{label}: fill (search avg)", f"samples.{name}.fill", "pct"),
        (f"{label}: honest target ads (search avg)", f"samples.{name}.honest_target_ads", "num"),
        (f"{label}: sybil share of target ads (search avg)", f"samples.{name}.target_sybil_share", "pct"),
        (f"{label}: sybil share of cache (search avg)", f"samples.{name}.cache_sybil_share", "pct"),
        (f"{label}: honest REGTOPIC/s", f"samples.{name}.honest_regtopic_rate", "num"),
        (f"{label}: sybil REGTOPIC/s", f"samples.{name}.sybil_regtopic_rate", "num"),
    ]


HOT = hot("hot_load", "20 most loaded") + hot("hot_target", "20 with most target ads") + hot("hot_set", "20 most queried for target")
REACH = (per_class("honest searchers reaching 50 %", "honest_reached50_frac", "pct")
         + per_class("honest searchers reaching 90 %", "honest_reached90_frac", "pct")
         + per_class("honest t50 median, if reached (s)", "honest_t50", "sec")
         + per_class("honest t90 median, if reached (s)", "honest_t90", "sec")
         + per_class("honest t90 median, never = inf (s)", "honest_t90_all", "sec"))
Q1 = (per_class("honest recall median", "recall_median", "pct") + REACH + [
    ("registrations per advertiser, median (all registrants)", "fanout.median", "num"),
    ("registrations per advertiser, p10 (all registrants)", "fanout.p10", "num"),
    ("distinct buckets per advertiser, median (all registrants)", "fanout_buckets.distinct_median", "num"),
    ("distinct buckets per advertiser, p10 (all registrants)", "fanout_buckets.distinct_p10", "num"),
    ("advertisers without a live ad (end)", "fanout_buckets.without_live_ad", "num"),
] + [(f"live ads in bucket {b}, class 1000+", f"fanout_buckets.by_class.1000+.share.{b}", "pct") for b in range(18)] + [
    ("ads held per registrar, max (end)", "ads_held.max", "num"),
    ("ads held per registrar, p99 (end)", "ads_held.p99", "num"),
    ("ads held per registrar, median (end)", "ads_held.median", "num"),
    ("ads held per registrar, max (search avg)", "samples.search_phase.ads_held.max", "num"),
    ("ads held per registrar, p99 (search avg)", "samples.search_phase.ads_held.p99", "num"),
    ("ads held per registrar, median (search avg)", "samples.search_phase.ads_held.median", "num"),
    ("advertisers ever admitted", "admission_s.admitted_frac", "pct"),
    ("time to first admission, median if admitted (s)", "admission_s.median", "sec"),
    ("time to first admission, p90 if admitted (s)", "admission_s.p90", "sec"),
    ("time to first admission, median, never = inf (s)", "admission_s.median_all", "sec"),
    ("attempts per admission", "reg_stats.all.attempts_per_admission", "num"),
    ("give-ups per advertiser", "giveups.honest_per_advertiser", "num"),
    ("give-ups, single quote", "giveups.honest.single", "num"),
    ("give-ups, cumulative", "giveups.honest.cumulative", "num"),
    ("quoted wait, median (s)", "quoted_wait_s.median", "sec"),
    ("quoted wait, p90 (s)", "quoted_wait_s.p90", "sec"),
    ("quoted wait, p99 (s)", "quoted_wait_s.p99", "sec"),
    ("quotes >= 1350 s", "quoted_wait_s.share_ge_1350", "pct"),
    ("quotes >= 24 h", "quoted_wait_s.share_ge_86400", "pct"),
    ("honest tickets presented late", "ticket_timings_advertiser.honest.late", "num"),
    ("honest tickets presented expired", "ticket_timings_advertiser.honest.expired", "num"),
    ("REGTOPIC/s honest, network (search phase)", "regtopic_rate.honest", "num"),
    ("REGTOPIC/s honest, target topic (search phase)", "regtopic_rate.honest_target", "num"),
])
TARGET_SEARCH = [
    ("target searchers, honest recall median", "target_search.recall_median", "pct"),
    ("target searchers reaching 50 %", "target_search.honest_reached50_frac", "pct"),
    ("target searchers reaching 90 %", "target_search.honest_reached90_frac", "pct"),
    ("target searchers, honest t50 median if reached (s)", "target_search.honest_t50", "sec"),
    ("target searchers, honest t90 median if reached (s)", "target_search.honest_t90", "sec"),
    ("target searchers, honest t90 median, never = inf (s)", "target_search.honest_t90_all", "sec"),
]
TARGET_REG = [
    ("honest target ads held, network (search avg)", "samples.search_phase.honest_target_ads", "num"),
    ("honest target advertisers ever admitted", "admission_s.target.admitted_frac", "pct"),
    ("honest target attempts per admission", "reg_stats.target.attempts_per_admission", "num"),
    ("honest target give-ups", "reg_stats.target.giveups", "num"),
    ("sybil share of served target results", "served_target.sybil_share_served", "pct"),
]
Q1 += TARGET_SEARCH + TARGET_REG + HOT
COST = [
    ("rate budget R (sybil REGTOPIC/s)", "config.flood_budget", "num"),
    ("sybil REGTOPIC/s, network (search phase)", "regtopic_rate.sybil", "num"),
    ("sybil REGTOPIC/s, hot set", "regtopic_rate.hot_set_sybil", "num"),
    ("sybil REGTOPIC per admission", "flood.requests_per_admission", "num"),
    ("sybil / honest target REGTOPIC", "flood.ratio_to_honest_target", "num"),
]
Q2 = [
    ("sybil share of served target results", "served_target.sybil_share_served", "pct"),
    ("sybil share of target ads at queried registrars", "served_target.target_share_mean", "pct"),
    ("sybil share of cache at queried registrars", "served_target.cache_share_mean", "pct"),
    ("target searchers, mean sybil share of results", "target_search.sybil_share_mean", "pct"),
    ("target searchers, mean sybil share of first 16", "target_search.first16_sybil_mean", "pct"),
    ("target searchers, first 16 all sybil", "target_search.eclipsed16", "pct"),
    ("target searchers, all results sybil", "target_search.eclipsed_mean", "pct"),
    ("target searchers without an honest result", "target_search.without_honest", "pct"),
    ("sybil share of target ads, network (search avg)", "samples.search_phase.target_sybil_share", "pct"),
] + TARGET_SEARCH + TARGET_REG[:4] + HOT + COST
Q3 = [
    ("sybil share of all ads (search avg)", "samples.search_phase.sybil_ad_share", "pct"),
    ("sybil share of all ads (end)", "sybil_ad_share", "pct"),
    ("honest target admissions", "reg_stats.target.admitted", "num"),
    ("advertisers ever admitted", "admission_s.admitted_frac", "pct"),
    ("advertisers without a live ad (end)", "fanout_buckets.without_live_ad", "num"),
    ("give-ups per advertiser", "giveups.honest_per_advertiser", "num"),
] + per_class("honest searchers reaching 90 %", "honest_reached90_frac", "pct") + TARGET_SEARCH + TARGET_REG[:4] + [
    ("ads held per registrar, max (search avg)", "samples.search_phase.ads_held.max", "num"),
    ("ads held per registrar, p99 (search avg)", "samples.search_phase.ads_held.p99", "num"),
] + HOT + COST
ROWS = {"none": Q1, "target": Q2, "occupy": Q3, "occupy-partitioned": Q3}


def scenario(config):
    return json.dumps({k: v for k, v in sorted(config.items()) if k not in IGNORED and not k.startswith("_")}, sort_keys=True)


def table(runs, rows):
    caps = sorted({c["cache"] for c, _ in runs})
    lines = ["| metric | " + " | ".join(f"C={c}" for c in caps) + " |", "|---" * (len(caps) + 1) + "|"]
    for label, key, kind in rows:
        cells = [fmt([f.get(key) for c, f in runs if c["cache"] == cap], kind) for cap in caps]
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    seeds = {cap: sorted(c["seed"] for c, _ in runs if c["cache"] == cap) for cap in caps}
    lines.append("")
    lines.append("seeds: " + "; ".join(f"C={cap}: {', '.join(map(str, s))}" for cap, s in seeds.items()))
    bases = sorted({c["_baseline"] for c, _ in runs if c.get("_baseline")})
    if bases:
        lines.append("")
        lines.append("baselines: " + ", ".join(bases))
    return "\n".join(lines)


def diff(config, core):
    keys = sorted(set(config) | set(core))
    return ", ".join(f"{k}={config.get(k)}" for k in keys if k not in IGNORED and not k.startswith("_") and config.get(k) != core.get(k)) or "core"


CORE_NODES = 25000


def report(run_dirs):
    """Q1-Q3 at the core size (25k, else the largest), its other scenarios as
    Q4 variants; every other size gets a section of its own, so its attack
    rows sit next to the baselines run at that size."""
    groups = {}
    for d in run_dirs:
        config, flat = load(d)
        groups.setdefault(scenario(config), []).append((config, flat))
    sizes = sorted({g[0][0].get("nodes") for g in groups.values()}, key=lambda n: (n is None, -(n or 0)))
    core_size = CORE_NODES if CORE_NODES in sizes else sizes[0]
    parts, variants, other = [], [], []
    for size in [core_size] + [n for n in sizes if n != core_size]:
        out = parts if size == core_size else other
        if size != core_size:
            out.append(f"## {size} nodes, with baselines at this size\n")
        for mode, title in SECTIONS.items():
            scen = [g for g in groups.values() if g[0][0].get("flood", "none") == mode and g[0][0].get("nodes") == size]
            if not scen:
                continue
            scen.sort(key=lambda g: (-len(g), scenario(g[0][0])))
            core = scen[0]
            out.append(f"{'##' if size == core_size else '###'} {title}\n\n{table(core, ROWS[mode])}\n")
            for g in scen[1:]:
                text = f"### {title}: {diff(g[0][0], core[0][0])}\n\n{table(g, ROWS[mode])}\n"
                (variants if size == core_size else out).append(text)
    if variants:
        parts.append("## Q4 variants\n\n" + "\n".join(variants))
    return "\n".join(parts + other)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("runs", nargs="+")
    ap.add_argument("--out", default="")
    ap.add_argument("--matrix", action="store_true", help="only runs launched or reused by runs/capacity/matrix.py")
    a = ap.parse_args(argv)
    dirs, seen = [], set()
    for d in a.runs:
        real = Path(d).resolve()           # a reused run is linked under its matrix name: count it once
        if (real / "summary.json").exists() and (not a.matrix or (real / "matrix.json").exists()) and real not in seen:
            seen.add(real)
            dirs.append(d)
    if not dirs:
        sys.exit("no summary.json in the given run directories")
    text = report(dirs)
    if a.out:
        Path(a.out).write_text(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
