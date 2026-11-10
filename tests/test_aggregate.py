"""Cross-seed tables: mean (min–max), grouping without seed/out/elapsed."""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from searchsim.aggregate import fmt, main, report  # noqa: E402


def write(d, seed, cache, recall, extra=None):
    d.mkdir()
    s = dict(config=dict(seed=seed, out=str(d), cache=cache, nodes=100, flood="none"), elapsed=seed * 10.0,
             by_class={"1000+": dict(recall_median=recall)}, ads_held=dict(max=10 * seed),
             quoted_wait_s=dict(n=4, ge_1350=seed, ge_86400=0))
    if extra:
        s["config"].update(extra)
    (d / "summary.json").write_text(json.dumps(s))
    return str(d)


def test_cells_grouping_and_missing(tmp_path):
    runs = [write(tmp_path / "a", 1, 1000, 0.5), write(tmp_path / "b", 2, 1000, 0.7),
            write(tmp_path / "c", 1, 5000, 0.9)]
    text = report(runs)
    assert "| metric | C=1000 | C=5000 |" in text
    assert "| honest recall median, 1000+ | 60.0% (50.0–70.0) | 90.0% (90.0–90.0) |" in text
    assert "| ads held per registrar, max (end) | 15.00 (10.00–20.00) | 10.00 (10.00–10.00) |" in text
    assert "| quotes >= 1350 s | 37.5% (25.0–50.0) | 25.0% (25.0–25.0) |" in text
    assert "| honest t50 median, if reached (s), 1000+ | n/a | n/a |" in text
    assert "seeds: C=1000: 1, 2; C=5000: 1" in text
    assert "Q4" not in text


def test_variant_table_and_cli(tmp_path):
    runs = [write(tmp_path / "a", 1, 1000, 0.5), write(tmp_path / "b", 2, 1000, 0.7),
            write(tmp_path / "v", 1, 1000, 0.1, dict(no_ip_score=True))]
    out = tmp_path / "R.md"
    main(runs + ["--out", str(out)])
    text = out.read_text()
    assert "## Q4 variants" in text and "no_ip_score=True" in text
    assert "10.0% (10.0–10.0)" in text
    assert fmt([None, float("nan")], "pct") == "n/a"


def test_censored_seeds_are_counted(tmp_path):
    def run(name, seed, t90):
        d = tmp_path / name
        d.mkdir()
        cls = {"1000+": dict(recall_median=1.0, honest_t90=t90, honest_reached90_frac=1.0 if t90 else 0.0)}
        (d / "summary.json").write_text(json.dumps(dict(config=dict(seed=seed, out=str(d), cache=1000, flood="none"),
                                                        by_class=cls)))
        return str(d)
    text = report([run("a", 1, 10.0), run("b", 2, None), run("c", 3, None)])
    assert "| honest t90 median, if reached (s), 1000+ | 10.0 (10.0–10.0) [2 of 3 seeds n/a] |" in text
    assert "| honest searchers reaching 90 %, 1000+ | 33.3% (0.0–100.0) |" in text


def test_calibrated_budget_groups_both_capacities(tmp_path):
    def run(name, cache, budget, policy=None):
        d = tmp_path / name
        d.mkdir()
        cfg = dict(seed=1, out=str(d), cache=cache, flood="target", flood_ratio=4.0, flood_budget=budget)
        (d / "summary.json").write_text(json.dumps(dict(config=cfg, flood=dict(requests=10, sybil_admissions=5))))
        if policy:
            (d / "matrix.json").write_text(json.dumps(dict(budget_policy=policy, baseline=f"h-c{cache}-s1")))
        return str(d)
    pol = "2.5 x honest target REGTOPIC/s of the matching no-flood run"
    text = report([run("a", 1000, 300.0, pol), run("b", 5000, 450.0, pol)])
    assert text.count("| metric |") == 1 and "| metric | C=1000 | C=5000 |" in text
    assert "| rate budget R (sybil REGTOPIC/s) | 300.00 (300.00–300.00) | 450.00 (450.00–450.00) |" in text
    assert "baselines: h-c1000-s1, h-c5000-s1" in text
    # fixed budgets without a calibration rule stay separate experiments
    text = report([run("c", 1000, 300.0), run("d", 5000, 450.0)])
    assert text.count("| metric |") == 2


def test_core_is_25k_and_other_sizes_get_their_own_section(tmp_path):
    def run(name, nodes, seed, cache, flood="none", recall=0.5):
        d = tmp_path / name
        d.mkdir()
        cfg = dict(seed=seed, out=str(d), cache=cache, nodes=nodes, flood=flood)
        (d / "summary.json").write_text(json.dumps(dict(config=cfg, by_class={"1000+": dict(recall_median=recall)})))
        return str(d)
    runs = []
    for nodes, recall in ((2000, 0.2), (10000, 0.1), (25000, 0.9)):     # equal run counts at three sizes
        for s in (1, 2):
            for c in (1000, 5000):
                runs.append(run(f"h-{nodes}-{s}-{c}", nodes, s, c, recall=recall))
    for s in (1, 2):
        runs.append(run(f"op-25k-{s}", 25000, s, 1000, "occupy-partitioned"))
        runs.append(run(f"op-2k-{s}", 2000, s, 1000, "occupy-partitioned"))
        runs.append(run(f"o-2k-{s}", 2000, s, 1000, "occupy"))
    text = report(runs)
    q1 = text.split("## Q1 honest cost\n", 1)[1]
    assert q1.split("\n")[3] == "| honest recall median, 1000+ | 90.0% (90.0–90.0) | 90.0% (90.0–90.0) |"
    assert "Q4" not in text
    k2 = text.split("## 2000 nodes, with baselines at this size\n", 1)[1].split("## 10000 nodes", 1)[0]
    assert "### Q1 honest cost" in k2 and "20.0% (20.0–20.0)" in k2
    assert "### Q3 cache occupation, every sybil on all T topics" in k2 and "### Q3 cache occupation, one fake topic per sybil" in k2
    assert text.index("## Q3 cache occupation, one fake topic per sybil") < text.index("## 2000 nodes")
    assert "10.0% (10.0–10.0)" in text.split("## 10000 nodes, with baselines at this size\n", 1)[1]


def test_matrix_filter_and_linked_run_counted_once(tmp_path):
    src = tmp_path / "stx"
    write(src, 1, 1000, 0.4)
    (src / "matrix.json").write_text(json.dumps(dict(baseline=None)))
    (tmp_path / "o-link").symlink_to(src)
    write(tmp_path / "gate", 2, 1000, 0.8)                   # no matrix.json
    out = tmp_path / "R.md"
    main(["--matrix", str(src), str(tmp_path / "o-link"), str(tmp_path / "gate"), "--out", str(out)])
    text = out.read_text()
    assert "40.0% (40.0–40.0)" in text and "seeds: C=1000: 1\n" in text + "\n"
