"""The matrix manifest and its scheduler, against a fake world executable."""
import importlib.util
import json
import os
import stat
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location("matrix", Path(__file__).resolve().parents[1] / "runs/capacity/matrix.py")
matrix = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(matrix)

FAKE = """#!/bin/bash
out=""; prev=""
for a in "$@"; do [ "$prev" = "--out" ] && out="$a"; prev="$a"; done
name=$(basename "$out")
case " $FAIL " in *" $name "*) exit 1;; esac
mkdir -p "$ROOT/$out"
rate=40; case " $NULLRATE " in *" $name "*) rate=null;; esac
echo "{\\"config\\": {}, \\"regtopic_rate\\": {\\"honest_target\\": $rate}}" > "$ROOT/$out/summary.json"
echo "$@" > "$ROOT/$out/argv"
"""


@pytest.fixture
def fake(tmp_path, monkeypatch):
    (tmp_path / "runs/capacity").mkdir(parents=True)
    py = tmp_path / "fakepy"
    py.write_text(FAKE)
    py.chmod(py.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setattr(matrix, "OUT", tmp_path / "runs/capacity")
    monkeypatch.setattr(matrix, "PY", str(py))
    monkeypatch.setenv("ROOT", str(tmp_path))
    monkeypatch.chdir(tmp_path)
    return tmp_path


def rows(names, rho4=10000, occupy=2000):
    allr = matrix.runs(rho4, occupy, 16.0)
    return [r for r in allr if r["name"] in names]


def test_every_size_has_its_own_baselines():
    allr = matrix.runs(10000, 2000, 16.0)
    names = [r["name"] for r in allr]
    assert len(names) == len(set(names))
    for c in (1000, 5000):
        for s in (1, 2, 3):
            assert f"h-n2000-c{c}-s{s}" in names
        assert f"h-n10000-c{c}-s1" in names and f"h-n10000-c{c}-s2" not in names   # rho=4 rows are seed 1 only
    assert all(f"op-n2000-c{c}-s{s}" in names for c in (1000, 5000) for s in (1, 2, 3))
    assert not any(n.startswith("op-n10000") for n in names)
    by = {r["name"]: r for r in allr}
    for r in allr:
        if r["kind"] != "h" or r["flags"]:
            base = by[r["baseline"]]
            assert (base["nodes"], base["cache"], base["seed"], base["flags"]) == (r["nodes"], r["cache"], r["seed"], [])
    assert by["o-n2000-c1000-s2"]["partitioned"] == "op-n2000-c1000-s2"
    assert by["vt-rho4-budget-n10000-c5000"]["budget_ref"] == "h-n10000-c5000-s1"
    same = [r["name"] for r in matrix.runs(2000, 2000, 16.0)]
    assert len(same) == len(set(same)) and "h-n2000-c1000-s1" in same
    assert not any(r["kind"] == "o" for r in matrix.runs(10000, 0, 0))


def test_failed_reference_drops_dependent_and_reports(fake, monkeypatch):
    monkeypatch.setenv("FAIL", "h-n10000-c1000-s1")
    todo = rows({"h-n10000-c1000-s1", "vt-rho4-budget-n10000-c1000", "h-n10000-c5000-s1", "vt-rho4-budget-n10000-c5000"})
    with open(fake / "log", "w") as log:
        failed = matrix.schedule(todo, 4, 500, log, poll=0.05, stagger=0)
    assert sorted(failed) == ["h-n10000-c1000-s1", "vt-rho4-budget-n10000-c1000"]
    argv = (fake / "runs/capacity/vt-rho4-budget-n10000-c5000/argv").read_text()
    assert "--flood-budget 100" in argv
    meta = json.loads((fake / "runs/capacity/vt-rho4-budget-n10000-c5000/matrix.json").read_text())
    assert meta["budget"] == 100 and meta["baseline"] == "h-n10000-c5000-s1" and "2.5 x" in meta["budget_policy"]
    assert "drop vt-rho4-budget-n10000-c1000: reference h-n10000-c1000-s1 failed" in (fake / "log").read_text()


def test_null_rate_drops_budget_run(fake, monkeypatch):
    monkeypatch.setenv("NULLRATE", "h-n10000-c1000-s1")
    with open(fake / "log", "w") as log:
        failed = matrix.schedule(rows({"h-n10000-c1000-s1", "vt-rho4-budget-n10000-c1000"}), 4, 500, log, poll=0.05, stagger=0)
    assert failed == ["vt-rho4-budget-n10000-c1000"]


def test_memory_cap_and_limits(fake):
    with pytest.raises(SystemExit):
        matrix.main(["--rho4-nodes", "10000", "--mem-gb", "1", "--launch", "--only", "h-c1000-s1"])
    for bad in (["--jobs", "0"], ["--mem-gb", "0"], ["--occupy-nodes", "2000"]):
        with pytest.raises(SystemExit):
            matrix.main(["--rho4-nodes", "10000"] + bad)
    # the cap holds even with nothing else running: 2 x 11.1 GB under 15 GB run one at a time
    todo = rows({"h-c1000-s1", "h-c1000-s2"})
    with open(fake / "log", "w") as log:
        assert matrix.schedule(todo, 4, 15, log, poll=0.05, stagger=0) == []
    starts = [l for l in (fake / "log").read_text().splitlines() if " start " in l]
    ends = [i for i, l in enumerate((fake / "log").read_text().splitlines()) if " end " in l]
    assert len(starts) == 2 and ends[0] < [i for i, l in enumerate((fake / "log").read_text().splitlines()) if " start " in l][1]


def test_only_pulls_in_budget_reference(fake, capsys):
    sel = matrix.select(matrix.runs(10000, 0, 0), "vt-rho4-budget")
    assert {r["name"] for r in sel} == {"vt-rho4-budget-n10000-c1000", "vt-rho4-budget-n10000-c5000",
                                        "h-n10000-c1000-s1", "h-n10000-c5000-s1"}
    assert matrix.main(["--rho4-nodes", "10000", "--only", "vt-rho4-budget"]) == 0


ROUND1 = dict(samples=dict(hot_load={}), regtopic_rate=dict(honest_target=40.0), admission_s=dict(admitted_frac=1.0),
              by_class={"1000+": dict(honest_t90_all=3.0)})
STX_CFG = dict(nodes=2000, cache=1000, seed=1, flood="occupy", flood_topics=300, flood_nets=4, depth=18,
               reg_bucket_size=10, search_frac=0.08, search_time=1800, topics=100, flood_budget=0.0)


def test_reuse_needs_round1_metrics(fake):
    src = fake / "runs/capacity/stx2k-c1000"
    src.mkdir()
    (src / "summary.json").write_text(json.dumps(dict(config=STX_CFG, regtopic_rate={}, by_class={"1000+": {}})))
    with open(fake / "log", "w") as log:
        assert matrix.schedule(rows({"o-n2000-c1000-s1"}), 4, 500, log, poll=0.05, stagger=0) == []
    out = fake / "runs/capacity/o-n2000-c1000-s1"
    assert not out.is_symlink() and (out / "argv").exists()


def test_running_reuse_source_holds_slot_and_memory(fake, monkeypatch):
    """With jobs = 1 and a 16 GB cap, a still running reuse source blocks any
    other start until it finishes."""
    polls = {"n": 0}
    def state(r):
        if r["name"] not in matrix.REUSE:
            return None
        polls["n"] += 1
        return "running" if polls["n"] <= 3 else "linked"
    monkeypatch.setattr(matrix, "reuse_state", state)
    todo = rows({"o-n2000-c1000-s1", "op-n2000-c1000-s1"})
    with open(fake / "log", "w") as log:
        assert matrix.schedule(todo, 1, 16, log, poll=0.05, stagger=0) == []
    lines = (fake / "log").read_text().splitlines()
    assert polls["n"] >= 4
    assert [l.split()[1] for l in lines][:2] == ["reuse", "start"]


def test_finished_stx2k_run_is_reused(fake):
    src = fake / "runs/capacity/stx2k-c1000"
    src.mkdir()
    cfg = dict(nodes=2000, cache=1000, seed=1, flood="occupy", flood_topics=300, flood_nets=4, depth=18,
               reg_bucket_size=10, search_frac=0.08, search_time=1800, topics=100, flood_budget=0.0)
    (src / "summary.json").write_text(json.dumps(dict(config=cfg, **ROUND1)))
    with open(fake / "log", "w") as log:
        assert matrix.schedule(rows({"o-n2000-c1000-s1"}), 4, 500, log, poll=0.05, stagger=0) == []
    link = fake / "runs/capacity/o-n2000-c1000-s1"
    assert link.is_symlink() and os.readlink(link) == "stx2k-c1000"
    assert json.loads((link / "matrix.json").read_text())["partitioned"] == "op-n2000-c1000-s1"
    # a mismatched config is not reused: the row runs
    (src / "summary.json").write_text(json.dumps(dict(config=dict(cfg, cache=5000), **ROUND1)))
    link.unlink()
    with open(fake / "log", "w") as log:
        assert matrix.schedule(rows({"o-n2000-c1000-s1"}), 4, 500, log, poll=0.05, stagger=0) == []
    assert (fake / "runs/capacity/o-n2000-c1000-s1/argv").exists()
