"""M001-S03: documented grouped CSV user path in separate CLI processes.

``miaproc eddy run-silver`` then ``miaproc eddy run-gold`` (hesseflux-native)
run as fresh interpreter processes of the candidate source on a deterministic
two-site synthetic input; this test process then reads the persisted files
back. The installed-wheel version of the same path, including the
reddyproc-reference engine, runs in the product Docker image under
``06_infra/verification/verify_integration.py --phase M001``.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import miaproc
import synthetic_eddy

SRC = Path(miaproc.__file__).resolve().parents[1]
# Two sites with different lengths and seeds share calendar timestamps, so
# mixing groups would show up as wrong per-site row counts or values.
SITES = [("SITE_A", 45, 20261101), ("SITE_B", 40, 20261102)]


def _cli(*argv: str, cwd: Path) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(SRC), env.get("PYTHONPATH", "")) if p)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "miaproc.cli", *argv],
        cwd=cwd, env=env, capture_output=True, text=True, timeout=600, check=False,
    )


class TestSiteFixture:
    def test_site_texts_are_deterministic_and_grouped(self):
        first = synthetic_eddy.render_site_csv_texts(site_id="SITE_A", days=2, seed=7)
        assert first == synthetic_eddy.render_site_csv_texts(site_id="SITE_A", days=2, seed=7)
        assert sorted(first) == ["biomet/SITE_A.csv", "full_output/SITE_A.csv"]
        for text in first.values():
            lines = text.splitlines()
            assert lines[0].endswith(",site_id")
            assert len(lines) > 1
            assert all(line.endswith(",SITE_A") for line in lines[1:])
            assert not any(line.startswith("[") for line in lines)

    def test_site_rows_match_single_site_generator(self):
        site = synthetic_eddy.render_site_csv_texts(site_id="S1", days=2, seed=7)
        legacy = synthetic_eddy.render_csv_texts(days=2, seed=7)
        for folder in ("full_output", "biomet"):
            legacy_lines = legacy[f"{folder}/{folder}.csv"].splitlines()
            site_lines = site[f"{folder}/S1.csv"].splitlines()
            assert site_lines[0] == legacy_lines[0] + ",site_id"
            assert site_lines[1:] == [line + ",S1" for line in legacy_lines[2:]]

    @pytest.mark.parametrize("bad", ["", "a/b", "../x", "a b"])
    def test_unsafe_site_labels_are_rejected(self, bad):
        with pytest.raises(ValueError, match="site_id"):
            synthetic_eddy.render_site_csv_texts(site_id=bad, days=1, seed=1)

    def test_duplicate_site_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="duplicate site_id"):
            synthetic_eddy.write_site_fixture(tmp_path, [("S1", 1, 1), ("S1", 1, 2)])


def test_grouped_silver_gold_readback_in_separate_processes(tmp_path):
    synthetic_eddy.write_site_fixture(tmp_path / "input", SITES)
    out = tmp_path / "out"
    # M002-S03: each invocation owns a fresh run directory (manifest last).
    sout, gout = out / "silver_run", out / "gold_run"
    silver = _cli(
        "eddy", "run-silver",
        "--flux-dir", str(tmp_path / "input" / "full_output"),
        "--biomet-dir", str(tmp_path / "input" / "biomet"),
        "--group-column", "site_id",
        "--output-table", str(sout / "silver.csv"),
        "--output-run-json", str(sout / "silver_run.json"),
        cwd=tmp_path,
    )
    assert silver.returncode == 0, silver.stderr[-2000:]
    gold = _cli(
        "eddy", "run-gold",
        "--silver-table", str(sout / "silver.csv"),
        "--engine", "hesseflux-native",
        "--group-column", "site_id",
        "--output-table", str(gout / "gold.parquet"),
        "--output-diagnostics-json", str(gout / "gold_diag.json"),
        "--output-run-json", str(gout / "gold_run.json"),
        cwd=tmp_path,
    )
    assert gold.returncode == 0, gold.stderr[-2000:]
    for run_dir, stage in ((sout, "silver"), (gout, "gold")):
        check = _cli("eddy", "validate-run", "--run-dir", str(run_dir), "--stage", stage, cwd=tmp_path)
        assert check.returncode == 0, check.stderr[-2000:]
        summary = json.loads(check.stdout.strip().splitlines()[-1])
        assert summary["ok"] is True and summary["stage"] == stage

    expected_rows = {site: days * 48 for site, days, _ in SITES}
    silver_df = pd.read_csv(sout / "silver.csv")
    gold_df = pd.read_parquet(gout / "gold.parquet")
    assert silver_df["site_id"].value_counts().to_dict() == expected_rows
    assert gold_df["site_id"].value_counts().to_dict() == expected_rows
    assert list(gold_df.columns).count("timestamp") == 1
    assert [c for c in silver_df.columns if c not in gold_df.columns] == []
    for site, frame in gold_df.groupby("site_id"):
        paired = np.isfinite(frame["GPP"].to_numpy(float)) & np.isfinite(frame["Reco"].to_numpy(float))
        assert int(paired.sum()) >= 48, site

    silver_run = json.loads((sout / "silver_run.json").read_text(encoding="utf-8"))
    assert silver_run["exit_code"] == 0
    assert [g["category_value"] for g in silver_run["groups"]] == ["SITE_A", "SITE_B"]
    run = json.loads((gout / "gold_run.json").read_text(encoding="utf-8"))
    assert (run["stage"], run["engine"], run["exit_code"]) == ("gold", "hesseflux-native", 0)
    assert {g["category_value"]: g["gold_output_rows"] for g in run["groups"]} == expected_rows
    diag = json.loads((gout / "gold_diag.json").read_text(encoding="utf-8"))
    assert diag["group_column"] == "site_id"
    assert [g["category_value"] for g in diag["groups"]] == ["SITE_A", "SITE_B"]
    for group in diag["groups"]:
        per_group = json.loads(Path(group["diagnostics_path"]).read_text(encoding="utf-8"))
        assert per_group["backend"] == "hesseflux"
        assert per_group["partitioning"]["method"] == "lasslop"
        assert per_group["ustar"]["mode"] == "dynamic"
        assert not any("partition" in str(w).lower() for w in per_group["warnings"])
