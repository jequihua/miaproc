"""M32 CLI-level tests for the eddy source-truth column contract (file workflow).

This required module (frozen ``required_modules``) previously drove the
BigQuery split CLI; since M002-S04 it holds the storage-neutral
replacements for the retired BigQuery CLI tests.

M002-S04 removed ``eddy run-bigquery``, ``run-bigquery-silver`` and
``run-bigquery-gold``. Their BigQuery transport assertions (table flags,
stage/final/control tables, MERGE, watermarks, dry-run payload files)
retired without replacement. The scientific/data regression assertions
they carried are pinned here through the file workflow
(``eddy run-silver`` / ``eddy run-gold`` on Parquet files), as named in
``retired_test_ids.json``:

- M32/M32A source-truth silver: exactly one ``timestamp``, no leaked
  ``DateTime`` or other internal backend names, ``air_temperature_c`` /
  ``VPD_hpa`` unit names, ``co2_flux`` / ``qc_co2_flux`` / ``u_star`` /
  ``SWIN_1_1_1`` / ``P_RAIN_1_1_1`` / ``RH_1_1_1``, flux-side ``RH``
  preserved separately from biomet ``RH_1_1_1``, carried identity;
- M28 bronze-only (sentinel) flux columns survive silver, and silver
  columns survive gold;
- M32A gold consumes a timestamp-only source-truth silver, reconstructs
  the internal ``DateTime`` from ``timestamp`` before backend dispatch
  (``timestamp`` wins over a leaked ``DateTime``) and writes one
  ``timestamp`` and no ``DateTime``;
- eddy namespace help and the silver argument surface (no engine, no
  R repo root).
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import miaproc.eddy as eddy_pkg
import synthetic_eddy
from miaproc import cli
from miaproc.eddy.io import read_parquet_table, write_parquet_table
from publish_helpers import publish_table

BIG = 2**53 + 1

SOURCE_TRUTH_NAMES = (
    "timestamp", "co2_flux", "qc_co2_flux", "air_temperature_c", "u_star", "VPD_hpa",
    "SWIN_1_1_1", "P_RAIN_1_1_1", "RH_1_1_1",
)
INTERNAL_NAMES = ("DateTime", "NEE", "QC_NEE", "Tair", "USTAR", "VPD", "Rg", "P_RAIN", "rH")
GOLD_DERIVED = ("dateAndTime", "nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f", "GPP", "Reco")


def _typed_inputs(days: int = 2, seed: int = 31):
    flux, biomet = synthetic_eddy.generate_raw_frames(days=days, seed=seed)
    for frame in (flux, biomet):
        frame.insert(0, "timestamp", frame.pop("DateTime").dt.tz_localize("UTC"))
        frame.insert(0, "site_id", "RBRL")
    flux.insert(0, "primary_key", pd.array(BIG + np.arange(len(flux)), dtype="Int64"))
    flux["bronze_only_flag"] = np.arange(len(flux)) % 2
    # Flux-side RH distinct from the biomet RH_1_1_1 record.
    flux["RH"] = 40.0 + (np.arange(len(flux)) % 7)
    return flux, biomet


def _run_silver(tmp_path: Path, flux: pd.DataFrame, biomet: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    fp, bp = tmp_path / "flux.parquet", tmp_path / "biomet.parquet"
    write_parquet_table(fp, flux)
    write_parquet_table(bp, biomet)
    argv = ["eddy", "run-silver", "--flux-parquet", str(fp), "--biomet-parquet", str(bp),
            "--output-table", str(tmp_path / "srun" / "silver.parquet"),
            "--output-run-json", str(tmp_path / "srun" / "silver_run.json")]
    assert cli.main(argv) == cli.SUCCESS_EXIT
    silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
    run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
    return silver, run


@pytest.fixture
def fake_engine(monkeypatch):
    """Backend stub returning the 13 contract columns; records its input."""
    seen = []

    def _fake_postproc(df, *, engine, hesseflux_config=None, reddyproc_config=None):
        seen.append(df.copy())
        out = pd.DataFrame({"DateTime": df["DateTime"]})
        for col in ("NEE", "Tair", "Rg", "VPD", "USTAR"):
            out[col] = pd.to_numeric(df[col], errors="coerce")
        out["NEE_f"] = out["NEE"].fillna(0.5)
        out["NEE_fqc"] = 0
        out["GPP"] = 1.0
        out["Reco"] = 2.0
        out["Tair_f"] = out["Tair"].fillna(20.0)
        out["Rg_f"] = out["Rg"].fillna(0.0)
        out["VPD_f"] = out["VPD"].fillna(5.0)
        out.attrs["miaproc_diagnostics"] = {"backend": "stub", "engine": engine}
        return out

    monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
    return seen


def _source_truth_silver(n: int = 4) -> pd.DataFrame:
    """Strict M32A source-truth silver: ``timestamp`` only, no ``DateTime``."""
    return pd.DataFrame({
        "primary_key": pd.array([BIG + i for i in range(n)], dtype="Int64"),
        "site_id": ["RBRL"] * n,
        "timestamp": pd.date_range("2025-08-01", periods=n, freq="30min", tz="UTC"),
        "co2_flux": [0.1, 0.2, -0.1, 0.3],
        "qc_co2_flux": [0, 0, 0, 0],
        "air_temperature_c": [20.0, 21.0, 22.0, 19.0],
        "u_star": [0.2, 0.3, 0.4, 0.1],
        "VPD_hpa": [5.0, 6.0, 7.0, 4.0],
        "SWIN_1_1_1": [0.0, 100.0, 200.0, 50.0],
        "P_RAIN_1_1_1": [0.0, 0.0, 0.0, 0.0],
        "RH_1_1_1": [85.0, 86.0, 87.0, 88.0],
        "RH": [60.0, 61.0, 62.0, 63.0],
        "H": [10.0, 20.0, 30.0, 40.0],
        "LE": [50.0, 60.0, 70.0, 80.0],
        "bronze_only_flag": [1, 0, 1, 0],
    })


def _run_gold(tmp_path: Path, silver_path: Path) -> tuple[pd.DataFrame, dict]:
    argv = ["eddy", "run-gold", "--silver-table", str(silver_path), "--engine", "hesseflux-native",
            "--output-table", str(tmp_path / "grun" / "gold.parquet"),
            "--output-diagnostics-json", str(tmp_path / "grun" / "gold_diag.json"),
            "--output-run-json", str(tmp_path / "grun" / "gold_run.json")]
    assert cli.main(argv) == cli.SUCCESS_EXIT
    gold = read_parquet_table(tmp_path / "grun" / "gold.parquet", flag="g", role="silver")
    run = json.loads((tmp_path / "grun" / "gold_run.json").read_text(encoding="utf-8"))
    return gold, run


class TestEddyNamespace:
    def test_eddy_namespace_help_exits_zero(self, capsys):
        with pytest.raises(SystemExit) as exc:
            cli.main(["eddy", "--help"])
        assert exc.value.code == 0
        out = capsys.readouterr().out
        for command in ("run-silver", "run-gold", "validate-run"):
            assert command in out

    @pytest.mark.parametrize("flag,value", [("--engine", "hesseflux-native"), ("--repo-root", ".")])
    def test_silver_does_not_accept_backend_flags(self, flag, value, tmp_path):
        """Silver never dispatches science or needs R: no engine, no R repo root."""
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["eddy", "run-silver", "--flux-parquet", "f.parquet", "--biomet-parquet",
                               "b.parquet", "--output-table", str(tmp_path / "s.parquet"),
                               "--output-run-json", str(tmp_path / "r.json"), flag, value])


class TestSilverSourceTruthFile:
    def test_silver_parquet_uses_source_truth_names_and_keeps_flux_columns(self, tmp_path):
        flux, biomet = _typed_inputs()
        silver, run = _run_silver(tmp_path, flux, biomet)
        cols = list(silver.columns)
        for final in SOURCE_TRUTH_NAMES:
            assert final in cols, final
        for internal in INTERNAL_NAMES:
            assert internal not in cols, internal
        assert cols.count("timestamp") == 1
        assert cols[:3] == ["primary_key", "site_id", "timestamp"]
        # Flux-side RH stays distinct from biomet RH_1_1_1.
        assert "RH" in cols and "RH_1_1_1" in cols
        observed = silver.dropna(subset=["primary_key"])
        src = flux.set_index("primary_key")
        np.testing.assert_allclose(observed["RH"].to_numpy(float),
                                   src.loc[observed["primary_key"].tolist(), "RH"].to_numpy(float))
        assert not np.allclose(observed["RH"].to_numpy(float), observed["RH_1_1_1"].to_numpy(float),
                               equal_nan=True)
        # Bronze-only flux columns (M28/M34 sentinel) survive with their values.
        assert "bronze_only_flag" in cols
        np.testing.assert_array_equal(
            observed["bronze_only_flag"].to_numpy(float),
            src.loc[observed["primary_key"].tolist(), "bronze_only_flag"].to_numpy(float))
        # Unit-aware aliases replace the bronze names.
        assert "air_temperature" not in cols and "u." not in cols
        assert run["column_collision_actions"] == []


class TestGoldSourceTruthFile:
    def test_gold_consumes_timestamp_only_silver_and_preserves_silver(self, tmp_path, fake_engine):
        silver = _source_truth_silver()
        silver_path = publish_table(tmp_path / "srun", silver)
        gold, run = _run_gold(tmp_path, silver_path)
        # Backend received DateTime reconstructed from the source timestamp.
        assert len(fake_engine) == 1
        sent = fake_engine[0]
        assert list(pd.DatetimeIndex(sent["DateTime"]).tz_convert("UTC")) == list(silver["timestamp"])
        for internal in ("NEE", "Tair", "USTAR", "VPD", "Rg"):
            assert internal in sent.columns, internal
        np.testing.assert_allclose(sent["NEE"].to_numpy(float), silver["co2_flux"].to_numpy(float))
        # Gold: one timestamp, no DateTime, every silver field retained unchanged.
        cols = list(gold.columns)
        assert cols.count("timestamp") == 1 and "DateTime" not in cols
        assert cols[:3] == ["primary_key", "site_id", "timestamp"]
        for name in silver.columns:
            assert name in cols, name
            pd.testing.assert_series_equal(gold[name], silver[name], check_names=False, check_dtype=False)
        assert gold["primary_key"].equals(silver["primary_key"])
        for derived in GOLD_DERIVED:
            assert derived in cols, derived
        assert run["gold_columns"] == cols

    def test_gold_timestamp_wins_over_leaked_DateTime(self, tmp_path, fake_engine):
        silver = _source_truth_silver()
        silver["DateTime"] = pd.date_range("2025-09-01", periods=len(silver), freq="30min", tz="UTC")  # stale leaked time
        silver_path = publish_table(tmp_path / "srun", silver)
        gold, _ = _run_gold(tmp_path, silver_path)
        sent = fake_engine[0]
        assert list(pd.DatetimeIndex(sent["DateTime"]).tz_convert("UTC")) == list(silver["timestamp"])
        assert list(gold.columns).count("timestamp") == 1 and "DateTime" not in gold.columns
        assert gold["timestamp"].equals(silver["timestamp"])


# M002-S04-F1 (M28 humidity policy through the file CLI): stage 1 is wrapped so
# its frame carries a second literal ``rH`` column, exactly as the retired
# BigQuery silver CLI tests fed a duplicate humidity series. The published
# silver Parquet and its run JSON must show the deterministic resolution.
HUMIDITY_CASES = {
    "divergent": ("renamed_divergent_duplicate", "rH_norm_s"),
    "equivalent": ("suppressed_equivalent_duplicate", None),
}


def _expected_humidity_action(case: str) -> dict:
    action, renamed_to = HUMIDITY_CASES[case]
    reason = ("derived duplicate 'rH' diverges from source; preserved both as 'rH' and 'rH_norm_s'."
              if case == "divergent" else
              "derived duplicate 'rH' equivalent to source; kept the first occurrence as 'rH'.")
    return {"column": "rH", "action": action, "renamed_to": renamed_to,
            "source_column_name": "rH", "reason": reason}


@pytest.fixture
def duplicate_humidity_stage1(monkeypatch):
    """Wrap the stage-1 seam the CLI calls so its frame gains a duplicate ``rH``."""
    real = eddy_pkg.load_stage1_from_dataframes
    state = {"case": None, "duplicates": []}

    def _with_duplicate_rh(*args, **kwargs):
        stage1 = real(*args, **kwargs)
        kept = stage1["rH"]
        assert kept.notna().any()
        dup = kept + 5.0 if state["case"] == "divergent" else kept.copy()
        state["duplicates"].append(dup.to_numpy(float))
        return pd.concat([stage1, dup.rename("rH")], axis=1)

    monkeypatch.setattr(eddy_pkg, "load_stage1_from_dataframes", _with_duplicate_rh)
    return state


def _two_site_inputs():
    flux, biomet = _typed_inputs(days=1)
    fluxes, biomets = [], []
    for offset, site in enumerate(("SITE_A", "SITE_B")):
        f, b = flux.copy(), biomet.copy()
        f["site_id"], b["site_id"] = site, site
        f["primary_key"] = pd.array(BIG + offset * 10_000 + np.arange(len(f)), dtype="Int64")
        fluxes.append(f)
        biomets.append(b)
    return pd.concat(fluxes, ignore_index=True), pd.concat(biomets, ignore_index=True)


class TestSilverHumidityCollisionFile:
    @pytest.mark.parametrize("grouped", [False, True], ids=["ungrouped", "grouped"])
    @pytest.mark.parametrize("case", ["divergent", "equivalent"])
    def test_silver_publishes_humidity_resolution_and_run_json_actions(
            self, tmp_path, duplicate_humidity_stage1, case, grouped):
        duplicate_humidity_stage1["case"] = case
        flux, biomet = _two_site_inputs() if grouped else _typed_inputs(days=1)
        fp, bp = tmp_path / "flux.parquet", tmp_path / "biomet.parquet"
        write_parquet_table(fp, flux)
        write_parquet_table(bp, biomet)
        run_dir = tmp_path / "srun"
        argv = ["eddy", "run-silver", "--flux-parquet", str(fp), "--biomet-parquet", str(bp),
                "--output-table", str(run_dir / "silver.parquet"),
                "--output-run-json", str(run_dir / "silver_run.json")]
        if grouped:
            argv += ["--group-column", "site_id"]
        assert cli.main(argv) == cli.SUCCESS_EXIT
        assert cli.main(["eddy", "validate-run", "--run-dir", str(run_dir), "--stage", "silver"]) == 0

        silver = read_parquet_table(run_dir / "silver.parquet", flag="s", role="silver")
        run = json.loads((run_dir / "silver_run.json").read_text(encoding="utf-8"))
        cols = list(silver.columns)
        # One humidity source column (stage-1 ``rH`` published as RH_1_1_1);
        # the divergent duplicate is kept as rH_norm_s, the equivalent one is suppressed.
        humidity = [c for c in cols if c.lower().startswith("rh_")]
        expected_humidity = ["RH_1_1_1", "rH_norm_s"] if case == "divergent" else ["RH_1_1_1"]
        assert humidity == expected_humidity
        assert "rH" not in cols and len(cols) == len(set(cols))
        assert run["silver_columns"] == cols
        if case == "divergent":
            np.testing.assert_allclose(silver["rH_norm_s"].to_numpy(float),
                                       silver["RH_1_1_1"].to_numpy(float) + 5.0, equal_nan=True)
            np.testing.assert_allclose(silver["rH_norm_s"].to_numpy(float),
                                       np.concatenate(duplicate_humidity_stage1["duplicates"]), equal_nan=True)

        action = _expected_humidity_action(case)
        if grouped:
            assert [g["category_value"] for g in run["groups"]] == ["SITE_A", "SITE_B"]
            for group in run["groups"]:
                assert group["column_collision_actions"] == [action], group["category_value"]
            assert run["column_collision_actions"] == [
                {**action, "category_value": "SITE_A"}, {**action, "category_value": "SITE_B"}]
            for path in (g["table_path"] for g in run["groups"]):
                group_cols = list(read_parquet_table(Path(path), flag="s", role="silver").columns)
                assert [c for c in group_cols if c.lower().startswith("rh_")] == expected_humidity
        else:
            assert run["groups"] == []
            assert run["column_collision_actions"] == [action]
