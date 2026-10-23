"""M002-S02: explicit single-file Parquet inputs and source-facing outputs.

Covers the PyArrow reader/writer (real files: UTC/timezone instants, mixed
timestamp precision, nullable types, integer IDs above 2**53, strings,
floats/NaN, wide columns, no index column), input validation failures
(URI schemes, ambiguous/missing sources, corrupt/truncated/empty files,
invalid schemas, duplicate fields), the local silver/gold payloads
(one ``timestamp``, no ``DateTime``, unit names, carried source identity,
gap rows, duplicate/collision policy for both stages - M002-S01-F3/F5),
grouped independence and CSV compatibility through ``eddy run-silver`` /
``eddy run-gold``.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import miaproc.eddy as eddy_pkg
import synthetic_eddy
from miaproc import cli
from miaproc.eddy.io import ParquetInputError, read_parquet_table, write_parquet_table
from miaproc.eddy.payload import (
    COLUMN_COLLISION_ATTRS_KEY,
    DuplicateStageColumnsError,
    ensure_unique_stage_columns,
    prepare_local_gold_payload,
    prepare_local_silver_payload,
)

BIG = 2**53 + 1  # not representable as float64


def _typed_inputs(days: int = 3, seed: int = 7, *, sites=("S1",), ids: bool = True):
    """Typed flux/biomet frames in the case-study Parquet shape."""
    flux_parts, biomet_parts = [], []
    for k, site in enumerate(sites):
        flux, biomet = synthetic_eddy.generate_raw_frames(days=days, seed=seed + k)
        for frame in (flux, biomet):
            frame.insert(0, "timestamp", frame.pop("DateTime").dt.tz_localize("UTC"))
            frame.insert(0, "site_id", site)
        if ids:
            flux.insert(0, "primary_key", pd.array(BIG + 1000 * k + np.arange(len(flux)), dtype="Int64"))
        flux_parts.append(flux)
        biomet_parts.append(biomet)
    return pd.concat(flux_parts, ignore_index=True), pd.concat(biomet_parts, ignore_index=True)


def _write_inputs(tmp_path: Path, flux: pd.DataFrame, biomet: pd.DataFrame) -> tuple[Path, Path]:
    fp, bp = tmp_path / "flux.parquet", tmp_path / "biomet.parquet"
    write_parquet_table(fp, flux)
    write_parquet_table(bp, biomet)
    return fp, bp


def _silver_argv(tmp_path: Path, fp: Path, bp: Path, out: str = "silver.parquet", *extra: str) -> list[str]:
    return ["eddy", "run-silver", "--flux-parquet", str(fp), "--biomet-parquet", str(bp),
            "--output-table", str(tmp_path / "srun" / out), "--output-run-json",
            str(tmp_path / "srun" / "silver_run.json"), *extra]


def _gold_argv(tmp_path: Path, silver: Path, out: str = "gold.parquet", *extra: str) -> list[str]:
    return ["eddy", "run-gold", "--silver-table", str(silver), "--engine", "hesseflux-native",
            "--output-table", str(tmp_path / "grun" / out), "--output-diagnostics-json",
            str(tmp_path / "grun" / "gold_diag.json"), "--output-run-json", str(tmp_path / "grun" / "gold_run.json"),
            *extra]


@pytest.fixture
def fake_engine(monkeypatch):
    """Backend stub returning the 13 contract columns (fast CLI contract tests)."""
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


# ---------------------------------------------------------------------------
# Reader / writer roundtrips on real files
# ---------------------------------------------------------------------------


class TestParquetRoundtrip:
    def test_types_ids_timezones_precision_and_no_index(self, tmp_path):
        n = 4
        df = pd.DataFrame({
            "timestamp": pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T00:30:00.123456789Z",
                                         "2025-01-01T01:00:00.5Z", "2025-01-01T01:30:00Z"], format="ISO8601"),
            "local": pd.date_range("2025-06-01 00:00", periods=n, freq="30min", tz="America/Mazatlan"),
            "primary_key": pd.array([BIG, None, 2**63 - 1, -BIG], dtype="Int64"),
            "dense_id": np.array([BIG, BIG + 2, 1, 2], dtype="int64"),
            "label": ["a", None, "ü", ""],
            "value": [1.5, np.nan, -0.0, 1e300],
            "flag": pd.array([True, None, False, True], dtype="boolean"),
        }, index=[10, 11, 12, 13])
        path = tmp_path / "t.parquet"
        write_parquet_table(path, df)
        assert "__index_level_0__" not in pq.read_schema(path).names
        back = read_parquet_table(path, flag="--silver-table", role="silver")
        assert list(back.columns) == list(df.columns)
        assert str(back["timestamp"].dtype) == "datetime64[ns, UTC]"
        assert back["timestamp"].iloc[1].nanosecond == 789  # no truncation
        assert str(back["local"].dtype) == "datetime64[ns, America/Mazatlan]"
        assert back["local"].equals(df["local"].reset_index(drop=True))
        assert str(back["primary_key"].dtype) == "Int64"
        assert back["primary_key"].tolist()[0] == BIG and back["primary_key"].tolist()[2] == 2**63 - 1
        assert back["primary_key"].isna().tolist() == [False, True, False, False]
        assert str(back["dense_id"].dtype) == "Int64" and back["dense_id"].tolist() == [BIG, BIG + 2, 1, 2]
        assert back["label"].tolist() == ["a", None, "ü", ""]
        assert np.isnan(back["value"].iloc[1]) and back["value"].iloc[3] == 1e300

    @pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
    def test_stored_timestamp_units_are_read_without_truncation(self, tmp_path, unit):
        values = {"s": 1_700_000_000, "ms": 1_700_000_000_123, "us": 1_700_000_000_123_456,
                  "ns": 1_700_000_000_123_456_789}[unit]
        table = pa.table({"timestamp": pa.array([values], type=pa.timestamp(unit, tz="UTC"))})
        pq.write_table(table, tmp_path / "u.parquet")
        back = read_parquet_table(tmp_path / "u.parquet", flag="f", role="silver")
        scale = {"s": 10**9, "ms": 10**6, "us": 10**3, "ns": 1}[unit]
        assert back["timestamp"].iloc[0].value == values * scale

    def test_wide_columns_roundtrip(self, tmp_path):
        wide = pd.DataFrame({f"c{i:03d}": [float(i), np.nan] for i in range(400)})
        wide.insert(0, "timestamp", pd.date_range("2025-01-01", periods=2, freq="30min", tz="UTC"))
        write_parquet_table(tmp_path / "w.parquet", wide)
        back = read_parquet_table(tmp_path / "w.parquet", flag="f", role="silver")
        pd.testing.assert_frame_equal(back, wide)

    def test_science_integer_columns_with_nulls_become_float_like_csv(self, tmp_path):
        flux, _ = _typed_inputs(days=1)
        flux["qc_co2_flux"] = pd.array([0, None] * (len(flux) // 2) + [0] * (len(flux) % 2), dtype="Int64")
        write_parquet_table(tmp_path / "f.parquet", flux)
        back = read_parquet_table(tmp_path / "f.parquet", flag="--flux-parquet", role="flux")
        assert back["qc_co2_flux"].dtype == np.float64
        assert str(back["primary_key"].dtype) == "Int64"  # identity keeps its lossless integer type


class TestReaderFailures:
    @pytest.mark.parametrize("uri", ["gs://bucket/flux.parquet", "s3://bucket/flux.parquet",
                                     "http://host/flux.parquet", "https://host/flux.parquet",
                                     "file:///data/flux.parquet"])
    def test_uri_schemes_rejected(self, uri):
        with pytest.raises(ParquetInputError, match="unsupported URI scheme"):
            read_parquet_table(uri, flag="--flux-parquet", role="flux")
        with pytest.raises(ParquetInputError, match="unsupported URI scheme"):
            read_parquet_table(Path(uri), flag="--flux-parquet", role="flux")

    def test_windows_drive_is_not_a_uri(self, tmp_path):
        with pytest.raises(ParquetInputError, match="does not exist"):
            read_parquet_table("C:/no/such/flux.parquet", flag="--flux-parquet", role="flux")

    def _bad(self, tmp_path, kind):
        good = pd.DataFrame({"timestamp": pd.date_range("2025-01-01", periods=3, freq="30min", tz="UTC"),
                             "co2_flux": [1.0, 2.0, 3.0]})
        path = tmp_path / "bad.parquet"
        if kind == "corrupt":
            path.write_bytes(b"PAR1 this is not parquet")
        elif kind == "truncated":
            write_parquet_table(path, good)
            path.write_bytes(path.read_bytes()[: path.stat().st_size // 2])
        elif kind == "empty_file":
            path.write_bytes(b"")
        elif kind == "zero_rows":
            write_parquet_table(path, good.iloc[:0])
        elif kind == "duplicate_fields":
            pq.write_table(pa.Table.from_arrays([pa.array([1.0]), pa.array([2.0]), pa.array([0], pa.timestamp("ns", "UTC"))],
                                                names=["co2_flux", "co2_flux", "timestamp"]), path)
        elif kind == "nested":
            pq.write_table(pa.table({"timestamp": pa.array([0], pa.timestamp("ns", "UTC")),
                                     "co2_flux": pa.array([[1.0]])}), path)
        elif kind == "no_time":
            write_parquet_table(path, good.drop(columns=["timestamp"]))
        elif kind == "integer_timestamp":
            write_parquet_table(path, good.assign(timestamp=[1, 2, 3]))
        elif kind == "directory":
            path.mkdir()
        elif kind == "suffix":
            path = tmp_path / "bad.csv"
            path.write_text("timestamp\n", encoding="utf-8")
        return path

    @pytest.mark.parametrize("kind,match", [
        ("corrupt", "cannot read Parquet"), ("truncated", "cannot read Parquet"),
        ("empty_file", "cannot read Parquet"), ("zero_rows", "has no rows"),
        ("duplicate_fields", "duplicate column names"), ("nested", "nested"),
        ("no_time", "no time column"), ("integer_timestamp", "timestamp or ISO-8601 string"),
        ("directory", "not a single file"), ("suffix", "expected a .parquet"),
    ])
    def test_invalid_files_fail_clearly(self, tmp_path, kind, match):
        with pytest.raises(ParquetInputError, match=match):
            read_parquet_table(self._bad(tmp_path, kind), flag="--silver-table", role="silver")

    @pytest.mark.parametrize("role,drop,match", [
        ("flux", "co2_flux", "missing required columns"), ("flux", "u.", "u_star"),
        ("biomet", "RH_1_1_1", "missing required columns"),
    ])
    def test_invalid_input_schemas(self, tmp_path, role, drop, match):
        flux, biomet = _typed_inputs(days=1)
        frame = (flux if role == "flux" else biomet).drop(columns=[drop])
        write_parquet_table(tmp_path / "x.parquet", frame)
        with pytest.raises(ParquetInputError, match=match):
            read_parquet_table(tmp_path / "x.parquet", flag=f"--{role}-parquet", role=role)


# ---------------------------------------------------------------------------
# Local payloads: identity, timestamp, duplicates/collisions (both stages)
# ---------------------------------------------------------------------------


def _stage1_like() -> pd.DataFrame:
    dt = pd.date_range("2025-01-01", periods=3, freq="30min", tz="UTC")
    return pd.DataFrame({
        "DateTime": dt, "primary_key": pd.array([BIG, None, BIG + 2], dtype="Int64"),
        "site_id": ["S1", "S1", "S1"], "timestamp": ["2025-01-01 00:00", None, "2025-01-01 01:00"],
        "NEE": [1.0, np.nan, 3.0], "Tair": [20.0, np.nan, 21.0], "VPD": [5.0, np.nan, 6.0],
        "USTAR": [0.2, np.nan, 0.3], "QC_NEE": [0.0, np.nan, 1.0], "Rg": [10.0, np.nan, 0.0],
        "P_RAIN": [0.0, np.nan, 0.0], "rH": [60.0, np.nan, 61.0], "RH": [59.0, np.nan, 60.0],
    })


class TestLocalSilverPayload:
    def test_source_facing_shape_and_carried_identity(self):
        payload, actions = prepare_local_silver_payload(_stage1_like())
        assert list(payload.columns[:3]) == ["primary_key", "site_id", "timestamp"]
        assert list(payload.columns).count("timestamp") == 1 and "DateTime" not in payload.columns
        for internal, final in (("NEE", "co2_flux"), ("Tair", "air_temperature_c"), ("VPD", "VPD_hpa"),
                                ("USTAR", "u_star"), ("rH", "RH_1_1_1"), ("Rg", "SWIN_1_1_1")):
            assert final in payload.columns and internal not in payload.columns
        assert "RH" in payload.columns  # independent flux humidity kept
        assert str(payload["primary_key"].dtype) == "Int64"
        assert payload["primary_key"].tolist()[0] == BIG and pd.isna(payload["primary_key"].iloc[1])
        assert payload["timestamp"].notna().all() and str(payload["timestamp"].dtype) == "datetime64[ns, UTC]"
        assert np.isnan(payload["co2_flux"].iloc[1])  # gap measurement stays missing
        assert actions == [] and payload.attrs[COLUMN_COLLISION_ATTRS_KEY] == []

    @pytest.mark.parametrize("second,expected_cols,action", [
        ([60.0, np.nan, 61.0], ["RH_1_1_1"], "suppressed_equivalent_duplicate"),
        ([1.0, 2.0, 3.0], ["RH_1_1_1", "rH_norm_s"], "renamed_divergent_duplicate"),
    ])
    def test_duplicate_humidity_is_deterministic_and_not_multiplied(self, second, expected_cols, action):
        base = _stage1_like()
        dup = pd.concat([base, pd.DataFrame({"rH": second})], axis=1)
        payload, actions = prepare_local_silver_payload(dup)
        assert [c for c in payload.columns if c.lower().startswith("rh_")] == expected_cols
        assert payload.shape[1] == len(set(payload.columns))
        assert [a["action"] for a in actions] == [action]

    def test_non_humidity_literal_duplicate_fails(self):
        dup = pd.concat([_stage1_like(), pd.DataFrame({"NEE": [9.0, 9.0, 9.0]})], axis=1)
        with pytest.raises(DuplicateStageColumnsError, match="NEE"):
            prepare_local_silver_payload(dup)

    def test_case_only_names_are_distinct_locally_but_collide_under_casefold_key(self):
        df = pd.DataFrame({"TA_f": [1.0], "ta_f": [1.0]})
        out, actions = ensure_unique_stage_columns(df, key=str)
        assert list(out.columns) == ["TA_f", "ta_f"] and actions == []
        with pytest.raises(DuplicateStageColumnsError):
            ensure_unique_stage_columns(df)


def _gold_like() -> pd.DataFrame:
    s = _stage1_like()
    gold = pd.DataFrame({"DateTime": s["DateTime"], "NEE": s["NEE"], "NEE_f": [1.0, 2.0, 3.0],
                         "NEE_fqc": [0, 1, 0], "GPP": [1.0, 1.0, 1.0], "Reco": [2.0, 2.0, 2.0],
                         "RECO": [2.5, 2.5, 2.5], "Tair": s["Tair"], "Tair_f": [20.0, 20.5, 21.0],
                         "TA_f": [20.0, 20.5, 21.0], "Rg": s["Rg"], "Rg_f": [10.0, 5.0, 0.0],
                         "VPD": s["VPD"], "VPD_f": [5.0, 5.5, 6.0], "USTAR": s["USTAR"]})
    silver, _ = prepare_local_silver_payload(s)
    extras = silver[[c for c in silver.columns if c not in gold.columns]].copy()
    return pd.concat([gold, extras], axis=1)


class TestLocalGoldPayload:
    def test_gold_contract(self):
        payload, actions = prepare_local_gold_payload(_gold_like())
        cols = list(payload.columns)
        assert cols[:3] == ["primary_key", "site_id", "timestamp"]
        assert cols.count("timestamp") == 1 and "DateTime" not in cols
        assert cols[3:] == sorted(cols[3:])
        for name in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f", "dateAndTime", "GPP", "Reco", "RECO", "TA_f",
                     "co2_flux", "air_temperature_c", "VPD_hpa", "u_star", "SWIN_1_1_1", "RH_1_1_1", "RH"):
            assert name in cols, name
        for internal in ("NEE", "Tair", "Rg", "VPD", "USTAR", "NEE_f", "Tair_f", "Rg_f", "VPD_f", "rH"):
            assert internal not in cols, internal
        assert payload["primary_key"].tolist()[0] == BIG and pd.isna(payload["primary_key"].iloc[1])
        assert payload["dateAndTime"].tolist()[0] == "2025-01-01 00:00:00"
        assert actions == []

    def test_silver_timestamp_has_precedence_over_backend_datetime(self):
        gold = _gold_like()
        gold["DateTime"] = gold["DateTime"] + pd.Timedelta(hours=1)
        payload, _ = prepare_local_gold_payload(gold)
        assert payload["timestamp"].iloc[0] == pd.Timestamp("2025-01-01 00:00", tz="UTC")

    @pytest.mark.parametrize("second,expect_norm", [([60.0, np.nan, 61.0], False), ([1.0, 2.0, 3.0], True)])
    def test_duplicate_rh_in_gold_is_deterministic(self, second, expect_norm):
        gold = pd.concat([_gold_like(), pd.DataFrame({"rH": [60.0, np.nan, 61.0]}),
                          pd.DataFrame({"rH": second})], axis=1)
        payload, actions = prepare_local_gold_payload(gold)
        assert "rH" not in payload.columns  # M32 drop of the kept copy (RH_1_1_1 present)
        assert ("rH_norm_s" in payload.columns) is expect_norm
        assert payload.shape[1] == len(set(payload.columns))
        assert len(actions) == 1

    def test_non_humidity_literal_duplicate_fails_in_gold(self):
        gold = pd.concat([_gold_like(), pd.DataFrame({"GPP": [0.0, 0.0, 0.0]})], axis=1)
        with pytest.raises(DuplicateStageColumnsError, match="GPP"):
            prepare_local_gold_payload(gold)


# ---------------------------------------------------------------------------
# CLI: run-silver / run-gold with Parquet
# ---------------------------------------------------------------------------


class TestCliParquet:
    def test_silver_from_parquet_is_source_facing(self, tmp_path):
        flux, biomet = _typed_inputs(days=3)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp)) == cli.SUCCESS_EXIT
        silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
        cols = list(silver.columns)
        assert cols[:3] == ["primary_key", "site_id", "timestamp"] and cols.count("timestamp") == 1
        assert "DateTime" not in cols and "air_temperature_c" in cols and "VPD_hpa" in cols
        assert str(silver["primary_key"].dtype) == "Int64"
        observed = silver["primary_key"].dropna()
        assert set(observed.tolist()) <= set(flux["primary_key"].tolist()) and BIG in observed.tolist()
        gaps = silver["primary_key"].isna()
        assert gaps.any() and len(silver) == 3 * 48
        assert (silver.loc[gaps, "site_id"] == "S1").all() and silver.loc[gaps, "timestamp"].notna().all()
        assert silver.loc[gaps, "co2_flux"].isna().all()
        assert silver["timestamp"].is_monotonic_increasing and silver["timestamp"].is_unique
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        assert run["inputs"]["flux_format"] == "parquet" and run["outputs"]["contract"] == "source-facing"
        assert run["row_counts"]["flux_input"] == len(flux) and run["column_collision_actions"] == []

    def test_parquet_and_csv_directory_inputs_give_the_same_silver(self, tmp_path):
        paths = synthetic_eddy.write_fixture(tmp_path / "csv", days=3, seed=11)
        csv_dir = tmp_path / "csv"
        assert cli.main(["eddy", "run-silver", "--flux-dir", str(csv_dir / "full_output"), "--biomet-dir",
                         str(csv_dir / "biomet"), "--output-table",
                         str(tmp_path / "csvrun" / "from_csv.parquet"), "--output-run-json",
                         str(tmp_path / "csvrun" / "r1.json")]) == 0
        typed = {}
        for side, rel in (("flux", "full_output/full_output.csv"), ("biomet", "biomet/biomet.csv")):
            frame = pd.read_csv(csv_dir / rel, skiprows=[1], na_values=["-9999"])
            frame.insert(0, "timestamp", pd.to_datetime(frame.pop("date") + " " + frame.pop("time"), utc=True))
            typed[side] = frame
        fp, bp = _write_inputs(tmp_path, typed["flux"], typed["biomet"])
        assert cli.main(_silver_argv(tmp_path, fp, bp, "from_pq.parquet")) == 0
        a = read_parquet_table(tmp_path / "csvrun" / "from_csv.parquet", flag="s", role="silver")
        b = read_parquet_table(tmp_path / "srun" / "from_pq.parquet", flag="s", role="silver")
        a = a.drop(columns=["date", "time"])
        assert list(a.columns) == list(b.columns)
        pd.testing.assert_series_equal(a["timestamp"], b["timestamp"])
        for col in ("co2_flux", "qc_co2_flux", "air_temperature_c", "VPD_hpa", "u_star", "SWIN_1_1_1",
                    "P_RAIN_1_1_1", "RH_1_1_1", "H", "LE"):
            np.testing.assert_allclose(pd.to_numeric(a[col]), pd.to_numeric(b[col]), rtol=0, atol=1e-9,
                                       equal_nan=True, err_msg=col)
        assert paths  # CSV fixture written

    def test_csv_output_keeps_the_legacy_shape(self, tmp_path):
        flux, biomet = _typed_inputs(days=1)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp, "silver.csv")) == 0
        df = pd.read_csv(tmp_path / "srun" / "silver.csv")
        assert list(df.columns).count("timestamp") == 1 and "DateTime" not in df.columns
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        assert run["outputs"]["contract"] == "legacy-csv"

    def test_timezone_naive_parquet_timestamps_follow_tz_in(self, tmp_path):
        flux, biomet = _typed_inputs(days=1, ids=False)
        for frame in (flux, biomet):
            frame["timestamp"] = frame["timestamp"].dt.tz_localize(None)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp, "s.parquet", "--tz-in", "America/Mazatlan")) == 0
        silver = read_parquet_table(tmp_path / "srun" / "s.parquet", flag="s", role="silver")
        first = flux["timestamp"].iloc[0].tz_localize("America/Mazatlan")
        assert silver["timestamp"].iloc[0] == first  # same instant, not re-read as UTC

    @pytest.mark.parametrize("argv_extra,code", [
        (["--flux-dir", "x"], "argparse"),  # ambiguous: two flux sources
    ])
    def test_ambiguous_sources_rejected(self, tmp_path, argv_extra, code):
        flux, biomet = _typed_inputs(days=1)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        with pytest.raises(SystemExit) as exc:
            cli.main(_silver_argv(tmp_path, fp, bp) + argv_extra)
        assert exc.value.code == 2

    def test_missing_source_rejected(self, tmp_path):
        with pytest.raises(SystemExit):
            cli.main(["eddy", "run-silver", "--biomet-parquet", "b.parquet", "--output-table",
                      str(tmp_path / "srun" / "s.parquet"), "--output-run-json", str(tmp_path / "r.json")])

    @pytest.mark.parametrize("mutate", ["uri_flux", "uri_output", "skip_with_parquet", "corrupt",
                                        "empty", "schema", "duplicate"])
    def test_silver_input_errors_exit_three(self, tmp_path, mutate):
        flux, biomet = _typed_inputs(days=1)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        argv = _silver_argv(tmp_path, fp, bp)
        if mutate == "uri_flux":
            argv[argv.index(str(fp))] = "gs://bucket/flux.parquet"
        elif mutate == "uri_output":
            argv[argv.index(str(tmp_path / "srun" / "silver.parquet"))] = "s3://bucket/silver.parquet"
        elif mutate == "skip_with_parquet":
            argv += ["--skip-flux", "1"]
        elif mutate == "corrupt":
            fp.write_bytes(fp.read_bytes()[:100])
        elif mutate == "empty":
            write_parquet_table(fp, flux.iloc[:0])
        elif mutate == "schema":
            write_parquet_table(bp, biomet.drop(columns=["SWIN_1_1_1"]))
        elif mutate == "duplicate":
            table = pq.read_table(fp)
            pq.write_table(table.append_column("H", table.column("H")), fp)
        assert cli.main(argv) == cli.VALIDATION_EXIT
        assert not (tmp_path / "srun" / "silver.parquet").exists()

    def test_gold_from_parquet_silver_is_source_facing(self, tmp_path, fake_engine):
        flux, biomet = _typed_inputs(days=3)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp)) == 0
        silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
        assert cli.main(_gold_argv(tmp_path, tmp_path / "srun" / "silver.parquet")) == 0
        # The backend received the internal calc frame reconstructed from timestamp.
        assert "DateTime" in fake_engine[0].columns and "NEE" in fake_engine[0].columns
        gold = read_parquet_table(tmp_path / "grun" / "gold.parquet", flag="g", role="silver")
        cols = list(gold.columns)
        assert cols[:3] == ["primary_key", "site_id", "timestamp"] and cols.count("timestamp") == 1
        assert "DateTime" not in cols and {"nee_f", "ta_f", "sw_in_f", "vpd_f", "nee_fqc", "dateAndTime"} <= set(cols)
        assert [c for c in silver.columns if c not in cols] == []  # gold retains silver fields
        assert gold["primary_key"].equals(silver["primary_key"])  # identity unchanged, incl. > 2**53
        assert gold["timestamp"].equals(silver["timestamp"])
        diag = json.loads((tmp_path / "grun" / "gold_diag.json").read_text(encoding="utf-8"))
        assert diag == {"backend": "stub", "engine": "hesseflux"}  # persisted, not only attrs
        run = json.loads((tmp_path / "grun" / "gold_run.json").read_text(encoding="utf-8"))
        assert run["outputs"]["contract"] == "source-facing" and run["inputs"]["silver_format"] == "parquet"
        assert run["gold_columns"] == cols and run["column_collision_actions"] == []

    @pytest.mark.parametrize("bad", ["corrupt", "no_time", "uri"])
    def test_gold_rejects_invalid_parquet_silver_before_preflight(self, tmp_path, monkeypatch, bad):
        called = []
        monkeypatch.setattr(cli, "_run_preflight_or_exit", lambda root: called.append(root))
        silver = tmp_path / "silver.parquet"
        if bad == "corrupt":
            silver.write_bytes(b"not parquet")
        else:
            write_parquet_table(silver, pd.DataFrame({"co2_flux": [1.0]}))
        argv = ["eddy", "run-gold", "--silver-table", "gs://b/silver.parquet" if bad == "uri" else str(silver),
                "--engine", "reddyproc-reference", "--repo-root", str(tmp_path),
                "--output-table", str(tmp_path / "grun" / "gold.parquet"),
                "--output-diagnostics-json", str(tmp_path / "grun" / "d.json"),
                "--output-run-json", str(tmp_path / "grun" / "r.json")]
        assert cli.main(argv) == cli.VALIDATION_EXIT
        assert called == [] and not (tmp_path / "grun").exists()

    def test_grouped_parquet_runs_groups_independently(self, tmp_path, fake_engine):
        flux, biomet = _typed_inputs(days=2, sites=("S/1", "site 3", "S2"))
        nulls = flux.iloc[:5].copy()
        nulls["site_id"] = None
        flux = pd.concat([flux, nulls], ignore_index=True)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        rc = cli.main(_silver_argv(tmp_path, fp, bp, "silver.parquet", "--group-column", "site_id"))
        assert rc == cli.SUCCESS_EXIT
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        assert [g["category_value"] for g in run["groups"]] == ["S/1", "S2", "site 3"]
        assert run["row_counts"]["null_category_rows_skipped"] == 5
        silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
        assert silver["site_id"].tolist() == sorted(silver["site_id"].tolist(), key=["S/1", "S2", "site 3"].index)
        assert silver["site_id"].notna().all()
        for site, frame in silver.groupby("site_id"):
            assert len(frame) == 2 * 48 and frame["timestamp"].is_unique, site
        # Filename-unsafe labels get distinct per-group files.
        paths = [g["table_path"] for g in run["groups"]]
        assert len(set(paths)) == 3 and all(Path(p).is_file() for p in paths)
        assert cli.main(_gold_argv(tmp_path, tmp_path / "srun" / "silver.parquet", "gold.parquet",
                                   "--group-column", "site_id")) == 0
        assert len(fake_engine) == 3 and [len(f) for f in fake_engine] == [96, 96, 96]
        gold = read_parquet_table(tmp_path / "grun" / "gold.parquet", flag="g", role="silver")
        assert gold["primary_key"].equals(silver["primary_key"])


    def test_colliding_group_labels_are_rejected(self, tmp_path):
        flux, biomet = _typed_inputs(days=1, sites=("S/1", "s 1"))
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        rc = cli.main(_silver_argv(tmp_path, fp, bp, "silver.parquet", "--group-column", "site_id"))
        assert rc == cli.RUNTIME_EXIT
        assert not (tmp_path / "srun" / "silver.parquet").exists() and not (tmp_path / "srun" / "silver__groups").exists()


def _no_outputs(tmp_path: Path, *names: str) -> list[str]:
    # M002-S03: outputs live in fresh run dirs (srun/grun); a failed run
    # publishes nothing there, not even the run directory.
    hits = [n for n in names for d in (tmp_path, tmp_path / "srun", tmp_path / "grun") if (d / n).exists()]
    if any(n.startswith("gold") for n in names) and (tmp_path / "grun").exists():
        hits.append("grun")
    if any(n.startswith(("silver", "s.")) for n in names) and not any(
            n.startswith("gold") for n in names) and (tmp_path / "srun").exists():
        hits.append("srun")
    return hits


class TestCliParquetC1Cases:
    """M002-S02-F1: duplicate instants, all-null groups and gold collisions."""

    def _with_duplicates(self, sites=("S1",)):
        flux, biomet = _typed_inputs(days=2, sites=sites)
        first_site = flux["site_id"] == sites[0]
        usable = first_site & flux["co2_flux"].notna() & (flux["qc_co2_flux"] < 2)
        ts = flux.loc[usable, "timestamp"].iloc[10]
        original = flux.loc[first_site & (flux["timestamp"] == ts)].iloc[0]
        dup = original.copy()
        dup["primary_key"] = BIG + 999_999
        dup["co2_flux"] = original["co2_flux"] + 100.0
        flux = pd.concat([flux, dup.to_frame().T], ignore_index=True).astype(flux.dtypes.to_dict())
        bdup = biomet.loc[biomet["site_id"] == sites[0]].iloc[[20]].assign(SWIN_1_1_1=-1.0)
        biomet = pd.concat([biomet, bdup], ignore_index=True)
        return flux, biomet, ts, original

    def test_duplicate_instants_keep_first_through_silver_and_gold(self, tmp_path, fake_engine):
        flux, biomet, ts, original = self._with_duplicates()
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp)) == cli.SUCCESS_EXIT
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        assert run["row_counts"]["duplicate_instants_dropped"] == {"flux": 1, "biomet": 1}
        silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
        assert silver["timestamp"].is_unique and len(silver) == 2 * 48
        row = silver.loc[silver["timestamp"] == ts]
        assert len(row) == 1
        assert row["primary_key"].iloc[0] == original["primary_key"]  # first occurrence survives
        assert row["co2_flux"].iloc[0] == original["co2_flux"]
        assert BIG + 999_999 not in silver["primary_key"].dropna().tolist()
        assert (silver["SWIN_1_1_1"].dropna() != -1.0).all()  # biomet duplicate dropped too
        assert cli.main(_gold_argv(tmp_path, tmp_path / "srun" / "silver.parquet")) == cli.SUCCESS_EXIT
        gold = read_parquet_table(tmp_path / "grun" / "gold.parquet", flag="g", role="silver")
        assert len(gold) == len(silver) and gold["timestamp"].is_unique
        assert gold["timestamp"].equals(silver["timestamp"])
        assert gold["primary_key"].equals(silver["primary_key"])

    def test_duplicate_instants_are_counted_per_group(self, tmp_path, fake_engine):
        flux, biomet, ts, original = self._with_duplicates(sites=("S1", "S2"))
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        rc = cli.main(_silver_argv(tmp_path, fp, bp, "silver.parquet", "--group-column", "site_id"))
        assert rc == cli.SUCCESS_EXIT
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        # The same instant at two different sites is not a duplicate.
        assert {g["category_value"]: g["duplicate_instants_dropped"] for g in run["groups"]} == {
            "S1": {"flux": 1, "biomet": 1}, "S2": {"flux": 0, "biomet": 0}}
        assert run["row_counts"]["duplicate_instants_dropped"] == {"flux": 1, "biomet": 1}
        silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
        s1 = silver.loc[silver["site_id"] == "S1"]
        assert s1["timestamp"].is_unique and len(s1) == 2 * 48
        assert s1.loc[s1["timestamp"] == ts, "primary_key"].tolist() == [original["primary_key"]]
        rc = cli.main(_gold_argv(tmp_path, tmp_path / "srun" / "silver.parquet", "gold.parquet", "--group-column", "site_id"))
        assert rc == cli.SUCCESS_EXIT
        gold = read_parquet_table(tmp_path / "grun" / "gold.parquet", flag="g", role="silver")
        assert len(gold) == len(silver)
        for site, frame in gold.groupby("site_id"):
            assert frame["timestamp"].is_unique, site
        assert gold["primary_key"].equals(silver["primary_key"])

    def test_csv_ungrouped_path_reports_duplicates_as_not_computed(self, tmp_path):
        synthetic_eddy.write_fixture(tmp_path / "csv", days=1, seed=3)
        argv = ["eddy", "run-silver", "--flux-dir", str(tmp_path / "csv" / "full_output"), "--biomet-dir",
                str(tmp_path / "csv" / "biomet"), "--output-table", str(tmp_path / "srun" / "s.parquet"),
                "--output-run-json", str(tmp_path / "srun" / "silver_run.json")]
        assert cli.main(argv) == cli.SUCCESS_EXIT
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        assert run["row_counts"]["duplicate_instants_dropped"] == {"flux": None, "biomet": None}

    def test_all_null_group_column_fails_silver_without_outputs(self, tmp_path):
        flux, biomet = _typed_inputs(days=1)
        flux["site_id"] = None
        biomet["site_id"] = None
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        rc = cli.main(_silver_argv(tmp_path, fp, bp, "silver.parquet", "--group-column", "site_id"))
        assert rc == cli.RUNTIME_EXIT
        assert _no_outputs(tmp_path, "silver.parquet", "silver_run.json", "silver__groups") == []

    def test_all_null_group_column_fails_gold_without_outputs(self, tmp_path, fake_engine):
        flux, biomet = _typed_inputs(days=1)
        flux["site_id"] = None
        biomet["site_id"] = None
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp)) == cli.SUCCESS_EXIT
        silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
        assert silver["site_id"].isna().all()
        rc = cli.main(_gold_argv(tmp_path, tmp_path / "srun" / "silver.parquet", "gold.parquet", "--group-column", "site_id"))
        assert rc == cli.RUNTIME_EXIT
        assert fake_engine == []
        assert _no_outputs(tmp_path, "gold.parquet", "gold_diag.json", "gold_run.json", "gold__groups") == []

    def test_gold_rejects_colliding_labels_before_any_group(self, tmp_path, fake_engine):
        flux, biomet = _typed_inputs(days=1)
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp, "one.parquet")) == cli.SUCCESS_EXIT
        one = read_parquet_table(tmp_path / "srun" / "one.parquet", flag="s", role="silver")
        both = pd.concat([one.assign(site_id="S/1"), one.assign(site_id="s 1")], ignore_index=True)
        from publish_helpers import publish_table

        both_silver = publish_table(tmp_path / "both_run", both, "silver.parquet")
        rc = cli.main(_gold_argv(tmp_path, both_silver, "gold.parquet", "--group-column", "site_id"))
        assert rc == cli.RUNTIME_EXIT
        assert fake_engine == []  # no group ran
        assert _no_outputs(tmp_path, "gold.parquet", "gold_diag.json", "gold_run.json", "gold__groups") == []

    def test_mixed_parquet_and_csv_directory_inputs(self, tmp_path):
        synthetic_eddy.write_fixture(tmp_path / "csv", days=1, seed=5)
        flux, _ = _typed_inputs(days=1, ids=False)
        write_parquet_table(tmp_path / "flux.parquet", flux.drop(columns=["site_id"]))
        argv = ["eddy", "run-silver", "--flux-parquet", str(tmp_path / "flux.parquet"), "--biomet-dir",
                str(tmp_path / "csv" / "biomet"), "--output-table", str(tmp_path / "srun" / "s.parquet"),
                "--output-run-json", str(tmp_path / "srun" / "silver_run.json")]
        assert cli.main(argv) == cli.SUCCESS_EXIT
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        assert (run["inputs"]["flux_format"], run["inputs"]["biomet_format"]) == ("parquet", "csv_dir")

    def test_mixed_mode_csv_side_failure_is_a_runtime_exit(self, tmp_path):
        flux, _ = _typed_inputs(days=1)
        write_parquet_table(tmp_path / "flux.parquet", flux)
        (tmp_path / "empty_biomet").mkdir()
        argv = ["eddy", "run-silver", "--flux-parquet", str(tmp_path / "flux.parquet"), "--biomet-dir",
                str(tmp_path / "empty_biomet"), "--output-table", str(tmp_path / "srun" / "s.parquet"),
                "--output-run-json", str(tmp_path / "srun" / "silver_run.json")]
        assert cli.main(argv) == cli.RUNTIME_EXIT
        assert _no_outputs(tmp_path, "s.parquet", "silver_run.json") == []

    def test_source_column_colliding_with_a_renamed_name_fails_clearly(self, tmp_path):
        flux, biomet = _typed_inputs(days=1)
        flux["air_temperature_c"] = 20.0
        fp, bp = _write_inputs(tmp_path, flux, biomet)
        assert cli.main(_silver_argv(tmp_path, fp, bp)) == cli.RUNTIME_EXIT
        assert _no_outputs(tmp_path, "silver.parquet", "silver_run.json") == []
        with pytest.raises(DuplicateStageColumnsError, match="air_temperature_c"):
            prepare_local_silver_payload(_stage1_like().assign(air_temperature_c=1.0))


def test_wide_typed_columns_survive_local_silver_and_gold_payloads(tmp_path):
    """M002-S02-F8: typed wide passthrough through both payloads and a Parquet file."""
    base = _stage1_like()
    n = len(base)
    extras = {}
    for i in range(60):
        extras[f"id_{i:02d}"] = pd.array([BIG + i, None, BIG + 2 + i], dtype="Int64")
        extras[f"x_{i:02d}"] = [float(i), np.nan, -float(i)]
        extras[f"s_{i:02d}"] = [f"v{i}", None, ""]
        extras[f"b_{i:02d}"] = pd.array([True, None, False], dtype="boolean")
        extras[f"t_{i:02d}"] = pd.date_range("2025-01-01", periods=n, freq="1s", tz="America/Mazatlan")
    wide = pd.concat([base, pd.DataFrame(extras)], axis=1)
    silver, _ = prepare_local_silver_payload(wide)
    gold_in = pd.concat([_gold_like(), pd.DataFrame(extras)], axis=1)
    gold, _ = prepare_local_gold_payload(gold_in)
    for frame, name in ((silver, "silver"), (gold, "gold")):
        write_parquet_table(tmp_path / f"{name}.parquet", frame)
        back = read_parquet_table(tmp_path / f"{name}.parquet", flag="f", role="silver")
        for col, values in extras.items():
            expected = pd.Series(values, name=col)
            if str(expected.dtype) == "boolean":
                expected = expected.astype(object).where(expected.notna(), None)
                pd.testing.assert_series_equal(back[col].astype(object), expected, check_names=False)
            else:
                pd.testing.assert_series_equal(back[col], expected, check_names=False)


# ---------------------------------------------------------------------------
# M002-S02-H1-F1: mixed timestamp-string precision (C3), published runs
# ---------------------------------------------------------------------------

_PRECISIONS = ("{:%Y-%m-%d %H:%M:%S} UTC", "{:%Y-%m-%d %H:%M:%S}.000000 UTC")
_ISO_VARIANTS = ("{:%Y-%m-%dT%H:%M:%S}Z", "{:%Y-%m-%dT%H:%M:%S}.000Z", "{:%Y-%m-%d %H:%M:%S}.000000+00:00",
                 "{:%Y-%m-%dT%H:%M:%S}.000000000Z", "{:%Y-%m-%d %H:%M:%S}.5Z")


def _observations(days: int = 2, seed: int = 21):
    flux, biomet = synthetic_eddy.generate_raw_frames(days=days, seed=seed)
    flux.insert(1, "obs_id", np.arange(len(flux), dtype="int64"))
    biomet.insert(1, "met_id", np.arange(len(biomet), dtype="int64"))
    return flux, biomet


def _check_every_observation_survives(tmp_path: Path, flux: pd.DataFrame, biomet: pd.DataFrame,
                                      silver_table: Path) -> pd.DataFrame:
    """Every source row is in silver once, at its exact instant, then in gold; runs validate."""
    silver = read_parquet_table(silver_table, flag="s", role="silver")
    expected = dict(zip(flux["obs_id"].tolist(), flux["DateTime"].dt.tz_localize("UTC")))
    got = silver.dropna(subset=["obs_id"])
    assert sorted(int(i) for i in got["obs_id"]) == sorted(expected)
    for obs_id, ts in zip(got["obs_id"], got["timestamp"]):
        assert ts == expected[int(obs_id)], obs_id
    met = dict(zip(biomet["DateTime"].dt.tz_localize("UTC"), biomet["SWIN_1_1_1"]))
    joined = silver.loc[silver["timestamp"].isin(list(met)) & silver["obs_id"].notna()]
    np.testing.assert_allclose(joined["SWIN_1_1_1"].to_numpy(float),
                               [met[t] for t in joined["timestamp"]], equal_nan=True)
    assert silver["timestamp"].is_unique
    assert cli.main(["eddy", "validate-run", "--run-dir", str(silver_table.parent), "--stage", "silver"]) == 0
    assert cli.main(_gold_argv(tmp_path, silver_table)) == cli.SUCCESS_EXIT
    gold = read_parquet_table(tmp_path / "grun" / "gold.parquet", flag="g", role="silver")
    assert len(gold) == len(silver) and gold["timestamp"].equals(silver["timestamp"])
    assert gold["obs_id"].equals(silver["obs_id"])
    assert cli.main(["eddy", "validate-run", "--run-dir", str(tmp_path / "grun"), "--stage", "gold"]) == 0
    return json.loads((silver_table.parent / "silver_run.json").read_text(encoding="utf-8"))


class TestMixedPrecisionTimestampStrings:
    @pytest.mark.parametrize("values,expected_ns", [
        (["2025-03-01 00:30:00 UTC", "2025-03-01 01:00:00.000000 UTC"], [0, 0]),
        (["2025-03-01T00:30:00Z", "2025-03-01T01:00:00.123456789Z"], [0, 123456789]),
        (["2025-03-01 00:30:00.5", "2025-03-01 01:00:00"], [500000000, 0]),
        (["2025-03-01 00:30", "2025-03-01 01:00:00.25"], [0, 250000000]),
    ])
    def test_parser_keeps_every_precision_exactly(self, values, expected_ns):
        from miaproc.eddy.time import _parse_timestamp_col

        parsed = _parse_timestamp_col(pd.Series(values))
        assert parsed.notna().all()
        assert [t.value % 10**9 for t in parsed] == expected_ns

    def test_legacy_date_time_with_fractional_seconds(self):
        from miaproc.eddy.time import _parse_datetime_multi

        parsed = _parse_datetime_multi(pd.Series(["2025-03-01 00:30", "2025-03-01 01:00:00.000000"]))
        assert parsed.tolist() == [pd.Timestamp("2025-03-01 00:30"), pd.Timestamp("2025-03-01 01:00")]

    @pytest.mark.parametrize("grouped", [False, True])
    def test_csv_directory_files_of_different_precision(self, tmp_path, fake_engine, grouped):
        flux, biomet = _observations()
        for name, frame in (("flux", flux), ("biomet", biomet)):
            folder = tmp_path / f"{name}_csv"
            folder.mkdir()
            half = len(frame) // 2
            for k, part in enumerate((frame.iloc[:half], frame.iloc[half:])):
                out = part.copy()
                out.insert(0, "timestamp", [_PRECISIONS[k].format(t) for t in out.pop("DateTime")])
                if grouped:
                    out.insert(0, "site_id", "S1")
                out.to_csv(folder / f"{name}_{k}.csv", index=False)
        argv = ["eddy", "run-silver", "--flux-dir", str(tmp_path / "flux_csv"), "--biomet-dir",
                str(tmp_path / "biomet_csv"), "--output-table", str(tmp_path / "srun" / "silver.parquet"),
                "--output-run-json", str(tmp_path / "srun" / "silver_run.json")]
        if grouped:
            argv += ["--group-column", "site_id"]
        assert cli.main(argv) == cli.SUCCESS_EXIT
        run = _check_every_observation_survives(tmp_path, flux, biomet, tmp_path / "srun" / "silver.parquet")
        if grouped:  # the CLI holds the raw frames: counts are recorded (none dropped)
            assert run["row_counts"]["unparsed_time_rows_dropped"] == {"flux": 0, "biomet": 0}
            assert run["row_counts"]["duplicate_instants_dropped"] == {"flux": 0, "biomet": 0}

    def test_legacy_date_time_directory_mixing_minutes_and_microseconds(self, tmp_path, fake_engine):
        flux, biomet = _observations(seed=22)
        for name, frame in (("flux", flux), ("biomet", biomet)):
            folder = tmp_path / f"{name}_csv"
            folder.mkdir()
            half = len(frame) // 2
            for k, (part, fmt) in enumerate(((frame.iloc[:half], "%H:%M"), (frame.iloc[half:], "%H:%M:%S.%f"))):
                out = part.copy()
                dt = out.pop("DateTime")
                out.insert(0, "time", dt.dt.strftime(fmt))
                out.insert(0, "date", dt.dt.strftime("%Y-%m-%d"))
                out.to_csv(folder / f"{name}_{k}.csv", index=False)
        argv = ["eddy", "run-silver", "--flux-dir", str(tmp_path / "flux_csv"), "--biomet-dir",
                str(tmp_path / "biomet_csv"), "--output-table", str(tmp_path / "srun" / "silver.parquet"),
                "--output-run-json", str(tmp_path / "srun" / "silver_run.json")]
        assert cli.main(argv) == cli.SUCCESS_EXIT
        _check_every_observation_survives(tmp_path, flux, biomet, tmp_path / "srun" / "silver.parquet")

    def test_parquet_iso_strings_of_varying_precision(self, tmp_path, fake_engine):
        flux, biomet = _observations(seed=23)
        frames = {}
        for name, frame in (("flux", flux), ("biomet", biomet)):
            out = frame.copy()
            variants = _ISO_VARIANTS[:4]  # grid instants written at four precisions
            out.insert(0, "timestamp", [variants[i % 4].format(t) for i, t in enumerate(out.pop("DateTime"))])
            frames[name] = out
        assert pq.read_schema(_write_inputs(tmp_path, frames["flux"], frames["biomet"])[0]).field(
            "timestamp").type == pa.string()
        fp, bp = tmp_path / "flux.parquet", tmp_path / "biomet.parquet"
        assert cli.main(_silver_argv(tmp_path, fp, bp)) == cli.SUCCESS_EXIT
        run = _check_every_observation_survives(tmp_path, flux, biomet, tmp_path / "srun" / "silver.parquet")
        assert run["row_counts"]["unparsed_time_rows_dropped"] == {"flux": 0, "biomet": 0}
        assert run["row_counts"]["flux_input"] == len(flux)

    def test_unparseable_time_rows_are_counted_not_silent(self, tmp_path):
        flux, biomet = _observations(seed=24)
        out = flux.copy()
        out.insert(0, "timestamp", [f"{t:%Y-%m-%dT%H:%M:%S}Z" for t in out.pop("DateTime")])
        out.loc[5, "timestamp"] = "not a time"
        bio = biomet.copy()
        bio.insert(0, "timestamp", [f"{t:%Y-%m-%dT%H:%M:%S}Z" for t in bio.pop("DateTime")])
        fp, bp = _write_inputs(tmp_path, out, bio)
        assert cli.main(_silver_argv(tmp_path, fp, bp)) == cli.SUCCESS_EXIT
        run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
        assert run["row_counts"]["unparsed_time_rows_dropped"] == {"flux": 1, "biomet": 0}
        silver = read_parquet_table(tmp_path / "srun" / "silver.parquet", flag="s", role="silver")
        assert 5 not in silver["obs_id"].dropna().astype(int).tolist()


def test_shared_biomet_drops_are_counted_once_at_top_level(tmp_path):
    """M002-S02-F9: a biomet input without the group column serves every group."""
    flux, biomet = _typed_inputs(days=1, sites=("S1", "S2", "S3"))
    shared = biomet.loc[biomet["site_id"] == "S1"].drop(columns=["site_id"])
    shared = pd.concat([shared, shared.iloc[[3, 7]]], ignore_index=True)  # two repeated instants
    fp, bp = _write_inputs(tmp_path, flux, shared)
    rc = cli.main(_silver_argv(tmp_path, fp, bp, "silver.parquet", "--group-column", "site_id"))
    assert rc == cli.SUCCESS_EXIT
    run = json.loads((tmp_path / "srun" / "silver_run.json").read_text(encoding="utf-8"))
    assert run["row_counts"]["duplicate_instants_dropped"] == {"flux": 0, "biomet": 2}
    assert [g["duplicate_instants_dropped"]["biomet"] for g in run["groups"]] == [2, 2, 2]
    assert run["row_counts"]["biomet_input"] == len(shared)


@pytest.mark.slow
def test_real_hesseflux_parquet_silver_gold_in_process(tmp_path):
    """Real backend on typed Parquet input with IDs above 2**53 and gap rows."""
    flux, biomet = _typed_inputs(days=45, seed=20261101)
    fp, bp = _write_inputs(tmp_path, flux, biomet)
    assert cli.main(_silver_argv(tmp_path, fp, bp)) == 0
    assert cli.main(_gold_argv(tmp_path, tmp_path / "srun" / "silver.parquet")) == 0
    gold = read_parquet_table(tmp_path / "grun" / "gold.parquet", flag="g", role="silver")
    paired = np.isfinite(gold["GPP"].to_numpy(float)) & np.isfinite(gold["Reco"].to_numpy(float))
    assert int(paired.sum()) >= 48
    assert gold["primary_key"].dropna().min() >= BIG and "DateTime" not in gold.columns
    diag = json.loads((tmp_path / "grun" / "gold_diag.json").read_text(encoding="utf-8"))
    assert diag["backend"] == "hesseflux" and diag["partitioning"]["method"] == "lasslop"
