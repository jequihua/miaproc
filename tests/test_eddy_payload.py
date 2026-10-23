"""M002-S01: transport-neutral source-facing payload helpers.

``miaproc.eddy.payload`` owns the source-truth rename, internal-frame
reconstruction, column-uniqueness / humidity-collision policy, the
silver stage-payload identity preparation and the transport-neutral
gold stage-payload subset that used to live inside the BigQuery
writeback module (removed with every BigQuery surface by M002-S04).
These tests pin:

- the neutral module imports and runs with every ``google`` import
  blocked (no client construction, no credential lookup possible),
  with a retained positive control proving the blocker fires;
- the ``miaproc.eddy`` package re-exports the very same objects, and
  the removed BigQuery modules and aliases stay gone;
- exact timestamps, identity, unit-baked names, wide pass-through
  columns, gap-row identity and collision handling, plus agreement
  with the lineage CSV (including its ``gold_derived`` rows).
"""
from __future__ import annotations

import csv
import json
import pathlib
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

import miaproc.eddy as eddy
from miaproc.eddy import payload
from miaproc.eddy.constants import SILVER_INTERNAL_TO_FINAL_RENAME
from miaproc.eddy.payload import (
    COLUMN_COLLISION_ATTRS_KEY,
    DuplicateStageColumnsError,
    GOLD_DERIVED_RENAME,
    apply_silver_source_truth_rename,
    column_field_key,
    derive_gold_stage_columns,
    ensure_unique_stage_columns,
    finalize_gold_stage_payload,
    prepare_gold_stage_payload,
    prepare_silver_stage_payload,
    silver_to_internal_calc_frame,
    validate_source_columns_unique,
)


BIG_IDS = [2**53 + 1, 2**53 + 3, 2**62 + 7]

SOURCE_TRUTH_COLUMNS = [
    "timestamp",
    "co2_flux",
    "qc_co2_flux",
    "air_temperature_c",
    "u_star",
    "VPD_hpa",
    "SWIN_1_1_1",
    "P_RAIN_1_1_1",
    "RH_1_1_1",
    "RH",
    "h2o_flux",
    "big_id",
]


def _internal_stage1_frame(tz: str = "UTC") -> pd.DataFrame:
    """Stage-1-shaped frame: the nine internal names, a flux-side
    ``RH`` distinct from biomet ``rH``, a NaN-bearing pass-through and
    an int64 pass-through above 2**53."""
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01 00:00", periods=3, freq="30min", tz=tz
            ),
            "NEE": [0.1, 0.2, 0.3],
            "QC_NEE": [0, 1, 2],
            "Tair": [20.0, 21.0, 22.0],
            "USTAR": [0.2, 0.3, 0.4],
            "VPD": [5.0, 6.0, 7.0],
            "Rg": [0.0, 100.0, 200.0],
            "P_RAIN": [0.0, 0.5, 0.0],
            "rH": [85.0, 86.0, 87.0],
            "RH": [60.0, 61.0, 62.0],
            "h2o_flux": [1.0, np.nan, 3.0],
            "big_id": np.array(BIG_IDS, dtype="int64"),
        }
    )


def _lineage_csv_rows() -> list[dict[str, str]]:
    relative = (
        pathlib.Path("06_infra")
        / "schemas"
        / "eddy_bronze_to_stage_column_lineage_contract.csv"
    )
    here = pathlib.Path(__file__).resolve()
    for ancestor in here.parents:
        candidate = ancestor / relative
        if candidate.is_file():
            with candidate.open(encoding="utf-8") as fh:
                return list(csv.DictReader(fh))
    raise FileNotFoundError(f"Could not locate {relative} above {here}.")


# ---------------------------------------------------------------------------
# Import boundary and re-export identity
# ---------------------------------------------------------------------------


_NO_GOOGLE_PROBE = r'''
import importlib.abc
import json
import sys

attempts = []


class _BlockGoogle(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name == "google" or name.startswith("google."):
            attempts.append(name)
            raise ImportError("blocked transport import: " + name)
        return None


sys.meta_path.insert(0, _BlockGoogle())

import pandas as pd

from miaproc.eddy import payload

internal = pd.DataFrame(
    {
        "DateTime": pd.date_range(
            "2025-08-01", periods=2, freq="30min", tz="UTC"
        ),
        "NEE": [0.1, 0.2],
        "Tair": [20.0, 21.0],
        "rH": [80.0, 81.0],
        "RH": [60.0, 61.0],
    }
)
silver = payload.apply_silver_source_truth_rename(internal)
calc = payload.silver_to_internal_calc_frame(silver)
stage, actions = payload.prepare_silver_stage_payload(
    silver, site_id="S1", source_flux_df=internal
)
unique, unique_actions = payload.ensure_unique_stage_columns(stage)
payload.validate_source_columns_unique(stage, side="stage")
gold = payload.prepare_gold_stage_payload(
    silver.assign(
        DateTime=internal["DateTime"], NEE=internal["NEE"], NEE_f=[0.3, 0.4]
    ),
    site_id="S1",
)


def _google_modules():
    return sorted(
        m for m in sys.modules if m == "google" or m.startswith("google.")
    )


neutral_attempts = list(attempts)
neutral_google_modules = _google_modules()

# Positive control: a Google client import must be rejected by the same
# blocker and recorded as an attempt.
control_error = None
try:
    import google.cloud.bigquery  # noqa: F401
except ImportError as exc:
    control_error = str(exc)

print(
    json.dumps(
        {
            "attempts": neutral_attempts,
            "google_modules": neutral_google_modules,
            "silver": list(silver.columns),
            "calc": list(calc.columns),
            "stage": list(stage.columns),
            "primary_key": stage["primary_key"].tolist(),
            "gold": list(gold.columns),
            "actions": actions + unique_actions,
            "control_attempts": attempts[len(neutral_attempts):],
            "control_error": control_error,
            "control_google_modules": _google_modules(),
        }
    )
)
'''


def test_neutral_helpers_import_and_run_with_google_imports_blocked(tmp_path):
    # A meta-path finder rejects every ``google``/``google.*`` import
    # (Google client, google.auth credential discovery) before the
    # package is imported; the helpers must still import and run.
    proc = subprocess.run(
        [sys.executable, "-c", _NO_GOOGLE_PROBE],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert proc.returncode == 0, proc.stderr
    result = json.loads(proc.stdout.strip().splitlines()[-1])
    assert result["attempts"] == []
    assert result["google_modules"] == []
    assert result["silver"] == [
        "timestamp", "co2_flux", "air_temperature_c", "RH_1_1_1", "RH",
    ]
    assert result["calc"] == ["DateTime", "NEE", "Tair", "rH", "RH"]
    assert result["stage"] == [
        "primary_key", "site_id", "timestamp",
        "co2_flux", "air_temperature_c", "RH_1_1_1", "RH",
    ]
    assert result["primary_key"] == [
        "S1|2025-08-01T00:00:00+0000",
        "S1|2025-08-01T00:30:00+0000",
    ]
    assert result["gold"] == [
        "primary_key", "site_id", "timestamp",
        "RH", "RH_1_1_1", "air_temperature_c", "co2_flux", "dateAndTime",
        "nee_f",
    ]
    assert result["actions"] == []
    # Retained positive control: the blocker is live in this process.
    assert result["control_attempts"] == ["google"]
    assert result["control_error"] == "blocked transport import: google"
    assert result["control_google_modules"] == []


_REEXPORTED_NAMES = (
    "COLUMN_COLLISION_ATTRS_KEY",
    "DuplicateStageColumnsError",
    "HUMIDITY_DERIVED_RENAME",
    "HUMIDITY_SOURCE_COLUMN",
    "apply_silver_source_truth_rename",
    "ensure_unique_stage_columns",
    "prepare_silver_stage_payload",
    "silver_to_internal_calc_frame",
    "validate_source_columns_unique",
)


@pytest.mark.parametrize("name", _REEXPORTED_NAMES)
def test_package_reexports_the_neutral_objects(name):
    neutral = getattr(payload, name)
    assert getattr(eddy, name) is neutral
    assert name in eddy.__all__


def test_removed_bigquery_modules_and_aliases_stay_gone():
    # M002-S04: the BigQuery adapters left with their transport; the
    # neutral names (``column_field_key``, ``GOLD_DERIVED_RENAME``) remain.
    import importlib.util

    for module in ("miaproc.eddy.bigquery_writeback", "miaproc.eddy.bigquery_runner"):
        assert importlib.util.find_spec(module) is None, module
    for alias in ("bigquery_field_key", "S2_FILT_1_RENAME_MAP", "prepare_stage_dataframe"):
        assert not hasattr(eddy, alias), alias
        assert not hasattr(payload, alias), alias
    assert payload.column_field_key("RH") == payload.column_field_key("rH")
    assert payload.GOLD_DERIVED_RENAME is GOLD_DERIVED_RENAME


@pytest.mark.parametrize(
    ("name", "key"),
    [("RH", "rh"), ("rH", "rh"), ("RH_1_1_1", "rh_1_1_1"), (5, "5")],
)
def test_column_field_key_is_case_insensitive(name, key):
    assert column_field_key(name) == key


# ---------------------------------------------------------------------------
# Source-truth rename and internal reconstruction
# ---------------------------------------------------------------------------


def test_source_truth_rename_maps_internal_names_and_keeps_values():
    frame = _internal_stage1_frame()
    before = frame.copy()

    out = apply_silver_source_truth_rename(frame)

    assert list(out.columns) == SOURCE_TRUTH_COLUMNS
    for internal, final in SILVER_INTERNAL_TO_FINAL_RENAME.items():
        pd.testing.assert_series_equal(
            out[final], frame[internal], check_names=False
        )
    pd.testing.assert_series_equal(out["RH"], frame["RH"])
    assert out["big_id"].dtype == np.dtype("int64")
    assert out["big_id"].tolist() == BIG_IDS
    pd.testing.assert_frame_equal(frame, before)


def test_source_truth_rename_prefers_attached_timestamp_over_datetime():
    frame = _internal_stage1_frame()
    source_ts = pd.date_range("2030-01-01", periods=3, freq="30min", tz="UTC")
    frame.insert(1, "timestamp", source_ts)

    out = apply_silver_source_truth_rename(frame)

    assert list(out.columns).count("timestamp") == 1
    assert "DateTime" not in out.columns
    assert out["timestamp"].tolist() == list(source_ts)


def test_source_truth_rename_skips_duplicated_internal_names():
    frame = _internal_stage1_frame()
    dup = pd.concat([frame, frame[["rH"]] + 1.0], axis=1)

    out = apply_silver_source_truth_rename(dup)

    assert list(out.columns).count("rH") == 2
    assert "RH_1_1_1" not in out.columns
    assert "co2_flux" in out.columns


def test_internal_reconstruction_round_trips_the_rename_exactly():
    frame = _internal_stage1_frame()
    silver = apply_silver_source_truth_rename(frame)
    before = silver.copy()

    calc = silver_to_internal_calc_frame(silver)

    pd.testing.assert_frame_equal(calc, frame)
    pd.testing.assert_frame_equal(silver, before)


def test_internal_reconstruction_prefers_timestamp_over_leaked_datetime():
    silver = apply_silver_source_truth_rename(_internal_stage1_frame())
    leaked = pd.date_range("1999-01-01", periods=3, freq="30min", tz="UTC")
    silver["DateTime"] = leaked

    calc = silver_to_internal_calc_frame(silver)

    assert list(calc.columns).count("DateTime") == 1
    assert "timestamp" not in calc.columns
    assert calc["DateTime"].tolist() == list(silver["timestamp"])


def test_lineage_csv_internal_rows_match_neutral_rename():
    # Flux and biomet both list ``DateTime -> timestamp``; keep the
    # first occurrence of each internal name in CSV order.
    i2f: dict[str, str] = {}
    for r in _lineage_csv_rows():
        internal_name, final_name = r["v_name_eddyproc"], r["v_name_final"]
        if internal_name and internal_name != final_name:
            i2f.setdefault(internal_name, final_name)
    assert i2f == SILVER_INTERNAL_TO_FINAL_RENAME
    internal = list(i2f)
    finals = list(i2f.values())
    frame = pd.DataFrame({name: [1.0] for name in internal})

    silver = apply_silver_source_truth_rename(frame)

    assert list(silver.columns) == finals
    assert list(silver_to_internal_calc_frame(silver).columns) == internal


# ---------------------------------------------------------------------------
# Silver stage-payload identity, units and wide columns
# ---------------------------------------------------------------------------


def test_silver_payload_identity_timestamps_units_and_order():
    frame = _internal_stage1_frame(tz="Etc/GMT+6")
    silver = apply_silver_source_truth_rename(frame)

    out, actions = prepare_silver_stage_payload(
        silver, site_id="S1", source_flux_df=frame
    )

    assert list(out.columns) == (
        ["primary_key", "site_id", "timestamp"] + SOURCE_TRUTH_COLUMNS[1:]
    )
    assert str(out["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert out["timestamp"].tolist() == list(frame["DateTime"])
    assert out["primary_key"].tolist() == [
        "S1|2025-08-01T06:00:00+0000",
        "S1|2025-08-01T06:30:00+0000",
        "S1|2025-08-01T07:00:00+0000",
    ]
    assert out["site_id"].tolist() == ["S1", "S1", "S1"]
    assert out["air_temperature_c"].tolist() == [20.0, 21.0, 22.0]
    assert out["VPD_hpa"].tolist() == [5.0, 6.0, 7.0]
    assert out["RH_1_1_1"].tolist() == [85.0, 86.0, 87.0]
    assert out["RH"].tolist() == [60.0, 61.0, 62.0]
    assert out["h2o_flux"].isna().tolist() == [False, True, False]
    assert out["big_id"].dtype == np.dtype("int64")
    assert out["big_id"].tolist() == BIG_IDS
    assert actions == []
    assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == []


def test_silver_payload_preserves_every_wide_column_in_order():
    silver = apply_silver_source_truth_rename(_internal_stage1_frame())
    wide = [f"wide_{i:03d}" for i in range(150)]
    extra = pd.DataFrame(
        {name: [float(i), np.nan, -float(i)] for i, name in enumerate(wide)}
    )
    silver = pd.concat([silver, extra], axis=1)

    out, actions = prepare_silver_stage_payload(silver, site_id="S1")

    assert list(out.columns)[3:] == SOURCE_TRUTH_COLUMNS[1:] + wide
    pd.testing.assert_frame_equal(
        out[wide].reset_index(drop=True), extra
    )
    assert actions == []


def test_silver_payload_prefers_source_timestamp_and_records_identity_overwrite():
    silver = apply_silver_source_truth_rename(_internal_stage1_frame())
    silver["DateTime"] = pd.date_range(
        "1999-01-01", periods=3, freq="30min", tz="UTC"
    )
    silver["primary_key"] = np.array(BIG_IDS, dtype="int64")
    silver["site_id"] = "stale"

    out, actions = prepare_silver_stage_payload(silver, site_id="S2")

    assert "DateTime" not in out.columns
    assert list(out.columns).count("timestamp") == 1
    assert out["timestamp"].tolist() == list(silver["timestamp"])
    assert out["primary_key"].tolist() == [
        "S2|2025-08-01T00:00:00+0000",
        "S2|2025-08-01T00:30:00+0000",
        "S2|2025-08-01T01:00:00+0000",
    ]
    assert out["site_id"].tolist() == ["S2", "S2", "S2"]
    expected = [
        {
            "column": ident,
            "action": "identity_overwrite",
            "renamed_to": None,
            "reason": (
                f"silver frame carried a pre-existing {ident!r} column; "
                "replaced with the writeback-synthesized identity value"
            ),
        }
        for ident in ("primary_key", "site_id")
    ]
    assert actions == expected
    assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == expected


def test_silver_payload_requires_a_time_column():
    silver = pd.DataFrame({"co2_flux": [0.1]})
    with pytest.raises(ValueError) as excinfo:
        prepare_silver_stage_payload(silver, site_id="S1")
    assert str(excinfo.value) == (
        "prepare_silver_stage_payload: silver frame missing both "
        "'DateTime' and 'timestamp' columns required for stage-"
        "identity derivation."
    )


def test_silver_payload_rejects_duplicate_source_flux_columns():
    silver = apply_silver_source_truth_rename(_internal_stage1_frame())
    source = pd.concat(
        [_internal_stage1_frame()[["h2o_flux"]]] * 2, axis=1
    )
    with pytest.raises(DuplicateStageColumnsError) as excinfo:
        prepare_silver_stage_payload(
            silver, site_id="S1", source_flux_df=source
        )
    assert str(excinfo.value) == (
        "bronze flux source input has duplicate column names: "
        "['h2o_flux']. Stage payload preparation cannot guess which "
        "one is authoritative; fix the upstream pipeline before staging."
    )
    assert excinfo.value.duplicate_columns == ("h2o_flux",)


def test_validate_source_columns_unique_accepts_none_and_empty():
    assert validate_source_columns_unique(None, side="x") is None
    assert validate_source_columns_unique(pd.DataFrame(), side="x") is None


# ---------------------------------------------------------------------------
# Column-uniqueness / humidity-collision policy
# ---------------------------------------------------------------------------


def _divergent(name: str, new: str) -> dict:
    return {
        "column": "rH",
        "action": "renamed_divergent_duplicate",
        "renamed_to": new,
        "source_column_name": name,
        "reason": (
            f"derived duplicate {name!r} diverges from source; preserved "
            f"both as 'rH' and {new!r}."
        ),
    }


_CANONICAL_RH = {
    "column": "RH",
    "action": "renamed_to_canonical_humidity",
    "renamed_to": "rH",
    "reason": (
        "field keys are compared case-insensitively; renamed humidity "
        "column 'RH' to 'rH' to match the canonical source name."
    ),
}


@pytest.mark.parametrize(
    ("columns", "values", "expected_columns", "expected_actions"),
    [
        pytest.param(
            ["a", "rH", "rH"],
            [[1.0, 2.0], [80.0, 81.0], [70.0, 71.0]],
            ["a", "rH", "rH_norm_s"],
            [_divergent("rH", "rH_norm_s")],
            id="divergent-literal-duplicate",
        ),
        pytest.param(
            ["a", "rH", "rH"],
            [[1.0, 2.0], [80.0, np.nan], [80.0, np.nan]],
            ["a", "rH"],
            [
                {
                    "column": "rH",
                    "action": "suppressed_equivalent_duplicate",
                    "renamed_to": None,
                    "source_column_name": "rH",
                    "reason": (
                        "derived duplicate 'rH' equivalent to source; kept "
                        "the first occurrence as 'rH'."
                    ),
                }
            ],
            id="equivalent-nan-aware-duplicate",
        ),
        pytest.param(
            ["RH", "rH"],
            [[60.0, 61.0], [80.0, 81.0]],
            ["rH", "rH_norm_s"],
            [_CANONICAL_RH, _divergent("rH", "rH_norm_s")],
            id="case-variant-canonicalized",
        ),
        pytest.param(
            ["rH", "rH_norm_s", "rh"],
            [[80.0, 81.0], [1.0, 1.0], [70.0, 71.0]],
            ["rH", "rH_norm_s", "rH_norm_s_2"],
            [_divergent("rh", "rH_norm_s_2")],
            id="reserved-name-suffix",
        ),
    ],
)
def test_humidity_collision_policy(
    columns, values, expected_columns, expected_actions
):
    df = pd.DataFrame(dict(enumerate(values)))
    df.columns = columns
    before = df.copy()

    out, actions = ensure_unique_stage_columns(df)

    assert list(out.columns) == expected_columns
    assert actions == expected_actions
    assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == expected_actions
    pd.testing.assert_frame_equal(df, before)


def test_unique_columns_return_a_copy_without_actions():
    df = pd.DataFrame({"RH": [1.0], "RH_1_1_1": [2.0]})

    out, actions = ensure_unique_stage_columns(df)

    assert out is not df
    pd.testing.assert_frame_equal(out, df)
    assert actions == []
    assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == []


def test_non_humidity_case_insensitive_duplicate_raises():
    df = pd.DataFrame({"co2_flux": [1.0], "CO2_flux": [2.0], "x": [3.0]})

    with pytest.raises(DuplicateStageColumnsError) as excinfo:
        ensure_unique_stage_columns(df)

    assert excinfo.value.duplicate_columns == ("CO2_flux", "co2_flux")
    assert str(excinfo.value) == (
        "Stage payload has duplicate column names with no deterministic "
        "resolution policy. "
        "Affected logical keys: ['co2_flux']. Affected column names: "
        "['CO2_flux', 'co2_flux']. The package refuses to write a "
        "duplicate-column payload; the upstream pipeline must be fixed "
        "before the stage output is written."
    )


# ---------------------------------------------------------------------------
# Gold stage-payload subset (identity, gold_derived names, M32 drop)
# ---------------------------------------------------------------------------


GOLD_PAYLOAD_COLUMNS = [
    "primary_key",
    "site_id",
    "timestamp",
    "GPP",
    "P_RAIN_1_1_1",
    "RH",
    "RH_1_1_1",
    "Reco",
    "SWIN_1_1_1",
    "VPD_hpa",
    "air_temperature_c",
    "big_id",
    "co2_flux",
    "dateAndTime",
    "h2o_flux",
    "nee_f",
    "nee_fqc",
    "qc_co2_flux",
    "sw_in_f",
    "ta_f",
    "u_star",
    "vpd_f",
]


def _processed_gold_frame(tz: str = "UTC") -> pd.DataFrame:
    """Backend output (13-column contract plus internal passthroughs)
    with the source-truth silver columns attached, as
    ``cli._attach_silver_columns_to_gold`` produces it."""
    frame = _internal_stage1_frame(tz=tz)
    silver = apply_silver_source_truth_rename(frame)
    backend = pd.DataFrame(
        {
            "DateTime": frame["DateTime"],
            "NEE": frame["NEE"],
            "NEE_f": [0.15, 0.25, 0.35],
            "NEE_fqc": [0, 1, 2],
            "GPP": [1.0, 2.0, 3.0],
            "Reco": [0.5, 0.6, 0.7],
            "Tair": frame["Tair"],
            "Tair_f": [20.5, 21.5, 22.5],
            "Rg": frame["Rg"],
            "Rg_f": [0.0, 110.0, 210.0],
            "VPD": frame["VPD"],
            "VPD_f": [5.5, 6.5, 7.5],
            "USTAR": frame["USTAR"],
            "QC_NEE": frame["QC_NEE"],
            "P_RAIN": frame["P_RAIN"],
            "rH": frame["rH"],
        }
    )
    return pd.concat([backend, silver], axis=1)


def test_gold_payload_literal_oracle():
    processed = _processed_gold_frame(tz="Etc/GMT+6")
    before = processed.copy()

    out = prepare_gold_stage_payload(processed, site_id="S1")

    assert list(out.columns) == GOLD_PAYLOAD_COLUMNS
    assert str(out["timestamp"].dtype) == "datetime64[ns, UTC]"
    assert out["timestamp"].tolist() == list(processed["DateTime"])
    assert out["primary_key"].tolist() == [
        "S1|2025-08-01T06:00:00+0000",
        "S1|2025-08-01T06:30:00+0000",
        "S1|2025-08-01T07:00:00+0000",
    ]
    assert out["site_id"].tolist() == ["S1", "S1", "S1"]
    assert out["dateAndTime"].tolist() == [
        "2025-08-01 06:00:00",
        "2025-08-01 06:30:00",
        "2025-08-01 07:00:00",
    ]
    assert out["nee_f"].tolist() == [0.15, 0.25, 0.35]
    assert out["nee_fqc"].tolist() == [0, 1, 2]
    assert out["sw_in_f"].tolist() == [0.0, 110.0, 210.0]
    assert out["ta_f"].tolist() == [20.5, 21.5, 22.5]
    assert out["vpd_f"].tolist() == [5.5, 6.5, 7.5]
    assert out["air_temperature_c"].tolist() == [20.0, 21.0, 22.0]
    assert out["VPD_hpa"].tolist() == [5.0, 6.0, 7.0]
    assert out["RH_1_1_1"].tolist() == [85.0, 86.0, 87.0]
    assert out["RH"].tolist() == [60.0, 61.0, 62.0]
    assert out["big_id"].dtype == np.dtype("int64")
    assert out["big_id"].tolist() == BIG_IDS
    assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == []
    pd.testing.assert_frame_equal(processed, before)


def test_gold_payload_gap_rows_get_identity_and_keep_missing_values():
    processed = _processed_gold_frame()
    measured = [
        c for c in processed.columns if c not in ("DateTime", "timestamp")
    ]
    processed[measured] = processed[measured].astype("float64")
    processed.loc[1, measured] = np.nan
    # A pre-M35A leak: the silver-carried timestamp is NaT on the gap row.
    processed.loc[1, "timestamp"] = pd.NaT

    out = prepare_gold_stage_payload(processed, site_id="S1")

    assert out["timestamp"].tolist() == list(processed["DateTime"])
    assert out["primary_key"].tolist() == [
        "S1|2025-08-01T00:00:00+0000",
        "S1|2025-08-01T00:30:00+0000",
        "S1|2025-08-01T01:00:00+0000",
    ]
    assert out["site_id"].tolist() == ["S1", "S1", "S1"]
    assert out["dateAndTime"].tolist()[1] == "2025-08-01 00:30:00"
    for col in ("nee_f", "co2_flux", "GPP", "RH_1_1_1", "big_id"):
        assert out[col].isna().tolist() == [False, True, False], col


def test_gold_payload_overwrites_stale_identity_and_lowercase_targets():
    processed = _processed_gold_frame()
    processed["primary_key"] = ["stale-0", "stale-1", "stale-2"]
    processed["site_id"] = "OLD"
    processed["nee_f"] = [9.0, 9.0, 9.0]

    out = prepare_gold_stage_payload(processed, site_id="S2")

    assert list(out.columns) == GOLD_PAYLOAD_COLUMNS
    assert out["primary_key"].tolist() == [
        "S2|2025-08-01T00:00:00+0000",
        "S2|2025-08-01T00:30:00+0000",
        "S2|2025-08-01T01:00:00+0000",
    ]
    assert out["site_id"].tolist() == ["S2", "S2", "S2"]
    assert out["nee_f"].tolist() == [0.15, 0.25, 0.35]


def test_gold_payload_requires_datetime():
    processed = _processed_gold_frame().drop(columns=["DateTime"])
    with pytest.raises(ValueError) as excinfo:
        prepare_gold_stage_payload(processed, site_id="S1")
    assert str(excinfo.value) == (
        "gold stage payload: processed output is missing the 'DateTime' "
        "column required for stage-identity derivation."
    )


def test_derive_gold_stage_columns_keeps_internal_passthroughs():
    processed = _processed_gold_frame()

    out = derive_gold_stage_columns(processed, site_id="S1")

    expected = [GOLD_DERIVED_RENAME.get(c, c) for c in processed.columns]
    expected += ["site_id", "primary_key", "dateAndTime"]
    assert list(out.columns) == expected
    assert not set(GOLD_DERIVED_RENAME) & set(out.columns)
    assert "DateTime" in out.columns and "NEE" in out.columns


def test_finalize_gold_stage_payload_applies_humidity_policy():
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2025-08-01", periods=2, freq="30min", tz="UTC"
            ),
            "site_id": ["S1", "S1"],
            "primary_key": ["a", "b"],
            "rH": [80.0, 81.0],
            "RH": [60.0, 61.0],
        }
    )

    out = finalize_gold_stage_payload(df)

    assert list(out.columns) == [
        "primary_key", "site_id", "timestamp", "rH", "rH_norm_s",
    ]
    assert out["rH"].tolist() == [60.0, 61.0]
    assert out["rH_norm_s"].tolist() == [80.0, 81.0]
    assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == [
        _CANONICAL_RH,
        _divergent("rH", "rH_norm_s"),
    ]


def test_lineage_csv_gold_derived_rows_match_neutral_gold_payload():
    gold_derived = {
        r["v_name_final"]
        for r in _lineage_csv_rows()
        if r["tbl_origin"] == "gold_derived"
    }
    assert gold_derived == set(GOLD_DERIVED_RENAME.values()) | {
        "dateAndTime", "GPP", "Reco",
    }
    assert GOLD_DERIVED_RENAME == {
        "NEE_f": "nee_f",
        "NEE_fqc": "nee_fqc",
        "Rg_f": "sw_in_f",
        "Tair_f": "ta_f",
        "VPD_f": "vpd_f",
    }

    out = prepare_gold_stage_payload(_processed_gold_frame(), site_id="S1")

    assert gold_derived <= set(out.columns)
    assert not set(GOLD_DERIVED_RENAME) & set(out.columns)
    assert list(out.columns).count("timestamp") == 1
    for leak in SILVER_INTERNAL_TO_FINAL_RENAME:
        assert leak not in out.columns, leak
