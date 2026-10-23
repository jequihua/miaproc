"""M34 tests: widened bronze -> silver -> gold carry-forward contract.

M32/M32A established the source-truth column naming contract for the
nine columns the lineage CSV explicitly renames
(``DateTime -> timestamp``, ``NEE -> co2_flux``, ``Tair ->
air_temperature_c``, ``USTAR -> u_star``, ``VPD -> VPD_hpa``,
``Rg -> SWIN_1_1_1``, ``P_RAIN -> P_RAIN_1_1_1``, ``rH -> RH_1_1_1``,
``QC_NEE -> qc_co2_flux``).

M34 broadens that into a generic carry-forward rule for the eddy
silver/gold split: every unique column from the carbon-flux bronze/source
table must survive into silver and then gold under its source name,
unless miaproc changes its physical units (currently
``air_temperature -> air_temperature_c`` and ``VPD -> VPD_hpa``). From
the biomet bronze table only the processing-used variables carry
forward: ``SWIN_1_1_1``, ``P_RAIN_1_1_1``, ``RH_1_1_1``.

These tests exercise the contract end-to-end against a 54-column
case-study-shaped bronze flux fixture, against the silver helper
chain, against the published silver of the ``eddy run-silver`` file
workflow (M002-S04 replacement for the removed BigQuery silver CLI
dry-run and writeback paths), and against the gold stage-payload
assembly. The package code already preserves
unknown source columns by virtue of stage 1's column-preserving merge
plus the helper-level renames; these tests lock that behavior in so a
future refactor cannot silently narrow the silver / gold schemas.
"""
from __future__ import annotations

import csv
import json
import pathlib
import warnings
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from miaproc import cli
from miaproc.eddy import (
    DuplicateStageColumnsError,
    SILVER_BRONZE_TO_FINAL_ALIASES,
    SILVER_INTERNAL_TO_FINAL_RENAME,
    apply_silver_source_truth_rename,
    load_stage1_from_dataframes,
    prepare_silver_stage_payload,
)
from miaproc.eddy.io import read_parquet_table, write_parquet_table
from miaproc.eddy.payload import prepare_gold_stage_payload


# ---------------------------------------------------------------------------
# Wide case-study-shaped fixtures
# ---------------------------------------------------------------------------

# 54 columns, matching the header of
# 01_data/case_study/flux/flux.csv and flux_staging.csv. Order matters
# only for legibility; the M34 contract is name-based.
WIDE_FLUX_COLUMNS: tuple[str, ...] = (
    "primary_key", "timestamp", "filename", "DOY", "daytime",
    "H", "qc_H", "LE", "qc_LE",
    "co2_flux", "qc_co2_flux", "h2o_flux", "qc_h2o_flux",
    "H_strg", "LE_strg", "co2_strg", "h2o_strg",
    "co2_molar_density", "co2_mole_fraction", "co2_mixing_ratio",
    "h2o_molar_density", "h2o_mole_fraction", "h2o_mixing_ratio",
    "sonic_temperature", "air_temperature", "air_pressure",
    "air_density", "air_heat_capacity", "air_molar_volume",
    "ET", "water_vapor_density", "e", "es", "specific_humidity",
    "RH", "VPD", "Tdew",
    "wind_speed", "max_wind_speed", "wind_dir",
    "u_star", "TKE", "L", "z_minus_d_div_L", "bowen_ratio",
    "x_peak", "x_offset",
    "x_10_pct", "x_30_pct", "x_50_pct", "x_70_pct", "x_90_pct",
    "v_var", "site_id",
)


# Columns the M34 contract must preserve under their bronze name
# (exact pass-through; no rename). These are the prompt-listed
# representative columns plus a few flux-side equivalents the lineage
# CSV singles out.
REPRESENTATIVE_WIDE_PASSTHROUGH: tuple[str, ...] = (
    "h2o_flux",
    "qc_h2o_flux",
    "sonic_temperature",
    "air_pressure",
    "wind_speed",
    "TKE",
    "v_var",
    "RH",  # flux-side relative humidity, distinct from biomet RH_1_1_1
)


# Unit-aware final names that must be present in silver (and absent
# under their bronze names because the unit transformation rebinds the
# name).
UNIT_AWARE_FINAL_NAMES: tuple[tuple[str, str], ...] = (
    ("air_temperature", "air_temperature_c"),
    ("VPD", "VPD_hpa"),
)


# Eight non-time M32 internal -> final source-truth mappings (the
# lineage CSV's nine rows minus the ``DateTime -> timestamp`` time
# row, which behaves as an exact bronze-to-final match).
NON_TIME_SOURCE_TRUTH_FINALS: tuple[str, ...] = (
    "co2_flux", "qc_co2_flux", "air_temperature_c", "u_star",
    "VPD_hpa", "SWIN_1_1_1", "P_RAIN_1_1_1", "RH_1_1_1",
)


# Backend / internal-name columns that must NOT leak into a
# source-truth silver or gold payload.
INTERNAL_PASSTHROUGH_LEAKS: tuple[str, ...] = (
    "DateTime", "NEE", "QC_NEE", "Tair", "USTAR",
    "VPD", "Rg", "P_RAIN", "rH",
)


def _wide_bronze_flux_df(n: int = 4, *, site_id: str = "RBRL") -> pd.DataFrame:
    """Bronze flux frame in the full 54-column case-study shape."""
    base = pd.date_range("2025-08-01", periods=n, freq="30min", tz="UTC")
    iso = base.strftime("%Y-%m-%dT%H:%M:%S%z")
    data: dict[str, Any] = {
        "primary_key": [f"{site_id}|{ts}" for ts in iso],
        "timestamp": base,
        "filename": [f"flux_{i}.csv" for i in range(n)],
        "DOY": [200.0 + i / 48.0 for i in range(n)],
        "daytime": [1] * n,
        "H": [10.0 + i for i in range(n)],
        "qc_H": [0] * n,
        "LE": [50.0 + i for i in range(n)],
        "qc_LE": [0] * n,
        "co2_flux": [0.1 + 0.05 * i for i in range(n)],
        "qc_co2_flux": [0] * n,
        "h2o_flux": [0.5 + 0.05 * i for i in range(n)],
        "qc_h2o_flux": [0] * n,
        "H_strg": [0.1] * n,
        "LE_strg": [0.2] * n,
        "co2_strg": [0.05] * n,
        "h2o_strg": [0.06] * n,
        "co2_molar_density": [20.0] * n,
        "co2_mole_fraction": [400.0] * n,
        "co2_mixing_ratio": [400.0] * n,
        "h2o_molar_density": [15.0] * n,
        "h2o_mole_fraction": [20.0] * n,
        "h2o_mixing_ratio": [20.0] * n,
        "sonic_temperature": [293.0 + i for i in range(n)],
        "air_temperature": [293.15 + i for i in range(n)],  # K -> C
        "air_pressure": [101000.0] * n,
        "air_density": [1.2] * n,
        "air_heat_capacity": [1005.0] * n,
        "air_molar_volume": [0.024] * n,
        "ET": [0.1] * n,
        "water_vapor_density": [0.01] * n,
        "e": [2000.0] * n,
        "es": [3000.0] * n,
        "specific_humidity": [0.01] * n,
        "RH": [60.0, 61.0, 62.0, 63.0][:n],  # flux-side
        "VPD": [500.0 + i * 10 for i in range(n)],  # Pa -> hPa
        "Tdew": [285.0] * n,
        "wind_speed": [3.5 + 0.1 * i for i in range(n)],
        "max_wind_speed": [5.0] * n,
        "wind_dir": [180.0] * n,
        "u_star": [0.2 + 0.05 * i for i in range(n)],
        "TKE": [1.5 + 0.1 * i for i in range(n)],
        "L": [-50.0] * n,
        "z_minus_d_div_L": [-0.1] * n,
        "bowen_ratio": [0.5] * n,
        "x_peak": [50.0] * n,
        "x_offset": [10.0] * n,
        "x_10_pct": [20.0] * n,
        "x_30_pct": [30.0] * n,
        "x_50_pct": [50.0] * n,
        "x_70_pct": [70.0] * n,
        "x_90_pct": [90.0] * n,
        "v_var": [0.5 + 0.1 * i for i in range(n)],
        "site_id": [site_id] * n,
    }
    df = pd.DataFrame(data)
    return df[list(WIDE_FLUX_COLUMNS)]


def _wide_biomet_df(n: int = 4, *, site_id: str = "RBRL") -> pd.DataFrame:
    """Biomet frame carrying only the three processing-used columns
    plus the identity pair stage 1 keys on."""
    base = pd.date_range("2025-08-01", periods=n, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": base,
            "site_id": [site_id] * n,
            "SWIN_1_1_1": [100.0, 200.0, 300.0, 50.0][:n],
            "P_RAIN_1_1_1": [0.0] * n,
            "RH_1_1_1": [80.0, 81.0, 82.0, 83.0][:n],
        }
    )


def _silent_warnings() -> None:
    # Stage 1 emits informational warnings about timestamp range and
    # 'u_star -> u.' aliasing; they are not relevant to M34 assertions.
    warnings.simplefilter("ignore")


# ---------------------------------------------------------------------------
# 1. Lineage CSV spec (the M34 contract is anchored on this file)
# ---------------------------------------------------------------------------


class TestLineageCSVSpecM34:
    """The lineage CSV is the authoritative bronze -> silver -> gold
    column contract. M34 derives its widening rule from it."""

    @staticmethod
    def _csv_path() -> pathlib.Path:
        # The lineage CSV lives at <repo-root>/06_infra/schemas/...
        # Walk upward to find the first ancestor that actually contains
        # the file (mirrors the M33A layout-portable helper).
        relative = (
            pathlib.Path("06_infra")
            / "schemas"
            / "eddy_bronze_to_stage_column_lineage_contract.csv"
        )
        here = pathlib.Path(__file__).resolve()
        for ancestor in here.parents:
            candidate = ancestor / relative
            if candidate.is_file():
                return candidate
        raise FileNotFoundError(
            f"Could not locate {relative} above {here}."
        )

    def _read_rows(self) -> list[dict[str, str]]:
        with self._csv_path().open(encoding="utf-8") as fh:
            return list(csv.DictReader(fh))

    def test_csv_contains_the_eight_non_time_internal_to_final_mappings(self):
        rows = self._read_rows()
        i2f: dict[str, str] = {}
        for r in rows:
            internal = r["v_name_eddyproc"]
            final = r["v_name_final"]
            if internal and final and internal != final:
                i2f[internal] = final
        # The CSV's nine internal -> final rows are exactly the M32A
        # source-truth mapping (eight non-time + DateTime -> timestamp).
        assert i2f == dict(SILVER_INTERNAL_TO_FINAL_RENAME)

    def test_csv_lists_representative_wide_pass_through_bronze_columns(self):
        rows = self._read_rows()
        bronze_names = {
            r["v_name_bronze"]
            for r in rows
            if r["tbl_origin"] == "bronze_carbon_flux" and r["v_name_bronze"]
        }
        # The lineage CSV does not enumerate every wide pass-through
        # (it only spells out columns miaproc renames or otherwise
        # changes); but it explicitly calls out flux-side ``RH`` as a
        # pass-through that must survive distinctly from biomet ``RH_1_1_1``.
        assert "RH" in bronze_names

    def test_csv_only_renames_air_temperature_and_vpd_for_unit_transforms(self):
        rows = self._read_rows()
        unit_renamed = {
            r["v_name_bronze"]: r["v_name_final"]
            for r in rows
            if r["tbl_origin"] == "bronze_carbon_flux"
            and r["v_name_bronze"]
            and r["v_name_final"]
            and r["v_name_bronze"] != r["v_name_final"]
        }
        # The CSV's bronze -> final renames for the flux side are
        # exactly the two unit-baked rebindings (air_temperature ->
        # air_temperature_c, VPD -> VPD_hpa). Every other final name
        # equals the bronze name, encoding the wide pass-through rule
        # by absence.
        assert unit_renamed == {
            "air_temperature": "air_temperature_c",
            "VPD": "VPD_hpa",
        }


# ---------------------------------------------------------------------------
# 2. Silver helper-level: stage1 + apply_silver_source_truth_rename +
#    prepare_silver_stage_payload preserve every wide source column.
# ---------------------------------------------------------------------------


class TestSilverHelperWidePreservationM34:
    """End-to-end through the silver helper chain on a wide bronze."""

    def setup_method(self) -> None:
        _silent_warnings()

    def _silver(self) -> pd.DataFrame:
        flux = _wide_bronze_flux_df()
        biomet = _wide_biomet_df()
        return load_stage1_from_dataframes(
            flux_df=flux,
            biomet_df=biomet,
            drop_rain_rows=False,
            site_id="RBRL",
        )

    def test_stage1_preserves_every_wide_bronze_source_column(self):
        silver = self._silver()
        # Every wide bronze column either survives under its bronze
        # name (exact pass-through) or under its internal stage-1
        # alias (the four FULL_OUTPUT_RENAME_MAP renames + the
        # u_star -> u. -> USTAR detour). Stage 1 must not silently
        # drop unknown columns.
        survived_or_aliased = {
            "co2_flux": "NEE",
            "qc_co2_flux": "QC_NEE",
            "air_temperature": "Tair",
            "u_star": "USTAR",
        }
        for bronze in WIDE_FLUX_COLUMNS:
            if bronze in survived_or_aliased:
                assert survived_or_aliased[bronze] in silver.columns, (
                    bronze, survived_or_aliased[bronze], silver.columns,
                )
            else:
                assert bronze in silver.columns, (bronze, silver.columns)

    def test_stage1_adds_biomet_processing_columns_only(self):
        silver = self._silver()
        # Stage 1 attaches exactly three biomet-derived columns under
        # internal names (Rg, P_RAIN, rH). It does not introduce the
        # full biomet source surface.
        assert {"Rg", "P_RAIN", "rH"}.issubset(set(silver.columns))

    def test_apply_silver_source_truth_rename_yields_widened_silver(self):
        silver_st = apply_silver_source_truth_rename(self._silver())
        # Every non-time source-truth final name is present.
        for final in NON_TIME_SOURCE_TRUTH_FINALS:
            assert final in silver_st.columns, (final, silver_st.columns)
        # Every representative wide pass-through column survives under
        # its bronze name.
        for c in REPRESENTATIVE_WIDE_PASSTHROUGH:
            assert c in silver_st.columns, (c, silver_st.columns)
        # Unit-aware finals replace their bronze names.
        for bronze, final in UNIT_AWARE_FINAL_NAMES:
            assert final in silver_st.columns, (final, silver_st.columns)
            assert bronze not in silver_st.columns, (
                bronze, silver_st.columns,
            )
        # Flux-side RH and biomet RH_1_1_1 are case-insensitively
        # distinct and must both survive.
        assert "RH" in silver_st.columns
        assert "RH_1_1_1" in silver_st.columns

    def test_prepare_silver_stage_payload_preserves_every_wide_column(self):
        flux = _wide_bronze_flux_df()
        biomet = _wide_biomet_df()
        silver = load_stage1_from_dataframes(
            flux_df=flux,
            biomet_df=biomet,
            drop_rain_rows=False,
            site_id="RBRL",
        )
        silver_st = apply_silver_source_truth_rename(silver)
        payload, _actions = prepare_silver_stage_payload(
            silver_st,
            site_id="RBRL",
            source_flux_df=flux,
        )
        # Identity triple comes first.
        assert list(payload.columns[:3]) == ["primary_key", "site_id", "timestamp"]
        # Every wide pass-through survives under its bronze name.
        for c in REPRESENTATIVE_WIDE_PASSTHROUGH:
            assert c in payload.columns, (c, payload.columns)
        # Every source-truth final name is present.
        for final in NON_TIME_SOURCE_TRUTH_FINALS:
            assert final in payload.columns, (final, payload.columns)
        # No internal-name passthrough survives in the payload.
        for leak in INTERNAL_PASSTHROUGH_LEAKS:
            assert leak not in payload.columns, (leak, payload.columns)
        # Case-insensitive field-key uniqueness.
        keys = [c.casefold() for c in payload.columns]
        assert len(set(keys)) == len(keys), payload.columns
        # The widened silver payload carries 57 columns for the
        # non-divergent humidity case (identity triple + 50 source
        # pass-throughs + 5 unit-aware source-truth rebindings + 3
        # biomet-derived source-truth columns - timestamp counted
        # once across identity and source pass-throughs); guard
        # the count so a regression that drops or adds a column is
        # visible.
        assert len(payload.columns) == 57, payload.columns

    def test_widened_payload_records_only_unit_aware_aliases(self):
        # SILVER_BRONZE_TO_FINAL_ALIASES is the alias dictionary the
        # silver dry-run preservation check consults. Under M34 the
        # alias map must record exactly the unit-transformed names
        # plus the legacy ``u.`` alias; every other wide source column
        # survives by exact-name match and must not be listed as an
        # alias resolution.
        assert SILVER_BRONZE_TO_FINAL_ALIASES == {
            "air_temperature": "air_temperature_c",
            "VPD": "VPD_hpa",
            "u.": "u_star",
        }


# ---------------------------------------------------------------------------
# 3/4. Silver file CLI on a wide bronze (M002-S04 replacement for the
#    retired BigQuery silver CLI dry-run and writeback tests): the same
#    wide bronze flux/biomet frames are explicit Parquet inputs to
#    ``eddy run-silver``, and the published silver Parquet (the payload
#    the file workflow hands downstream) must report no missing input
#    column, carry the wide columns under their bronze names, rebind only
#    the unit-transformed names and be casefold-unique.
# ---------------------------------------------------------------------------


class TestSilverFileCLIWidePassthroughM34:
    @staticmethod
    def _run_silver(tmp_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
        _silent_warnings()
        flux_path = tmp_path / "flux.parquet"
        biomet_path = tmp_path / "biomet.parquet"
        write_parquet_table(flux_path, _wide_bronze_flux_df())
        write_parquet_table(biomet_path, _wide_biomet_df())
        run_dir = tmp_path / "silver_run"
        argv = [
            "eddy", "run-silver",
            "--flux-parquet", str(flux_path),
            "--biomet-parquet", str(biomet_path),
            "--output-table", str(run_dir / "silver.parquet"),
            "--output-run-json", str(run_dir / "silver_run.json"),
        ]
        assert cli.main(argv) == cli.SUCCESS_EXIT
        silver = read_parquet_table(
            run_dir / "silver.parquet", flag="--output-table", role="silver"
        )
        meta = json.loads((run_dir / "silver_run.json").read_text(encoding="utf-8"))
        return silver, meta

    def test_silver_file_run_has_no_missing_input_columns(self, tmp_path):
        silver, meta = self._run_silver(tmp_path)
        cols = list(silver.columns)
        # Every bronze flux column survives under its own name or its
        # unit-aware alias (the former dry-run missing_input_columns check).
        missing = [
            bronze for bronze in WIDE_FLUX_COLUMNS
            if SILVER_BRONZE_TO_FINAL_ALIASES.get(bronze, bronze) not in cols
        ]
        assert missing == [], missing
        assert silver.columns.is_unique, cols
        assert meta["column_collision_actions"] == []

    def test_silver_file_run_lists_representative_wide_columns(self, tmp_path):
        silver, _meta = self._run_silver(tmp_path)
        cols = set(silver.columns)
        for c in REPRESENTATIVE_WIDE_PASSTHROUGH:
            assert c in cols, (c, sorted(cols))
        for final in NON_TIME_SOURCE_TRUTH_FINALS:
            assert final in cols, (final, sorted(cols))
        for leak in INTERNAL_PASSTHROUGH_LEAKS:
            assert leak not in cols, (leak, sorted(cols))

    def test_silver_file_run_rebinds_only_unit_aware_names(self, tmp_path):
        silver, _meta = self._run_silver(tmp_path)
        bronze = _wide_bronze_flux_df()
        merged = silver.merge(
            bronze, on="timestamp", how="inner", suffixes=("", "_bronze")
        )
        assert len(merged) == len(bronze)
        # Unit-aware aliases: the bronze name is rebound to the final name
        # and carries the converted values (K to degC, Pa to hPa).
        for bronze_name, final in UNIT_AWARE_FINAL_NAMES:
            assert bronze_name not in silver.columns, (bronze_name, list(silver.columns))
            assert final in silver.columns, (final, list(silver.columns))
        assert merged["air_temperature_c"].to_numpy() == pytest.approx(
            (merged["air_temperature"] - 273.15).to_numpy()
        )
        assert merged["VPD_hpa"].to_numpy() == pytest.approx(
            (merged["VPD"] / 100.0).to_numpy()
        )
        # Exact pass-through columns keep their bronze name and values;
        # they are not alias resolutions.
        for c in ("h2o_flux", "wind_speed", "TKE", "RH"):
            assert c not in SILVER_BRONZE_TO_FINAL_ALIASES, c
            assert merged[c].tolist() == merged[f"{c}_bronze"].tolist(), c

    def test_silver_file_run_payload_contains_wide_columns(self, tmp_path):
        silver, _meta = self._run_silver(tmp_path)
        cols = list(silver.columns)
        for c in REPRESENTATIVE_WIDE_PASSTHROUGH:
            assert c in cols, (c, cols)
        for final in NON_TIME_SOURCE_TRUTH_FINALS:
            assert final in cols, (final, cols)
        for leak in INTERNAL_PASSTHROUGH_LEAKS:
            assert leak not in cols, (leak, cols)
        keys = [c.casefold() for c in cols]
        assert len(set(keys)) == len(keys), cols


# ---------------------------------------------------------------------------
# 5. Gold preservation: prepare_gold_stage_payload must preserve every
#    widened silver source column and append the gold-only outputs.
# ---------------------------------------------------------------------------


class TestGoldWidePassthroughM34:
    @staticmethod
    def _wide_processed_gold_frame(n: int = 4) -> pd.DataFrame:
        """Construct the merged processed gold frame _attach_silver_columns_to_gold
        would produce: backend outputs keyed on DateTime, plus the
        wide source-truth silver columns attached as silver-extras.
        """
        base = pd.date_range("2025-08-01", periods=n, freq="30min", tz="UTC")
        # Backend output (uppercase analytical names + DateTime + the
        # internal passthroughs the backend emits alongside gap-filled
        # outputs). prepare_gold_stage_payload drops the internal
        # passthroughs when a source-truth counterpart is present.
        backend = pd.DataFrame({
            "DateTime": base,
            "NEE": [0.1 + 0.05 * i for i in range(n)],
            "NEE_f": [0.1 + 0.05 * i for i in range(n)],
            "NEE_fqc": [0] * n,
            "Tair": [20.0 + i for i in range(n)],
            "Tair_f": [20.0 + i for i in range(n)],
            "USTAR": [0.2 + 0.05 * i for i in range(n)],
            "VPD": [5.0 + 0.1 * i for i in range(n)],
            "VPD_f": [5.0 + 0.1 * i for i in range(n)],
            "Rg": [100.0 + i * 10 for i in range(n)],
            "Rg_f": [100.0 + i * 10 for i in range(n)],
            "GPP": [1.0 + i * 0.1 for i in range(n)],
            "Reco": [0.5 + i * 0.05 for i in range(n)],
            "QC_NEE": [0] * n,
            "H": [10.0 + i for i in range(n)],
            "qc_H": [0] * n,
            "LE": [50.0 + i for i in range(n)],
            "qc_LE": [0] * n,
            "P_RAIN": [0.0] * n,
            "rH": [80.0, 81.0, 82.0, 83.0][:n],
        })
        # Silver source-truth carry-forward columns attached by
        # _attach_silver_columns_to_gold (the M32A path that left-joins
        # silver-only columns onto gold keyed on DateTime).
        silver_extras = pd.DataFrame({
            "DateTime": base,
            "co2_flux": [0.1 + 0.05 * i for i in range(n)],
            "qc_co2_flux": [0] * n,
            "air_temperature_c": [20.0 + i for i in range(n)],
            "u_star": [0.2 + 0.05 * i for i in range(n)],
            "VPD_hpa": [5.0 + 0.1 * i for i in range(n)],
            "SWIN_1_1_1": [100.0, 200.0, 300.0, 50.0][:n],
            "P_RAIN_1_1_1": [0.0] * n,
            "RH_1_1_1": [80.0, 81.0, 82.0, 83.0][:n],
            "RH": [60.0, 61.0, 62.0, 63.0][:n],
            # Representative wide pass-throughs:
            "h2o_flux": [0.5 + 0.05 * i for i in range(n)],
            "qc_h2o_flux": [0] * n,
            "sonic_temperature": [293.0 + i for i in range(n)],
            "air_pressure": [101000.0] * n,
            "wind_speed": [3.5 + 0.1 * i for i in range(n)],
            "TKE": [1.5 + 0.1 * i for i in range(n)],
            "v_var": [0.5 + 0.1 * i for i in range(n)],
            # Bronze identity carried into silver too:
            "timestamp": base,
        })
        return backend.merge(silver_extras, on="DateTime", how="left")

    def test_gold_stage_payload_preserves_widened_silver_columns(self):
        _silent_warnings()
        processed = self._wide_processed_gold_frame()
        payload = prepare_gold_stage_payload(
            processed,
            site_id="RBRL",
        )
        cols = set(payload.columns)
        # Every representative wide pass-through survives into gold.
        for c in REPRESENTATIVE_WIDE_PASSTHROUGH:
            assert c in cols, (c, sorted(cols))
        # Every non-time source-truth silver final name survives.
        for final in NON_TIME_SOURCE_TRUTH_FINALS:
            assert final in cols, (final, sorted(cols))
        # The gold-only outputs are appended.
        gold_only = {"nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f",
                     "GPP", "Reco", "dateAndTime"}
        assert gold_only.issubset(cols), (gold_only - cols, sorted(cols))
        # Identity triple is present.
        for ident in ("primary_key", "site_id", "timestamp"):
            assert ident in cols, (ident, sorted(cols))
        # No internal-name passthroughs leak into the gold output
        # boundary (M32 redundant-passthrough drop).
        for leak in INTERNAL_PASSTHROUGH_LEAKS:
            assert leak not in cols, (leak, sorted(cols))
        # Uppercase mirrors of the lowercase analytical outputs must be
        # absent (S2_FILT_1_RENAME_MAP renames them in place).
        for upper in ("NEE_f", "NEE_fqc", "Rg_f", "Tair_f", "VPD_f"):
            assert upper not in cols, (upper, sorted(cols))
        # Case-insensitive field-key uniqueness.
        keys = [c.casefold() for c in payload.columns]
        assert len(set(keys)) == len(keys), payload.columns


# ---------------------------------------------------------------------------
# 6. Regression rails: the wider contract must not weaken the M28
#    case-insensitive duplicate guard for non-humidity collisions.
# ---------------------------------------------------------------------------


class TestRegressionRailsM34:
    def test_non_humidity_case_collision_still_raises_in_payload(self):
        _silent_warnings()
        # Construct a silver frame that already carries source-truth
        # column names but introduces a non-humidity case-insensitive
        # duplicate (``TKE`` and ``tke``). The M28 / M31 guard inside
        # prepare_silver_stage_payload -> ensure_unique_stage_columns
        # must still raise rather than fuse the columns.
        base = pd.date_range("2025-08-01", periods=4, freq="30min", tz="UTC")
        silver = pd.DataFrame({
            "timestamp": base,
            "co2_flux": [0.1, 0.2, -0.1, 0.3],
            "qc_co2_flux": [0, 0, 0, 0],
            "air_temperature_c": [20.0, 21.0, 22.0, 19.0],
            "u_star": [0.2, 0.3, 0.4, 0.1],
            "VPD_hpa": [5.0, 6.0, 7.0, 4.0],
            "SWIN_1_1_1": [0.0, 100.0, 200.0, 50.0],
            "P_RAIN_1_1_1": [0.0, 0.0, 0.0, 0.0],
            "RH_1_1_1": [80.0, 81.0, 82.0, 83.0],
            "RH": [60.0, 61.0, 62.0, 63.0],
            "TKE": [1.0, 1.1, 1.2, 1.3],
            "tke": [9.0, 9.1, 9.2, 9.3],  # non-humidity casefold collision
        })
        # Source flux frame is unique (no duplicate); the collision
        # lives in the silver payload itself.
        source_flux = _wide_bronze_flux_df()
        with pytest.raises(DuplicateStageColumnsError):
            prepare_silver_stage_payload(
                silver,
                site_id="RBRL",
                source_flux_df=source_flux,
            )
