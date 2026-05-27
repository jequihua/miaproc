"""M32: tests for the eddy source-truth column contract.

Stage 1 still computes under internal aliases (``NEE``, ``QC_NEE``,
``Tair``, ``USTAR``, ``VPD``, ``Rg``, ``P_RAIN``, ``rH``); silver and
gold payloads expose source-facing final names (``co2_flux``,
``qc_co2_flux``, ``air_temperature_c``, ``u_star``, ``VPD_hpa``,
``SWIN_1_1_1``, ``P_RAIN_1_1_1``, ``RH_1_1_1``) so the BigQuery
silver/gold tables look like source-truth products instead of
renamed processing tables.

These tests cover:

- the new ``apply_silver_source_truth_rename`` and
  ``silver_to_internal_calc_frame`` helpers;
- the source-truth final names appearing in ``prepare_silver_stage_payload``
  outputs;
- the dropped redundant backend passthroughs from
  ``prepare_stage_dataframe`` outputs when the source-truth silver
  columns are attached;
- the preservation of flux-side ``RH`` separately from biomet
  ``RH_1_1_1``;
- the unit-baked final names ``air_temperature_c`` (Celsius) and
  ``VPD_hpa`` (hPa);
- the case-insensitive BigQuery field-name uniqueness invariant.
"""
from __future__ import annotations

import pandas as pd
import pytest

from miaproc.eddy.bigquery_writeback import (
    DuplicateStageColumnsError,
    HUMIDITY_DERIVED_RENAME,
    apply_silver_source_truth_rename,
    bigquery_field_key,
    prepare_silver_stage_payload,
    prepare_stage_dataframe,
    silver_to_internal_calc_frame,
)


def _internal_silver_cloud_shape(n: int = 3) -> pd.DataFrame:
    """Stage-1-shaped silver: internal aliases for the inherited
    flux + biomet columns, with a flux-side ``RH`` pass-through and
    a bronze-only sentinel that has no canonical alias."""
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, 0.3],
            "QC_NEE": [0, 0, 0],
            "Tair": [20.0, 21.0, 22.0],   # post convert_units Celsius
            "USTAR": [0.2, 0.3, 0.4],
            "VPD": [5.0, 6.0, 7.0],       # post convert_units hPa
            "Rg": [0.0, 100.0, 200.0],
            "P_RAIN": [0.0, 0.0, 0.0],
            "rH": [85.0, 86.0, 87.0],
            "RH": [60.0, 61.0, 62.0],     # flux-side pass-through
            "H": [10.0, 20.0, 30.0],
            "LE": [50.0, 60.0, 70.0],
            "bronze_only_flag": [1, 0, 1],
        }
    )


# ---------------------------------------------------------------------------
# Source-truth rename helpers
# ---------------------------------------------------------------------------


class TestApplySilverSourceTruthRename:
    def test_internal_aliases_renamed_to_source_truth(self):
        df = _internal_silver_cloud_shape()
        out = apply_silver_source_truth_rename(df)
        # M32A: the lineage CSV says ``DateTime -> timestamp`` so the
        # internal time column is renamed too. All nine mappings:
        for src, final in (
            ("DateTime", "timestamp"),
            ("NEE", "co2_flux"),
            ("QC_NEE", "qc_co2_flux"),
            ("Tair", "air_temperature_c"),
            ("USTAR", "u_star"),
            ("VPD", "VPD_hpa"),
            ("Rg", "SWIN_1_1_1"),
            ("P_RAIN", "P_RAIN_1_1_1"),
            ("rH", "RH_1_1_1"),
        ):
            assert src not in out.columns, src
            assert final in out.columns, final
        for kept in ("RH", "H", "LE", "bronze_only_flag"):
            assert kept in out.columns, kept
        # Caller's frame is untouched.
        assert "NEE" in df.columns
        assert "DateTime" in df.columns
        # Values flow through unchanged: position must match.
        assert list(out["co2_flux"]) == [0.1, 0.2, 0.3]
        assert list(out["air_temperature_c"]) == [20.0, 21.0, 22.0]
        assert list(out["VPD_hpa"]) == [5.0, 6.0, 7.0]

    def test_already_source_truth_is_no_op(self):
        # M32A: an already-source-truth frame uses ``timestamp`` as
        # its time column (the lineage CSV's final name); no
        # ``DateTime`` should be present.
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range(
                    "2025-08-01", periods=2, freq="30min", tz="UTC"
                ),
                "co2_flux": [0.1, 0.2],
                "u_star": [0.2, 0.3],
                "air_temperature_c": [20.0, 21.0],
                "RH": [60.0, 61.0],
                "RH_1_1_1": [85.0, 86.0],
            }
        )
        before = df.copy(deep=True)
        out = apply_silver_source_truth_rename(df)
        pd.testing.assert_frame_equal(out, before)

    def test_datetime_renamed_to_timestamp_when_timestamp_absent(self):
        """M32A: a stage-1 frame with ``DateTime`` but no
        ``timestamp`` is renamed cleanly."""
        df = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=2, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2],
            }
        )
        out = apply_silver_source_truth_rename(df)
        assert "timestamp" in out.columns
        assert "DateTime" not in out.columns
        assert "co2_flux" in out.columns

    def test_datetime_dropped_when_timestamp_also_present(self):
        """M32A defensive: when both internal ``DateTime`` and
        source-truth ``timestamp`` are present, the source-truth
        ``timestamp`` value wins at the silver output boundary and
        the internal ``DateTime`` is dropped so the rename does not
        synthesize a literal-duplicate ``timestamp`` field."""
        ts = pd.date_range(
            "2025-08-01", periods=3, freq="30min", tz="UTC"
        )
        df = pd.DataFrame(
            {
                "DateTime": ts,
                "timestamp": ts,
                "NEE": [0.1, 0.2, 0.3],
            }
        )
        out = apply_silver_source_truth_rename(df)
        assert list(out.columns).count("timestamp") == 1
        assert "DateTime" not in out.columns

    def test_duplicate_internal_name_left_for_unique_guard(self):
        """If stage 1 ever emits a duplicate of an internal name, the
        rename is skipped for that name so the legacy
        ``ensure_unique_stage_columns`` humidity policy (``rH`` +
        ``rH_norm_s``) can still resolve it."""
        base = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
            }
        )
        df = pd.concat(
            [
                base,
                pd.Series([85.0, 86.0, 87.0]).rename("rH"),
                pd.Series([88.0, 89.0, 90.0]).rename("rH"),
            ],
            axis=1,
        )
        out = apply_silver_source_truth_rename(df)
        # Two ``rH`` columns are preserved; NEE is unique so it is
        # still renamed.
        assert list(out.columns).count("rH") == 2
        assert "co2_flux" in out.columns
        assert "NEE" not in out.columns
        assert "RH_1_1_1" not in out.columns


class TestSilverToInternalCalcFrame:
    def test_source_truth_silver_maps_back_to_internal(self):
        # M32A: a strict source-truth silver frame uses ``timestamp``
        # (no internal ``DateTime``). The helper must reconstruct
        # ``DateTime`` from ``timestamp`` so the backend can run.
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "co2_flux": [0.1, 0.2, 0.3],
                "qc_co2_flux": [0, 0, 0],
                "air_temperature_c": [20.0, 21.0, 22.0],
                "u_star": [0.2, 0.3, 0.4],
                "VPD_hpa": [5.0, 6.0, 7.0],
                "SWIN_1_1_1": [0.0, 100.0, 200.0],
                "P_RAIN_1_1_1": [0.0, 0.0, 0.0],
                "RH_1_1_1": [85.0, 86.0, 87.0],
                "RH": [60.0, 61.0, 62.0],
                "H": [10.0, 20.0, 30.0],
            }
        )
        out = silver_to_internal_calc_frame(df)
        for src, final in (
            ("timestamp", "DateTime"),
            ("co2_flux", "NEE"),
            ("qc_co2_flux", "QC_NEE"),
            ("air_temperature_c", "Tair"),
            ("u_star", "USTAR"),
            ("VPD_hpa", "VPD"),
            ("SWIN_1_1_1", "Rg"),
            ("P_RAIN_1_1_1", "P_RAIN"),
            ("RH_1_1_1", "rH"),
        ):
            assert src not in out.columns, src
            assert final in out.columns, final
        # Flux-side RH and other pass-throughs survive untouched.
        for kept in ("RH", "H"):
            assert kept in out.columns, kept

    def test_timestamp_wins_when_datetime_also_leaked(self):
        """M32A defensive: a silver frame that carries both the
        source-truth ``timestamp`` and a leaked internal ``DateTime``
        keeps the source-truth value because the BigQuery silver table
        the gold CLI reads back under M32A will only carry
        ``timestamp``."""
        ts_truth = pd.to_datetime(
            ["2025-08-01 00:00:00+00:00", "2025-08-01 00:30:00+00:00"]
        )
        ts_stale = pd.to_datetime(
            ["2020-01-01 00:00:00+00:00", "2020-01-01 00:30:00+00:00"]
        )
        df = pd.DataFrame(
            {
                "timestamp": ts_truth,
                "DateTime": ts_stale,
                "co2_flux": [0.1, 0.2],
            }
        )
        out = silver_to_internal_calc_frame(df)
        assert "timestamp" not in out.columns
        assert "DateTime" in out.columns
        # Source-truth timestamp value wins; the stale DateTime is gone.
        assert list(out["DateTime"]) == list(ts_truth)

    def test_caller_frame_not_mutated(self):
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range(
                    "2025-08-01", periods=2, freq="30min", tz="UTC"
                ),
                "co2_flux": [0.1, 0.2],
                "air_temperature_c": [20.0, 21.0],
            }
        )
        before = df.copy(deep=True)
        _ = silver_to_internal_calc_frame(df)
        pd.testing.assert_frame_equal(df, before)

    def test_timestamp_only_source_truth_drives_prepare_reddyproc_input(self):
        """M32A end-to-end: a strict source-truth silver frame with
        ``timestamp`` and no internal ``DateTime`` must be acceptable
        to :func:`prepare_reddyproc_input` after passing through
        :func:`silver_to_internal_calc_frame`."""
        from miaproc.eddy.stage2 import (
            _REQUIRED_INPUT_COLUMNS,
            prepare_reddyproc_input,
        )

        silver = pd.DataFrame(
            {
                "timestamp": pd.date_range(
                    "2025-08-01", periods=4, freq="30min", tz="UTC"
                ),
                "co2_flux": [0.1, 0.2, -0.1, 0.3],
                "qc_co2_flux": [0, 0, 0, 0],
                "air_temperature_c": [20.0, 21.0, 22.0, 19.0],
                "u_star": [0.2, 0.3, 0.4, 0.1],
                "VPD_hpa": [5.0, 6.0, 7.0, 4.0],
                "SWIN_1_1_1": [0.0, 100.0, 200.0, 50.0],
                "P_RAIN_1_1_1": [0.0, 0.0, 0.0, 0.0],
                "RH_1_1_1": [85.0, 86.0, 87.0, 88.0],
                "RH": [60.0, 61.0, 62.0, 63.0],
            }
        )
        calc = silver_to_internal_calc_frame(silver)
        for col in _REQUIRED_INPUT_COLUMNS:
            assert col in calc.columns, col
        stage2 = prepare_reddyproc_input(calc)
        # The reconstructed calc frame drives the stage-2 contract
        # successfully (Year / DoY / Hour derived from the renamed
        # ``DateTime``; Ustar / QF aliases applied).
        assert len(stage2) == 4
        assert {"DateTime", "Year", "DoY", "Hour", "NEE", "Ustar", "QF"}.issubset(
            stage2.columns
        )


# ---------------------------------------------------------------------------
# prepare_silver_stage_payload under the M32 contract
# ---------------------------------------------------------------------------


class TestPrepareSilverStagePayloadSourceTruth:
    def test_payload_carries_source_truth_inherited_names(self):
        silver = _internal_silver_cloud_shape()
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        cols = list(payload.columns)
        for ident in ("primary_key", "site_id", "timestamp"):
            assert ident in cols, ident
        # M32 source-truth final names for inherited variables.
        for final in (
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
            "RH",
            "H",
            "LE",
            "bronze_only_flag",
        ):
            assert final in cols, final
        # Backend-only inherited names are absent.
        for backend in (
            "NEE",
            "QC_NEE",
            "Tair",
            "USTAR",
            "VPD",
            "Rg",
            "P_RAIN",
            "rH",
        ):
            assert backend not in cols, backend
        assert actions == []

    def test_flux_rh_and_biomet_rh_1_1_1_both_survive_when_diverge(self):
        silver = _internal_silver_cloud_shape()
        silver["RH"] = [50.0, 51.0, 52.0]
        silver["rH"] = [80.0, 81.0, 82.0]
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        assert "RH" in payload.columns
        assert "RH_1_1_1" in payload.columns
        assert list(payload["RH"]) == [50.0, 51.0, 52.0]
        assert list(payload["RH_1_1_1"]) == [80.0, 81.0, 82.0]
        # No legacy rH_norm_s — the case-insensitive collision is
        # gone under M32 so no humidity policy fires.
        assert HUMIDITY_DERIVED_RENAME not in payload.columns
        assert actions == []

    def test_payload_unique_under_bigquery_field_key(self):
        silver = _internal_silver_cloud_shape()
        payload, _ = prepare_silver_stage_payload(silver, site_id="RBRL")
        keys = [bigquery_field_key(c) for c in payload.columns]
        assert len(set(keys)) == len(keys), list(payload.columns)

    def test_air_temperature_c_carries_celsius_values(self):
        silver = _internal_silver_cloud_shape()
        payload, _ = prepare_silver_stage_payload(silver, site_id="RBRL")
        # Values match the post-convert_units Celsius numbers, not
        # the bronze Kelvin equivalents.
        assert list(payload["air_temperature_c"]) == [20.0, 21.0, 22.0]

    def test_VPD_hpa_carries_processed_hpa_values(self):
        silver = _internal_silver_cloud_shape()
        payload, _ = prepare_silver_stage_payload(silver, site_id="RBRL")
        # Values match the post-convert_units hPa numbers, not the
        # bronze Pa equivalents.
        assert list(payload["VPD_hpa"]) == [5.0, 6.0, 7.0]

    def test_non_humidity_case_insensitive_duplicate_still_raises(self):
        """M32 keeps the strict M28/M31 non-humidity rule: a literal
        case-only collision on a non-humidity field is an upstream
        defect and must raise rather than silently fuse."""
        silver = _internal_silver_cloud_shape()
        # ``H`` (sensible heat) collides with a hypothetical ``h``
        # only on case.
        silver = pd.concat(
            [silver, pd.Series([1, 2, 3]).rename("h")], axis=1
        )
        with pytest.raises(DuplicateStageColumnsError):
            prepare_silver_stage_payload(silver, site_id="RBRL")


# ---------------------------------------------------------------------------
# prepare_stage_dataframe under the M32 contract for gold
# ---------------------------------------------------------------------------


def _gold_with_source_truth_silver() -> pd.DataFrame:
    """Gold backend output joined with source-truth silver columns.

    Models the post-``_attach_silver_columns_to_gold`` state under
    the M32 contract: the backend emits its raw passthroughs and
    new gap-filled outputs; the gold CLI attaches the source-truth
    silver columns afterwards.
    """
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            ),
            # Backend internal passthroughs (duplicates of source-truth).
            "NEE": [0.1, 0.2, 0.3],
            "Tair": [20.0, 21.0, 22.0],
            "USTAR": [0.2, 0.3, 0.4],
            "Rg": [0.0, 100.0, 200.0],
            "VPD": [5.0, 6.0, 7.0],
            # New backend gap-filled outputs.
            "NEE_f": [0.11, 0.21, 0.31],
            "NEE_fqc": [0, 0, 0],
            "Tair_f": [20.5, 21.5, 22.5],
            "Rg_f": [0.0, 100.0, 200.0],
            "VPD_f": [5.1, 6.1, 7.1],
            "GPP": [0.0, 1.0, 2.0],
            "Reco": [1.0, 1.0, 1.0],
            # Source-truth silver columns attached by the gold CLI.
            "co2_flux": [0.1, 0.2, 0.3],
            "qc_co2_flux": [0, 0, 0],
            "air_temperature_c": [20.0, 21.0, 22.0],
            "u_star": [0.2, 0.3, 0.4],
            "VPD_hpa": [5.0, 6.0, 7.0],
            "SWIN_1_1_1": [0.0, 100.0, 200.0],
            "P_RAIN_1_1_1": [0.0, 0.0, 0.0],
            "RH_1_1_1": [85.0, 86.0, 87.0],
            "RH": [60.0, 61.0, 62.0],
            "H": [10.0, 20.0, 30.0],
            "LE": [50.0, 60.0, 70.0],
            "bronze_only_flag": [1, 0, 1],
        }
    )


class TestPrepareStageDataframeSourceTruthGold:
    def test_source_truth_silver_columns_preserved_in_gold_payload(self):
        gold = _gold_with_source_truth_silver()
        out = prepare_stage_dataframe(
            gold, site_id="RBRL", preserve_payload_columns=True
        )
        cols = list(out.columns)
        for col in (
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
            "RH",
            "H",
            "LE",
            "bronze_only_flag",
        ):
            assert col in cols, col

    def test_backend_internal_passthroughs_dropped_when_source_truth_present(
        self,
    ):
        gold = _gold_with_source_truth_silver()
        out = prepare_stage_dataframe(
            gold, site_id="RBRL", preserve_payload_columns=True
        )
        cols = list(out.columns)
        # Backend passthroughs that duplicate source-truth columns
        # are removed; only the source-truth columns survive.
        for redundant in ("NEE", "Tair", "USTAR", "Rg", "VPD"):
            assert redundant not in cols, redundant

    def test_new_gold_processing_outputs_renamed_to_lowercase(self):
        gold = _gold_with_source_truth_silver()
        out = prepare_stage_dataframe(
            gold, site_id="RBRL", preserve_payload_columns=True
        )
        cols = list(out.columns)
        for new in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f"):
            assert new in cols, new
        for old in ("NEE_f", "NEE_fqc", "Rg_f", "Tair_f", "VPD_f"):
            assert old not in cols, old
        # New partitioning outputs without a lowercase mapping are
        # preserved as-is.
        assert "GPP" in cols
        assert "Reco" in cols

    def test_payload_unique_under_bigquery_field_key(self):
        gold = _gold_with_source_truth_silver()
        out = prepare_stage_dataframe(
            gold, site_id="RBRL", preserve_payload_columns=True
        )
        keys = [bigquery_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)

    def test_legacy_backend_only_gold_keeps_internal_names(self):
        """When no source-truth silver columns are attached (e.g.
        legacy fixtures, or callers that build a stage payload from
        a backend output directly), the redundant-drop policy is a
        no-op so existing M28-shaped tests stay green."""
        legacy_gold = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
                "Tair": [20.0, 21.0, 22.0],
                "USTAR": [0.2, 0.3, 0.4],
                "NEE_f": [0.11, 0.21, 0.31],
            }
        )
        out = prepare_stage_dataframe(
            legacy_gold, site_id="RBRL", preserve_payload_columns=True
        )
        cols = list(out.columns)
        for backend in ("NEE", "Tair", "USTAR"):
            assert backend in cols, backend
