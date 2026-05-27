"""M35A: regularized gap rows must preserve source-truth ``timestamp``.

The M35 live BigQuery stage-write smoke surfaced a real defect:
``stage1_from_raw_frames`` runs ``regularize_time_grid`` to fill in
half-hour grid gaps; inserted rows carried a valid grid ``DateTime``
but the source ``timestamp`` column was NaT, so the M32A source-truth
boundary downstream kept ``timestamp = NaT`` for those rows.
``prepare_silver_stage_payload`` synthesized
``primary_key = site_id|<iso(NaT)>`` → NaT → BigQuery NULL, and
``validate_stage_table`` correctly refused.

M35A fixes the defect at the regularization boundary:
``regularize_time_grid`` now fills the source-truth ``timestamp`` from
the regularized ``DateTime`` value on inserted rows, and propagates a
uniform ``site_id`` from the group context onto those rows.
Measurement columns remain NaN on inserted rows; only the identity
columns (``timestamp``, ``site_id``) are filled.

This module verifies the fix end-to-end:

- ``regularize_time_grid`` itself fills the source ``timestamp`` and
  ``site_id`` on inserted rows;
- the full silver helper chain
  (``load_stage1_from_dataframes`` → ``apply_silver_source_truth_rename``
  → ``prepare_silver_stage_payload``) produces a payload with zero
  NaT timestamps and zero NULL ``primary_key`` against a deliberately
  gapped fixture;
- grouped processing across multiple sites still produces unique
  ``(site_id, timestamp)`` identity pairs;
- legacy callers passing ``DateTime``-only frames (no source
  ``timestamp``) keep the pre-M35A behavior.
"""
from __future__ import annotations

import warnings

import pandas as pd

from miaproc.eddy import (
    apply_silver_source_truth_rename,
    load_stage1_from_dataframes,
    prepare_silver_stage_payload,
    stage1_from_raw_frames,
)
from miaproc.eddy.time import regularize_time_grid


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _silent_warnings() -> None:
    warnings.simplefilter("ignore")


def _frame_with_gap(
    *,
    site_id: str = "RBRL",
    drop_idx: tuple[int, ...] = (3, 5),
) -> pd.DataFrame:
    """Build a small frame with `DateTime` + source `timestamp` +
    `site_id` + a measurement column, then drop the rows at
    ``drop_idx`` so ``regularize_time_grid`` has to insert them back."""
    base = pd.date_range("2025-08-01", periods=8, freq="30min", tz="UTC")
    df = pd.DataFrame(
        {
            "DateTime": base,
            "timestamp": base,
            "site_id": [site_id] * 8,
            "co2_flux": [0.1 + 0.05 * i for i in range(8)],
            "u_star": [0.2 + 0.05 * i for i in range(8)],
        }
    )
    return df.drop(index=list(drop_idx)).reset_index(drop=True)


def _gapped_flux_for_stage1(
    *, site_id: str, drop_idx: tuple[int, ...]
) -> pd.DataFrame:
    """A flux fixture in bronze shape (54-ish columns, source-truth
    names where applicable) that drops the rows at ``drop_idx`` so the
    stage-1 ``regularize_time_grid`` step inserts them as gap rows."""
    n = 8
    base = pd.date_range("2025-08-01", periods=n, freq="30min", tz="UTC")
    iso = base.strftime("%Y-%m-%dT%H:%M:%S%z")
    df = pd.DataFrame(
        {
            "primary_key": [f"{site_id}|{ts}" for ts in iso],
            "timestamp": base,
            "site_id": [site_id] * n,
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
            "sonic_temperature": [293.0 + i for i in range(n)],
            "air_temperature": [293.15 + i for i in range(n)],  # K
            "air_pressure": [101000.0] * n,
            "RH": [60.0, 61.0, 62.0, 63.0, 64.0, 65.0, 66.0, 67.0],
            "VPD": [500.0 + 10 * i for i in range(n)],  # Pa
            "wind_speed": [3.5 + 0.1 * i for i in range(n)],
            "max_wind_speed": [5.0] * n,
            "wind_dir": [180.0] * n,
            "u_star": [0.2 + 0.05 * i for i in range(n)],
            "TKE": [1.5 + 0.1 * i for i in range(n)],
            "v_var": [0.5 + 0.1 * i for i in range(n)],
        }
    )
    return df.drop(index=list(drop_idx)).reset_index(drop=True)


def _biomet_for_stage1(*, site_id: str) -> pd.DataFrame:
    """Synthetic biomet that covers the gapped flux's full date range
    (no biomet gaps; only the flux side has gaps in these tests)."""
    base = pd.date_range("2025-08-01", periods=8, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": base,
            "site_id": [site_id] * 8,
            "SWIN_1_1_1": [100.0 + 10 * i for i in range(8)],
            "P_RAIN_1_1_1": [0.0] * 8,
            "RH_1_1_1": [80.0 + i for i in range(8)],
        }
    )


# ---------------------------------------------------------------------------
# TestRegularizedGapRowsTimestampM35A
# ---------------------------------------------------------------------------


class TestRegularizedGapRowsTimestampM35A:
    def setup_method(self) -> None:
        _silent_warnings()

    def test_regularize_time_grid_fills_source_timestamp_for_inserted_rows(self):
        df = _frame_with_gap(site_id="RBRL", drop_idx=(3, 5))
        out = regularize_time_grid(df, datetime_col="DateTime", freq="30min")
        # Two rows were dropped from the 8-row grid → regularize inserts
        # them back.
        assert len(out) == 8
        assert len(df) == 6
        # Identity columns must be non-null on every row, including the
        # two inserted gap rows.
        assert out["DateTime"].isna().sum() == 0
        assert out["timestamp"].isna().sum() == 0
        # Inserted rows: their ``timestamp`` must equal the regularized
        # ``DateTime`` value, not NaT.
        original_dts = set(df["DateTime"])
        inserted_mask = ~out["DateTime"].isin(original_dts)
        assert int(inserted_mask.sum()) == 2
        inserted = out.loc[inserted_mask]
        assert (inserted["timestamp"] == inserted["DateTime"]).all()
        # Measurement columns remain NaN on inserted rows.
        assert inserted["co2_flux"].isna().all()
        assert inserted["u_star"].isna().all()
        # site_id is propagated from the uniform group context.
        assert inserted["site_id"].notna().all()
        assert (inserted["site_id"] == "RBRL").all()
        # Existing rows: timestamp == DateTime by construction (and
        # measurement columns are non-null on those original rows).
        existing = out.loc[~inserted_mask]
        assert (existing["timestamp"] == existing["DateTime"]).all()
        assert existing["co2_flux"].notna().all()

    def test_stage1_source_truth_payload_has_no_nat_timestamp_or_null_primary_key(self):
        flux = _gapped_flux_for_stage1(
            site_id="RBRL", drop_idx=(3, 5)
        )
        biomet = _biomet_for_stage1(site_id="RBRL")
        # Source has 6 flux rows (8 - 2 dropped); stage1's
        # regularize_time_grid step inserts 2 gap rows back. Pre-M35A
        # those inserted rows produced NaT timestamps; post-M35A they
        # carry the regularized grid value as `timestamp`.
        silver = load_stage1_from_dataframes(
            flux_df=flux,
            biomet_df=biomet,
            drop_rain_rows=False,
            site_id="RBRL",
        )
        # Regularized rows must still be present, not dropped.
        assert len(silver) == 8
        # Apply the M32A source-truth rename and build the silver
        # stage payload, exactly the path the silver BigQuery
        # writeback runs.
        silver_st = apply_silver_source_truth_rename(silver)
        payload, _actions = prepare_silver_stage_payload(
            silver_st,
            site_id="RBRL",
            source_flux_df=flux,
        )
        # M32A boundary: payload has ``timestamp`` and no final
        # ``DateTime``.
        assert "timestamp" in payload.columns
        assert "DateTime" not in payload.columns
        # M35A invariant: zero NaT timestamps + zero NULL primary keys
        # on every row, including the inserted regularization rows.
        assert int(payload["timestamp"].isna().sum()) == 0
        assert int(payload["primary_key"].isna().sum()) == 0
        # M9/M10 identity contract: (site_id, timestamp) unique across
        # the payload, including the regularization-inserted rows.
        assert payload.duplicated(subset=["site_id", "timestamp"]).sum() == 0
        # Source-truth aliases still hold from M32A.
        for final in (
            "co2_flux", "qc_co2_flux", "air_temperature_c", "u_star",
            "VPD_hpa", "SWIN_1_1_1", "P_RAIN_1_1_1", "RH_1_1_1",
        ):
            assert final in payload.columns, (final, payload.columns)
        # M34 representative wide pass-throughs survive.
        for c in (
            "h2o_flux", "qc_h2o_flux", "sonic_temperature",
            "air_pressure", "wind_speed", "TKE", "v_var", "RH",
        ):
            assert c in payload.columns, (c, payload.columns)
        # No internal-name passthroughs leak.
        for leak in (
            "DateTime", "NEE", "QC_NEE", "Tair", "USTAR",
            "VPD", "Rg", "P_RAIN", "rH",
        ):
            assert leak not in payload.columns, (leak, payload.columns)
        # Case-insensitive BigQuery field-key uniqueness.
        keys = [c.casefold() for c in payload.columns]
        assert len(set(keys)) == len(keys), payload.columns

    def test_grouped_regularization_keeps_per_site_timestamp_identity_unique(self):
        # Two sites with different gaps inside the same 8-slot grid.
        sites = ("RBMNN", "RBRL")
        gaps = {"RBMNN": (1, 4), "RBRL": (3, 5, 6)}
        per_site_payloads: list[pd.DataFrame] = []
        for site in sites:
            flux = _gapped_flux_for_stage1(site_id=site, drop_idx=gaps[site])
            biomet = _biomet_for_stage1(site_id=site)
            silver_group = stage1_from_raw_frames(
                flux,
                biomet,
                drop_rain_rows=False,
                site_id=None,
            )
            silver_group = apply_silver_source_truth_rename(silver_group)
            part, _actions = prepare_silver_stage_payload(
                silver_group,
                site_id=site,
                source_flux_df=flux,
            )
            per_site_payloads.append(part)
        stacked = pd.concat(per_site_payloads, ignore_index=True)
        # M35A invariant across sites: zero NaT timestamps + zero NULL
        # primary keys on the stacked payload, including all
        # regularization-inserted rows.
        assert int(stacked["timestamp"].isna().sum()) == 0
        assert int(stacked["primary_key"].isna().sum()) == 0
        # Per-site identity uniqueness: no duplicate (site_id,
        # timestamp) pairs across the stacked all-category payload.
        assert stacked.duplicated(subset=["site_id", "timestamp"]).sum() == 0
        # Primary keys are deterministic and include both site_id and
        # an ISO timestamp segment.
        for site in sites:
            site_rows = stacked.loc[stacked["site_id"] == site]
            assert (site_rows["primary_key"].str.startswith(f"{site}|")).all()
        # Per-site row counts: each site sees 8 - len(gaps) original
        # rows + len(gaps) inserted rows = 8.
        for site in sites:
            assert (stacked["site_id"] == site).sum() == 8

    def test_existing_frame_without_source_timestamp_keeps_legacy_behavior(self):
        # Legacy callers may pass a frame with only ``DateTime`` (no
        # separate source ``timestamp`` column). regularize_time_grid
        # must not synthesize a ``timestamp`` column that did not
        # exist in the input — the M35A fill is conditional on the
        # source column being present already.
        base = pd.date_range("2025-08-01", periods=6, freq="30min", tz="UTC")
        df = pd.DataFrame(
            {
                "DateTime": base,
                "site_id": ["RBRL"] * 6,
                "co2_flux": [0.1 + 0.05 * i for i in range(6)],
            }
        ).drop(index=[2, 4]).reset_index(drop=True)
        out = regularize_time_grid(df, datetime_col="DateTime", freq="30min")
        assert len(out) == 6  # 4 original + 2 inserted gap rows
        # DateTime column is non-null on every row.
        assert out["DateTime"].isna().sum() == 0
        # No `timestamp` column was synthesized.
        assert "timestamp" not in out.columns
        # `site_id` is propagated to inserted rows from the uniform
        # value (this is the only non-time-column fill the M35A
        # patch does; it is conditional on a single-uniform-value
        # `site_id` being present).
        assert out["site_id"].isna().sum() == 0
        # Measurement column stays NaN on inserted rows.
        inserted_mask = ~out["DateTime"].isin(df["DateTime"])
        assert int(inserted_mask.sum()) == 2
        assert out.loc[inserted_mask, "co2_flux"].isna().all()

    def test_inserted_timestamp_value_round_trips_to_iso_for_primary_key(self):
        # End-to-end: the inserted-row `timestamp` must round-trip
        # through `pd.to_datetime(..., utc=True).strftime(...)` (the
        # exact code path prepare_silver_stage_payload uses to build
        # `primary_key`) and produce a non-NULL ISO string. This
        # guards against tz-naive / NaT regressions in the fill
        # logic.
        flux = _gapped_flux_for_stage1(site_id="RBRL", drop_idx=(3,))
        biomet = _biomet_for_stage1(site_id="RBRL")
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
        # Every primary_key must be a non-null str matching
        # "<site_id>|<iso>" with a tz-aware suffix.
        assert payload["primary_key"].notna().all()
        for pk in payload["primary_key"]:
            assert isinstance(pk, str)
            assert pk.startswith("RBRL|")
            # tz-aware ISO suffix includes "+0000" (UTC offset).
            assert "+0000" in pk or pk.endswith("+00:00")
