from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from miaproc.eddy import (
    MissingColumnsError,
    STAGE2_OUTPUT_COLUMNS,
    load_stage1,
    prepare_reddyproc_input,
)


def _synthetic_stage1(n: int = 6) -> pd.DataFrame:
    """Minimal stage-1-like frame with all required columns."""
    start = pd.Timestamp("2025-08-27 16:30:00", tz="UTC")
    dt = pd.date_range(start, periods=n, freq="30min")
    return pd.DataFrame(
        {
            "DateTime": dt,
            "NEE": np.linspace(-5.0, 5.0, n),
            "USTAR": np.linspace(0.05, 0.3, n),
            "QC_NEE": [0, 0, 1, 2, 0, 1][:n],
            "Tair": np.linspace(20.0, 30.0, n),
            "VPD": np.linspace(5.0, 20.0, n),
            "Rg": [100.0, -3.0, 0.0, 250.0, -10.0, 500.0][:n],
            "rH": np.linspace(40.0, 80.0, n),
            "P_RAIN": [0.0] * n,   # extra stage-1 column: should be dropped
            "H": np.linspace(10.0, 20.0, n),   # another extra: dropped
        }
    )


class TestHappyPath:
    def test_returns_exact_stage2_columns_in_order(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        assert tuple(out.columns) == STAGE2_OUTPUT_COLUMNS

    def test_length_preserved_when_all_datetimes_parse(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        assert len(out) == len(df)

    def test_datetime_is_datetimelike(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        assert pd.api.types.is_datetime64_any_dtype(out["DateTime"])

    def test_numeric_columns_are_numeric(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        for col in ("NEE", "Ustar", "Tair", "VPD", "Rg", "rH", "QF"):
            assert pd.api.types.is_numeric_dtype(out[col]), col


class TestCalendarFields:
    def test_year_doy_hour_for_known_timestamps(self):
        # 2025-01-01 00:00 UTC -> Year 2025, DoY 1, Hour 0.0
        # 2025-03-15 14:30 UTC -> Year 2025, DoY 74, Hour 14.5
        # 2024-12-31 23:30 UTC -> Year 2024, DoY 366 (leap), Hour 23.5
        dt = pd.to_datetime(
            [
                "2025-01-01 00:00:00",
                "2025-03-15 14:30:00",
                "2024-12-31 23:30:00",
            ],
            utc=True,
        )
        df = pd.DataFrame(
            {
                "DateTime": dt,
                "NEE": [0.0, 0.0, 0.0],
                "USTAR": [0.1, 0.1, 0.1],
                "QC_NEE": [0, 0, 0],
                "Tair": [20.0, 20.0, 20.0],
                "VPD": [5.0, 5.0, 5.0],
                "Rg": [100.0, 100.0, 100.0],
                "rH": [50.0, 50.0, 50.0],
            }
        )
        out = prepare_reddyproc_input(df)
        assert list(out["Year"]) == [2025, 2025, 2024]
        assert list(out["DoY"]) == [1, 74, 366]
        assert list(out["Hour"]) == [0.0, 14.5, 23.5]

    def test_local_tz_shifts_calendar_fields_only(self):
        # Midnight UTC on 2025-01-01 -> still 2024-12-31 17:00 in
        # America/Mazatlan (UTC-7). Year/DoY/Hour should reflect the local
        # wall clock; DateTime itself must stay the UTC instant.
        dt = pd.to_datetime(["2025-01-01 00:00:00"], utc=True)
        df = pd.DataFrame(
            {
                "DateTime": dt,
                "NEE": [0.0],
                "USTAR": [0.1],
                "QC_NEE": [0],
                "Tair": [20.0],
                "VPD": [5.0],
                "Rg": [100.0],
                "rH": [50.0],
            }
        )
        out = prepare_reddyproc_input(df, local_tz="America/Mazatlan")
        assert out.loc[0, "Year"] == 2024
        assert out.loc[0, "DoY"] == 366
        assert out.loc[0, "Hour"] == 17.0
        # Original DateTime preserved (as the UTC instant).
        assert out.loc[0, "DateTime"] == pd.Timestamp("2025-01-01 00:00:00", tz="UTC")

    def test_tz_naive_input_with_local_tz_is_utc_localized_then_converted(self):
        # Documented fallback: tz-naive DateTime + local_tz => localize as UTC
        # then convert. 2025-06-15 12:00 (naive) + local_tz=America/Mazatlan
        # must become 2025-06-15 05:00 local (UTC-7; Mazatlan does not
        # observe DST).
        dt = pd.to_datetime(["2025-06-15 12:00:00"])  # naive
        df = pd.DataFrame(
            {
                "DateTime": dt,
                "NEE": [0.0],
                "USTAR": [0.1],
                "QC_NEE": [0],
                "Tair": [20.0],
                "VPD": [5.0],
                "Rg": [100.0],
                "rH": [50.0],
            }
        )
        out = prepare_reddyproc_input(df, local_tz="America/Mazatlan")
        assert out.loc[0, "Year"] == 2025
        # 2025-06-15 is DoY 166 (non-leap).
        assert out.loc[0, "DoY"] == 166
        assert out.loc[0, "Hour"] == 5.0
        # Returned DateTime keeps the original naive instant.
        assert out.loc[0, "DateTime"] == pd.Timestamp("2025-06-15 12:00:00")
        assert out["DateTime"].dt.tz is None

    def test_seconds_are_ignored_in_hour(self):
        # R's hour(...) + minute(...)/60 drops seconds. Same must hold in
        # the Python helper. 14:30:45 must give Hour == 14.5, not 14.5125.
        dt = pd.to_datetime(["2025-03-15 14:30:45"], utc=True)
        df = pd.DataFrame(
            {
                "DateTime": dt,
                "NEE": [0.0],
                "USTAR": [0.1],
                "QC_NEE": [0],
                "Tair": [20.0],
                "VPD": [5.0],
                "Rg": [100.0],
                "rH": [50.0],
            }
        )
        out = prepare_reddyproc_input(df)
        assert out.loc[0, "Hour"] == 14.5

    def test_dst_active_zone_uses_zoneinfo_rules(self):
        # America/New_York observes DST. 2025-07-01 04:00 UTC is 00:00 EDT
        # (UTC-4); 2025-01-01 04:00 UTC is 23:00 EST (UTC-5) on 2024-12-31.
        # The calendar-field derivation must honor zoneinfo rules, not a
        # fixed offset.
        dt = pd.to_datetime(
            ["2025-07-01 04:00:00", "2025-01-01 04:00:00"], utc=True
        )
        df = pd.DataFrame(
            {
                "DateTime": dt,
                "NEE": [0.0, 0.0],
                "USTAR": [0.1, 0.1],
                "QC_NEE": [0, 0],
                "Tair": [20.0, 20.0],
                "VPD": [5.0, 5.0],
                "Rg": [100.0, 100.0],
                "rH": [50.0, 50.0],
            }
        )
        out = prepare_reddyproc_input(df, local_tz="America/New_York")
        # Summer row: 00:00 on 2025-07-01 EDT => Year 2025, DoY 182,
        # Hour 0.0.
        assert out.loc[0, "Year"] == 2025
        assert out.loc[0, "DoY"] == 182
        assert out.loc[0, "Hour"] == 0.0
        # Winter row: 23:00 on 2024-12-31 EST => Year 2024, DoY 366,
        # Hour 23.0.
        assert out.loc[1, "Year"] == 2024
        assert out.loc[1, "DoY"] == 366
        assert out.loc[1, "Hour"] == 23.0


class TestRenames:
    def test_ustar_becomes_Ustar(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        assert "Ustar" in out.columns
        assert "USTAR" not in out.columns
        # Values transferred unchanged (after numeric coercion).
        assert np.allclose(out["Ustar"].to_numpy(), df["USTAR"].to_numpy())

    def test_qc_nee_becomes_QF(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        assert "QF" in out.columns
        assert "QC_NEE" not in out.columns
        assert list(out["QF"]) == list(df["QC_NEE"])


class TestRgClamp:
    def test_negative_rg_clamped_to_zero(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        # Synthetic has -3.0 and -10.0 in Rg; both must become 0.0; positives
        # must be unchanged.
        expected = [100.0, 0.0, 0.0, 250.0, 0.0, 500.0]
        assert list(out["Rg"]) == expected

    def test_zero_rg_preserved(self):
        df = _synthetic_stage1()
        out = prepare_reddyproc_input(df)
        assert out.loc[2, "Rg"] == 0.0  # was exactly 0.0 in the input


class TestMissingColumns:
    @pytest.mark.parametrize(
        "drop",
        ["DateTime", "NEE", "USTAR", "QC_NEE", "Tair", "VPD", "Rg", "rH"],
    )
    def test_missing_single_column_raises(self, drop):
        df = _synthetic_stage1().drop(columns=[drop])
        with pytest.raises(MissingColumnsError) as excinfo:
            prepare_reddyproc_input(df)
        assert drop in excinfo.value.missing
        assert drop in str(excinfo.value)

    def test_missing_multiple_columns_all_reported(self):
        df = _synthetic_stage1().drop(columns=["VPD", "rH"])
        with pytest.raises(MissingColumnsError) as excinfo:
            prepare_reddyproc_input(df)
        assert set(excinfo.value.missing) == {"VPD", "rH"}


class TestImmutability:
    def test_input_dataframe_not_mutated(self):
        df = _synthetic_stage1()
        before_cols = list(df.columns)
        before_rg = df["Rg"].copy()
        before_len = len(df)
        _ = prepare_reddyproc_input(df)
        assert list(df.columns) == before_cols
        assert list(df["Rg"]) == list(before_rg)   # negative Rg preserved on input
        assert len(df) == before_len
        # Original still has USTAR/QC_NEE (not renamed).
        assert "USTAR" in df.columns
        assert "QC_NEE" in df.columns


class TestUnparseableDateTime:
    def test_rows_with_unparseable_datetime_are_dropped(self):
        df = _synthetic_stage1()
        df = df.copy()
        df["DateTime"] = df["DateTime"].astype(object)
        df.loc[0, "DateTime"] = "not-a-date"
        df.loc[3, "DateTime"] = None
        out = prepare_reddyproc_input(df)
        # Two invalid rows dropped; four remain.
        assert len(out) == len(df) - 2
        # Surviving rows correspond to the valid input timestamps.
        assert out["DateTime"].notna().all()


class TestIntegrationWithStage1:
    def test_load_stage1_output_is_acceptable_input(self):
        base = Path(__file__).parent / "data"
        df = load_stage1(
            path_full_output=base / "full_output",
            path_biomet=base / "biomet",
            tz_in="UTC",
            tz_out="UTC",
            skip_full_output=0,
            skip_biomet=0,
            drop_rain_rows=True,
        )
        out = prepare_reddyproc_input(df)
        assert tuple(out.columns) == STAGE2_OUTPUT_COLUMNS
        assert len(out) > 0
        assert (out["Rg"].dropna() >= 0).all()   # clamp holds end-to-end
        # Year/DoY/Hour derived.
        assert out["Year"].notna().all()
        assert out["DoY"].between(1, 366).all()
        assert out["Hour"].between(0.0, 24.0).all()
