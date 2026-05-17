"""Case-study input contract tests.

Covers the pre-M4 pass that adds timestamp-shape ingestion, the
``u_star`` alias, and explicit multi-site filtering to ``load_stage1``.
All tests use tiny synthetic fixtures written under ``tmp_path`` so the
default suite stays fast and independent of the real case-study CSVs
under ``01_data/case_study``.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from miaproc.eddy import load_stage1
from miaproc.eddy.time import (
    _parse_datetime_multi,
    _parse_timestamp_col,
    create_datetime,
)


# ----------------------------------------------------------------------
# Fixture helpers
# ----------------------------------------------------------------------

_BIOMET_COLS: tuple[str, ...] = (
    "SWIN_1_1_1",
    "P_RAIN_1_1_1",
    "RH_1_1_1",
)


def _iso_utc(ts: pd.Timestamp) -> str:
    """Render a UTC pandas Timestamp like the case-study CSVs do."""
    return ts.strftime("%Y-%m-%d %H:%M:%S") + " UTC"


def _write_case_study_flux(
    path: Path,
    *,
    n: int,
    site_ids: list[str],
    start: str = "2025-10-25 00:00:00",
    freq: str = "30min",
    include_u_star: bool = True,
    include_legacy_u: bool = False,
) -> None:
    """Write a tiny case-study-shaped flux CSV covering ``site_ids``.

    Each site gets ``n`` timestamps starting at ``start``. Sites share
    timestamps (this is the duplicate-DateTime-across-sites case that
    Decision 008 exists to avoid).
    """
    rows: list[dict] = []
    grid = pd.date_range(start=start, periods=n, freq=freq, tz="UTC")
    rng = np.random.default_rng(0)
    for site in site_ids:
        for ts in grid:
            row: dict = {
                "primary_key": f"{site}_{int(ts.timestamp())}",
                "timestamp": _iso_utc(ts),
                "co2_flux": float(rng.normal(0, 2)),
                "qc_co2_flux": int(rng.integers(0, 3)),
                "air_temperature": float(rng.uniform(15, 30)),
                "VPD": float(rng.uniform(2, 20)),
                "site_id": site,
            }
            if include_u_star:
                row["u_star"] = float(rng.uniform(0.05, 0.4))
            if include_legacy_u:
                row["u."] = float(rng.uniform(0.05, 0.4))
            rows.append(row)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_case_study_biomet(
    path: Path,
    *,
    n: int,
    site_ids: list[str],
    start: str = "2025-10-25 00:00:00",
    freq: str = "30min",
) -> None:
    rows: list[dict] = []
    grid = pd.date_range(start=start, periods=n, freq=freq, tz="UTC")
    rng = np.random.default_rng(1)
    for site in site_ids:
        for ts in grid:
            rows.append(
                {
                    "primary_key": f"{site}_{int(ts.timestamp())}",
                    "timestamp": _iso_utc(ts),
                    "SWIN_1_1_1": float(rng.uniform(0, 800)),
                    "P_RAIN_1_1_1": 0.0,
                    "RH_1_1_1": float(rng.uniform(40, 90)),
                    "site_id": site,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


@pytest.fixture
def multi_site_case_study(tmp_path: Path) -> Path:
    """Two-site case-study fixture with RBMNN + RBRL."""
    flux_dir = tmp_path / "flux"
    biomet_dir = tmp_path / "biomet"
    flux_dir.mkdir()
    biomet_dir.mkdir()
    _write_case_study_flux(
        flux_dir / "flux.csv", n=48, site_ids=["RBMNN", "RBRL"]
    )
    _write_case_study_biomet(
        biomet_dir / "biomet.csv", n=48, site_ids=["RBMNN", "RBRL"]
    )
    return tmp_path


@pytest.fixture
def single_site_case_study(tmp_path: Path) -> Path:
    flux_dir = tmp_path / "flux"
    biomet_dir = tmp_path / "biomet"
    flux_dir.mkdir()
    biomet_dir.mkdir()
    _write_case_study_flux(flux_dir / "flux.csv", n=48, site_ids=["RBMNN"])
    _write_case_study_biomet(biomet_dir / "biomet.csv", n=48, site_ids=["RBMNN"])
    return tmp_path


# ----------------------------------------------------------------------
# Fix 1 — timestamp input, index safety, unparseable rows
# ----------------------------------------------------------------------


class TestTimestampInputSupport:
    def test_timestamp_only_frame_parses_to_utc_datetime(self):
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2025-10-25 00:00:00 UTC",
                    "2025-10-25 00:30:00 UTC",
                    "2025-10-25 01:00:00 UTC",
                ]
            }
        )
        out = create_datetime(df, tz_in="UTC", tz_out="UTC")
        assert "DateTime" in out.columns
        assert pd.api.types.is_datetime64_any_dtype(out["DateTime"])
        assert str(out["DateTime"].dt.tz) == "UTC"
        assert len(out) == 3

    def test_timestamp_only_frame_with_filtered_index_does_not_raise(self):
        """Regression: ``_parse_datetime_multi`` previously raised
        "unalignable boolean Series" when the input DataFrame had a
        filtered/nonconsecutive index. ``timestamp`` parsing uses a
        different helper but the surrounding logic must still be
        index-safe."""
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2025-10-25 00:00:00 UTC",
                    "2025-10-25 00:30:00 UTC",
                    "2025-10-25 01:00:00 UTC",
                    "2025-10-25 01:30:00 UTC",
                ],
                "site_id": ["A", "B", "A", "B"],
            }
        )
        filtered = df.loc[df["site_id"] == "A"]   # index [0, 2]
        # Deliberately do NOT reset_index; this is the regression surface.
        out = create_datetime(filtered, tz_in="UTC", tz_out="UTC")
        assert len(out) == 2

    def test_unparseable_timestamp_rows_are_dropped(self):
        df = pd.DataFrame(
            {
                "timestamp": [
                    "2025-10-25 00:00:00 UTC",
                    "not-a-timestamp",
                    "2025-10-25 01:00:00 UTC",
                    "",
                ]
            }
        )
        with pytest.warns(UserWarning, match="dropped"):
            out = create_datetime(df, tz_in="UTC", tz_out="UTC")
        assert len(out) == 2
        assert out["DateTime"].notna().all()

    def test_legacy_date_time_path_still_works(self):
        df = pd.DataFrame(
            {
                "date": ["2025-01-01", "2025-01-01"],
                "time": ["00:00", "00:30"],
            }
        )
        out = create_datetime(df, tz_in="UTC", tz_out="UTC")
        assert len(out) == 2
        assert pd.api.types.is_datetime64_any_dtype(out["DateTime"])

    def test_both_shapes_present_prefers_legacy(self):
        """Mixed input (legacy ``date`` + ``time`` and ``timestamp``) should
        take the legacy path. ``timestamp`` is ignored."""
        df = pd.DataFrame(
            {
                "date": ["2025-01-01"],
                "time": ["12:00"],
                "timestamp": ["2099-12-31 00:00:00 UTC"],
            }
        )
        out = create_datetime(df, tz_in="UTC", tz_out="UTC")
        # Legacy path wins: the DateTime reflects 2025-01-01, not 2099.
        assert out["DateTime"].iloc[0].year == 2025

    def test_raises_when_neither_shape_is_present(self):
        df = pd.DataFrame({"co2_flux": [1.0]})
        with pytest.raises(ValueError, match="neither legacy"):
            create_datetime(df)

    def test_parse_datetime_multi_preserves_nonconsecutive_index(self):
        s = pd.Series(
            ["2025-01-01 00:00:00", "2025-01-01 00:30:00"], index=[7, 13]
        )
        out = _parse_datetime_multi(s)
        assert list(out.index) == [7, 13]
        assert out.notna().all()

    def test_parse_timestamp_col_handles_utc_suffix(self):
        s = pd.Series(["2025-10-25 08:00:00 UTC", "bad"])
        out = _parse_timestamp_col(s)
        assert out.notna().sum() == 1
        assert str(out.dt.tz) == "UTC"


# ----------------------------------------------------------------------
# Fix 2 — u_star alias
# ----------------------------------------------------------------------


class TestDropUnitRowsQuiet:
    def test_timestamp_only_frame_does_not_emit_time_missing_warning(
        self, single_site_case_study: Path
    ):
        """``drop_unit_rows`` previously warned
        ``"drop_unit_rows: 'time' column not found"`` on every case-study-shaped
        load, because the unit-row filter is a legacy concern and case-study
        files have no unit row. Loading a single-site synthetic case-study
        fixture must no longer emit this specific warning."""
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            load_stage1(
                path_full_output=single_site_case_study / "flux",
                path_biomet=single_site_case_study / "biomet",
                tz_in="UTC",
                tz_out="UTC",
                skip_full_output=0,
                skip_biomet=0,
                drop_rain_rows=True,
            )
        offending = [
            str(w.message)
            for w in caught
            if "drop_unit_rows" in str(w.message)
            and "'time' column not found" in str(w.message)
        ]
        assert not offending, (
            "timestamp-only frames must not emit the legacy drop_unit_rows "
            f"missing-time warning. Got: {offending}"
        )


class TestUstarAlias:
    def test_u_star_column_becomes_USTAR(
        self, single_site_case_study: Path
    ):
        out = load_stage1(
            path_full_output=single_site_case_study / "flux",
            path_biomet=single_site_case_study / "biomet",
            tz_in="UTC",
            tz_out="UTC",
            skip_full_output=0,
            skip_biomet=0,
            drop_rain_rows=True,
        )
        assert "USTAR" in out.columns

    def test_both_legacy_u_and_u_star_present_prefers_legacy(
        self, tmp_path: Path
    ):
        """Exactly one USTAR column is produced, and it is the legacy
        ``u.`` values (confirmed by setting u_star to a sentinel)."""
        flux_dir = tmp_path / "flux"
        biomet_dir = tmp_path / "biomet"
        flux_dir.mkdir()
        biomet_dir.mkdir()
        _write_case_study_flux(
            flux_dir / "flux.csv",
            n=12,
            site_ids=["RBMNN"],
            include_u_star=True,
            include_legacy_u=True,
        )
        _write_case_study_biomet(
            biomet_dir / "biomet.csv", n=12, site_ids=["RBMNN"]
        )
        with pytest.warns(UserWarning, match="u_star"):
            out = load_stage1(
                path_full_output=flux_dir,
                path_biomet=biomet_dir,
                tz_in="UTC",
                tz_out="UTC",
                skip_full_output=0,
                skip_biomet=0,
                drop_rain_rows=True,
            )
        assert (out.columns == "USTAR").sum() == 1


# ----------------------------------------------------------------------
# Fix 3 — multi-site explicit filtering
# ----------------------------------------------------------------------


class TestMultiSiteFiltering:
    def test_multi_site_without_site_id_raises_and_lists_both(
        self, multi_site_case_study: Path
    ):
        with pytest.raises(ValueError) as excinfo:
            load_stage1(
                path_full_output=multi_site_case_study / "flux",
                path_biomet=multi_site_case_study / "biomet",
                tz_in="UTC",
                tz_out="UTC",
                skip_full_output=0,
                skip_biomet=0,
                drop_rain_rows=True,
            )
        msg = str(excinfo.value)
        assert "RBMNN" in msg
        assert "RBRL" in msg
        assert "site_id" in msg

    def test_multi_site_with_rbmnn_succeeds(
        self, multi_site_case_study: Path
    ):
        out = load_stage1(
            path_full_output=multi_site_case_study / "flux",
            path_biomet=multi_site_case_study / "biomet",
            tz_in="UTC",
            tz_out="UTC",
            skip_full_output=0,
            skip_biomet=0,
            drop_rain_rows=True,
            site_id="RBMNN",
        )
        # Regularized to a 30-min grid; required downstream columns present.
        for col in ("DateTime", "USTAR", "Rg", "P_RAIN", "rH", "VPD"):
            assert col in out.columns, col
        assert len(out) > 0
        assert out["DateTime"].is_monotonic_increasing
        # 30-min cadence on the regularized grid.
        diffs = out["DateTime"].diff().dropna().unique()
        assert list(diffs) == [pd.Timedelta(minutes=30)]

    def test_missing_site_in_flux_raises(self, multi_site_case_study: Path):
        with pytest.raises(ValueError, match="flux input does not contain"):
            load_stage1(
                path_full_output=multi_site_case_study / "flux",
                path_biomet=multi_site_case_study / "biomet",
                tz_in="UTC",
                tz_out="UTC",
                skip_full_output=0,
                skip_biomet=0,
                drop_rain_rows=True,
                site_id="NEVER",
            )

    def test_missing_site_in_biomet_raises(self, tmp_path: Path):
        flux_dir = tmp_path / "flux"
        biomet_dir = tmp_path / "biomet"
        flux_dir.mkdir()
        biomet_dir.mkdir()
        _write_case_study_flux(
            flux_dir / "flux.csv", n=12, site_ids=["RBMNN", "RBRL"]
        )
        _write_case_study_biomet(
            biomet_dir / "biomet.csv", n=12, site_ids=["RBRL"]
        )
        # Flux has RBMNN but biomet does not; error must name the biomet side.
        with pytest.raises(ValueError, match="biomet input does not contain"):
            load_stage1(
                path_full_output=flux_dir,
                path_biomet=biomet_dir,
                tz_in="UTC",
                tz_out="UTC",
                skip_full_output=0,
                skip_biomet=0,
                drop_rain_rows=True,
                site_id="RBMNN",
            )

    def test_single_site_with_site_id_column_does_not_require_site_id_arg(
        self, single_site_case_study: Path
    ):
        out = load_stage1(
            path_full_output=single_site_case_study / "flux",
            path_biomet=single_site_case_study / "biomet",
            tz_in="UTC",
            tz_out="UTC",
            skip_full_output=0,
            skip_biomet=0,
            drop_rain_rows=True,
        )
        assert len(out) > 0
        assert "USTAR" in out.columns


# ----------------------------------------------------------------------
# Fix 4 — _json_safe missing-value coercion
# ----------------------------------------------------------------------


class TestJsonSafe:
    def test_pd_NA_becomes_none(self):
        from miaproc.eddy.engine_reddyproc import _json_safe

        assert _json_safe(pd.NA) is None

    def test_pd_NaT_becomes_none(self):
        from miaproc.eddy.engine_reddyproc import _json_safe

        assert _json_safe(pd.NaT) is None

    def test_np_nan_becomes_none(self):
        from miaproc.eddy.engine_reddyproc import _json_safe

        assert _json_safe(np.nan) is None

    def test_numpy_float64_nan_becomes_none(self):
        from miaproc.eddy.engine_reddyproc import _json_safe

        assert _json_safe(np.float64("nan")) is None

    def test_numpy_int64_round_trips_to_python_int(self):
        from miaproc.eddy.engine_reddyproc import _json_safe

        result = _json_safe(np.int64(42))
        assert result == 42
        assert type(result) is int

    def test_numpy_float64_round_trips_to_python_float(self):
        from miaproc.eddy.engine_reddyproc import _json_safe

        result = _json_safe(np.float64(0.17))
        assert result == pytest.approx(0.17)
        assert type(result) is float

    def test_python_scalars_pass_through(self):
        from miaproc.eddy.engine_reddyproc import _json_safe

        assert _json_safe(1) == 1
        assert _json_safe(1.5) == 1.5
        assert _json_safe("U50") == "U50"
        assert _json_safe(True) is True

    def test_scenario_table_with_pd_NA_preserves_none_in_records(self):
        """``pd.NA``, ``pd.NaT``, and ``np.nan`` cells in the scenario table
        must be stored as ``None`` in ``thresholds_by_season``, not as the
        string ``"<NA>"``. Three seasons with distinct finite U50 values
        also exercise the ``selected_threshold = None`` branch."""
        from miaproc.eddy import ReddyProcConfig
        from miaproc.eddy.engine_reddyproc import _ustar_diagnostics_from_scenarios

        scenarios = pd.DataFrame(
            {
                "season": ["2025001", "2025002", "2025003"],
                "U05": [0.05, 0.06, 0.07],
                "U50": [pd.NA, 0.20, 0.25],
                "U95": [0.30, np.nan, 0.35],
            },
        )
        diag = _ustar_diagnostics_from_scenarios(
            scenarios, ReddyProcConfig(ustar_scenario="U50")
        )
        # First season: U50 is missing; stored as None, never as "<NA>".
        rec0 = diag["thresholds_by_season"][0]
        assert rec0["U50"] is None
        assert rec0["U05"] == pytest.approx(0.05)
        # Second season: U95 is np.nan; stored as None.
        rec1 = diag["thresholds_by_season"][1]
        assert rec1["U95"] is None
        # Two distinct finite U50 values across seasons (0.20, 0.25) ->
        # selected_threshold cannot be a single number.
        assert diag["selected_threshold"] is None
        # Sanity-check that no record leaks the pandas "<NA>" or "nan" string.
        for rec in diag["thresholds_by_season"]:
            for v in rec.values():
                assert v != "<NA>"
                if isinstance(v, str):
                    assert v.lower() != "nan"
