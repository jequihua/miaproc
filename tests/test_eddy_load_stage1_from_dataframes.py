"""Tests for the in-memory stage-1 ingestion entrypoint (M7).

Covers ``miaproc.eddy.load_stage1_from_dataframes``: same scientific
contract as ``load_stage1`` but consumes pandas DataFrames directly so
the BigQuery-native eddy path does not have to round-trip through
synthetic CSV files.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from miaproc.eddy import load_stage1, load_stage1_from_dataframes


def _make_case_study_flux() -> pd.DataFrame:
    n = 6
    timestamps = pd.date_range("2025-08-01 00:00", periods=n, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": timestamps.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "site_id": ["RBRL"] * n,
            "co2_flux": [0.1, 0.2, -0.1, 0.3, 0.4, 0.0],
            "air_temperature": [293.15, 294.15, 295.15, 296.15, 297.15, 298.15],
            "u_star": [0.2, 0.3, 0.4, 0.5, 0.1, 0.25],
            "qc_co2_flux": [0, 0, 0, 0, 0, 0],
            "VPD": [500.0, 600.0, 700.0, 800.0, 900.0, 1000.0],
            "qc_H": [0, 0, 0, 0, 0, 0],
            "H": [10.0, 20.0, 30.0, 40.0, 50.0, 60.0],
            "qc_LE": [0, 0, 0, 0, 0, 0],
            "LE": [5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        }
    )


def _make_case_study_biomet() -> pd.DataFrame:
    n = 6
    timestamps = pd.date_range("2025-08-01 00:00", periods=n, freq="30min", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": timestamps.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "site_id": ["RBRL"] * n,
            "SWIN_1_1_1": [0.0, 50.0, 100.0, 200.0, 150.0, 75.0],
            "P_RAIN_1_1_1": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "RH_1_1_1": [80.0, 75.0, 70.0, 65.0, 70.0, 78.0],
        }
    )


class TestLoadStage1FromDataframes:
    def test_basic_pipeline_runs_and_produces_expected_columns(self):
        flux = _make_case_study_flux()
        biomet = _make_case_study_biomet()
        out = load_stage1_from_dataframes(
            flux_df=flux, biomet_df=biomet, site_id="RBRL", drop_rain_rows=False
        )
        for col in ("DateTime", "NEE", "USTAR", "Tair", "VPD", "Rg", "rH", "QC_NEE"):
            assert col in out.columns, f"missing column {col!r}"
        # Tair is converted to Celsius (K - 273.15).
        finite = out["Tair"].dropna()
        assert (finite < 100).all(), "Tair should be in °C after unit conversion"
        # USTAR should be numeric and present.
        assert out["USTAR"].notna().any()

    def test_input_frames_are_not_mutated(self):
        flux = _make_case_study_flux()
        biomet = _make_case_study_biomet()
        flux_before = flux.copy(deep=True)
        biomet_before = biomet.copy(deep=True)
        _ = load_stage1_from_dataframes(
            flux_df=flux, biomet_df=biomet, site_id="RBRL", drop_rain_rows=False
        )
        pd.testing.assert_frame_equal(flux, flux_before)
        pd.testing.assert_frame_equal(biomet, biomet_before)

    def test_multi_site_without_site_id_raises(self):
        flux = _make_case_study_flux()
        biomet = _make_case_study_biomet()
        flux2 = flux.copy()
        flux2["site_id"] = ["RBRL", "RBRL", "RBMNN", "RBMNN", "RBRL", "RBRL"]
        with pytest.raises(ValueError, match="multiple site IDs"):
            load_stage1_from_dataframes(
                flux_df=flux2, biomet_df=biomet, site_id=None, drop_rain_rows=False
            )

    def test_unknown_site_id_raises(self):
        flux = _make_case_study_flux()
        biomet = _make_case_study_biomet()
        with pytest.raises(ValueError, match="does not contain site_id"):
            load_stage1_from_dataframes(
                flux_df=flux, biomet_df=biomet, site_id="DOES_NOT_EXIST",
                drop_rain_rows=False,
            )

    def test_typeerror_on_non_dataframe_input(self):
        with pytest.raises(TypeError, match="flux_df must be a pandas DataFrame"):
            load_stage1_from_dataframes(
                flux_df=["not", "a", "df"],  # type: ignore[arg-type]
                biomet_df=_make_case_study_biomet(),
                site_id="RBRL",
            )
        with pytest.raises(TypeError, match="biomet_df must be a pandas DataFrame"):
            load_stage1_from_dataframes(
                flux_df=_make_case_study_flux(),
                biomet_df={"not": "a df"},  # type: ignore[arg-type]
                site_id="RBRL",
            )

    def test_dataframe_mode_matches_file_mode(self, tmp_path: Path):
        """Same scientific contract: writing the case-study frames to
        CSV and running ``load_stage1`` should produce the same output
        rows as feeding them directly to ``load_stage1_from_dataframes``."""
        flux = _make_case_study_flux()
        biomet = _make_case_study_biomet()

        flux_dir = tmp_path / "flux"
        biomet_dir = tmp_path / "biomet"
        flux_dir.mkdir()
        biomet_dir.mkdir()
        flux.to_csv(flux_dir / "flux.csv", index=False)
        biomet.to_csv(biomet_dir / "biomet.csv", index=False)

        out_file = load_stage1(
            path_full_output=str(flux_dir),
            path_biomet=str(biomet_dir),
            skip_full_output=0,
            skip_biomet=0,
            site_id="RBRL",
            drop_rain_rows=False,
        )
        out_df = load_stage1_from_dataframes(
            flux_df=flux, biomet_df=biomet, site_id="RBRL", drop_rain_rows=False
        )

        assert len(out_file) == len(out_df)
        common = sorted(set(out_file.columns) & set(out_df.columns))
        # Compare on the columns that survive both ingestion paths.
        pd.testing.assert_frame_equal(
            out_file[common].reset_index(drop=True),
            out_df[common].reset_index(drop=True),
            check_dtype=False,
        )
