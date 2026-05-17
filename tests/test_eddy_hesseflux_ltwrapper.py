"""Default (Python-only) tests for the optional Lloyd-Taylor alignment
wrapper in the hesseflux backend (Coding Prompt 022 / H2 corrective).

Covers:

- unit-level fit + predict determinism and parameter recovery;
- default-path regression when ``reco_fit_mode="native"``;
- wrapper end-to-end on a synthetic Lloyd-Taylor-shaped fixture;
- wrapper failure path — sparse nighttime, no silent fallback, and
  unknown ``reco_fit_mode`` raises;
- 13-column contract preserved under wrapper mode.

No network, no rpy2, no R required.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from miaproc.eddy import (
    HESSEFLUX_COMMON_OUTPUT_COLUMNS,
    HessefluxConfig,
    LTFitResult,
    LTWrapperError,
    fit_lloyd_taylor,
    postproc,
    predict_reco,
)
from miaproc.eddy.lt_reco_wrapper import LT_T0_C, LT_TREF_C


def _lt_synthetic_stage1(
    days: int = 100,
    *,
    rref_true: float = 2.5,
    e0_true: float = 200.0,
    seed: int = 0,
) -> pd.DataFrame:
    """Stage-1-shaped frame with a genuine Lloyd-Taylor nighttime signal.

    - Temperature diurnal cycle centered around 22 °C (± 6 °C).
    - Nighttime NEE = LT(T; Rref, E0) + small noise.
    - Daytime NEE = GPP-like sink (−α·Rg + small Tair term).
    - u*, Rg, VPD, rH, QC all fixture-typical; mostly-finite data so
      hesseflux gap-fill has enough substrate.
    """
    rng = np.random.default_rng(seed)
    n = days * 48
    dt = pd.date_range("2025-06-01 00:00", periods=n, freq="30min", tz="UTC")
    hour = dt.hour + dt.minute / 60.0
    rg = np.clip(600 * np.sin(np.pi * (hour - 6) / 12), 0, None)
    tair = 22 + 6 * np.sin(np.pi * (hour - 6) / 12) + rng.normal(0, 0.4, n)

    is_night = rg <= 10.0
    denom = tair - LT_T0_C
    reco_lt = rref_true * np.exp(
        e0_true * (1.0 / (LT_TREF_C - LT_T0_C) - 1.0 / denom)
    )
    nee_night = reco_lt + rng.normal(0, 0.15, n)
    nee_day = -0.020 * rg + 0.05 * (tair - 22) + rng.normal(0, 0.25, n)
    nee = np.where(is_night, nee_night, nee_day)

    return pd.DataFrame(
        {
            "DateTime": dt,
            "NEE": nee,
            "USTAR": rng.uniform(0.10, 0.45, n),
            "QC_NEE": rng.integers(0, 2, n),
            "Tair": tair,
            "VPD": 5 + 10 * np.clip(np.sin(np.pi * (hour - 6) / 12), 0, None),
            "Rg": rg,
            "rH": rng.uniform(40, 90, n),
            "P_RAIN": np.zeros(n),
        }
    )


# ----------------------------------------------------------------------
# Unit-level: fit_lloyd_taylor / predict_reco
# ----------------------------------------------------------------------


class TestFitLloydTaylor:
    def test_recovers_synthetic_parameters(self):
        """Fit must recover near-true Rref/E0 on a clean LT signal."""
        rng = np.random.default_rng(0)
        tair = rng.uniform(16.0, 28.0, 2000)
        denom = tair - LT_T0_C
        rref_true, e0_true = 2.5, 200.0
        reco_true = rref_true * np.exp(
            e0_true * (1.0 / (LT_TREF_C - LT_T0_C) - 1.0 / denom)
        )
        nee_obs = reco_true + rng.normal(0, 0.10, 2000)
        result = fit_lloyd_taylor(nee_obs, tair, min_night_samples=100)
        assert isinstance(result, LTFitResult)
        assert result.fit_status == "ok"
        assert result.n_night_samples == 2000
        assert abs(result.rref - rref_true) < 0.5, result.rref
        assert abs(result.e0 - e0_true) < 60.0, result.e0
        assert result.night_fit_rmse < 0.5

    def test_fit_is_deterministic(self):
        rng = np.random.default_rng(42)
        tair = rng.uniform(18.0, 30.0, 800)
        nee = 2.0 * np.exp(
            200.0 * (1.0 / (LT_TREF_C - LT_T0_C) - 1.0 / (tair - LT_T0_C))
        ) + rng.normal(0, 0.05, 800)
        r1 = fit_lloyd_taylor(nee, tair, min_night_samples=100)
        r2 = fit_lloyd_taylor(nee, tair, min_night_samples=100)
        assert r1.rref == r2.rref
        assert r1.e0 == r2.e0

    def test_fit_raises_on_insufficient_samples(self):
        rng = np.random.default_rng(0)
        tair = rng.uniform(20.0, 25.0, 50)
        nee = rng.uniform(1.0, 3.0, 50)
        with pytest.raises(LTWrapperError, match="insufficient"):
            fit_lloyd_taylor(nee, tair, min_night_samples=500)

    def test_fit_raises_on_domain_violation(self):
        tair = np.concatenate(
            [np.full(300, LT_T0_C - 10.0), np.full(300, 22.0)]
        )
        nee = np.full(600, 2.0)
        with pytest.raises(LTWrapperError, match="domain"):
            fit_lloyd_taylor(nee, tair, min_night_samples=100)

    def test_fit_raises_on_shape_mismatch(self):
        with pytest.raises(LTWrapperError, match="shape mismatch"):
            fit_lloyd_taylor(np.zeros(100), np.zeros(90), min_night_samples=10)


class TestPredictReco:
    def test_predict_is_finite_and_positive_on_valid_domain(self):
        rng = np.random.default_rng(0)
        tair_fit = rng.uniform(16.0, 28.0, 1200)
        nee_fit = 2.5 * np.exp(
            200.0
            * (1.0 / (LT_TREF_C - LT_T0_C) - 1.0 / (tair_fit - LT_T0_C))
        ) + rng.normal(0, 0.10, 1200)
        res = fit_lloyd_taylor(nee_fit, tair_fit, min_night_samples=100)
        tair_pred = np.linspace(0.0, 35.0, 400)
        pred = predict_reco(tair_pred, res.rref, res.e0)
        assert np.isfinite(pred).all()
        assert (pred > 0).all()

    def test_predict_returns_nan_outside_domain(self):
        tair = np.array([LT_T0_C - 10.0, 22.0, np.nan, 25.0])
        pred = predict_reco(tair, 2.0, 200.0)
        assert np.isnan(pred[0])
        assert np.isfinite(pred[1])
        assert np.isnan(pred[2])
        assert np.isfinite(pred[3])


# ----------------------------------------------------------------------
# End-to-end via postproc
# ----------------------------------------------------------------------


class TestNativeDefaultRegression:
    """Default path must be unchanged by the wrapper addition."""

    def test_default_reco_fit_mode_is_native(self):
        assert HessefluxConfig().reco_fit_mode == "native"

    def test_native_diagnostics_report_mode_native(self):
        df = _lt_synthetic_stage1(days=45)
        cfg = HessefluxConfig(
            ustar_mode="dynamic",
            ustar_min_night_samples=200,
            partition_method="lasslop",
        )
        out = postproc(df, engine="hesseflux", hesseflux_config=cfg)
        diag = out.attrs["miaproc_diagnostics"]
        part = diag["partitioning"]
        assert part["method"] == "lasslop"
        assert part["reco_fit_mode"] == "native"
        assert part["lt_wrapper"] is None


class TestWrapperHappyPath:
    def test_wrapper_runs_end_to_end_and_recovers_parameters(self):
        df = _lt_synthetic_stage1(days=100, rref_true=2.5, e0_true=200.0)
        cfg = HessefluxConfig(
            ustar_mode="dynamic",
            ustar_min_night_samples=200,
            partition_method="lasslop",
            reco_fit_mode="lt_reddyproc_aligned",
        )
        out = postproc(df, engine="hesseflux", hesseflux_config=cfg)

        for col in HESSEFLUX_COMMON_OUTPUT_COLUMNS:
            assert col in out.columns, f"missing contract column {col}"

        reco = pd.to_numeric(out["Reco"], errors="coerce")
        gpp = pd.to_numeric(out["GPP"], errors="coerce")
        assert reco.notna().sum() > 100
        assert gpp.notna().sum() > 100
        # Reco must be positive everywhere it is finite (ecosystem
        # respiration is a positive outgoing flux).
        assert (reco[reco.notna()] > 0).all()

        diag = out.attrs["miaproc_diagnostics"]
        part = diag["partitioning"]
        assert part["method"] == "lasslop"
        assert part["reco_fit_mode"] == "lt_reddyproc_aligned"
        lt = part["lt_wrapper"]
        assert lt is not None
        assert lt["fit_status"] == "ok"
        assert isinstance(lt["n_night_samples"], int)
        assert lt["n_night_samples"] >= 200
        assert 0.0 < lt["rref"] < 50.0
        assert 0.0 < lt["e0"] < 500.0
        # Loose parameter recovery on the synthetic truth.
        assert abs(lt["rref"] - 2.5) < 1.5
        assert abs(lt["e0"] - 200.0) < 120.0


class TestWrapperFailurePath:
    def test_sparse_nighttime_raises_without_silent_fallback(self):
        df = _lt_synthetic_stage1(days=7)
        cfg = HessefluxConfig(
            ustar_mode="fixed",
            ustar_fixed=0.1,
            reco_fit_mode="lt_reddyproc_aligned",
            lt_min_night_samples=5000,  # impossible on 7 days
        )
        with pytest.raises(LTWrapperError):
            postproc(df, engine="hesseflux", hesseflux_config=cfg)

    def test_unknown_reco_fit_mode_raises(self):
        df = _lt_synthetic_stage1(days=20)
        cfg = HessefluxConfig(
            ustar_mode="fixed",
            ustar_fixed=0.1,
            reco_fit_mode="bogus",  # type: ignore[arg-type]
        )
        with pytest.raises(ValueError, match="reco_fit_mode"):
            postproc(df, engine="hesseflux", hesseflux_config=cfg)


class TestContractInvariance:
    def test_wrapper_output_preserves_13_column_contract(self):
        df = _lt_synthetic_stage1(days=60)
        cfg = HessefluxConfig(
            ustar_mode="dynamic",
            ustar_min_night_samples=200,
            partition_method="lasslop",
            reco_fit_mode="lt_reddyproc_aligned",
        )
        out = postproc(df, engine="hesseflux", hesseflux_config=cfg)
        for col in HESSEFLUX_COMMON_OUTPUT_COLUMNS:
            assert col in out.columns, f"contract column {col} missing"
