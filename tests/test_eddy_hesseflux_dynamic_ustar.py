"""Default (Python-only) M4 tests for the dynamic u* hesseflux backend.

These tests do not require R, REddyProc, or rpy2 and must not read the real
case-study files. They cover:

- fixed mode remains default and behavior-preserving;
- ``ustar_mode="fixed"`` uses ``ustar_fixed``; ``dynamic`` does not;
- invalid ``ustar_mode`` raises a clear error;
- the estimator returns ``U05``/``U50``/``U95`` on a synthetic
  plateau-shaped dataset;
- sparse nighttime data raises ``DynamicUstarEstimationError``;
- the dynamic hesseflux output includes the 13 common backend columns plus
  legacy aliases;
- diagnostics are attached and complete.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from miaproc.eddy import (
    DynamicUstarEstimationError,
    DynamicUstarResult,
    HESSEFLUX_COMMON_OUTPUT_COLUMNS,
    HessefluxConfig,
    estimate_dynamic_ustar_thresholds,
    load_stage1,
    postproc,
)


# ----------------------------------------------------------------------
# Synthetic data builders
# ----------------------------------------------------------------------


def _synthetic_plateau_stage1(
    days: int = 30,
    *,
    true_threshold: float = 0.17,
    seed: int = 0,
) -> pd.DataFrame:
    """Build a stage-1-like frame with a clear nighttime NEE plateau above
    ``true_threshold``.

    Daytime (Rg>0) flux is dominated by a negative GPP-like sinusoid; the
    dynamic estimator ignores those rows. Nighttime flux is constructed so
    that below ``true_threshold`` the mean NEE is attenuated (simulating
    under-developed turbulence releasing less respiration) and at/above the
    threshold it reaches a plateau.
    """
    rng = np.random.default_rng(seed)
    n = days * 48
    dt = pd.date_range("2025-08-01 00:00", periods=n, freq="30min", tz="UTC")
    hour = dt.hour + dt.minute / 60.0
    rg = np.clip(800 * np.sin(np.pi * (hour - 6) / 12), 0, None)
    tair = 20 + 8 * np.sin(np.pi * (hour - 6) / 12) + rng.normal(0, 1.0, n)
    ustar = rng.uniform(0.02, 0.5, n)

    # Plateau Reco at night: full magnitude above the threshold, linearly
    # attenuated below. Daytime: a strongly negative GPP-like signal.
    is_night = rg <= 10.0
    plateau_reco = 3.5   # umol m-2 s-1
    ratio = np.clip(ustar / true_threshold, 0.0, 1.0)
    nee_night = plateau_reco * ratio + rng.normal(0, 0.2, n)
    nee_day = -0.02 * rg + 0.1 * (tair - 20) + rng.normal(0, 0.3, n)
    nee = np.where(is_night, nee_night, nee_day)

    return pd.DataFrame(
        {
            "DateTime": dt,
            "NEE": nee,
            "USTAR": ustar,
            "QC_NEE": rng.integers(0, 3, n),
            "Tair": tair,
            "VPD": 5 + 10 * np.clip(np.sin(np.pi * (hour - 6) / 12), 0, None),
            "Rg": rg,
            "rH": rng.uniform(40, 90, n),
            "P_RAIN": np.zeros(n),
        }
    )


# ----------------------------------------------------------------------
# Estimator: helper-level tests (no hesseflux)
# ----------------------------------------------------------------------


class TestDynamicEstimatorHelper:
    def test_returns_all_three_scenarios(self):
        df = _synthetic_plateau_stage1(days=30)
        result = estimate_dynamic_ustar_thresholds(
            df,
            ustar_probs=(0.05, 0.5, 0.95),
            ustar_scenario="U50",
            ustar_min_night_samples=100,
        )
        assert isinstance(result, DynamicUstarResult)
        assert set(result.available_scenarios) == {"U05", "U50", "U95"}
        assert result.selected_scenario == "U50"
        # Thresholds strictly increase across quantile scenarios.
        u05 = result.thresholds_by_scenario["U05"]
        u50 = result.thresholds_by_scenario["U50"]
        u95 = result.thresholds_by_scenario["U95"]
        assert u05 <= u50 <= u95

    def test_selected_threshold_near_true_plateau(self):
        df = _synthetic_plateau_stage1(days=45, true_threshold=0.17)
        result = estimate_dynamic_ustar_thresholds(
            df,
            ustar_min_night_samples=200,
        )
        # The estimator is not REddyProc; a loose tolerance is honest.
        assert abs(result.selected_threshold - 0.17) < 0.1, (
            f"selected_threshold={result.selected_threshold:.3f} not within "
            "0.1 of the synthetic plateau threshold 0.17"
        )

    def test_method_string_is_stable(self):
        df = _synthetic_plateau_stage1(days=30)
        result = estimate_dynamic_ustar_thresholds(
            df, ustar_min_night_samples=100
        )
        # Stable method string so downstream diagnostics consumers can pin.
        assert result.method == "hesseflux-plateau-v1"

    def test_thresholds_by_season_records_have_expected_shape(self):
        df = _synthetic_plateau_stage1(days=30)
        result = estimate_dynamic_ustar_thresholds(
            df,
            ustar_temp_bins=4,
            ustar_min_night_samples=100,
        )
        assert len(result.thresholds_by_season) == 4
        for rec in result.thresholds_by_season:
            assert {"temp_bin", "temp_lower", "temp_upper", "night_sample_count"}.issubset(
                rec.keys()
            )

    def test_is_deterministic(self):
        df = _synthetic_plateau_stage1(days=30)
        r1 = estimate_dynamic_ustar_thresholds(df, ustar_min_night_samples=100)
        r2 = estimate_dynamic_ustar_thresholds(df, ustar_min_night_samples=100)
        assert r1.thresholds_by_scenario == r2.thresholds_by_scenario

    def test_missing_required_column_raises(self):
        df = _synthetic_plateau_stage1(days=30).drop(columns=["USTAR"])
        with pytest.raises(DynamicUstarEstimationError, match="USTAR"):
            estimate_dynamic_ustar_thresholds(df, ustar_min_night_samples=100)

    def test_sparse_night_data_raises(self):
        # Trim to a handful of rows - far fewer than the minimum.
        df = _synthetic_plateau_stage1(days=1).head(5)
        with pytest.raises(DynamicUstarEstimationError, match="nighttime"):
            estimate_dynamic_ustar_thresholds(df, ustar_min_night_samples=100)

    def test_requested_scenario_not_in_probs_raises(self):
        df = _synthetic_plateau_stage1(days=30)
        with pytest.raises(DynamicUstarEstimationError, match="U50"):
            estimate_dynamic_ustar_thresholds(
                df,
                ustar_probs=(0.10, 0.25, 0.75),
                ustar_scenario="U50",
                ustar_min_night_samples=100,
            )


# ----------------------------------------------------------------------
# Backend-level tests (hesseflux)
# ----------------------------------------------------------------------


def _legacy_fixture_stage1() -> pd.DataFrame:
    """Reuse the migrated tests/data fixtures for legacy-mode assertions."""
    base = Path(__file__).parent / "data"
    return load_stage1(
        path_full_output=base / "full_output",
        path_biomet=base / "biomet",
        tz_in="UTC",
        tz_out="UTC",
        skip_full_output=0,
        skip_biomet=0,
        drop_rain_rows=True,
    )


class TestFixedModeStillDefaultAndPreserved:
    def test_default_mode_is_fixed(self):
        assert HessefluxConfig().ustar_mode == "fixed"
        assert HessefluxConfig().partition_method == "lasslop"

    def test_postproc_with_default_config_runs(self):
        df = _legacy_fixture_stage1()
        out = postproc(df, engine="hesseflux")
        # Legacy columns still present.
        for col in ("SW_IN_f", "TA_f", "VPD_f", "NEE_f", "NEE_fqc"):
            assert col in out.columns, col

    def test_fixed_mode_diagnostics_report_fixed(self):
        df = _legacy_fixture_stage1()
        out = postproc(
            df,
            engine="hesseflux",
            hesseflux_config=HessefluxConfig(ustar_fixed=0.1),
        )
        diag = out.attrs.get("miaproc_diagnostics")
        assert diag is not None
        assert diag["backend"] == "hesseflux"
        assert diag["ustar"]["mode"] == "fixed"
        assert diag["ustar"]["selected_threshold"] == pytest.approx(0.1)
        # No scenario information in fixed mode.
        assert diag["ustar"]["available_scenarios"] == ()
        assert diag["ustar"]["thresholds_by_scenario"] == {}
        # Coding Prompt 022: partitioning is now a dict with method +
        # reco_fit_mode + lt_wrapper keys.
        assert diag["partitioning"]["method"] == "lasslop"
        assert diag["partitioning"]["reco_fit_mode"] == "native"
        assert diag["partitioning"]["lt_wrapper"] is None


class TestUnknownMode:
    def test_invalid_mode_raises(self):
        df = _legacy_fixture_stage1()
        cfg = HessefluxConfig(ustar_mode="neither")   # type: ignore[arg-type]
        with pytest.raises(ValueError, match="ustar_mode"):
            postproc(df, engine="hesseflux", hesseflux_config=cfg)


class TestDynamicModeOnSyntheticData:
    def test_dynamic_run_produces_contract_schema_and_diagnostics(self):
        df = _synthetic_plateau_stage1(days=45)
        cfg = HessefluxConfig(
            ustar_mode="dynamic",
            ustar_scenario="U50",
            ustar_min_night_samples=200,
            partition_method="reichstein",
        )
        out = postproc(df, engine="hesseflux", hesseflux_config=cfg)

        # All 13 contract columns must be present.
        for col in HESSEFLUX_COMMON_OUTPUT_COLUMNS:
            assert col in out.columns, f"missing contract column {col}"

        # Legacy aliases preserved.
        for col in ("SW_IN_f", "TA_f", "VPD_f", "NEE_f", "NEE_fqc"):
            assert col in out.columns, f"legacy alias {col} missing"

        # Diagnostics.
        diag = out.attrs.get("miaproc_diagnostics")
        assert diag is not None
        assert diag["backend"] == "hesseflux"
        assert diag["ustar"]["mode"] == "dynamic"
        assert diag["ustar"]["scenario"] == "U50"
        assert diag["ustar"]["available_scenarios"] == ("U05", "U50", "U95")
        assert diag["ustar"]["selected_threshold"] > 0
        assert 0.0 <= diag["ustar"]["fraction_nee_filtered"] <= 1.0
        assert diag["ustar"]["method"] == "hesseflux-plateau-v1"
        # Coding Prompt 022: partitioning is now a dict; default
        # reco_fit_mode is "native".
        assert diag["partitioning"]["method"] == "reichstein"
        assert diag["partitioning"]["reco_fit_mode"] == "native"
        assert diag["partitioning"]["lt_wrapper"] is None

    def test_dynamic_threshold_differs_from_ustar_fixed(self):
        """Dynamic mode must derive its threshold from data, not echo
        ``ustar_fixed``. We give the config a deliberately wrong
        ``ustar_fixed`` and assert the selected threshold is different."""
        df = _synthetic_plateau_stage1(days=45, true_threshold=0.17)
        cfg = HessefluxConfig(
            ustar_mode="dynamic",
            ustar_fixed=0.99,   # nonsensical value; dynamic must ignore
            ustar_min_night_samples=200,
        )
        out = postproc(df, engine="hesseflux", hesseflux_config=cfg)
        diag = out.attrs["miaproc_diagnostics"]
        assert diag["ustar"]["mode"] == "dynamic"
        assert diag["ustar"]["selected_threshold"] != pytest.approx(0.99)
        # And within a generous band of the synthetic plateau.
        assert abs(diag["ustar"]["selected_threshold"] - 0.17) < 0.2

    def test_dynamic_filters_more_nee_than_low_fixed_threshold(self):
        """Dynamic mode should mark more nighttime low-u* NEE values as
        gap-fill candidates than a very low fixed threshold would. This is
        the observable evidence that the dynamic filter is actually
        applied."""
        df = _synthetic_plateau_stage1(days=45)

        out_dyn = postproc(
            df,
            engine="hesseflux",
            hesseflux_config=HessefluxConfig(
                ustar_mode="dynamic", ustar_min_night_samples=200
            ),
        )
        out_fixed_low = postproc(
            df,
            engine="hesseflux",
            hesseflux_config=HessefluxConfig(
                ustar_mode="fixed", ustar_fixed=0.01
            ),
        )
        dyn_frac = out_dyn.attrs["miaproc_diagnostics"]["ustar"]["fraction_nee_filtered"]
        fixed_frac = out_fixed_low.attrs["miaproc_diagnostics"]["ustar"]["fraction_nee_filtered"]
        assert dyn_frac > fixed_frac

    def test_dynamic_sparse_input_raises_no_silent_fallback(self):
        """Risk R4 explicit behavior: dynamic mode must raise
        ``DynamicUstarEstimationError`` rather than fall back to
        ``ustar_fixed`` silently."""
        df = _synthetic_plateau_stage1(days=1).head(10)
        cfg = HessefluxConfig(
            ustar_mode="dynamic",
            ustar_fixed=0.5,   # would succeed if silent fallback existed
            ustar_min_night_samples=100,
        )
        with pytest.raises(DynamicUstarEstimationError):
            postproc(df, engine="hesseflux", hesseflux_config=cfg)


# ----------------------------------------------------------------------
# M4 hardening follow-ups: validation ordering + cow restore
# ----------------------------------------------------------------------


class _HesserequireSentinel(Exception):
    """Sentinel exception used by ordering tests to detect when
    ``_require_hesseflux`` is (or is not) reached."""


class TestValidationOrderingBeforeHesseflux:
    """The M4 review's P1 fix requires that Python-side validation runs
    before the hesseflux optional-dependency guard. These tests prove
    each validation path either reaches or bypasses the guard as
    designed."""

    def test_dynamic_sparse_raises_before_hesseflux_guard(self, monkeypatch):
        """Sparse dynamic input must raise ``DynamicUstarEstimationError``
        without ever calling ``_require_hesseflux``. A sentinel patched
        over the guard lets the test fail loudly if the order inverts."""
        import miaproc.eddy.engine_hesseflux as eh

        def sentinel():
            raise _HesserequireSentinel("hesseflux guard was reached")

        monkeypatch.setattr(eh, "_require_hesseflux", sentinel)

        df = _synthetic_plateau_stage1(days=1).head(10)
        cfg = HessefluxConfig(
            ustar_mode="dynamic", ustar_min_night_samples=100
        )
        with pytest.raises(DynamicUstarEstimationError):
            eh.run_hesseflux_engine(df, config=cfg)

    def test_unknown_mode_raises_before_hesseflux_guard(self, monkeypatch):
        import miaproc.eddy.engine_hesseflux as eh

        def sentinel():
            raise _HesserequireSentinel("hesseflux guard was reached")

        monkeypatch.setattr(eh, "_require_hesseflux", sentinel)

        df = _synthetic_plateau_stage1(days=1).head(10)
        cfg = HessefluxConfig(ustar_mode="neither")   # type: ignore[arg-type]
        with pytest.raises(ValueError, match="ustar_mode"):
            eh.run_hesseflux_engine(df, config=cfg)

    def test_fixed_mode_still_reaches_hesseflux_guard(self, monkeypatch):
        """Fixed mode still requires hesseflux to execute. If
        ``_require_hesseflux`` is patched to raise a sentinel, a fixed-mode
        call must propagate that sentinel — proving fixed mode reaches the
        guard after threshold resolution."""
        import miaproc.eddy.engine_hesseflux as eh

        def sentinel():
            raise _HesserequireSentinel("expected: hesseflux required in fixed mode")

        monkeypatch.setattr(eh, "_require_hesseflux", sentinel)

        # Fixed mode does not read the frame for threshold resolution, so a
        # small valid frame is enough to reach the guard.
        df = _synthetic_plateau_stage1(days=1).head(48)
        cfg = HessefluxConfig(ustar_mode="fixed", ustar_fixed=0.1)
        with pytest.raises(_HesserequireSentinel):
            eh.run_hesseflux_engine(df, config=cfg)


class TestCopyOnWriteRestoration:
    """The M4 review's P1 fix requires that
    ``pd.options.mode.copy_on_write`` is restored even if an engine step
    raises after it has been toggled."""

    def test_copy_on_write_restored_on_engine_failure(self, monkeypatch):
        import pandas as pd

        import miaproc.eddy.engine_hesseflux as eh

        # Record + force a known starting state. The test's own finally
        # block guarantees the option is restored even if the assertion
        # below fails, so the suite remains clean.
        original = pd.options.mode.copy_on_write
        pd.options.mode.copy_on_write = True
        try:
            # Patch hesseflux's gapfill (the first step after the cow
            # override) to raise. Using a lightweight fake hesseflux
            # module isolates the failure from real hesseflux behavior.
            class _FakeHf:
                @staticmethod
                def gapfill(*_args, **_kwargs):
                    raise RuntimeError("synthetic gapfill failure")

                @staticmethod
                def nee2gpp(*_args, **_kwargs):   # pragma: no cover
                    raise AssertionError("nee2gpp must not be reached")

            monkeypatch.setattr(eh, "hf", _FakeHf)

            # A fixed-mode run with a valid minimal frame is enough to
            # reach gapfill. Fixed mode skips the dynamic estimator and
            # arrives at the first gapfill call quickly.
            df = _synthetic_plateau_stage1(days=2)

            with pytest.raises(RuntimeError, match="synthetic gapfill"):
                eh.run_hesseflux_engine(
                    df,
                    config=HessefluxConfig(
                        ustar_mode="fixed", ustar_fixed=0.1
                    ),
                )

            # The engine sets cow to False internally and must restore it
            # to the starting value (True here) via its finally block.
            assert pd.options.mode.copy_on_write is True
        finally:
            pd.options.mode.copy_on_write = original

    def test_copy_on_write_restored_on_successful_run(self, monkeypatch):
        """Mirror-image test: the finally block also fires on the happy
        path. Uses the real hesseflux engine path (no monkeypatch on
        ``hf``) with the legacy fixture, forcing a non-default starting
        value and asserting it is restored."""
        import pandas as pd

        import miaproc.eddy.engine_hesseflux as eh   # noqa: F401

        original = pd.options.mode.copy_on_write
        pd.options.mode.copy_on_write = True
        try:
            df = _legacy_fixture_stage1()
            postproc(
                df,
                engine="hesseflux",
                hesseflux_config=HessefluxConfig(
                    ustar_mode="fixed", ustar_fixed=0.1
                ),
            )
            assert pd.options.mode.copy_on_write is True
        finally:
            pd.options.mode.copy_on_write = original
