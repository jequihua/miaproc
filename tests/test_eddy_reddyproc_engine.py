"""Python-only tests for the reddyproc-rpy2 backend.

These tests cover everything up to (and including) the rpy2 dependency guard
and the output-normalization helper. They do NOT invoke any R code. The full
live R + REddyProc + rpy2 path is covered by the opt-in
``@pytest.mark.reddyproc`` suite in ``test_eddy_reddyproc_live.py``.
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd
import pytest

from miaproc.eddy import (
    MissingOptionalDependencyError,
    REDDYPROC_OUTPUT_COLUMNS,
    ReddyProcConfig,
    UnsupportedScenarioError,
    postproc,
    run_reddyproc_engine,
)
from miaproc.eddy import engine_reddyproc as er
from miaproc.eddy import engines as engines_mod


def _synthetic_stage1(n: int = 6) -> pd.DataFrame:
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
        }
    )


def _fake_gapfill_export(
    n: int, *, scenarios: tuple[str, ...] = ("U05", "U50", "U95")
) -> pd.DataFrame:
    """Produce a fake ``sExportResults()`` frame for the gap-filling step."""
    cols: dict[str, np.ndarray] = {}
    for s in scenarios:
        cols[f"NEE_{s}_f"] = np.linspace(-4.0, 4.0, n) + (ord(s[1]) - ord("0"))
        cols[f"NEE_{s}_fsd"] = np.full(n, 0.5)
        cols[f"NEE_{s}_fqc"] = np.arange(n) % 3
    cols["Tair_f"] = np.linspace(20.0, 30.0, n)
    cols["Tair_fsd"] = np.full(n, 0.3)
    cols["Tair_fqc"] = np.arange(n) % 3
    cols["Rg_f"] = np.linspace(0.0, 800.0, n)
    cols["Rg_fsd"] = np.full(n, 5.0)
    cols["Rg_fqc"] = np.arange(n) % 3
    cols["VPD_f"] = np.linspace(5.0, 25.0, n)
    return pd.DataFrame(cols)


def _fake_partition_export(n: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "GPP_DT": np.linspace(0.0, 12.0, n),
            "Reco_DT": np.linspace(1.0, 5.0, n),
        }
    )


class TestReddyProcConfigDefaults:
    def test_defaults_match_r_manglaria(self):
        cfg = ReddyProcConfig()
        assert cfg.site_name == "SiteName"
        assert cfg.latitude is None
        assert cfg.longitude is None
        assert cfg.timezone_hour is None
        assert cfg.local_tz is None
        assert cfg.dts == 48
        assert cfg.ustar_n_sample == 200
        assert cfg.ustar_probs == (0.05, 0.5, 0.95)
        assert cfg.ustar_scenario == "U50"

    def test_config_is_frozen(self):
        cfg = ReddyProcConfig()
        with pytest.raises(Exception):
            cfg.dts = 24   # type: ignore[misc]

    def test_marismas_parity_config_is_constructible(self):
        cfg = ReddyProcConfig(
            site_name="Marismas_Nacionales",
            latitude=22.25,
            longitude=-105.50,
            timezone_hour=-7,
            local_tz="America/Mazatlan",
        )
        assert cfg.site_name == "Marismas_Nacionales"
        assert cfg.local_tz == "America/Mazatlan"
        assert cfg.timezone_hour == -7


class TestScenarioLabel:
    @pytest.mark.parametrize(
        "prob,expected",
        [(0.05, "U05"), (0.5, "U50"), (0.95, "U95"), (0.1, "U10")],
    )
    def test_scenario_label_from_prob(self, prob, expected):
        assert er._scenario_label_from_prob(prob) == expected

    def test_scenarios_from_config_default(self):
        assert er._scenarios_from_config(ReddyProcConfig()) == ("U05", "U50", "U95")

    def test_validate_scenario_accepts_present(self):
        er._validate_scenario("U50", ["U05", "U50", "U95"])

    def test_validate_scenario_rejects_absent_with_clear_error(self):
        with pytest.raises(UnsupportedScenarioError) as excinfo:
            er._validate_scenario("U99", ["U05", "U50", "U95"])
        msg = str(excinfo.value)
        assert "U99" in msg
        assert "U05" in msg and "U50" in msg and "U95" in msg


class TestMissingOptionalDependencyError:
    def _hide_rpy2(self, monkeypatch):
        """Simulate rpy2 being absent. Works whether rpy2 is installed or not."""
        monkeypatch.setitem(sys.modules, "rpy2", None)
        monkeypatch.setitem(sys.modules, "rpy2.robjects", None)
        monkeypatch.setitem(sys.modules, "rpy2.robjects.packages", None)

    def test_require_raises_clear_error(self, monkeypatch):
        self._hide_rpy2(monkeypatch)
        with pytest.raises(MissingOptionalDependencyError) as excinfo:
            er._require_rpy2_and_reddyproc()
        msg = str(excinfo.value)
        assert "miaproc[reddyproc]" in msg
        assert "REddyProc" in msg
        assert "R" in msg

    def test_error_is_importerror_subclass(self):
        assert issubclass(MissingOptionalDependencyError, ImportError)

    def test_run_engine_raises_clear_error_without_rpy2(self, monkeypatch):
        self._hide_rpy2(monkeypatch)
        df = _synthetic_stage1()
        with pytest.raises(MissingOptionalDependencyError):
            run_reddyproc_engine(df)

    def test_postproc_raises_clear_error_without_rpy2(self, monkeypatch):
        self._hide_rpy2(monkeypatch)
        df = _synthetic_stage1()
        with pytest.raises(MissingOptionalDependencyError):
            postproc(df, engine="reddyproc-rpy2")


class TestDispatch:
    def _hide_rpy2(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "rpy2", None)
        monkeypatch.setitem(sys.modules, "rpy2.robjects", None)
        monkeypatch.setitem(sys.modules, "rpy2.robjects.packages", None)

    def test_reddyproc_rpy2_routes_to_new_backend(self, monkeypatch):
        calls = {}

        def fake_run(df, *, config):
            calls["df"] = df
            calls["config"] = config
            return pd.DataFrame({"DateTime": df["DateTime"].to_numpy()})

        monkeypatch.setattr(engines_mod, "run_reddyproc_engine", fake_run)
        df = _synthetic_stage1()
        out = postproc(df, engine="reddyproc-rpy2")
        assert "df" in calls
        assert isinstance(calls["config"], ReddyProcConfig)
        assert "DateTime" in out.columns

    def test_reddyproc_alias_routes_to_same_backend(self, monkeypatch):
        calls = {"count": 0}

        def fake_run(df, *, config):
            calls["count"] += 1
            return pd.DataFrame({"DateTime": df["DateTime"].to_numpy()})

        monkeypatch.setattr(engines_mod, "run_reddyproc_engine", fake_run)
        df = _synthetic_stage1()
        postproc(df, engine="reddyproc-rpy2")
        postproc(df, engine="reddyproc")
        assert calls["count"] == 2

    def test_reddyproc_config_is_forwarded(self, monkeypatch):
        received: dict[str, object] = {}

        def fake_run(df, *, config):
            received["config"] = config
            return pd.DataFrame({"DateTime": df["DateTime"].to_numpy()})

        monkeypatch.setattr(engines_mod, "run_reddyproc_engine", fake_run)
        cfg = ReddyProcConfig(
            site_name="Marismas_Nacionales",
            latitude=22.25,
            longitude=-105.50,
            timezone_hour=-7,
            local_tz="America/Mazatlan",
        )
        postproc(_synthetic_stage1(), engine="reddyproc-rpy2", reddyproc_config=cfg)
        assert received["config"] is cfg

    def test_unknown_engine_raises(self, monkeypatch):
        with pytest.raises(ValueError) as excinfo:
            postproc(_synthetic_stage1(), engine="notarealengine")   # type: ignore[arg-type]
        assert "Unknown engine" in str(excinfo.value)
        assert "reddyproc-rpy2" in str(excinfo.value)


class TestLocalTzPropagation:
    def test_run_engine_forwards_local_tz_to_stage2(self, monkeypatch):
        """``run_reddyproc_engine`` must call ``prepare_reddyproc_input`` with
        ``local_tz=config.local_tz`` so the Marismas workflow can reproduce the
        R calendar-field semantics without extra wiring."""
        captured: dict[str, object] = {}

        def fake_prepare(df, *, local_tz=None):
            captured["local_tz"] = local_tz
            # Return a plausible stage-2 frame; subsequent rpy2 check will
            # fail, which is the expected stopping point for this test.
            return pd.DataFrame(
                {
                    "DateTime": df["DateTime"].to_numpy(),
                    "Year": [2025] * len(df),
                    "DoY": [1] * len(df),
                    "Hour": [0.0] * len(df),
                    "NEE": df["NEE"].to_numpy(),
                    "Ustar": df["USTAR"].to_numpy(),
                    "Tair": df["Tair"].to_numpy(),
                    "VPD": df["VPD"].to_numpy(),
                    "Rg": df["Rg"].to_numpy(),
                    "rH": df["rH"].to_numpy(),
                    "QF": df["QC_NEE"].to_numpy(),
                }
            )

        monkeypatch.setattr(er, "prepare_reddyproc_input", fake_prepare)
        # Force rpy2 absent so execution stops cleanly after stage-2 prep.
        monkeypatch.setitem(sys.modules, "rpy2", None)

        df = _synthetic_stage1()
        cfg = ReddyProcConfig(
            local_tz="America/Mazatlan", ustar_scenario="U50"
        )
        with pytest.raises(MissingOptionalDependencyError):
            run_reddyproc_engine(df, config=cfg)
        assert captured["local_tz"] == "America/Mazatlan"

    def test_run_engine_forwards_none_local_tz_by_default(self, monkeypatch):
        captured: dict[str, object] = {}

        def fake_prepare(df, *, local_tz=None):
            captured["local_tz"] = local_tz
            return pd.DataFrame(
                {
                    "DateTime": df["DateTime"].to_numpy(),
                    "Year": [2025] * len(df),
                    "DoY": [1] * len(df),
                    "Hour": [0.0] * len(df),
                    "NEE": df["NEE"].to_numpy(),
                    "Ustar": df["USTAR"].to_numpy(),
                    "Tair": df["Tair"].to_numpy(),
                    "VPD": df["VPD"].to_numpy(),
                    "Rg": df["Rg"].to_numpy(),
                    "rH": df["rH"].to_numpy(),
                    "QF": df["QC_NEE"].to_numpy(),
                }
            )

        monkeypatch.setattr(er, "prepare_reddyproc_input", fake_prepare)
        monkeypatch.setitem(sys.modules, "rpy2", None)

        with pytest.raises(MissingOptionalDependencyError):
            run_reddyproc_engine(_synthetic_stage1())
        assert captured["local_tz"] is None


class TestScenarioValidationBeforeR:
    def test_invalid_scenario_raises_before_rpy2_check(self, monkeypatch):
        """If the configured scenario is not in ``ustar_probs``, the backend
        must fail with ``UnsupportedScenarioError`` before importing rpy2."""

        def boom(*a, **kw):
            raise AssertionError("rpy2 must not be touched for invalid scenario")

        monkeypatch.setattr(er, "_require_rpy2_and_reddyproc", boom)

        df = _synthetic_stage1()
        cfg = ReddyProcConfig(ustar_scenario="U99")
        with pytest.raises(UnsupportedScenarioError):
            run_reddyproc_engine(df, config=cfg)


class TestSiteMetadataValidation:
    def test_no_metadata_returns_false(self):
        assert er._validate_site_metadata(ReddyProcConfig()) is False

    def test_all_three_metadata_returns_true(self):
        cfg = ReddyProcConfig(latitude=22.25, longitude=-105.50, timezone_hour=-7)
        assert er._validate_site_metadata(cfg) is True

    @pytest.mark.parametrize(
        "kwargs,missing,supplied",
        [
            ({"latitude": 22.25}, ["longitude", "timezone_hour"], ["latitude"]),
            (
                {"latitude": 22.25, "longitude": -105.50},
                ["timezone_hour"],
                ["latitude", "longitude"],
            ),
            ({"timezone_hour": -7}, ["latitude", "longitude"], ["timezone_hour"]),
            (
                {"longitude": -105.50, "timezone_hour": -7},
                ["latitude"],
                ["longitude", "timezone_hour"],
            ),
        ],
    )
    def test_partial_metadata_raises_with_clear_message(
        self, kwargs, missing, supplied
    ):
        cfg = ReddyProcConfig(**kwargs)
        with pytest.raises(ValueError) as excinfo:
            er._validate_site_metadata(cfg)
        msg = str(excinfo.value)
        for f in missing:
            assert f in msg, f"missing field {f!r} not named in error"
        for f in supplied:
            assert f in msg, f"supplied field {f!r} not named in error"
        assert "all-or-nothing" in msg

    def test_partial_metadata_raises_before_rpy2_check_in_run_engine(
        self, monkeypatch
    ):
        """Partial metadata must be rejected Python-side, not after the rpy2
        import path."""

        def boom(*a, **kw):
            raise AssertionError(
                "rpy2 must not be touched when site metadata is invalid"
            )

        monkeypatch.setattr(er, "_require_rpy2_and_reddyproc", boom)
        df = _synthetic_stage1()
        cfg = ReddyProcConfig(latitude=22.25)   # partial
        with pytest.raises(ValueError) as excinfo:
            run_reddyproc_engine(df, config=cfg)
        assert "all-or-nothing" in str(excinfo.value)

    def test_full_metadata_reaches_dependency_guard(self, monkeypatch):
        """With all three metadata fields, partial-metadata validation must
        not short-circuit; execution must reach the rpy2 guard."""
        import sys as _sys

        monkeypatch.setitem(_sys.modules, "rpy2", None)
        df = _synthetic_stage1()
        cfg = ReddyProcConfig(
            latitude=22.25, longitude=-105.50, timezone_hour=-7
        )
        with pytest.raises(MissingOptionalDependencyError):
            run_reddyproc_engine(df, config=cfg)

    def test_no_metadata_reaches_dependency_guard(self, monkeypatch):
        import sys as _sys

        monkeypatch.setitem(_sys.modules, "rpy2", None)
        with pytest.raises(MissingOptionalDependencyError):
            run_reddyproc_engine(_synthetic_stage1())


class TestNormalizeOutput:
    def test_produces_contract_schema_in_order(self):
        df = _synthetic_stage1(n=6)
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(df)
        out = er._normalize_reddyproc_output(
            stage2,
            _fake_gapfill_export(len(stage2)),
            _fake_partition_export(len(stage2)),
            ReddyProcConfig(),
        )
        assert tuple(out.columns) == REDDYPROC_OUTPUT_COLUMNS
        assert len(out) == len(stage2)

    def test_selects_configured_scenario_columns(self):
        n = 4
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1(n=n))
        gapfill = _fake_gapfill_export(len(stage2))
        partition = _fake_partition_export(len(stage2))

        out_u50 = er._normalize_reddyproc_output(
            stage2, gapfill, partition, ReddyProcConfig(ustar_scenario="U50")
        )
        out_u05 = er._normalize_reddyproc_output(
            stage2, gapfill, partition, ReddyProcConfig(ustar_scenario="U05")
        )
        # Fake U50 and U05 NEE_f differ by exactly the encoded offset.
        assert not np.allclose(out_u50["NEE_f"].to_numpy(), out_u05["NEE_f"].to_numpy())
        assert np.allclose(
            out_u50["NEE_f"].to_numpy(),
            gapfill["NEE_U50_f"].to_numpy(),
        )
        assert np.allclose(
            out_u05["NEE_f"].to_numpy(),
            gapfill["NEE_U05_f"].to_numpy(),
        )

    def test_raw_columns_come_from_stage2(self):
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1())
        gapfill = _fake_gapfill_export(len(stage2))
        partition = _fake_partition_export(len(stage2))
        out = er._normalize_reddyproc_output(
            stage2, gapfill, partition, ReddyProcConfig()
        )
        assert np.allclose(out["NEE"].to_numpy(), stage2["NEE"].to_numpy())
        assert np.allclose(out["Tair"].to_numpy(), stage2["Tair"].to_numpy())
        assert np.allclose(out["Rg"].to_numpy(), stage2["Rg"].to_numpy())
        assert np.allclose(out["VPD"].to_numpy(), stage2["VPD"].to_numpy())
        assert np.allclose(out["USTAR"].to_numpy(), stage2["Ustar"].to_numpy())

    def test_gpp_reco_come_from_partition_export(self):
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1())
        gapfill = _fake_gapfill_export(len(stage2))
        partition = _fake_partition_export(len(stage2))
        out = er._normalize_reddyproc_output(
            stage2, gapfill, partition, ReddyProcConfig()
        )
        assert np.allclose(out["GPP"].to_numpy(), partition["GPP_DT"].to_numpy())
        assert np.allclose(out["Reco"].to_numpy(), partition["Reco_DT"].to_numpy())

    def test_unsupported_scenario_raises_with_available_list(self):
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1())
        # Fake export only has U05/U50/U95 scenarios.
        gapfill = _fake_gapfill_export(len(stage2))
        partition = _fake_partition_export(len(stage2))
        with pytest.raises(UnsupportedScenarioError) as excinfo:
            er._normalize_reddyproc_output(
                stage2, gapfill, partition, ReddyProcConfig(ustar_scenario="U90")
            )
        msg = str(excinfo.value)
        assert "U90" in msg
        assert "U05" in msg and "U50" in msg and "U95" in msg

    def test_length_mismatch_raises(self):
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1(n=6))
        # Build exports with wrong length.
        gapfill = _fake_gapfill_export(len(stage2) + 1)
        partition = _fake_partition_export(len(stage2))
        with pytest.raises(ValueError) as excinfo:
            er._normalize_reddyproc_output(
                stage2, gapfill, partition, ReddyProcConfig()
            )
        assert "row counts" in str(excinfo.value)

    @pytest.mark.parametrize(
        "drop_gapfill",
        ["Tair_f", "Rg_f", "VPD_f", "NEE_U50_fqc"],
    )
    def test_missing_gapfill_column_raises(self, drop_gapfill):
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1())
        gapfill = _fake_gapfill_export(len(stage2)).drop(columns=[drop_gapfill])
        partition = _fake_partition_export(len(stage2))
        with pytest.raises(ValueError) as excinfo:
            er._normalize_reddyproc_output(
                stage2, gapfill, partition, ReddyProcConfig()
            )
        msg = str(excinfo.value)
        assert drop_gapfill in msg
        assert "gap-fill export missing" in msg

    @pytest.mark.parametrize("drop_partition", ["GPP_DT", "Reco_DT"])
    def test_missing_partition_column_raises(self, drop_partition):
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1())
        gapfill = _fake_gapfill_export(len(stage2))
        partition = _fake_partition_export(len(stage2)).drop(columns=[drop_partition])
        with pytest.raises(ValueError) as excinfo:
            er._normalize_reddyproc_output(
                stage2, gapfill, partition, ReddyProcConfig()
            )
        msg = str(excinfo.value)
        assert drop_partition in msg
        assert "partition export missing" in msg

    def test_missing_columns_across_both_exports_listed_together(self):
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1())
        gapfill = _fake_gapfill_export(len(stage2)).drop(
            columns=["Tair_f", "NEE_U50_fqc"]
        )
        partition = _fake_partition_export(len(stage2)).drop(columns=["GPP_DT"])
        with pytest.raises(ValueError) as excinfo:
            er._normalize_reddyproc_output(
                stage2, gapfill, partition, ReddyProcConfig()
            )
        msg = str(excinfo.value)
        # All three missing columns must be named in one error.
        assert "Tair_f" in msg
        assert "NEE_U50_fqc" in msg
        assert "GPP_DT" in msg

    def test_missing_scenario_f_raises_unsupported_scenario_not_value_error(self):
        """Scenario selection error has priority over the generic
        missing-columns error."""
        from miaproc.eddy.stage2 import prepare_reddyproc_input

        stage2 = prepare_reddyproc_input(_synthetic_stage1())
        # Keep only U05/U95; drop all U50 columns and unrelated columns.
        gapfill = _fake_gapfill_export(len(stage2), scenarios=("U05", "U95"))
        partition = _fake_partition_export(len(stage2))
        with pytest.raises(UnsupportedScenarioError) as excinfo:
            er._normalize_reddyproc_output(
                stage2, gapfill, partition, ReddyProcConfig(ustar_scenario="U50")
            )
        # Must surface as scenario error, not a generic ValueError.
        assert "U50" in str(excinfo.value)


class TestUstarDiagnosticsHelper:
    def test_none_scenarios_return_empty_diagnostics(self):
        diag = er._ustar_diagnostics_from_scenarios(None, ReddyProcConfig())
        assert diag["available_scenarios"] == ()
        assert diag["selected_threshold"] is None
        assert diag["thresholds_by_season"] == ()

    def test_empty_frame_returns_empty_diagnostics(self):
        diag = er._ustar_diagnostics_from_scenarios(
            pd.DataFrame(), ReddyProcConfig()
        )
        assert diag["available_scenarios"] == ()
        assert diag["selected_threshold"] is None
        assert diag["thresholds_by_season"] == ()

    def test_single_season_yields_numeric_selected_threshold(self):
        # REddyProc sGetUstarScenarios: first column is season, rest are
        # scenario thresholds.
        scenarios = pd.DataFrame(
            {
                "season": ["2025001"],
                "U05": [0.05],
                "U50": [0.17],
                "U95": [0.32],
            }
        )
        diag = er._ustar_diagnostics_from_scenarios(
            scenarios, ReddyProcConfig(ustar_scenario="U50")
        )
        assert diag["available_scenarios"] == ("U05", "U50", "U95")
        assert diag["selected_threshold"] == 0.17
        assert len(diag["thresholds_by_season"]) == 1
        record = diag["thresholds_by_season"][0]
        assert record["season"] == "2025001"
        assert record["U50"] == 0.17

    def test_varying_thresholds_across_seasons_yield_none_but_preserve_records(
        self,
    ):
        scenarios = pd.DataFrame(
            {
                "season": ["2025001", "2025002"],
                "U05": [0.04, 0.06],
                "U50": [0.15, 0.20],
                "U95": [0.30, 0.35],
            }
        )
        diag = er._ustar_diagnostics_from_scenarios(
            scenarios, ReddyProcConfig(ustar_scenario="U50")
        )
        # U50 varies (0.15 vs 0.20) -> no single threshold available.
        assert diag["selected_threshold"] is None
        # But the season-level records must still be captured.
        assert len(diag["thresholds_by_season"]) == 2
        seasons = [r["season"] for r in diag["thresholds_by_season"]]
        assert seasons == ["2025001", "2025002"]

    def test_missing_configured_scenario_yields_none_threshold(self):
        scenarios = pd.DataFrame(
            {
                "season": ["2025001"],
                "U05": [0.05],
                "U95": [0.32],
            }
        )
        diag = er._ustar_diagnostics_from_scenarios(
            scenarios, ReddyProcConfig(ustar_scenario="U50")
        )
        # Available scenarios reported honestly; no threshold selectable.
        assert diag["available_scenarios"] == ("U05", "U95")
        assert diag["selected_threshold"] is None

    def test_frame_with_only_season_column_returns_empty(self):
        scenarios = pd.DataFrame({"season": ["2025001"]})
        diag = er._ustar_diagnostics_from_scenarios(scenarios, ReddyProcConfig())
        assert diag["available_scenarios"] == ()
        assert diag["selected_threshold"] is None
        assert diag["thresholds_by_season"] == ()


class TestDiagnosticsBuilder:
    def test_diagnostics_include_backend_and_site(self):
        cfg = ReddyProcConfig(
            site_name="Marismas_Nacionales",
            latitude=22.25,
            longitude=-105.50,
            timezone_hour=-7,
            local_tz="America/Mazatlan",
        )
        diag = er._build_diagnostics(
            cfg,
            available_scenarios=("U05", "U50", "U95"),
            selected_threshold=0.17,
            thresholds_by_season=(
                {"season": "2025001", "U05": 0.05, "U50": 0.17, "U95": 0.32},
            ),
            r_version="4.3.2",
            reddyproc_version="1.3.3",
            rpy2_version="3.5.15",
        )
        assert diag["backend"] == "reddyproc-rpy2"
        assert diag["site"]["site_name"] == "Marismas_Nacionales"
        assert diag["site"]["local_tz"] == "America/Mazatlan"
        assert diag["dts"] == 48
        assert diag["ustar"]["scenario"] == "U50"
        assert diag["ustar"]["probs"] == (0.05, 0.5, 0.95)
        assert diag["ustar"]["available_scenarios"] == ("U05", "U50", "U95")
        assert diag["ustar"]["selected_threshold"] == 0.17
        assert len(diag["ustar"]["thresholds_by_season"]) == 1
        assert diag["ustar"]["thresholds_by_season"][0]["U50"] == 0.17
        assert diag["versions"]["R"] == "4.3.2"
        assert diag["partitioning"] == "lasslop"

    def test_diagnostics_tolerate_missing_versions(self):
        diag = er._build_diagnostics(ReddyProcConfig())
        assert diag["versions"]["R"] is None
        assert diag["versions"]["REddyProc"] is None
        assert diag["versions"]["rpy2"] is None
        assert diag["ustar"]["available_scenarios"] == ()
        assert diag["ustar"]["thresholds_by_season"] == ()
        assert diag["ustar"]["selected_threshold"] is None
