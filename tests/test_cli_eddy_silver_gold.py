"""Tests for the M14 ``miaproc eddy run-silver`` / ``run-gold`` split.

These tests stub ``load_stage1`` and ``postproc`` so they remain fast,
deterministic, and runnable on default CI (no live R, no real
case-study data, no hesseflux import). They cover:

- argument parsing for both new subcommands;
- silver-stage end-to-end (load → write CSV / parquet → run JSON shape);
- gold-stage end-to-end (read silver → postproc → column preservation +
  diagnostics + run JSON);
- the silver→gold column-preservation contract (silver-only columns
  appear on gold output, keyed on DateTime; gold's 13-column backend
  contract is preserved verbatim);
- CSV roundtrip safety (silver written as CSV is consumed by gold and
  the join still works after timezone-stripped CSV parsing);
- preflight posture (silver never invokes preflight; gold with
  ``reddyproc-reference`` invokes it and exits 2 if not approved;
  gold with ``hesseflux-native`` does not invoke preflight);
- gold's default engine remains ``reddyproc-reference`` (Decision 010
  / M14 Docker-runtime anchor).
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from miaproc import cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_silver_df(n: int = 4) -> pd.DataFrame:
    """A silver-shaped frame with stage-1 columns + extras the backend
    will not produce (so gold has something silver-only to append)."""
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "QC_NEE": [0, 0, 0, 0],
            # Silver-only extras (not present in the 13-column gold contract)
            "H": [10.0, 20.0, 30.0, 40.0],
            "LE": [50.0, 60.0, 70.0, 80.0],
            "P_RAIN": [0.0, 0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0, 90.0],
        }
    )


def _stub_gold_df(diagnostics: dict[str, Any] | None = None) -> pd.DataFrame:
    """The 13-column backend gold contract output."""
    df = pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=4, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "NEE_f": [0.1, 0.2, -0.1, 0.3],
            "NEE_fqc": [0, 0, 0, 0],
            "GPP": [0.0, 1.5, 3.0, 0.5],
            "Reco": [1.0, 1.0, 1.0, 1.0],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "Tair_f": [20.0, 21.0, 22.0, 19.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "Rg_f": [0.0, 100.0, 200.0, 50.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "VPD_f": [5.0, 6.0, 7.0, 4.0],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
        }
    )
    df.attrs["miaproc_diagnostics"] = diagnostics or {
        "backend": "hesseflux",
        "ustar": {"mode": "dynamic", "selected_threshold": 0.18},
        "partitioning": {
            "method": "lasslop",
            "reco_fit_mode": "native",
            "lt_wrapper": None,
        },
        "warnings": (),
    }
    return df


def _make_silver_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    flux = tmp_path / "flux"
    biomet = tmp_path / "biomet"
    flux.mkdir(exist_ok=True)
    biomet.mkdir(exist_ok=True)
    base = {
        "--flux-dir": str(flux),
        "--biomet-dir": str(biomet),
        "--output-table": str(tmp_path / "silver.csv"),
        "--output-run-json": str(tmp_path / "silver_run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["eddy", "run-silver"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


def _make_gold_argv(
    tmp_path: Path,
    silver_path: Path,
    **overrides: Any,
) -> list[str]:
    base = {
        "--silver-table": str(silver_path),
        "--engine": "hesseflux-native",
        "--output-table": str(tmp_path / "gold.csv"),
        "--output-diagnostics-json": str(tmp_path / "gold_diag.json"),
        "--output-run-json": str(tmp_path / "gold_run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["eddy", "run-gold"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


@pytest.fixture
def patch_load_stage1(monkeypatch):
    """Stub ``miaproc.eddy.load_stage1`` to return a silver-shaped frame."""
    calls: dict[str, Any] = {"load_kwargs": None}

    def _fake_load_stage1(**kwargs):
        calls["load_kwargs"] = kwargs
        return _stub_silver_df()

    import miaproc.eddy as eddy_pkg

    monkeypatch.setattr(eddy_pkg, "load_stage1", _fake_load_stage1)
    return SimpleNamespace(calls=calls)


@pytest.fixture
def patch_postproc(monkeypatch):
    """Stub ``miaproc.eddy.postproc`` to return a 13-column gold frame."""
    calls: dict[str, Any] = {
        "engine": None,
        "input_df": None,
        "hesseflux_config": None,
        "reddyproc_config": None,
    }

    def _fake_postproc(df, *, engine, hesseflux_config=None, reddyproc_config=None):
        calls["engine"] = engine
        calls["input_df"] = df
        calls["hesseflux_config"] = hesseflux_config
        calls["reddyproc_config"] = reddyproc_config
        return _stub_gold_df()

    import miaproc.eddy as eddy_pkg

    monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
    return SimpleNamespace(calls=calls)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestArgParsing:
    def test_silver_help_exits_zero(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["eddy", "run-silver", "--help"])
        assert exc.value.code == 0

    def test_gold_help_exits_zero(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["eddy", "run-gold", "--help"])
        assert exc.value.code == 0

    def test_gold_engine_default_is_reddyproc_reference(self, tmp_path):
        """M14 / Decision 010 anchor: the gold default must remain
        ``reddyproc-reference`` so the Docker story stays on the
        accepted packaged R runtime even if no --engine is passed."""
        silver = tmp_path / "silver.csv"
        silver.write_text("DateTime\n", encoding="utf-8")
        parser = cli._build_parser()
        ns = parser.parse_args(
            [
                "eddy",
                "run-gold",
                "--silver-table",
                str(silver),
                "--output-table",
                str(tmp_path / "gold.csv"),
                "--output-diagnostics-json",
                str(tmp_path / "diag.json"),
                "--output-run-json",
                str(tmp_path / "run.json"),
                "--repo-root",
                str(tmp_path),
            ]
        )
        assert ns.engine == "reddyproc-reference"

    def test_gold_required_flags_missing_exits(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["eddy", "run-gold"])

    def test_silver_required_flags_missing_exits(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["eddy", "run-silver"])


# ---------------------------------------------------------------------------
# Silver stage
# ---------------------------------------------------------------------------


class TestSilverStage:
    def test_silver_writes_table_and_run_json_csv(
        self, tmp_path, patch_load_stage1
    ):
        rc = cli.main(_make_silver_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT

        silver_path = tmp_path / "silver.csv"
        run_path = tmp_path / "silver_run.json"
        assert silver_path.exists()
        assert run_path.exists()

        df = pd.read_csv(silver_path)
        # Silver carries the joined input + stage-1 columns.
        for col in ("DateTime", "NEE", "USTAR", "Tair", "Rg", "QC_NEE"):
            assert col in df.columns, col
        # Silver-only extras (joined but not produced by the gold backend).
        for col in ("H", "LE", "P_RAIN", "rH"):
            assert col in df.columns, col

        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["stage"] == "silver"
        assert run["exit_code"] == cli.SUCCESS_EXIT
        assert run["row_counts"]["silver"] == 4
        assert run["outputs"]["table_format"] == "csv"
        # M24: --site-id is no longer a CLI surface; the ungrouped run
        # records group_column=None and the loader runs site_id=None.
        assert run["inputs"]["group_column"] is None
        assert run["inputs"]["tz_in"] == "UTC"
        assert run["inputs"]["tz_out"] == "UTC"
        kwargs = patch_load_stage1.calls["load_kwargs"]
        assert kwargs["site_id"] is None
        assert kwargs["drop_rain_rows"] is False

    def test_silver_writes_parquet_when_extension_matches(
        self, tmp_path, patch_load_stage1
    ):
        out = tmp_path / "silver.parquet"
        rc = cli.main(
            _make_silver_argv(tmp_path, **{"--output-table": str(out)})
        )
        assert rc == cli.SUCCESS_EXIT
        assert out.exists()
        df = pd.read_parquet(out)
        assert "DateTime" in df.columns
        # Parquet preserves tz-aware DateTime by design.
        assert pd.api.types.is_datetime64_any_dtype(df["DateTime"])

    def test_silver_unsupported_extension_exits_three(
        self, tmp_path, patch_load_stage1
    ):
        rc = cli.main(
            _make_silver_argv(
                tmp_path, **{"--output-table": str(tmp_path / "silver.xlsx")}
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_silver_missing_input_dir_exits_three(
        self, tmp_path, patch_load_stage1
    ):
        argv = _make_silver_argv(tmp_path)
        for i, token in enumerate(argv):
            if token == "--flux-dir":
                argv[i + 1] = str(tmp_path / "no_such_dir")
                break
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_silver_does_not_invoke_preflight(
        self, tmp_path, patch_load_stage1, monkeypatch
    ):
        """Silver is stage-1 only — no engine, no R. Preflight must not
        be reached even by accident."""
        called: dict[str, bool] = {"preflight": False}

        def _trip(*_args, **_kw):
            called["preflight"] = True
            raise AssertionError("preflight should not run during silver")

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "preflight_reddyproc_r_environment", _trip
        )
        rc = cli.main(_make_silver_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        assert called["preflight"] is False

    def test_silver_runtime_failure_returns_four(
        self, tmp_path, patch_load_stage1, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _raises(**_kw):
            raise RuntimeError("simulated stage-1 failure")

        monkeypatch.setattr(eddy_pkg, "load_stage1", _raises)
        rc = cli.main(_make_silver_argv(tmp_path))
        assert rc == cli.RUNTIME_EXIT


# ---------------------------------------------------------------------------
# Gold stage
# ---------------------------------------------------------------------------


def _write_silver_csv(tmp_path: Path) -> Path:
    silver = _stub_silver_df()
    out = tmp_path / "silver.csv"
    silver.to_csv(out, index=False)
    return out


def _write_silver_parquet(tmp_path: Path) -> Path:
    silver = _stub_silver_df()
    out = tmp_path / "silver.parquet"
    silver.to_parquet(out, index=False)
    return out


class TestGoldStage:
    def test_gold_consumes_silver_csv_and_writes_artifacts(
        self, tmp_path, patch_postproc
    ):
        silver = _write_silver_csv(tmp_path)
        rc = cli.main(_make_gold_argv(tmp_path, silver))
        assert rc == cli.SUCCESS_EXIT

        gold_path = tmp_path / "gold.csv"
        diag_path = tmp_path / "gold_diag.json"
        run_path = tmp_path / "gold_run.json"
        assert gold_path.exists()
        assert diag_path.exists()
        assert run_path.exists()

        # postproc was called with the silver frame.
        assert patch_postproc.calls["engine"] == "hesseflux"
        passed_in = patch_postproc.calls["input_df"]
        assert "DateTime" in passed_in.columns
        assert "H" in passed_in.columns  # silver-only column reached postproc

        # Diagnostics survived the column-attach merge.
        diag = json.loads(diag_path.read_text(encoding="utf-8"))
        assert diag["backend"] == "hesseflux"
        assert diag["partitioning"]["method"] == "lasslop"

        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["stage"] == "gold"
        assert run["engine"] == "hesseflux-native"
        assert run["exit_code"] == cli.SUCCESS_EXIT
        assert run["row_counts"]["silver_input"] == 4
        assert run["row_counts"]["gold_output"] == 4
        # Silver-only columns appear in the appended-list metadata.
        # QC_NEE is also silver-only (the 13-column gold contract has
        # NEE_fqc but not QC_NEE), so the full appended set is 5 cols.
        appended = run["silver_columns_appended"]
        for col in ("H", "LE", "P_RAIN", "QC_NEE", "rH"):
            assert col in appended, col
        # Gold's 13-column contract is preserved verbatim.
        gold_df = pd.read_csv(gold_path)
        for col in (
            "DateTime",
            "NEE",
            "NEE_f",
            "NEE_fqc",
            "GPP",
            "Reco",
            "Tair",
            "Tair_f",
            "Rg",
            "Rg_f",
            "VPD",
            "VPD_f",
            "USTAR",
        ):
            assert col in gold_df.columns, col
        # Silver-only extras appended after the backend output.
        for col in ("H", "LE", "P_RAIN", "QC_NEE", "rH"):
            assert col in gold_df.columns, col
        assert run["column_counts"]["silver_only_appended"] == 5

    def test_gold_consumes_silver_parquet(self, tmp_path, patch_postproc):
        silver = _write_silver_parquet(tmp_path)
        rc = cli.main(_make_gold_argv(tmp_path, silver))
        assert rc == cli.SUCCESS_EXIT
        gold_df = pd.read_csv(tmp_path / "gold.csv")
        assert "GPP" in gold_df.columns
        assert "H" in gold_df.columns

    def test_gold_invalid_silver_extension_exits_three(
        self, tmp_path, patch_postproc
    ):
        bogus = tmp_path / "silver.xlsx"
        bogus.write_text("not a real silver file", encoding="utf-8")
        rc = cli.main(_make_gold_argv(tmp_path, bogus))
        assert rc == cli.VALIDATION_EXIT

    def test_gold_missing_silver_exits_three(self, tmp_path, patch_postproc):
        rc = cli.main(
            _make_gold_argv(tmp_path, tmp_path / "missing_silver.csv")
        )
        assert rc == cli.VALIDATION_EXIT

    def test_gold_runtime_failure_returns_four(
        self, tmp_path, patch_postproc, monkeypatch
    ):
        silver = _write_silver_csv(tmp_path)
        import miaproc.eddy as eddy_pkg

        def _raises(*_a, **_kw):
            raise RuntimeError("simulated engine failure")

        monkeypatch.setattr(eddy_pkg, "postproc", _raises)
        rc = cli.main(_make_gold_argv(tmp_path, silver))
        assert rc == cli.RUNTIME_EXIT

    def test_gold_hesseflux_does_not_invoke_preflight(
        self, tmp_path, patch_postproc, monkeypatch
    ):
        silver = _write_silver_csv(tmp_path)
        called: dict[str, bool] = {"preflight": False}

        def _trip(*_a, **_kw):
            called["preflight"] = True
            raise AssertionError("preflight should not run for hesseflux gold")

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "preflight_reddyproc_r_environment", _trip
        )
        rc = cli.main(_make_gold_argv(tmp_path, silver))
        assert rc == cli.SUCCESS_EXIT
        assert called["preflight"] is False


# ---------------------------------------------------------------------------
# Preflight gate (Decision 010 / R11) — must hold for gold reddyproc-reference
# ---------------------------------------------------------------------------


class TestGoldPreflightGate:
    def test_gold_reddyproc_unapproved_preflight_exits_two(
        self, tmp_path, patch_postproc, monkeypatch
    ):
        silver = _write_silver_csv(tmp_path)
        from miaproc.eddy import RRuntimePreflightResult

        bad_result = RRuntimePreflightResult(
            status="ok",
            approved=True,
            is_project_scoped=False,
            approval_source="MIAPROC_ALLOW_GLOBAL_R=1 env var",
            r_executable="/system/R",
            r_home="/system/R-home",
            r_version="R 4.5.3",
            r_lib_paths=("/system/lib",),
            reddyproc_version="1.3.4",
            rpy2_version="3.6.0",
            repo_root=str(tmp_path),
        )
        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg,
            "preflight_reddyproc_r_environment",
            lambda *, policy=None: bad_result,
        )
        monkeypatch.setattr(
            eddy_pkg, "render_r_preflight_report", lambda r: "preflight report"
        )

        argv = _make_gold_argv(
            tmp_path,
            silver,
            **{
                "--engine": "reddyproc-reference",
                "--repo-root": str(tmp_path),
            },
        )
        with pytest.raises(SystemExit) as exc:
            cli.main(argv)
        assert exc.value.code == cli.PREFLIGHT_NOT_APPROVED_EXIT
        # postproc must not have been reached.
        assert patch_postproc.calls["engine"] is None

    def test_gold_reddyproc_requires_repo_root(
        self, tmp_path, patch_postproc
    ):
        """Validation failure → exit 3 (no preflight invoked)."""
        silver = _write_silver_csv(tmp_path)
        argv = _make_gold_argv(
            tmp_path,
            silver,
            **{"--engine": "reddyproc-reference"},
        )
        # Strip any auto-default --repo-root if present (none added by helper).
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT
        assert patch_postproc.calls["engine"] is None

    def test_gold_reddyproc_approved_preflight_dispatches(
        self, tmp_path, patch_postproc, monkeypatch
    ):
        silver = _write_silver_csv(tmp_path)
        from miaproc.eddy import RRuntimePreflightResult

        ok_result = RRuntimePreflightResult(
            status="ok",
            approved=True,
            is_project_scoped=True,
            approval_source="project-scoped (renv.lock; lib under repo)",
            r_executable="/fake/R",
            r_home="/fake/R-home",
            r_version="R 4.5.3",
            r_lib_paths=("/fake/lib",),
            reddyproc_version="1.3.4",
            rpy2_version="3.6.0",
            repo_root=str(tmp_path),
        )
        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg,
            "preflight_reddyproc_r_environment",
            lambda *, policy=None: ok_result,
        )
        monkeypatch.setattr(
            eddy_pkg, "render_r_preflight_report", lambda r: "preflight report"
        )

        argv = _make_gold_argv(
            tmp_path,
            silver,
            **{
                "--engine": "reddyproc-reference",
                "--repo-root": str(tmp_path),
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert patch_postproc.calls["engine"] == "reddyproc-rpy2"

        # Run JSON records the preflight outcome.
        run = json.loads(
            (tmp_path / "gold_run.json").read_text(encoding="utf-8")
        )
        assert "preflight" in run
        assert run["preflight"]["approval_source"].startswith("project-scoped")


# ---------------------------------------------------------------------------
# Column-attach helper unit tests
# ---------------------------------------------------------------------------


class TestAttachSilverColumnsHelper:
    def test_appends_silver_only_columns_keyed_on_datetime(self):
        silver = _stub_silver_df()
        gold = _stub_gold_df()
        merged, extras = cli._attach_silver_columns_to_gold(gold, silver)
        # Gold contract preserved verbatim, in order.
        for col in (
            "DateTime",
            "NEE",
            "NEE_f",
            "NEE_fqc",
            "GPP",
            "Reco",
            "Tair",
            "Tair_f",
            "Rg",
            "Rg_f",
            "VPD",
            "VPD_f",
            "USTAR",
        ):
            assert col in merged.columns, col
        # Silver-only extras appended.
        for col in ("H", "LE", "P_RAIN", "QC_NEE", "rH"):
            assert col in merged.columns, col
        assert sorted(extras) == sorted(["H", "LE", "P_RAIN", "QC_NEE", "rH"])
        # df.attrs preserved across merge.
        assert merged.attrs.get("miaproc_diagnostics", {}).get(
            "backend"
        ) == "hesseflux"

    def test_no_silver_extras_returns_gold_unchanged(self):
        silver = _stub_silver_df().drop(
            columns=["H", "LE", "P_RAIN", "rH", "QC_NEE"]
        )
        gold = _stub_gold_df()
        merged, extras = cli._attach_silver_columns_to_gold(gold, silver)
        assert extras == []
        assert list(merged.columns) == list(gold.columns)

    def test_missing_datetime_degrades_gracefully(self):
        silver = _stub_silver_df().drop(columns=["DateTime"])
        gold = _stub_gold_df()
        merged, extras = cli._attach_silver_columns_to_gold(gold, silver)
        assert extras == []
        assert list(merged.columns) == list(gold.columns)


# ---------------------------------------------------------------------------
# CSV roundtrip safety: silver written by run-silver is consumable by run-gold
# ---------------------------------------------------------------------------


class TestSilverGoldRoundtrip:
    def test_run_silver_then_run_gold_roundtrip_csv(
        self, tmp_path, patch_load_stage1, patch_postproc
    ):
        # Step 1: run-silver writes silver.csv
        rc1 = cli.main(_make_silver_argv(tmp_path))
        assert rc1 == cli.SUCCESS_EXIT

        # Step 2: run-gold consumes silver.csv
        rc2 = cli.main(
            _make_gold_argv(tmp_path, tmp_path / "silver.csv")
        )
        assert rc2 == cli.SUCCESS_EXIT

        # Gold preserves silver-only columns even after CSV tz round-trip.
        gold_df = pd.read_csv(tmp_path / "gold.csv")
        for col in ("H", "LE", "P_RAIN", "rH"):
            assert col in gold_df.columns, col
        for col in ("GPP", "Reco", "NEE_f"):
            assert col in gold_df.columns, col
