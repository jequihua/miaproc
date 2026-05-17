"""Tests for the production CLI (M6 Task 2).

These tests stub ``load_stage1`` and ``postproc`` so they remain fast,
deterministic, and runnable on default CI (no live R, no real
case-study data). They cover:

- argument parsing and required-flag enforcement;
- engine dispatch for all three CLI run modes;
- the ``reddyproc-reference`` preflight gate (Decision 010);
- wrapper-mode reco_fit_mode wiring (no fallback to native);
- output artifact writing (table, diagnostics JSON, run JSON);
- exit-code semantics for the documented failure paths.
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


def _stub_stage1_df(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "DateTime": pd.date_range("2025-08-01", periods=n, freq="30min", tz="UTC"),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "QC_NEE": [0, 0, 0, 0],
        }
    )


def _stub_postproc_df(diagnostics: dict[str, Any] | None = None) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "DateTime": pd.date_range("2025-08-01", periods=4, freq="30min", tz="UTC"),
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


def _make_io_args(tmp_path: Path, **overrides: Any) -> list[str]:
    flux = tmp_path / "flux"
    biomet = tmp_path / "biomet"
    flux.mkdir(exist_ok=True)
    biomet.mkdir(exist_ok=True)
    base = {
        "--engine": "hesseflux-native",
        "--flux-dir": str(flux),
        "--biomet-dir": str(biomet),
        "--output-table": str(tmp_path / "out.csv"),
        "--output-diagnostics-json": str(tmp_path / "diag.json"),
        "--output-run-json": str(tmp_path / "run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["run"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


@pytest.fixture
def patch_pipeline(monkeypatch):
    """Default-success stubs for ``load_stage1`` + ``postproc``.

    Returns a dict of mutable hooks the test can inspect after
    invocation:

    - ``calls["postproc_kwargs"]`` is the kwargs the CLI passed in;
    - ``calls["postproc_engine"]`` is the engine string dispatched;
    - ``diag`` is the diagnostics payload that will be attached.
    """
    calls: dict[str, Any] = {
        "postproc_kwargs": None,
        "postproc_engine": None,
        "load_kwargs": None,
    }
    diag: dict[str, Any] = {
        "backend": "hesseflux",
        "ustar": {"mode": "dynamic", "selected_threshold": 0.18},
        "partitioning": {
            "method": "lasslop",
            "reco_fit_mode": "native",
            "lt_wrapper": None,
        },
        "warnings": (),
    }

    def _fake_load_stage1(**kwargs):
        calls["load_kwargs"] = kwargs
        return _stub_stage1_df()

    def _fake_postproc(df, *, engine, hesseflux_config=None, reddyproc_config=None):
        calls["postproc_engine"] = engine
        calls["postproc_kwargs"] = {
            "hesseflux_config": hesseflux_config,
            "reddyproc_config": reddyproc_config,
        }
        return _stub_postproc_df(diag)

    import miaproc.eddy as eddy_pkg

    monkeypatch.setattr(eddy_pkg, "load_stage1", _fake_load_stage1)
    monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
    return SimpleNamespace(calls=calls, diag=diag)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestArgParsing:
    def test_run_help_exits_zero(self, capsys):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["run", "--help"])
        assert exc.value.code == 0

    def test_engine_choice_is_enforced(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "run",
                    "--engine", "bogus",
                    "--flux-dir", "x",
                    "--biomet-dir", "y",
                    "--output-table", "t.csv",
                    "--output-diagnostics-json", "d.json",
                    "--output-run-json", "r.json",
                ]
            )

    def test_required_flags_missing_exits(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["run", "--engine", "hesseflux-native"])


# ---------------------------------------------------------------------------
# Engine dispatch
# ---------------------------------------------------------------------------


class TestEngineDispatch:
    def test_hesseflux_native_dispatches_to_hesseflux(self, tmp_path, patch_pipeline):
        rc = cli.main(_make_io_args(tmp_path, **{"--engine": "hesseflux-native"}))
        assert rc == cli.SUCCESS_EXIT
        assert patch_pipeline.calls["postproc_engine"] == "hesseflux"
        cfg = patch_pipeline.calls["postproc_kwargs"]["hesseflux_config"]
        assert cfg.reco_fit_mode == "native"
        assert cfg.partition_method == "lasslop"
        assert cfg.swthr == 20.0
        assert cfg.nogppnight is False
        assert cfg.ustar_mode == "dynamic"

    def test_hesseflux_ltwrapper_sets_lt_mode(self, tmp_path, patch_pipeline):
        rc = cli.main(
            _make_io_args(
                tmp_path,
                **{
                    "--engine": "hesseflux-ltwrapper",
                    "--lt-min-night-samples": "75",
                },
            )
        )
        assert rc == cli.SUCCESS_EXIT
        cfg = patch_pipeline.calls["postproc_kwargs"]["hesseflux_config"]
        assert cfg.reco_fit_mode == "lt_reddyproc_aligned"
        assert cfg.lt_min_night_samples == 75

    def test_reddyproc_reference_dispatches_after_preflight(
        self, tmp_path, patch_pipeline, monkeypatch
    ):
        """When the preflight returns a project-scoped approval, the CLI
        must call postproc with ``engine='reddyproc-rpy2'``."""
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        # Stub the preflight + render to skip live R discovery.
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
            repo_root=str(repo_root),
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

        rc = cli.main(
            _make_io_args(
                tmp_path,
                **{
                    "--engine": "reddyproc-reference",
                    "--repo-root": str(repo_root),
                },
            )
        )
        assert rc == cli.SUCCESS_EXIT
        assert patch_pipeline.calls["postproc_engine"] == "reddyproc-rpy2"
        rcfg = patch_pipeline.calls["postproc_kwargs"]["reddyproc_config"]
        assert rcfg.site_name == "Marismas_Nacionales"
        assert rcfg.local_tz == "America/Mazatlan"


# ---------------------------------------------------------------------------
# Preflight gate
# ---------------------------------------------------------------------------


class TestPreflightGate:
    def test_unapproved_preflight_exits_two(
        self, tmp_path, patch_pipeline, monkeypatch
    ):
        """Decision 010 / R11: if approval_source does not start with
        'project-scoped', the CLI must exit 2 and never call
        ``postproc``."""
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
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
            repo_root=str(repo_root),
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

        with pytest.raises(SystemExit) as exc:
            cli.main(
                _make_io_args(
                    tmp_path,
                    **{
                        "--engine": "reddyproc-reference",
                        "--repo-root": str(repo_root),
                    },
                )
            )
        assert exc.value.code == cli.PREFLIGHT_NOT_APPROVED_EXIT
        # postproc must not have been reached.
        assert patch_pipeline.calls["postproc_engine"] is None

    def test_reddyproc_reference_requires_repo_root(self, tmp_path, patch_pipeline):
        """Validation failure → exit 3 (no preflight invoked)."""
        argv = _make_io_args(tmp_path, **{"--engine": "reddyproc-reference"})
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT
        assert patch_pipeline.calls["postproc_engine"] is None


# ---------------------------------------------------------------------------
# Output artifact contract
# ---------------------------------------------------------------------------


class TestOutputArtifacts:
    def test_writes_table_diagnostics_and_run_metadata(
        self, tmp_path, patch_pipeline
    ):
        argv = _make_io_args(tmp_path)
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        table = tmp_path / "out.csv"
        diag = tmp_path / "diag.json"
        run = tmp_path / "run.json"
        assert table.exists()
        assert diag.exists()
        assert run.exists()

        # Table contains the contract columns.
        df = pd.read_csv(table)
        for col in ("DateTime", "NEE_f", "GPP", "Reco", "USTAR"):
            assert col in df.columns

        # Diagnostics JSON contains the backend payload.
        diag_payload = json.loads(diag.read_text(encoding="utf-8"))
        assert diag_payload["backend"] == "hesseflux"
        assert diag_payload["partitioning"]["method"] == "lasslop"
        assert diag_payload["partitioning"]["reco_fit_mode"] == "native"

        # Run metadata JSON has the documented top-level keys.
        run_payload = json.loads(run.read_text(encoding="utf-8"))
        for key in (
            "engine",
            "config",
            "timestamps",
            "row_counts",
            "inputs",
            "outputs",
            "versions",
            "exit_code",
        ):
            assert key in run_payload, key
        assert run_payload["engine"] == "hesseflux-native"
        assert run_payload["exit_code"] == cli.SUCCESS_EXIT
        assert run_payload["outputs"]["table_format"] == "csv"

    def test_unsupported_table_extension_exits_three(self, tmp_path, patch_pipeline):
        argv = _make_io_args(
            tmp_path, **{"--output-table": str(tmp_path / "out.xlsx")}
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------


class TestFailurePaths:
    def test_runtime_failure_returns_four(
        self, tmp_path, patch_pipeline, monkeypatch
    ):
        """When ``postproc`` raises (e.g. wrapper LTWrapperError or any
        other engine failure), the CLI must surface exit code 4 — not
        silently fall back."""
        import miaproc.eddy as eddy_pkg

        def _raises(*a, **kw):
            raise RuntimeError("simulated engine failure")

        monkeypatch.setattr(eddy_pkg, "postproc", _raises)
        rc = cli.main(_make_io_args(tmp_path))
        assert rc == cli.RUNTIME_EXIT

    def test_missing_input_dir_exits_three(self, tmp_path, patch_pipeline):
        """flux-dir does not exist → validation failure (exit 3)."""
        argv = _make_io_args(tmp_path)
        # Replace --flux-dir with a path that does not exist.
        bogus = str(tmp_path / "does_not_exist")
        for i, token in enumerate(argv):
            if token == "--flux-dir":
                argv[i + 1] = bogus
                break
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT
