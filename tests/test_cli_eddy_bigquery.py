"""Tests for the BigQuery-native CLI subcommand (M7).

Stubs the BigQuery read + ``load_stage1_from_dataframes`` + ``postproc``
so the tests are fast, deterministic, and runnable on default CI (no
live BigQuery, no live R, no real case-study data). They cover:

- the new module-aware namespace (``miaproc eddy run-bigquery``);
- argument parsing for the BigQuery flags;
- engine dispatch for all three CLI run modes;
- the ``reddyproc-reference`` preflight gate (Decision 010);
- the local-disk-only artifact contract;
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
from miaproc.eddy import BigQueryReadResult


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


def _stub_postproc_df() -> pd.DataFrame:
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
    df.attrs["miaproc_diagnostics"] = {
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


def _make_bq_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    base = {
        "--engine": "hesseflux-native",
        "--bq-input-project": "manglaria",
        "--bq-input-dataset": "manglaria_lakehouse_ds",
        "--bq-flux-table": "carbon_flux_eddycovariance",
        "--bq-biomet-table": "carbon_flux_biomet",
        "--output-table": str(tmp_path / "out.csv"),
        "--output-diagnostics-json": str(tmp_path / "diag.json"),
        "--output-run-json": str(tmp_path / "run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["eddy", "run-bigquery"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


@pytest.fixture
def patch_bq_pipeline(monkeypatch):
    """Default-success stubs for the BigQuery + stage-1 + postproc chain."""
    calls: dict[str, Any] = {
        "bq_cfg": None,
        "stage1_kwargs": None,
        "postproc_engine": None,
        "postproc_kwargs": None,
    }

    flux_df = pd.DataFrame({"timestamp": ["2025-08-01"], "site_id": ["RBRL"]})
    biomet_df = pd.DataFrame({"timestamp": ["2025-08-01"], "site_id": ["RBRL"]})
    fake_result = BigQueryReadResult(
        flux_df=flux_df,
        biomet_df=biomet_df,
        flux_rows=len(flux_df),
        biomet_rows=len(biomet_df),
        flux_query="SELECT * FROM flux WHERE site_id = @site_id",
        biomet_query="SELECT * FROM biomet WHERE site_id = @site_id",
        query_parameters={"site_id": "RBRL"},
    )

    def _fake_read(cfg, *, client=None):
        calls["bq_cfg"] = cfg
        return fake_result

    def _fake_load(**kwargs):
        calls["stage1_kwargs"] = kwargs
        return _stub_stage1_df()

    def _fake_postproc(df, *, engine, hesseflux_config=None, reddyproc_config=None):
        calls["postproc_engine"] = engine
        calls["postproc_kwargs"] = {
            "hesseflux_config": hesseflux_config,
            "reddyproc_config": reddyproc_config,
        }
        return _stub_postproc_df()

    import miaproc.eddy as eddy_pkg

    monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
    monkeypatch.setattr(eddy_pkg, "load_stage1_from_dataframes", _fake_load)
    monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
    monkeypatch.setattr(
        eddy_pkg,
        "read_final_table_columns",
        lambda cfg, *, client=None: [
            "primary_key",
            "site_id",
            "timestamp",
            "dateAndTime",
            "nee_f",
            "nee_fqc",
            "sw_in_f",
            "ta_f",
            "vpd_f",
        ],
    )
    monkeypatch.setattr(
        eddy_pkg,
        "read_final_table_schema",
        lambda cfg, *, client=None: {},
    )
    return SimpleNamespace(calls=calls)


# ---------------------------------------------------------------------------
# Argument parsing + namespace shape
# ---------------------------------------------------------------------------


class TestNamespaceAndParsing:
    def test_eddy_namespace_help_exits_zero(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["eddy", "--help"])
        assert exc.value.code == 0

    def test_eddy_run_bigquery_help_exits_zero(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["eddy", "run-bigquery", "--help"])
        assert exc.value.code == 0

    def test_existing_run_namespace_unchanged(self):
        """Regression: 'miaproc run --help' must still work."""
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["run", "--help"])
        assert exc.value.code == 0

    def test_required_bq_flags_are_enforced(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["eddy", "run-bigquery", "--engine", "hesseflux-native"])


# ---------------------------------------------------------------------------
# Engine dispatch (all three modes)
# ---------------------------------------------------------------------------


class TestBigQueryEngineDispatch:
    def test_hesseflux_native_dispatch(self, tmp_path, patch_bq_pipeline):
        rc = cli.main(_make_bq_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        assert patch_bq_pipeline.calls["postproc_engine"] == "hesseflux"
        cfg = patch_bq_pipeline.calls["postproc_kwargs"]["hesseflux_config"]
        assert cfg.reco_fit_mode == "native"
        assert cfg.partition_method == "lasslop"
        # Stage-1 is called with the dataframes from the fake BQ read.
        # M24: ungrouped CLI runs pass site_id=None to the loader.
        stage1 = patch_bq_pipeline.calls["stage1_kwargs"]
        assert stage1["site_id"] is None
        assert isinstance(stage1["flux_df"], pd.DataFrame)
        assert isinstance(stage1["biomet_df"], pd.DataFrame)

    def test_hesseflux_ltwrapper_dispatch(self, tmp_path, patch_bq_pipeline):
        rc = cli.main(
            _make_bq_argv(
                tmp_path, **{"--engine": "hesseflux-ltwrapper"}
            )
        )
        assert rc == cli.SUCCESS_EXIT
        cfg = patch_bq_pipeline.calls["postproc_kwargs"]["hesseflux_config"]
        assert cfg.reco_fit_mode == "lt_reddyproc_aligned"

    def test_reddyproc_reference_after_preflight(
        self, tmp_path, patch_bq_pipeline, monkeypatch
    ):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
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
            _make_bq_argv(
                tmp_path,
                **{
                    "--engine": "reddyproc-reference",
                    "--repo-root": str(repo_root),
                },
            )
        )
        assert rc == cli.SUCCESS_EXIT
        assert patch_bq_pipeline.calls["postproc_engine"] == "reddyproc-rpy2"


# ---------------------------------------------------------------------------
# Preflight gate (Decision 010 / R11) on the BigQuery path
# ---------------------------------------------------------------------------


class TestBigQueryPreflightGate:
    def test_unapproved_preflight_exits_two(
        self, tmp_path, patch_bq_pipeline, monkeypatch
    ):
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
                _make_bq_argv(
                    tmp_path,
                    **{
                        "--engine": "reddyproc-reference",
                        "--repo-root": str(repo_root),
                    },
                )
            )
        assert exc.value.code == cli.PREFLIGHT_NOT_APPROVED_EXIT
        assert patch_bq_pipeline.calls["postproc_engine"] is None

    def test_reddyproc_reference_requires_repo_root(self, tmp_path, patch_bq_pipeline):
        argv = _make_bq_argv(tmp_path, **{"--engine": "reddyproc-reference"})
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT
        assert patch_bq_pipeline.calls["postproc_engine"] is None


# ---------------------------------------------------------------------------
# Output artifact contract (local-disk only; no BigQuery write-back)
# ---------------------------------------------------------------------------


class TestBigQueryOutputArtifacts:
    def test_writes_table_diagnostics_and_run_metadata(
        self, tmp_path, patch_bq_pipeline
    ):
        argv = _make_bq_argv(tmp_path)
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        table = tmp_path / "out.csv"
        diag = tmp_path / "diag.json"
        run = tmp_path / "run.json"
        assert table.exists()
        assert diag.exists()
        assert run.exists()

        run_payload = json.loads(run.read_text(encoding="utf-8"))
        assert run_payload["engine"] == "hesseflux-native"
        assert run_payload["exit_code"] == cli.SUCCESS_EXIT
        # BigQuery-mode-specific input metadata.
        inputs = run_payload["inputs"]
        assert inputs["mode"] == "bigquery"
        assert inputs["input_project"] == "manglaria"
        assert inputs["input_dataset"] == "manglaria_lakehouse_ds"
        assert inputs["flux_table"] == "carbon_flux_eddycovariance"
        assert inputs["biomet_table"] == "carbon_flux_biomet"
        # M24: --site-id is no longer a CLI surface; the BQ read no
        # longer injects a WHERE site_id filter from the CLI and the
        # ungrouped run records group_column=None.
        assert "site_id" not in inputs
        assert inputs["group_column"] is None
        assert inputs["read_row_counts"]["flux"] == 1
        assert inputs["read_row_counts"]["biomet"] == 1
        # Local-disk only contract: no BigQuery write-back is claimed.
        assert run_payload["outputs"]["bigquery_writeback"] is False

    def test_unsupported_extension_exits_three(self, tmp_path, patch_bq_pipeline):
        argv = _make_bq_argv(
            tmp_path, **{"--output-table": str(tmp_path / "out.xlsx")}
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------


class TestPrepareProcessedForStage:
    """M9 helper that materializes the BigQuery stage identity columns
    (``site_id``, ``timestamp``, ``primary_key``) on the processed
    output before it goes to ``run_writeback``."""

    def test_attaches_identity_columns_keyed_on_site_and_datetime(self):
        from miaproc.cli import _prepare_processed_for_stage

        df = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE_f": [0.1, 0.2, 0.3],
            }
        )
        out = _prepare_processed_for_stage(df, site_id="RBRL")
        assert list(out.columns[:3]) == ["primary_key", "site_id", "timestamp"]
        assert (out["site_id"] == "RBRL").all()
        # primary_key uniqueness is equivalent to (site_id, timestamp)
        # uniqueness by construction.
        assert out["primary_key"].is_unique
        # Original DateTime column survives.
        assert "DateTime" in out.columns
        # timestamp dtype is tz-aware UTC, so BigQuery loads it as TIMESTAMP.
        assert str(out["timestamp"].dtype).startswith("datetime64[ns, UTC")

    def test_missing_datetime_raises(self):
        from miaproc.cli import _prepare_processed_for_stage

        with pytest.raises(ValueError, match="missing the 'DateTime' column"):
            _prepare_processed_for_stage(
                pd.DataFrame({"NEE_f": [0.1]}), site_id="RBRL"
            )


class TestBigQueryWritebackWiring:
    """M8 writeback flag wiring on top of the M7 read path."""

    def _run_id(self) -> str:
        return "local-test-run"

    def test_partial_writeback_flags_exit_three(self, tmp_path, patch_bq_pipeline):
        """Setting --bq-output-project without --bq-stage-table is a
        validation failure (writeback flags are not silently ignored)."""
        argv = _make_bq_argv(
            tmp_path,
            **{"--bq-output-project": "manglaria-staging"},
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_allow_final_merge_without_stage_exit_three(
        self, tmp_path, patch_bq_pipeline
    ):
        """--bq-allow-final-merge requires --bq-stage-table + --bq-final-table."""
        argv = _make_bq_argv(tmp_path)
        # Inject the bare flag with no companion writeback flags.
        argv.extend(["--bq-allow-final-merge"])
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_output_project_must_differ_from_input_project(
        self, tmp_path, patch_bq_pipeline
    ):
        """Writeback against the production input project is forbidden
        at the CLI argument-validation layer."""
        argv = _make_bq_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_test",
                "--bq-output-project": "manglaria",  # same as input
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_stage_only_default_engages_writeback_without_merge(
        self, tmp_path, patch_bq_pipeline, monkeypatch
    ):
        """When --bq-stage-table is set but --bq-allow-final-merge is
        not, the CLI engages run_writeback with allow_final_merge=False
        and records the writeback outcome in run.json."""
        from miaproc.eddy import WritebackResult

        captured: dict[str, Any] = {}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["cfg"] = cfg
            captured["run_id"] = run_id
            captured["site_id"] = site_id
            captured["stage_df"] = df
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=4,
                merge_attempted=False,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": 4},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)

        argv = _make_bq_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
                "--bq-run-id": self._run_id(),
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert captured["cfg"].allow_final_merge is False
        assert captured["cfg"].stage_table == "cf_s2_stage_test"
        assert captured["cfg"].output_project == "manglaria-staging"
        # M24: ungrouped CLI runs no longer pass site_id to run_writeback.
        assert captured["site_id"] is None
        assert captured["run_id"] == self._run_id()

        run_payload = json.loads((tmp_path / "run.json").read_text("utf-8"))
        assert run_payload["outputs"]["bigquery_writeback"] is True
        assert run_payload["writeback"]["status"] == "stage_only_succeeded"
        assert run_payload["writeback"]["merge_attempted"] is False
        assert run_payload["writeback"]["merge_authorized"] is False
        assert run_payload["writeback"]["watermark_advanced"] is False

    def test_explicit_merge_passes_authorization_through(
        self, tmp_path, patch_bq_pipeline, monkeypatch
    ):
        from miaproc.eddy import WritebackResult

        captured: dict[str, Any] = {}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["cfg"] = cfg
            return WritebackResult(
                run_id=run_id,
                status="succeeded",
                stage_rows=4,
                merge_attempted=True,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=4,
                merge_updated_rows=0,
                watermark_advanced=True,
                watermark_value="2025-08-01T01:30:00+00:00",
                validation_metrics={"row_count": 4},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)

        argv = _make_bq_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
                "--bq-final-table": "carbon_flux_eddycovariance_s2_filt_1",
            },
        )
        argv.extend(["--bq-allow-final-merge"])
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert captured["cfg"].allow_final_merge is True
        assert (
            captured["cfg"].final_table
            == "carbon_flux_eddycovariance_s2_filt_1"
        )

        run_payload = json.loads((tmp_path / "run.json").read_text("utf-8"))
        assert run_payload["writeback"]["merge_attempted"] is True
        assert run_payload["writeback"]["merge_authorized"] is True
        assert run_payload["writeback"]["watermark_advanced"] is True

    def test_writeback_failure_returns_four(
        self, tmp_path, patch_bq_pipeline, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _boom(*a, **kw):
            raise RuntimeError("simulated stage write failure")

        monkeypatch.setattr(eddy_pkg, "run_writeback", _boom)

        argv = _make_bq_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT
        # run.json was still written (best-effort audit trail).
        run_payload = json.loads((tmp_path / "run.json").read_text("utf-8"))
        assert run_payload["writeback"]["status"] == "failed"
        assert run_payload["exit_code"] == cli.RUNTIME_EXIT

    def test_failed_merge_local_runjson_reports_merge_attempted(
        self, tmp_path, patch_bq_pipeline, monkeypatch
    ):
        """M10 fix for the M9 P2 drift: when MERGE itself fails, the
        local best-effort ``run.json`` must record
        ``writeback.merge_attempted = true`` so it agrees with the
        authoritative ``cf_s2_runs`` row. Implementation hook: the CLI
        reads ``exc.miaproc_writeback_state`` that ``run_writeback``
        attaches before re-raising."""
        import miaproc.eddy as eddy_pkg

        def _merge_boom(*a, **kw):
            exc = RuntimeError("simulated MERGE failure (Unrecognized name: X)")
            exc.miaproc_writeback_state = {
                "merge_attempted": True,
                "merge_authorized": True,
                "stage_rows": 4813,
                "status": "failed",
            }
            raise exc

        monkeypatch.setattr(eddy_pkg, "run_writeback", _merge_boom)

        argv = _make_bq_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
                "--bq-final-table": "carbon_flux_eddycovariance_s2_filt_1",
            },
        )
        argv.extend(["--bq-allow-final-merge"])
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT
        run_payload = json.loads((tmp_path / "run.json").read_text("utf-8"))
        assert run_payload["writeback"]["status"] == "failed"
        assert run_payload["writeback"]["merge_attempted"] is True
        assert run_payload["writeback"]["merge_authorized"] is True
        assert run_payload["writeback"]["stage_rows"] == 4813


class TestBigQueryFailurePaths:
    def test_runtime_failure_returns_four(self, tmp_path, patch_bq_pipeline, monkeypatch):
        import miaproc.eddy as eddy_pkg

        def _raises(*a, **kw):
            raise RuntimeError("simulated engine failure")

        monkeypatch.setattr(eddy_pkg, "postproc", _raises)
        rc = cli.main(_make_bq_argv(tmp_path))
        assert rc == cli.RUNTIME_EXIT

    def test_bigquery_read_failure_returns_four(
        self, tmp_path, patch_bq_pipeline, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _raises(*a, **kw):
            raise RuntimeError("simulated BigQuery read failure")

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _raises)
        rc = cli.main(_make_bq_argv(tmp_path))
        assert rc == cli.RUNTIME_EXIT
