"""Tests for the M22 BigQuery silver/gold split.

Stubs the BigQuery read + ``load_stage1_from_dataframes`` + ``postproc``
+ ``run_writeback`` so the tests are fast, deterministic, and runnable
on default CI (no live BigQuery, no live R, no real case-study data).

Coverage:

- argument parsing for both new subcommands;
- required flag validation;
- ``run-bigquery-silver`` does not accept ``--engine`` / ``--repo-root``
  and does not invoke the project-scoped preflight;
- ``run-bigquery-gold --engine reddyproc-reference`` requires
  ``--repo-root`` and runs the preflight gate;
- BigQuery read wiring for bronze/source -> silver and silver -> gold;
- stage-only writeback engagement for silver and gold;
- gold final MERGE gate: no final mutation without
  ``--bq-allow-final-merge``;
- failure run JSON shape and exit-code propagation;
- silver columns appended on gold output (M14 column-preservation
  contract preserved over the BigQuery split);
- non-regression of the existing one-shot ``run-bigquery``.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from miaproc import cli
from miaproc.eddy import (
    BigQueryReadResult,
    BigQuerySilverReadResult,
    WritebackResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stub_silver_df(n: int = 4) -> pd.DataFrame:
    """A silver-shaped frame: stage-1 columns + extras the backend
    will not produce (so the gold-side column-preservation contract
    has something silver-only to append)."""
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
            "H": [10.0, 20.0, 30.0, 40.0],
            "LE": [50.0, 60.0, 70.0, 80.0],
            "P_RAIN": [0.0, 0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0, 90.0],
        }
    )


def _stub_gold_df() -> pd.DataFrame:
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


def _make_silver_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    base = {
        "--bq-input-project": "manglaria",
        "--bq-input-dataset": "manglaria_lakehouse_ds",
        "--bq-flux-table": "carbon_flux_eddycovariance",
        "--bq-biomet-table": "carbon_flux_biomet",
        "--output-table": str(tmp_path / "silver.csv"),
        "--output-run-json": str(tmp_path / "silver_run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["eddy", "run-bigquery-silver"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


def _make_gold_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    base = {
        "--bq-input-project": "manglaria-staging",
        "--bq-input-dataset": "manglaria_lakehouse_ds",
        "--bq-silver-table": "cf_s2_stage_silver_rbrl",
        "--engine": "hesseflux-native",
        "--output-table": str(tmp_path / "gold.csv"),
        "--output-diagnostics-json": str(tmp_path / "gold_diag.json"),
        "--output-run-json": str(tmp_path / "gold_run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["eddy", "run-bigquery-gold"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


@pytest.fixture
def patch_silver_pipeline(monkeypatch):
    calls: dict[str, Any] = {
        "bq_cfg": None,
        "stage1_kwargs": None,
    }

    flux_df = pd.DataFrame({"timestamp": ["2025-08-01"], "site_id": ["RBRL"]})
    biomet_df = pd.DataFrame(
        {"timestamp": ["2025-08-01"], "site_id": ["RBRL"]}
    )
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
        return _stub_silver_df()

    import miaproc.eddy as eddy_pkg

    monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
    monkeypatch.setattr(eddy_pkg, "load_stage1_from_dataframes", _fake_load)
    return SimpleNamespace(calls=calls)


@pytest.fixture
def patch_gold_pipeline(monkeypatch):
    calls: dict[str, Any] = {
        "silver_cfg": None,
        "postproc_engine": None,
        "postproc_input": None,
    }

    fake_silver_result = BigQuerySilverReadResult(
        silver_df=_stub_silver_df(),
        silver_rows=4,
        silver_query=(
            "SELECT * FROM `manglaria-staging.manglaria_lakehouse_ds."
            "cf_s2_stage_silver_rbrl`\nWHERE site_id = @site_id"
        ),
        query_parameters={"site_id": "RBRL"},
    )

    def _fake_silver_read(cfg, *, client=None):
        calls["silver_cfg"] = cfg
        return fake_silver_result

    def _fake_postproc(df, *, engine, hesseflux_config=None, reddyproc_config=None):
        calls["postproc_engine"] = engine
        calls["postproc_input"] = df
        return _stub_gold_df()

    import miaproc.eddy as eddy_pkg

    monkeypatch.setattr(
        eddy_pkg, "read_bigquery_silver_input", _fake_silver_read
    )
    monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
    return SimpleNamespace(calls=calls)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestArgParsing:
    def test_silver_help_exits_zero(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["eddy", "run-bigquery-silver", "--help"])
        assert exc.value.code == 0

    def test_gold_help_exits_zero(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["eddy", "run-bigquery-gold", "--help"])
        assert exc.value.code == 0

    def test_silver_required_flags_missing_exits(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["eddy", "run-bigquery-silver"])

    def test_gold_required_flags_missing_exits(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["eddy", "run-bigquery-gold"])

    def test_silver_does_not_accept_engine(self, tmp_path):
        """Silver is stage-1 only; --engine is not part of its surface."""
        parser = cli._build_parser()
        argv = _make_silver_argv(tmp_path)
        argv.extend(["--engine", "hesseflux-native"])
        with pytest.raises(SystemExit):
            parser.parse_args(argv)

    def test_silver_does_not_accept_repo_root(self, tmp_path):
        parser = cli._build_parser()
        argv = _make_silver_argv(tmp_path)
        argv.extend(["--repo-root", str(tmp_path)])
        with pytest.raises(SystemExit):
            parser.parse_args(argv)

    def test_silver_does_not_accept_final_or_merge_flags(self, tmp_path):
        """M22 silver writeback is stage-only — no merge surface."""
        parser = cli._build_parser()
        argv = _make_silver_argv(tmp_path)
        argv.extend(["--bq-allow-final-merge"])
        with pytest.raises(SystemExit):
            parser.parse_args(argv)

    def test_gold_engine_default_is_reddyproc_reference(self, tmp_path):
        parser = cli._build_parser()
        ns = parser.parse_args(
            [
                "eddy",
                "run-bigquery-gold",
                "--bq-input-project",
                "manglaria-staging",
                "--bq-input-dataset",
                "manglaria_lakehouse_ds",
                "--bq-silver-table",
                "cf_s2_stage_silver_rbrl",
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


# ---------------------------------------------------------------------------
# Silver stage
# ---------------------------------------------------------------------------


class TestBigQuerySilverStage:
    def test_silver_writes_table_and_run_json(
        self, tmp_path, patch_silver_pipeline
    ):
        rc = cli.main(_make_silver_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        silver_path = tmp_path / "silver.csv"
        run_path = tmp_path / "silver_run.json"
        assert silver_path.exists()
        assert run_path.exists()

        df = pd.read_csv(silver_path)
        for col in ("DateTime", "NEE", "USTAR", "Tair", "Rg", "QC_NEE"):
            assert col in df.columns, col
        for col in ("H", "LE", "P_RAIN", "rH"):
            assert col in df.columns, col

        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["stage"] == "silver"
        assert run["command"] == "eddy run-bigquery-silver"
        assert run["exit_code"] == cli.SUCCESS_EXIT
        assert run["row_counts"]["silver"] == 4
        assert run["outputs"]["table_format"] == "csv"
        assert run["outputs"]["bigquery_writeback"] is False
        assert run["inputs"]["mode"] == "bigquery"
        # M24: --site-id is no longer a CLI surface; ungrouped run
        # records group_column=None and the BQ read does not inject a
        # site_id filter.
        assert "site_id" not in run["inputs"]
        assert run["inputs"]["group_column"] is None
        kwargs = patch_silver_pipeline.calls["stage1_kwargs"]
        assert kwargs["site_id"] is None
        assert kwargs["drop_rain_rows"] is False
        assert isinstance(kwargs["flux_df"], pd.DataFrame)
        assert isinstance(kwargs["biomet_df"], pd.DataFrame)

    def test_silver_does_not_invoke_preflight(
        self, tmp_path, patch_silver_pipeline, monkeypatch
    ):
        called: dict[str, bool] = {"preflight": False}

        def _trip(*_a, **_kw):
            called["preflight"] = True
            raise AssertionError("preflight should not run during silver")

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "preflight_reddyproc_r_environment", _trip
        )
        rc = cli.main(_make_silver_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        assert called["preflight"] is False

    def test_silver_unsupported_extension_exits_three(
        self, tmp_path, patch_silver_pipeline
    ):
        argv = _make_silver_argv(
            tmp_path, **{"--output-table": str(tmp_path / "silver.xlsx")}
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_silver_runtime_failure_returns_four(
        self, tmp_path, patch_silver_pipeline, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _raises(**_kw):
            raise RuntimeError("simulated stage-1 failure")

        monkeypatch.setattr(eddy_pkg, "load_stage1_from_dataframes", _raises)
        rc = cli.main(_make_silver_argv(tmp_path))
        assert rc == cli.RUNTIME_EXIT

    def test_silver_writeback_partial_flags_exit_three(
        self, tmp_path, patch_silver_pipeline
    ):
        """--bq-output-project without --bq-stage-table is rejected."""
        argv = _make_silver_argv(
            tmp_path,
            **{"--bq-output-project": "manglaria-staging"},
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_silver_staging_to_staging_writeback_accepted(
        self, tmp_path, patch_silver_pipeline, monkeypatch
    ):
        """M26: bronze/source mirrored into ``manglaria-staging`` may be
        read and the silver stage written back to the same staging
        project. The CLI no longer rejects same-project input/output."""
        captured: dict[str, Any] = {}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["cfg"] = cfg
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-input-project": "manglaria-staging",
                "--bq-output-project": "manglaria-staging",
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        cfg = captured["cfg"]
        assert cfg.output_project == "manglaria-staging"
        assert cfg.final_table is None
        assert cfg.allow_final_merge is False

    def test_silver_writes_to_forbidden_production_project_runtime_fails(
        self, tmp_path, patch_silver_pipeline
    ):
        """The hard production-read-only invariant lives in
        ``BigQueryWritebackConfig.forbidden_write_projects=("manglaria",)``;
        attempting to write to that project surfaces as a writeback
        failure (RUNTIME_EXIT). M26 removed the CLI equality guard
        because bronze/source may legitimately live in the staging
        project, but production writes remain blocked."""
        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT

    def test_silver_stage_only_writeback_engages(
        self, tmp_path, patch_silver_pipeline, monkeypatch
    ):
        captured: dict[str, Any] = {}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["cfg"] = cfg
            captured["run_id"] = run_id
            captured["site_id"] = site_id
            captured["df_columns"] = list(df.columns)
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_silver_rbrl",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
                "--bq-run-id": "local-test-silver",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        # Stage-only is the only mode for silver.
        assert captured["cfg"].allow_final_merge is False
        assert captured["cfg"].final_table is None
        assert captured["cfg"].stage_table == "cf_s2_stage_silver_rbrl"
        assert captured["cfg"].output_project == "manglaria-staging"
        # M24: ungrouped CLI runs do not pass site_id to run_writeback.
        assert captured["site_id"] is None
        assert captured["run_id"] == "local-test-silver"
        # Identity columns are added before staging.
        for col in ("primary_key", "site_id", "timestamp"):
            assert col in captured["df_columns"], col

        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        assert run["outputs"]["bigquery_writeback"] is True
        assert run["writeback"]["status"] == "stage_only_succeeded"
        assert run["writeback"]["merge_attempted"] is False
        assert run["writeback"]["merge_authorized"] is False
        assert run["writeback"]["watermark_advanced"] is False

    def test_silver_writeback_failure_returns_four(
        self, tmp_path, patch_silver_pipeline, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _boom(*_a, **_kw):
            raise RuntimeError("simulated silver stage write failure")

        monkeypatch.setattr(eddy_pkg, "run_writeback", _boom)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_silver_rbrl",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT
        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        assert run["writeback"]["status"] == "failed"
        assert run["exit_code"] == cli.RUNTIME_EXIT


# ---------------------------------------------------------------------------
# Gold stage
# ---------------------------------------------------------------------------


class TestBigQueryGoldStage:
    def test_gold_consumes_silver_and_writes_artifacts(
        self, tmp_path, patch_gold_pipeline
    ):
        rc = cli.main(_make_gold_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT

        gold_path = tmp_path / "gold.csv"
        diag_path = tmp_path / "gold_diag.json"
        run_path = tmp_path / "gold_run.json"
        assert gold_path.exists()
        assert diag_path.exists()
        assert run_path.exists()

        # postproc was called with the silver frame from BigQuery.
        assert patch_gold_pipeline.calls["postproc_engine"] == "hesseflux"
        passed_in = patch_gold_pipeline.calls["postproc_input"]
        assert "DateTime" in passed_in.columns
        assert "H" in passed_in.columns

        # M14 silver-to-gold column-preservation contract over BigQuery.
        gold_df = pd.read_csv(gold_path)
        for col in (
            "DateTime", "NEE", "NEE_f", "NEE_fqc", "GPP", "Reco",
            "Tair", "Tair_f", "Rg", "Rg_f", "VPD", "VPD_f", "USTAR",
        ):
            assert col in gold_df.columns, col
        for col in ("H", "LE", "P_RAIN", "QC_NEE", "rH"):
            assert col in gold_df.columns, col

        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["stage"] == "gold"
        assert run["command"] == "eddy run-bigquery-gold"
        assert run["engine"] == "hesseflux-native"
        assert run["exit_code"] == cli.SUCCESS_EXIT
        assert run["inputs"]["mode"] == "bigquery"
        assert run["inputs"]["silver_table"] == "cf_s2_stage_silver_rbrl"
        # M24: --site-id is no longer a CLI surface; the silver BQ
        # read does not inject a WHERE site_id filter from the CLI.
        assert "site_id" not in run["inputs"]
        assert run["inputs"]["group_column"] is None
        assert run["row_counts"]["silver_input"] == 4
        assert run["row_counts"]["gold_output"] == 4
        # Silver-only columns appended to gold.
        appended = run["silver_columns_appended"]
        for col in ("H", "LE", "P_RAIN", "QC_NEE", "rH"):
            assert col in appended, col

    def test_gold_reddyproc_requires_repo_root(
        self, tmp_path, patch_gold_pipeline
    ):
        argv = _make_gold_argv(
            tmp_path, **{"--engine": "reddyproc-reference"}
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT
        assert patch_gold_pipeline.calls["postproc_engine"] is None

    def test_gold_reddyproc_unapproved_preflight_exits_two(
        self, tmp_path, patch_gold_pipeline, monkeypatch
    ):
        from miaproc.eddy import RRuntimePreflightResult

        bad = RRuntimePreflightResult(
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
            lambda *, policy=None: bad,
        )
        monkeypatch.setattr(
            eddy_pkg, "render_r_preflight_report", lambda r: "preflight report"
        )

        argv = _make_gold_argv(
            tmp_path,
            **{
                "--engine": "reddyproc-reference",
                "--repo-root": str(tmp_path),
            },
        )
        with pytest.raises(SystemExit) as exc:
            cli.main(argv)
        assert exc.value.code == cli.PREFLIGHT_NOT_APPROVED_EXIT
        assert patch_gold_pipeline.calls["postproc_engine"] is None

    def test_gold_hesseflux_does_not_invoke_preflight(
        self, tmp_path, patch_gold_pipeline, monkeypatch
    ):
        called: dict[str, bool] = {"preflight": False}

        def _trip(*_a, **_kw):
            called["preflight"] = True
            raise AssertionError(
                "preflight should not run for hesseflux gold"
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "preflight_reddyproc_r_environment", _trip
        )
        rc = cli.main(_make_gold_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        assert called["preflight"] is False

    def test_gold_runtime_failure_returns_four(
        self, tmp_path, patch_gold_pipeline, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _raises(*_a, **_kw):
            raise RuntimeError("simulated engine failure")

        monkeypatch.setattr(eddy_pkg, "postproc", _raises)
        rc = cli.main(_make_gold_argv(tmp_path))
        assert rc == cli.RUNTIME_EXIT

    def test_gold_silver_read_failure_returns_four(
        self, tmp_path, patch_gold_pipeline, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _raises(*_a, **_kw):
            raise RuntimeError("simulated silver-table read failure")

        monkeypatch.setattr(eddy_pkg, "read_bigquery_silver_input", _raises)
        rc = cli.main(_make_gold_argv(tmp_path))
        assert rc == cli.RUNTIME_EXIT

    def test_gold_partial_writeback_flags_exit_three(
        self, tmp_path, patch_gold_pipeline
    ):
        argv = _make_gold_argv(
            tmp_path, **{"--bq-output-project": "manglaria-staging"}
        )
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_gold_allow_final_merge_without_stage_exit_three(
        self, tmp_path, patch_gold_pipeline
    ):
        argv = _make_gold_argv(tmp_path)
        argv.extend(["--bq-allow-final-merge"])
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_gold_allow_final_merge_requires_final_table(
        self, tmp_path, patch_gold_pipeline
    ):
        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_gold_rbrl",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        argv.extend(["--bq-allow-final-merge"])
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_gold_writes_to_forbidden_production_project_runtime_fails(
        self, tmp_path, patch_gold_pipeline
    ):
        """The hard production-read-only invariant lives in
        ``BigQueryWritebackConfig.forbidden_write_projects=("manglaria",)``;
        attempting to write to that project surfaces as a writeback
        failure (RUNTIME_EXIT). The CLI does not additionally require
        output != input for gold, because silver typically lives in
        the same staging project the gold writeback targets."""
        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_gold_rbrl",
                "--bq-output-project": "manglaria",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT

    def test_gold_stage_only_writeback_engages_without_merge(
        self, tmp_path, patch_gold_pipeline, monkeypatch
    ):
        captured: dict[str, Any] = {}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["cfg"] = cfg
            captured["site_id"] = site_id
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)
        # Skip the read_final_table_columns/schema lookups; no final table here.
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_columns", lambda cfg, **_kw: None
        )
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_schema", lambda cfg, **_kw: None
        )

        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_gold_rbrl",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert captured["cfg"].allow_final_merge is False
        assert captured["cfg"].stage_table == "cf_s2_stage_gold_rbrl"
        # M24: ungrouped CLI runs do not pass site_id to run_writeback.
        assert captured["site_id"] is None

        run = json.loads(
            (tmp_path / "gold_run.json").read_text(encoding="utf-8")
        )
        assert run["outputs"]["bigquery_writeback"] is True
        assert run["writeback"]["status"] == "stage_only_succeeded"
        assert run["writeback"]["merge_attempted"] is False
        assert run["writeback"]["merge_authorized"] is False

    def test_gold_explicit_merge_passes_authorization_through(
        self, tmp_path, patch_gold_pipeline, monkeypatch
    ):
        captured: dict[str, Any] = {}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["cfg"] = cfg
            return WritebackResult(
                run_id=run_id,
                status="succeeded",
                stage_rows=int(len(df)),
                merge_attempted=True,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=int(len(df)),
                merge_updated_rows=0,
                watermark_advanced=True,
                watermark_value="2025-08-01T01:30:00+00:00",
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_columns", lambda cfg, **_kw: None
        )
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_schema", lambda cfg, **_kw: None
        )

        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_gold_rbrl",
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

        run = json.loads(
            (tmp_path / "gold_run.json").read_text(encoding="utf-8")
        )
        assert run["writeback"]["merge_attempted"] is True
        assert run["writeback"]["merge_authorized"] is True
        assert run["writeback"]["watermark_advanced"] is True

    def test_gold_writeback_failure_returns_four(
        self, tmp_path, patch_gold_pipeline, monkeypatch
    ):
        import miaproc.eddy as eddy_pkg

        def _boom(*_a, **_kw):
            raise RuntimeError("simulated gold stage write failure")

        monkeypatch.setattr(eddy_pkg, "run_writeback", _boom)
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_columns", lambda cfg, **_kw: None
        )
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_schema", lambda cfg, **_kw: None
        )

        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_stage_gold_rbrl",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT
        run = json.loads(
            (tmp_path / "gold_run.json").read_text(encoding="utf-8")
        )
        assert run["writeback"]["status"] == "failed"
        assert run["exit_code"] == cli.RUNTIME_EXIT


# ---------------------------------------------------------------------------
# Non-regression: existing one-shot run-bigquery still parses
# ---------------------------------------------------------------------------


class TestOneShotRunBigQueryUnchanged:
    def test_one_shot_help_still_works(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["eddy", "run-bigquery", "--help"])
        assert exc.value.code == 0

    def test_one_shot_required_flags_still_enforced(self):
        parser = cli._build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["eddy", "run-bigquery"])


# ---------------------------------------------------------------------------
# M28: stage-payload column-uniqueness + bronze->silver / silver->gold
# preservation as exercised through the CLI silver/gold subcommands.
# The BigQuery client is stubbed; ``run_writeback`` is mocked so we
# can inspect the DataFrame that the CLI would have handed to
# ``load_table_from_dataframe``.
# ---------------------------------------------------------------------------


def _silver_with_bronze_sentinel() -> pd.DataFrame:
    """A silver-shaped fixture carrying a source-only sentinel column
    (``bronze_only_flag``) so the M28 bronze->silver preservation
    contract can be asserted end-to-end through the CLI."""
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=4, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "QC_NEE": [0, 0, 0, 0],
            "H": [10.0, 20.0, 30.0, 40.0],
            "LE": [50.0, 60.0, 70.0, 80.0],
            "P_RAIN": [0.0, 0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0, 90.0],
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


def _silver_with_duplicate_rh() -> pd.DataFrame:
    df = _silver_with_bronze_sentinel()
    extra = pd.DataFrame({"rH": [55.0, 75.0, 85.0, 95.0]})
    return pd.concat([df, extra], axis=1)


def _gold_with_silver_sentinel() -> pd.DataFrame:
    df = _stub_gold_df()
    # ``_attach_silver_columns_to_gold`` appends silver-only cols on
    # top of gold via LEFT-join. To emulate the post-attach state
    # carrying a silver-only sentinel, add the column here.
    df = df.copy()
    df["silver_only_flag"] = [True, False, True, False]
    df["H"] = [10.0, 20.0, 30.0, 40.0]
    df["LE"] = [50.0, 60.0, 70.0, 80.0]
    df["P_RAIN"] = [0.0, 0.0, 0.0, 0.0]
    df["QC_NEE"] = [0, 0, 0, 0]
    df["rH"] = [60.0, 70.0, 80.0, 90.0]
    return df


class TestSilverPayloadM28:
    def test_silver_writeback_preserves_bronze_sentinel(
        self, tmp_path, monkeypatch
    ):
        """The CLI silver writeback path must hand a DataFrame to
        ``run_writeback`` that still contains the bronze-only source
        column. M28 bronze->silver preservation."""
        captured: dict[str, Any] = {}

        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=pd.DataFrame(
                    {
                        "timestamp": pd.date_range(
                            "2025-08-01", periods=4, freq="30min", tz="UTC"
                        ),
                        "site_id": ["RBRL"] * 4,
                        "bronze_only_flag": [1, 0, 1, 0],
                    }
                ),
                biomet_df=pd.DataFrame(
                    {
                        "timestamp": pd.date_range(
                            "2025-08-01", periods=4, freq="30min", tz="UTC"
                        ),
                        "site_id": ["RBRL"] * 4,
                    }
                ),
                flux_rows=4,
                biomet_rows=4,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        def _fake_load(**kwargs):
            return _silver_with_bronze_sentinel()

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["staged_columns"] = list(df.columns)
            captured["staged_df"] = df.copy()
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )
        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        staged_cols = captured["staged_columns"]
        # Bronze-only sentinel survives into the staged payload.
        assert "bronze_only_flag" in staged_cols, staged_cols
        # Identity triple is materialized.
        for c in ("primary_key", "site_id", "timestamp"):
            assert c in staged_cols, c
        # Stage payload has unique column names — the M28 contract
        # that protects ``load_table_from_dataframe`` from BigQuery's
        # "Field X already exists in schema" failure.
        staged_df = captured["staged_df"]
        assert staged_df.columns.is_unique

        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        wb_record = run["writeback"]
        assert wb_record["stage_payload_columns_unique"] is True
        # No collision actions for the equivalent-source case.
        assert wb_record["column_collision_actions"] == []

    def test_silver_writeback_resolves_divergent_rH_collision(
        self, tmp_path, monkeypatch
    ):
        """When the silver frame carries two diverging ``rH`` columns
        (the live production failure shape), the CLI must hand a
        deduplicated payload to ``run_writeback`` and record the
        ``rH``/``rH_norm_s`` rename action in run metadata."""
        captured: dict[str, Any] = {}

        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=pd.DataFrame(
                    {
                        "timestamp": pd.date_range(
                            "2025-08-01", periods=4, freq="30min", tz="UTC"
                        ),
                        "site_id": ["RBRL"] * 4,
                    }
                ),
                biomet_df=pd.DataFrame(
                    {"timestamp": [], "site_id": []}
                ),
                flux_rows=4,
                biomet_rows=0,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        def _fake_load(**kwargs):
            return _silver_with_duplicate_rh()

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["staged_df"] = df.copy()
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=False,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )
        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        staged_df = captured["staged_df"]
        assert staged_df.columns.is_unique, list(staged_df.columns)
        assert list(staged_df.columns).count("rH") == 1
        assert "rH_norm_s" in staged_df.columns

        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        wb = run["writeback"]
        assert wb["stage_payload_columns_unique"] is True
        actions = wb["column_collision_actions"]
        assert any(
            a["column"] == "rH"
            and a["action"] == "renamed_divergent_duplicate"
            and a["renamed_to"] == "rH_norm_s"
            for a in actions
        ), actions


class TestGoldPayloadM28:
    def test_gold_writeback_preserves_silver_sentinel(
        self, tmp_path, monkeypatch
    ):
        """M28 silver->gold preservation: every incoming silver column
        survives into the gold stage payload even when the live final
        table schema is narrower."""
        captured: dict[str, Any] = {}

        fake_silver_result = BigQuerySilverReadResult(
            silver_df=_silver_with_bronze_sentinel(),
            silver_rows=4,
            silver_query="SELECT * FROM silver",
            query_parameters={},
        )

        def _fake_silver_read(cfg, *, client=None):
            return fake_silver_result

        def _fake_postproc(
            df, *, engine, hesseflux_config=None, reddyproc_config=None,
        ):
            return _stub_gold_df()

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["staged_columns"] = list(df.columns)
            captured["staged_df"] = df.copy()
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=cfg.allow_final_merge,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "read_bigquery_silver_input", _fake_silver_read
        )
        monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)

        # Narrow final-table schema; preservation must still keep
        # the silver-only sentinel + the EddyPro-source pass-through.
        from miaproc.eddy import bigquery_writeback as wb_mod

        narrow_schema = [
            "primary_key", "site_id", "timestamp",
            "dateAndTime", "nee_f", "nee_fqc",
            "sw_in_f", "ta_f", "vpd_f",
        ]
        monkeypatch.setattr(
            wb_mod,
            "read_final_table_columns",
            lambda cfg, *, client=None: narrow_schema,
        )
        monkeypatch.setattr(
            wb_mod,
            "read_final_table_schema",
            lambda cfg, *, client=None: None,
        )

        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_gold_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        staged_cols = captured["staged_columns"]
        # Silver-only columns (not in narrow final schema) preserved.
        for col in ("H", "LE", "P_RAIN", "QC_NEE", "rH", "bronze_only_flag"):
            assert col in staged_cols, col
        # Identity triple is materialized.
        for col in ("primary_key", "site_id", "timestamp"):
            assert col in staged_cols, col
        # Stage payload columns are unique.
        assert captured["staged_df"].columns.is_unique

        run = json.loads(
            (tmp_path / "gold_run.json").read_text(encoding="utf-8")
        )
        wb_record = run["writeback"]
        assert wb_record["stage_payload_columns_unique"] is True
        assert wb_record["column_collision_actions"] == []


# ---------------------------------------------------------------------------
# M29: --stage-payload-dry-run-dir on silver + gold. Build the exact
# stage payload that the writeback path would hand to BigQuery, write
# local CSV + metadata artifacts, and skip every BigQuery write
# (load_table_from_dataframe, validation SQL, MERGE, watermark advance).
# ---------------------------------------------------------------------------


def _silver_with_bronze_sentinel_4row() -> pd.DataFrame:
    """4-row silver fixture so the writeback-side ``timestamp`` column
    has enough rows to round-trip cleanly through CSV."""
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=4, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "QC_NEE": [0, 0, 0, 0],
            "H": [10.0, 20.0, 30.0, 40.0],
            "LE": [50.0, 60.0, 70.0, 80.0],
            "P_RAIN": [0.0, 0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0, 90.0],
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


def _bronze_flux_df_with_sentinel(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "site_id": ["RBRL"] * n,
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


class TestArgParsingM29:
    def test_silver_accepts_stage_payload_dry_run_dir(self, tmp_path):
        parser = cli._build_parser()
        ns = parser.parse_args(
            _make_silver_argv(
                tmp_path,
                **{"--stage-payload-dry-run-dir": str(tmp_path / "dry")},
            )
        )
        assert ns.stage_payload_dry_run_dir == tmp_path / "dry"

    def test_gold_accepts_stage_payload_dry_run_dir(self, tmp_path):
        parser = cli._build_parser()
        ns = parser.parse_args(
            _make_gold_argv(
                tmp_path,
                **{"--stage-payload-dry-run-dir": str(tmp_path / "dry")},
            )
        )
        assert ns.stage_payload_dry_run_dir == tmp_path / "dry"


class TestSilverStagePayloadDryRunM29:
    def _patch_silver_pipeline_with_sentinels(self, monkeypatch):
        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=_bronze_flux_df_with_sentinel(),
                biomet_df=pd.DataFrame({"timestamp": [], "site_id": []}),
                flux_rows=4,
                biomet_rows=0,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        def _fake_load(**kwargs):
            return _silver_with_bronze_sentinel_4row()

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )

    def test_silver_dry_run_writes_payload_and_metadata(
        self, tmp_path, monkeypatch
    ):
        """The dry-run mode writes ``stage_payload.csv`` and
        ``stage_payload_metadata.json`` into the chosen directory and
        returns SUCCESS_EXIT without engaging writeback."""
        self._patch_silver_pipeline_with_sentinels(monkeypatch)

        # If anything calls run_writeback during dry-run, fail loudly.
        def _no_writeback(*_a, **_kw):
            raise AssertionError(
                "run_writeback must not be called during a dry-run"
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _no_writeback)

        dry_dir = tmp_path / "silver_dry"
        argv = _make_silver_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        payload_csv = dry_dir / "stage_payload.csv"
        meta_json = dry_dir / "stage_payload_metadata.json"
        assert payload_csv.exists()
        assert meta_json.exists()

        meta = json.loads(meta_json.read_text(encoding="utf-8"))
        assert meta["dry_run"] is True
        assert meta["stage"] == "silver"
        assert meta["command"] == "eddy run-bigquery-silver"
        assert meta["payload_format"] == "csv"
        assert meta["bigquery_write_attempted"] is False
        assert meta["validation_sql_attempted"] is False
        assert meta["merge_attempted"] is False
        assert meta["watermark_advanced"] is False
        assert meta["columns_unique"] is True
        assert meta["duplicate_columns"] == []
        assert meta["identity_columns_present"] == {
            "primary_key": True, "site_id": True, "timestamp": True,
        }
        # Bronze-only sentinel was preserved into the staged payload.
        assert "bronze_only_flag" in meta["columns"]
        assert "bronze_only_flag" in meta["preserved_input_columns"]
        # Bronze source ``site_id`` / ``timestamp`` column names are
        # also preserved (values are overwritten by identity logic but
        # the column names survive — the M29 preservation contract is
        # by column name).
        assert "site_id" in meta["preserved_input_columns"]
        assert "timestamp" in meta["preserved_input_columns"]
        assert meta["missing_input_columns"] == []
        # No writeback flags supplied -> would_write is empty.
        assert meta["would_write"] == {}

        # Run JSON records the dry-run status + artifact paths.
        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        assert run["outputs"]["bigquery_writeback"] is False
        assert run["outputs"]["stage_payload_dry_run"] is True
        assert run["outputs"]["stage_payload_dry_run_dir"] == str(dry_dir)
        wb = run["writeback"]
        assert wb["status"] == "stage_payload_dry_run_succeeded"
        assert wb["bigquery_write_attempted"] is False
        assert wb["merge_attempted"] is False
        assert wb["merge_authorized"] is False
        assert wb["watermark_advanced"] is False
        assert wb["stage_payload_columns_unique"] is True
        assert wb["payload_artifacts"]["stage_payload_csv"] == str(
            payload_csv
        )
        assert wb["payload_artifacts"][
            "stage_payload_metadata_json"
        ] == str(meta_json)

    def test_silver_dry_run_wins_over_bq_stage_table(
        self, tmp_path, monkeypatch
    ):
        """When both dry-run and --bq-stage-table are supplied, dry-run
        wins: no BigQuery write happens, and ``would_write`` records
        what the operator named."""
        self._patch_silver_pipeline_with_sentinels(monkeypatch)

        def _no_writeback(*_a, **_kw):
            raise AssertionError(
                "run_writeback must not be called during a dry-run"
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _no_writeback)

        dry_dir = tmp_path / "silver_dry_with_targets"
        argv = _make_silver_argv(
            tmp_path,
            **{
                "--stage-payload-dry-run-dir": str(dry_dir),
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        assert meta["would_write"]["bq_stage_table"] == (
            "cf_s2_silver_stage"
        )
        assert meta["would_write"]["bq_output_project"] == (
            "manglaria-staging"
        )
        assert meta["would_write"]["bq_control_dataset"] == "_orch"
        assert meta["bigquery_write_attempted"] is False

    def test_silver_dry_run_records_rh_collision_actions(
        self, tmp_path, monkeypatch
    ):
        """A divergent-rH silver frame still resolves to the M28 ``rH``
        / ``rH_norm_s`` policy under dry-run, and the metadata records
        the collision action."""
        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=_bronze_flux_df_with_sentinel(),
                biomet_df=pd.DataFrame({"timestamp": [], "site_id": []}),
                flux_rows=4,
                biomet_rows=0,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        def _fake_load(**kwargs):
            base = _silver_with_bronze_sentinel_4row()
            extra = pd.DataFrame({"rH": [55.0, 75.0, 85.0, 95.0]})
            return pd.concat([base, extra], axis=1)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )
        monkeypatch.setattr(
            eddy_pkg, "run_writeback",
            lambda *_a, **_kw: (_ for _ in ()).throw(
                AssertionError("must not be called")
            ),
        )

        dry_dir = tmp_path / "silver_dry_rh"
        argv = _make_silver_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        # The staged payload is unique and contains both rH and
        # rH_norm_s; the collision action is surfaced.
        assert meta["columns_unique"] is True
        assert "rH" in meta["columns"]
        assert "rH_norm_s" in meta["columns"]
        actions = meta["column_collision_actions"]
        assert any(
            a["column"] == "rH"
            and a["action"] == "renamed_divergent_duplicate"
            and a["renamed_to"] == "rH_norm_s"
            for a in actions
        ), actions

    def test_silver_dry_run_artifact_write_failure_returns_runtime_exit(
        self, tmp_path, monkeypatch
    ):
        """If artifact writing fails, the command returns RUNTIME_EXIT
        and the run JSON records the dry-run failure, NOT success."""
        self._patch_silver_pipeline_with_sentinels(monkeypatch)
        # Point the dry-run dir at an existing file so mkdir raises.
        blocker = tmp_path / "blocker"
        blocker.write_text("not a directory")

        argv = _make_silver_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(blocker)},
        )
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT
        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        assert run["writeback"]["status"] == "stage_payload_dry_run_failed"
        assert run["writeback"]["bigquery_write_attempted"] is False


class TestGoldStagePayloadDryRunM29:
    def _patch_gold_pipeline_with_sentinels(self, monkeypatch):
        fake_silver_result = BigQuerySilverReadResult(
            silver_df=_silver_with_bronze_sentinel_4row(),
            silver_rows=4,
            silver_query="SELECT * FROM silver",
            query_parameters={},
        )

        def _fake_silver_read(cfg, *, client=None):
            return fake_silver_result

        def _fake_postproc(
            df, *, engine, hesseflux_config=None, reddyproc_config=None,
        ):
            return _stub_gold_df()

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "read_bigquery_silver_input", _fake_silver_read
        )
        monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)

    def test_gold_dry_run_writes_payload_and_metadata(
        self, tmp_path, monkeypatch
    ):
        self._patch_gold_pipeline_with_sentinels(monkeypatch)

        def _no_writeback(*_a, **_kw):
            raise AssertionError(
                "run_writeback must not be called during a dry-run"
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _no_writeback)

        dry_dir = tmp_path / "gold_dry"
        argv = _make_gold_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        payload_csv = dry_dir / "stage_payload.csv"
        meta_json = dry_dir / "stage_payload_metadata.json"
        assert payload_csv.exists()
        assert meta_json.exists()

        meta = json.loads(meta_json.read_text(encoding="utf-8"))
        assert meta["dry_run"] is True
        assert meta["stage"] == "gold"
        assert meta["command"] == "eddy run-bigquery-gold"
        assert meta["payload_format"] == "csv"
        assert meta["bigquery_write_attempted"] is False
        assert meta["validation_sql_attempted"] is False
        assert meta["merge_attempted"] is False
        assert meta["watermark_advanced"] is False
        assert meta["columns_unique"] is True
        assert meta["identity_columns_present"] == {
            "primary_key": True, "site_id": True, "timestamp": True,
        }
        # Bronze-derived silver columns survive into the gold payload.
        for col in ("H", "LE", "P_RAIN", "QC_NEE", "rH", "bronze_only_flag"):
            assert col in meta["columns"], col
            assert col in meta["preserved_input_columns"], col
        # Gold analytical columns are appended.
        for col in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f", "dateAndTime"):
            assert col in meta["columns"], col
        # No M28 collisions in this fixture.
        assert meta["column_collision_actions"] == []
        assert meta["missing_input_columns"] == []
        assert meta["would_write"] == {}

        run = json.loads(
            (tmp_path / "gold_run.json").read_text(encoding="utf-8")
        )
        assert run["outputs"]["bigquery_writeback"] is False
        assert run["outputs"]["stage_payload_dry_run"] is True
        wb = run["writeback"]
        assert wb["status"] == "stage_payload_dry_run_succeeded"
        assert wb["bigquery_write_attempted"] is False
        assert wb["merge_attempted"] is False
        assert wb["merge_authorized"] is False
        assert wb["watermark_advanced"] is False
        assert wb["stage_payload_columns_unique"] is True

    def test_gold_dry_run_wins_over_stage_and_merge_flags(
        self, tmp_path, monkeypatch
    ):
        """When dry-run is supplied alongside --bq-stage-table and
        --bq-allow-final-merge (with --bq-final-table), the command
        must NOT call run_writeback and must NOT MERGE; ``would_write``
        records the authorized targets."""
        self._patch_gold_pipeline_with_sentinels(monkeypatch)

        def _no_writeback(*_a, **_kw):
            raise AssertionError(
                "run_writeback must not be called during a dry-run"
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _no_writeback)

        dry_dir = tmp_path / "gold_dry_with_merge"
        argv = _make_gold_argv(
            tmp_path,
            **{
                "--stage-payload-dry-run-dir": str(dry_dir),
                "--bq-stage-table": "cf_s2_gold_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
                "--bq-final-table": "carbon_flux_eddycovariance_s2_filt_1",
                "--bq-allow-final-merge": "",
            },
        )
        # Strip the empty value placed for --bq-allow-final-merge by
        # the _make_gold_argv kv loop: argparse expects no value for a
        # store_true flag. Replace the (flag, "") pair with just the
        # flag.
        argv = [a for a in argv if a != ""]
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        ww = meta["would_write"]
        assert ww["bq_stage_table"] == "cf_s2_gold_stage"
        assert ww["bq_final_table"] == (
            "carbon_flux_eddycovariance_s2_filt_1"
        )
        assert ww["bq_allow_final_merge"] is True
        assert ww["bq_output_project"] == "manglaria-staging"
        assert meta["merge_attempted"] is False
        assert meta["watermark_advanced"] is False


class TestRealWritebackUnchangedWhenDryRunFlagAbsent:
    """Regression: with --stage-payload-dry-run-dir absent, the silver
    + gold paths still call run_writeback exactly as before M29."""

    def test_silver_real_writeback_still_calls_run_writeback(
        self, tmp_path, monkeypatch
    ):
        called: dict[str, bool] = {"writeback": False}

        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=_bronze_flux_df_with_sentinel(),
                biomet_df=pd.DataFrame({"timestamp": [], "site_id": []}),
                flux_rows=4,
                biomet_rows=0,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        def _fake_load(**kwargs):
            return _silver_with_bronze_sentinel_4row()

        def _fake_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            called["writeback"] = True
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=False,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )
        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_writeback)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert called["writeback"] is True

    def test_gold_real_writeback_still_calls_run_writeback(
        self, tmp_path, monkeypatch
    ):
        called: dict[str, bool] = {"writeback": False}

        fake_silver_result = BigQuerySilverReadResult(
            silver_df=_silver_with_bronze_sentinel_4row(),
            silver_rows=4,
            silver_query="SELECT * FROM silver",
            query_parameters={},
        )

        def _fake_silver_read(cfg, *, client=None):
            return fake_silver_result

        def _fake_postproc(
            df, *, engine, hesseflux_config=None, reddyproc_config=None,
        ):
            return _stub_gold_df()

        def _fake_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            called["writeback"] = True
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=False,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "read_bigquery_silver_input", _fake_silver_read
        )
        monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_writeback)

        from miaproc.eddy import bigquery_writeback as wb_mod

        monkeypatch.setattr(
            wb_mod, "read_final_table_columns",
            lambda cfg, *, client=None: None,
        )
        monkeypatch.setattr(
            wb_mod, "read_final_table_schema",
            lambda cfg, *, client=None: None,
        )

        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_gold_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert called["writeback"] is True


# ---------------------------------------------------------------------------
# M31: real-cloud-shaped silver fixtures (raw bronze names + RH/rH case
# collision). The cloud silver writeback failed with
# ``Field rH already exists in schema`` and the dry-run guard failed
# with ``unique input columns missing ... co2_flux ...``. Both shapes
# are covered here so a future regression of either is loud.
# ---------------------------------------------------------------------------


def _cloud_bronze_flux_df() -> pd.DataFrame:
    """Bronze flux frame in the real cloud column shape.

    Columns mirror the live ``carbon_flux_eddycovariance`` source
    table the cloud engineers fed to ``run-bigquery-silver``:
    EddyPro-style raw names (``co2_flux``, ``qc_co2_flux``,
    ``air_temperature``, ``u_star``), a flux-side source ``RH``
    pass-through, identity (``timestamp``, ``site_id``), and a
    bronze-only sentinel that has no canonical alias and so must
    still surface as a true preservation column.
    """
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2025-08-01", periods=4, freq="30min", tz="UTC"
            ),
            "site_id": ["RBRL"] * 4,
            "co2_flux": [0.1, 0.2, -0.1, 0.3],
            "qc_co2_flux": [0, 0, 0, 0],
            "air_temperature": [293.0, 294.0, 295.0, 292.0],
            "u_star": [0.2, 0.3, 0.4, 0.1],
            "RH": [60.0, 61.0, 62.0, 63.0],
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


def _cloud_silver_df_with_rh_case_collision(
    *, diverge: bool = True
) -> pd.DataFrame:
    """Silver frame post-stage1 in the real cloud shape.

    Stage 1 normalizes the bronze flux/biomet columns to their
    canonical names (``NEE``, ``QC_NEE``, ``Tair``, ``USTAR``,
    ``rH``, etc.) but leaves the flux-side ``RH`` pass-through under
    its source name. The biomet-derived ``rH`` reaches the silver
    frame separately. ``diverge`` controls whether the two humidity
    series carry the same values (suppression) or different values
    (rename to ``rH_norm_s``).
    """
    rh = (
        [55.0, 75.0, 85.0, 95.0]
        if diverge
        else [60.0, 61.0, 62.0, 63.0]
    )
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=4, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "QC_NEE": [0, 0, 0, 0],
            "H": [10.0, 20.0, 30.0, 40.0],
            "LE": [50.0, 60.0, 70.0, 80.0],
            "P_RAIN": [0.0, 0.0, 0.0, 0.0],
            "RH": [60.0, 61.0, 62.0, 63.0],
            "rH": rh,
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


def _patch_cloud_silver_pipeline(monkeypatch, *, diverge_rh: bool = True):
    """Stub the silver read + stage1 path with the cloud-shape
    fixtures. Returns a ``SimpleNamespace`` with a ``calls`` dict so
    tests can assert what would have been written to BigQuery
    without ever reaching live credentials."""
    calls: dict[str, Any] = {"writeback_df": None, "writeback_cfg": None}

    def _fake_read(cfg, *, client=None):
        return BigQueryReadResult(
            flux_df=_cloud_bronze_flux_df(),
            biomet_df=pd.DataFrame(
                {"timestamp": [], "site_id": []}
            ),
            flux_rows=4,
            biomet_rows=0,
            flux_query="SELECT * FROM flux",
            biomet_query="SELECT * FROM biomet",
            query_parameters={},
        )

    def _fake_load(**kwargs):
        return _cloud_silver_df_with_rh_case_collision(diverge=diverge_rh)

    import miaproc.eddy as eddy_pkg

    monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
    monkeypatch.setattr(
        eddy_pkg, "load_stage1_from_dataframes", _fake_load
    )
    return SimpleNamespace(calls=calls)


class TestSilverDryRunCloudShapeAliasResolutionM31:
    """M31 fix A: the dry-run preservation guard must understand
    that raw bronze column names (``co2_flux``, ``u_star``, ...) are
    represented in the silver payload by their canonical stage1
    aliases (``NEE``, ``USTAR``, ...). The cloud engineers' run
    failed with ``unique input columns missing ... co2_flux, ...``
    even though the silver payload was correct."""

    def test_silver_dry_run_accepts_normalized_aliases(
        self, tmp_path, monkeypatch
    ):
        _patch_cloud_silver_pipeline(monkeypatch, diverge_rh=True)

        def _no_writeback(*_a, **_kw):
            raise AssertionError(
                "run_writeback must not be called during a dry-run"
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _no_writeback)

        dry_dir = tmp_path / "silver_dry_cloud_alias"
        argv = _make_silver_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT, (
            "silver dry-run must not raise on normalized bronze "
            "columns; cloud engineers reported "
            "ValueError 'unique input columns missing ... co2_flux ...'"
        )

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        # The cloud-failure-shaped raw columns are all preserved
        # under their canonical silver names, and the alias map
        # records the resolutions explicitly.
        assert meta["missing_input_columns"] == [], meta
        aliases = meta["input_column_payload_aliases"]
        assert aliases["co2_flux"] == "NEE"
        assert aliases["qc_co2_flux"] == "QC_NEE"
        assert aliases["air_temperature"] == "Tair"
        assert aliases["u_star"] == "USTAR"
        # ``RH`` was canonicalized to ``rH`` by the M31 humidity
        # policy and so also resolves via alias.
        assert aliases["RH"] == "rH"
        for raw_name in (
            "co2_flux", "qc_co2_flux", "air_temperature", "u_star", "RH",
        ):
            assert raw_name in meta["preserved_input_columns"], raw_name

        # Bronze-only sentinel (no canonical alias) survives
        # naturally via the silver frame and is preserved directly.
        assert "bronze_only_flag" in meta["preserved_input_columns"]
        assert "bronze_only_flag" not in aliases

        # Identity columns sourced from bronze are still preserved.
        assert "site_id" in meta["preserved_input_columns"]
        assert "timestamp" in meta["preserved_input_columns"]

        # Run JSON records the dry-run success and never engaged BQ.
        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        assert run["writeback"]["status"] == (
            "stage_payload_dry_run_succeeded"
        )
        assert run["writeback"]["bigquery_write_attempted"] is False
        assert run["writeback"]["stage_payload_columns_unique"] is True

    def test_silver_dry_run_columns_have_no_case_insensitive_duplicates(
        self, tmp_path, monkeypatch
    ):
        _patch_cloud_silver_pipeline(monkeypatch, diverge_rh=True)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "run_writeback",
            lambda *_a, **_kw: (_ for _ in ()).throw(
                AssertionError("run_writeback must not be called")
            ),
        )

        dry_dir = tmp_path / "silver_dry_case"
        argv = _make_silver_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        # M31 strictness: columns_unique is true only when
        # case-insensitive keys are all distinct.
        assert meta["columns_unique"] is True
        assert meta["duplicate_columns"] == []
        # The cloud failure ``Field rH already exists in schema`` is
        # now impossible: ``RH`` is gone, ``rH`` and ``rH_norm_s``
        # coexist with distinct case-folded keys.
        assert "RH" not in meta["columns"]
        assert "rH" in meta["columns"]
        assert "rH_norm_s" in meta["columns"]
        keys_lower = [c.casefold() for c in meta["columns"]]
        assert len(set(keys_lower)) == len(keys_lower), meta["columns"]

        # The collision action surfaces the divergent-derived
        # rename so the operator sees the M28/M31 resolution.
        actions = meta["column_collision_actions"]
        assert any(
            a["column"] == "rH"
            and a["action"] == "renamed_divergent_duplicate"
            and a["renamed_to"] == "rH_norm_s"
            for a in actions
        ), actions

        # Payload CSV survives a round-trip with the same column
        # names (no silent lowercase by pandas/CSV).
        df = pd.read_csv(dry_dir / "stage_payload.csv")
        assert "rH" in df.columns
        assert "rH_norm_s" in df.columns
        assert "RH" not in df.columns

    def test_silver_dry_run_equivalent_RH_rH_collapses_via_collision(
        self, tmp_path, monkeypatch
    ):
        _patch_cloud_silver_pipeline(monkeypatch, diverge_rh=False)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "run_writeback",
            lambda *_a, **_kw: (_ for _ in ()).throw(
                AssertionError("run_writeback must not be called")
            ),
        )

        dry_dir = tmp_path / "silver_dry_equiv"
        argv = _make_silver_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        assert "rH" in meta["columns"]
        assert "rH_norm_s" not in meta["columns"]
        actions = meta["column_collision_actions"]
        # Both: canonicalization of the first variant + suppression
        # of the equivalent derived duplicate.
        kinds = {a["action"] for a in actions}
        assert "renamed_to_canonical_humidity" in kinds
        assert "suppressed_equivalent_duplicate" in kinds

    def test_silver_dry_run_strict_when_bronze_column_truly_unrepresented(
        self, tmp_path, monkeypatch
    ):
        """Regression guard: the alias-aware fix must not turn
        ``missing_input_columns`` into a permissive no-op. If the
        bronze input carries a column whose name has no configured
        alias and is not literally present in the payload, the
        dry-run must still fail loudly rather than write a
        misleading artifact."""

        # Compose a bronze frame that has a real unrepresented
        # column on top of the cloud-shape fixture.
        bronze = _cloud_bronze_flux_df()
        bronze["truly_bronze_only_no_alias"] = [1, 2, 3, 4]

        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=bronze,
                biomet_df=pd.DataFrame(
                    {"timestamp": [], "site_id": []}
                ),
                flux_rows=4,
                biomet_rows=0,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        # Silver frame deliberately omits the new bronze-only column.
        def _fake_load(**kwargs):
            return _cloud_silver_df_with_rh_case_collision(diverge=True)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )
        monkeypatch.setattr(
            eddy_pkg, "run_writeback",
            lambda *_a, **_kw: (_ for _ in ()).throw(
                AssertionError("must not be called")
            ),
        )

        dry_dir = tmp_path / "silver_dry_strict"
        argv = _make_silver_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        rc = cli.main(argv)
        # The dry-run-side preservation guard surfaces as
        # RUNTIME_EXIT so the operator sees the failure loudly.
        assert rc == cli.RUNTIME_EXIT
        # The CSV is written first (artifact ordering), but the
        # metadata is what would have misled the operator and is
        # the file the dry-run guard refuses to write.
        meta_path = dry_dir / "stage_payload_metadata.json"
        assert not meta_path.exists()
        run = json.loads(
            (tmp_path / "silver_run.json").read_text(encoding="utf-8")
        )
        assert run["writeback"]["status"] == (
            "stage_payload_dry_run_failed"
        )
        assert "truly_bronze_only_no_alias" in run["writeback"].get(
            "error_text", ""
        )


class TestSilverNonDryRunHandsBigQueryCompatiblePayloadM31:
    """M31 fix B: the non-dry-run silver writeback path must hand
    ``run_writeback`` a DataFrame with no case-insensitive duplicate
    field keys, so the live ``load_table_from_dataframe`` call never
    surfaces ``Field rH already exists in schema`` again."""

    def test_real_writeback_payload_has_no_case_insensitive_duplicates(
        self, tmp_path, monkeypatch
    ):
        _patch_cloud_silver_pipeline(monkeypatch, diverge_rh=True)

        captured: dict[str, Any] = {}

        def _fake_writeback(df, cfg, **kwargs):
            captured["df"] = df.copy(deep=False)
            captured["cfg"] = cfg
            return WritebackResult(
                run_id=kwargs.get("run_id", "rid"),
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=False,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=None,
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
                error_text=None,
                watermark_values_by_site={},
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_writeback)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT, rc

        staged = captured["df"]
        assert staged is not None, "run_writeback was not called"
        # pandas-level uniqueness still holds.
        assert staged.columns.is_unique
        # M31 strictness: BigQuery field keys are case-insensitive.
        keys = [str(c).casefold() for c in staged.columns]
        assert len(set(keys)) == len(keys), list(staged.columns)
        # ``RH`` + ``rH`` -> ``rH`` + ``rH_norm_s``; the original
        # case-variant ``RH`` is gone.
        assert "RH" not in staged.columns
        assert "rH" in staged.columns
        assert "rH_norm_s" in staged.columns
        # Production read-only stays intact: the operator pointed
        # writes at the staging project.
        assert captured["cfg"].output_project == "manglaria-staging"
