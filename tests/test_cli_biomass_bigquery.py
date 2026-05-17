"""Tests for the M19 ``miaproc biomass run-bigquery`` CLI subcommand.

Stubs ``read_bigquery_input`` so the tests are fast, deterministic,
and runnable on default CI without live BigQuery credentials. They
cover:

- argument parsing (help, required flags, defaults match
  ``enrich-table``, validation of `--bq-row-limit`);
- end-to-end CLI flow with a stubbed BigQuery read +
  packaged-equation enrichment;
- the same row-preservation + exactly-two-appended-columns +
  `equation_used`-masked-when-NaN contract as the file-based
  ``enrich-table``;
- BigQuery provenance recorded under ``inputs`` in the run JSON;
- failure paths for invalid args + output-column collision.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from miaproc import cli
from miaproc.biomass import BigQueryBiomassReadResult


def _make_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    base = {
        "--bq-input-project": "manglaria",
        "--bq-input-dataset": "manglaria_lakehouse_ds",
        "--bq-input-table": "forest_structure_biomass",
        "--output-table": str(tmp_path / "out.csv"),
        "--output-run-json": str(tmp_path / "run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["biomass", "run-bigquery"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


def _stub_adult_mangroves() -> pd.DataFrame:
    """All four mangrove species, all Adult, all eligible for direct biomass."""
    return pd.DataFrame(
        [
            {
                "primary_key": "syn_001",
                "species": "Avicennia germinans",
                "dbh_cm": 10.0,
                "tree_height_m": 7.5,
                "life_stage": "Adult",
            },
            {
                "primary_key": "syn_002",
                "species": "Rhizophora mangle",
                "dbh_cm": 12.0,
                "tree_height_m": 8.0,
                "life_stage": "Adult",
            },
            {
                "primary_key": "syn_003",
                "species": "Laguncularia racemosa",
                "dbh_cm": 9.0,
                "tree_height_m": 6.5,
                "life_stage": "Adult",
            },
            {
                "primary_key": "syn_004",
                "species": "Conocarpus erectus",
                "dbh_cm": 11.0,
                "tree_height_m": 7.0,
                "life_stage": "Adult",
            },
        ]
    )


def _patch_read(monkeypatch, df: pd.DataFrame) -> dict:
    """Patch ``miaproc.biomass.read_bigquery_input`` to return ``df``.

    Returns a dict the test can inspect to confirm the cfg the CLI
    constructed and passed in.
    """
    captured: dict[str, Any] = {"cfg": None}

    def _fake_read(cfg, *, client=None):
        captured["cfg"] = cfg
        return BigQueryBiomassReadResult(
            input_df=df,
            input_rows=int(len(df)),
            input_query=f"SELECT * FROM `{cfg.input_project}.{cfg.input_dataset}.{cfg.input_table}`",
            query_parameters={},
        )

    import miaproc.biomass as bm

    monkeypatch.setattr(bm, "read_bigquery_input", _fake_read)
    return captured


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


class TestArgParsing:
    def test_help_exits_zero(self):
        parser = cli._build_parser()
        try:
            parser.parse_args(["biomass", "run-bigquery", "--help"])
        except SystemExit as exc:
            assert exc.code == 0
        else:
            raise AssertionError("Expected SystemExit(0) from --help")

    def test_required_flags_missing_exits(self):
        parser = cli._build_parser()
        try:
            parser.parse_args(["biomass", "run-bigquery"])
        except SystemExit:
            pass
        else:
            raise AssertionError("Expected SystemExit on missing required flags")

    def test_defaults_match_enrich_table(self, tmp_path):
        parser = cli._build_parser()
        ns = parser.parse_args(_make_argv(tmp_path))
        assert ns.dataset == "dina"
        assert ns.equations_path is None
        assert ns.species_col == "species"
        assert ns.dbh_col == "dbh_cm"
        assert ns.height_col == "tree_height_m"
        assert ns.life_stage_col == "life_stage"
        assert ns.biomass_estimate_col == "biomass_estimate"
        assert ns.equation_used_col == "equation_used"
        assert ns.bq_row_limit is None
        assert ns.bq_billing_project is None
        assert ns.bq_no_storage_api is False


# ---------------------------------------------------------------------------
# End-to-end CLI flow
# ---------------------------------------------------------------------------


class TestCLIEndToEnd:
    def test_bigquery_read_to_csv_default_dina(self, tmp_path, monkeypatch):
        captured = _patch_read(monkeypatch, _stub_adult_mangroves())

        rc = cli.main(_make_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        out_path = tmp_path / "out.csv"
        run_path = tmp_path / "run.json"
        assert out_path.exists()
        assert run_path.exists()

        # Cfg the CLI constructed.
        cfg = captured["cfg"]
        assert cfg is not None
        assert cfg.input_project == "manglaria"
        assert cfg.input_dataset == "manglaria_lakehouse_ds"
        assert cfg.input_table == "forest_structure_biomass"
        assert cfg.row_limit is None
        assert cfg.billing_project is None
        assert cfg.bq_storage_api is True

        # Output table: row preservation + exactly two appended cols.
        out_df = pd.read_csv(out_path)
        assert len(out_df) == 4
        assert list(out_df.columns)[-2:] == [
            "biomass_estimate",
            "equation_used",
        ]
        assert out_df["biomass_estimate"].notna().all()
        assert out_df["equation_used"].tolist() == [
            "dina_001",
            "dina_002",
            "dina_003",
            "dina_004",
        ]

        # Run JSON shape.
        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["command"] == "biomass run-bigquery"
        assert run["stage"] == "biomass_run_bigquery"
        assert run["row_counts"] == {
            "input": 4,
            "output": 4,
            "estimated": 4,
            "skipped": 0,
        }
        assert run["output_columns_appended"] == [
            "biomass_estimate",
            "equation_used",
        ]
        assert run["config"]["dataset"] == "dina"
        assert run["config"]["equations_source"] == "packaged_default"
        # BigQuery provenance recorded.
        inputs = run["inputs"]
        assert inputs["mode"] == "bigquery"
        assert inputs["input_project"] == "manglaria"
        assert inputs["input_dataset"] == "manglaria_lakehouse_ds"
        assert inputs["input_table"] == "forest_structure_biomass"
        assert inputs["billing_project"] == "manglaria"  # falls back
        assert inputs["row_limit"] is None
        assert inputs["read_row_counts"] == {"input": 4}
        assert "forest_structure_biomass" in inputs["input_query"]
        assert run["exit_code"] == cli.SUCCESS_EXIT

    def test_billing_project_override(self, tmp_path, monkeypatch):
        captured = _patch_read(monkeypatch, _stub_adult_mangroves())

        rc = cli.main(
            _make_argv(
                tmp_path,
                **{"--bq-billing-project": "manglaria-staging"},
            )
        )
        assert rc == cli.SUCCESS_EXIT
        cfg = captured["cfg"]
        assert cfg.billing_project == "manglaria-staging"
        run = json.loads((tmp_path / "run.json").read_text(encoding="utf-8"))
        assert run["inputs"]["billing_project"] == "manglaria-staging"

    def test_row_limit_propagates_to_cfg_and_run_json(
        self, tmp_path, monkeypatch
    ):
        captured = _patch_read(monkeypatch, _stub_adult_mangroves())

        rc = cli.main(_make_argv(tmp_path, **{"--bq-row-limit": 100}))
        assert rc == cli.SUCCESS_EXIT
        cfg = captured["cfg"]
        assert cfg.row_limit == 100
        run = json.loads((tmp_path / "run.json").read_text(encoding="utf-8"))
        assert run["inputs"]["row_limit"] == 100

    def test_no_storage_api_propagates(self, tmp_path, monkeypatch):
        captured = _patch_read(monkeypatch, _stub_adult_mangroves())

        argv = _make_argv(tmp_path)
        argv.append("--bq-no-storage-api")
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert captured["cfg"].bq_storage_api is False

    def test_parquet_output(self, tmp_path, monkeypatch):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        out_path = tmp_path / "out.parquet"
        rc = cli.main(
            _make_argv(tmp_path, **{"--output-table": str(out_path)})
        )
        assert rc == cli.SUCCESS_EXIT
        out_df = pd.read_parquet(out_path)
        assert "biomass_estimate" in out_df.columns
        assert out_df["biomass_estimate"].notna().all()

    def test_mixed_eligibility_preserves_rows(self, tmp_path, monkeypatch):
        df = pd.DataFrame(
            [
                {
                    "primary_key": "mix_001",
                    "species": "Avicennia germinans",
                    "dbh_cm": 10.0,
                    "tree_height_m": None,
                    "life_stage": "Adult",
                },
                {
                    "primary_key": "mix_002",
                    "species": "Rhizophora mangle",
                    "dbh_cm": 5.0,
                    "tree_height_m": None,
                    "life_stage": "Juvenile",
                },
                {
                    "primary_key": "mix_003",
                    "species": "Conocarpus erectus",
                    "dbh_cm": None,
                    "tree_height_m": 5.0,
                    "life_stage": "Adult",
                },
            ]
        )
        _patch_read(monkeypatch, df)
        rc = cli.main(_make_argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        out_df = pd.read_csv(tmp_path / "out.csv")
        assert len(out_df) == 3
        # mix_001 eligible; others ineligible — equation_used null for them.
        assert out_df.loc[0, "equation_used"] == "dina_001"
        assert pd.isna(out_df.loc[1, "equation_used"])
        assert pd.isna(out_df.loc[2, "equation_used"])
        run = json.loads((tmp_path / "run.json").read_text(encoding="utf-8"))
        assert run["row_counts"]["estimated"] == 1
        assert run["row_counts"]["skipped"] == 2


# ---------------------------------------------------------------------------
# Failure paths
# ---------------------------------------------------------------------------


class TestFailurePaths:
    def test_invalid_row_limit_zero_exits_three(self, tmp_path, monkeypatch):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(_make_argv(tmp_path, **{"--bq-row-limit": 0}))
        assert rc == cli.VALIDATION_EXIT

    def test_invalid_row_limit_negative_exits_three(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(_make_argv(tmp_path, **{"--bq-row-limit": -5}))
        assert rc == cli.VALIDATION_EXIT

    def test_unsupported_output_extension_exits_three(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(
            _make_argv(
                tmp_path, **{"--output-table": str(tmp_path / "out.xlsx")}
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_same_output_column_name_twice_exits_three(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(
            _make_argv(
                tmp_path,
                **{
                    "--biomass-estimate-col": "x",
                    "--equation-used-col": "x",
                },
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_output_column_collision_with_input(self, tmp_path, monkeypatch):
        # Input table already has a column named "biomass_estimate" — CLI
        # should refuse rather than silently overwrite.
        df = _stub_adult_mangroves()
        df["biomass_estimate"] = 0.0
        _patch_read(monkeypatch, df)
        rc = cli.main(_make_argv(tmp_path))
        assert rc != cli.SUCCESS_EXIT
        assert not (tmp_path / "out.csv").exists()

    def test_runtime_failure_returns_four(self, tmp_path, monkeypatch):
        # Simulate a BigQuery read failure.
        def _fake_read(cfg, *, client=None):
            raise RuntimeError("simulated BigQuery failure")

        import miaproc.biomass as bm

        monkeypatch.setattr(bm, "read_bigquery_input", _fake_read)
        rc = cli.main(_make_argv(tmp_path))
        assert rc == cli.RUNTIME_EXIT


# ---------------------------------------------------------------------------
# M20 writeback flag-set validation + handler integration
# ---------------------------------------------------------------------------


class TestM20WritebackFlagValidation:
    def test_default_no_writeback_flags_are_present(self, tmp_path):
        parser = cli._build_parser()
        ns = parser.parse_args(_make_argv(tmp_path))
        assert ns.bq_stage_table is None
        assert ns.bq_output_project is None
        assert ns.bq_output_dataset is None
        assert ns.bq_final_table is None
        assert ns.bq_control_dataset is None
        assert ns.bq_allow_final_merge is False
        assert ns.bq_merge_key == "primary_key"
        assert ns.bq_run_id is None

    def test_partial_writeback_config_without_stage_exits_three(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(
            _make_argv(
                tmp_path,
                **{"--bq-output-project": "manglaria-staging"},
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_writeback_engaged_requires_output_project(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(
            _make_argv(
                tmp_path,
                **{
                    "--bq-stage-table": "cf_biomass_stage_test",
                    "--bq-output-dataset": "manglaria_lakehouse_ds",
                    "--bq-control-dataset": "_orch",
                },
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_writeback_engaged_requires_output_dataset(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(
            _make_argv(
                tmp_path,
                **{
                    "--bq-stage-table": "cf_biomass_stage_test",
                    "--bq-output-project": "manglaria-staging",
                    "--bq-control-dataset": "_orch",
                },
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_writeback_engaged_requires_control_dataset(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(
            _make_argv(
                tmp_path,
                **{
                    "--bq-stage-table": "cf_biomass_stage_test",
                    "--bq-output-project": "manglaria-staging",
                    "--bq-output-dataset": "manglaria_lakehouse_ds",
                },
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_output_project_must_differ_from_input(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        rc = cli.main(
            _make_argv(
                tmp_path,
                **{
                    "--bq-stage-table": "cf_biomass_stage_test",
                    "--bq-output-project": "manglaria",  # same as input
                    "--bq-output-dataset": "manglaria_lakehouse_ds",
                    "--bq-control-dataset": "_orch",
                },
            )
        )
        assert rc == cli.VALIDATION_EXIT

    def test_allow_final_merge_requires_final_table(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        argv = _make_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_biomass_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        argv.append("--bq-allow-final-merge")
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT

    def test_allow_final_merge_without_stage_exits_three(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        argv = _make_argv(tmp_path)
        argv.append("--bq-allow-final-merge")
        rc = cli.main(argv)
        assert rc == cli.VALIDATION_EXIT


class TestM20WritebackHandlerIntegration:
    """Stub ``run_writeback`` to test the CLI's writeback wiring +
    run.json shape without exercising the BigQuery client end-to-end
    (the writeback module's own tests cover the orchestration)."""

    def _patch_writeback_success(self, monkeypatch, *, status="stage_only_succeeded",
                                  merge_attempted=False, merge_authorized=False,
                                  inserted=None, updated=None):
        from miaproc.biomass import WritebackResult

        captured: dict[str, Any] = {}

        def _fake_run(df, cfg, *, run_id, started_at, run_payload_extras=None,
                      client=None):
            captured["df"] = df
            captured["cfg"] = cfg
            captured["run_id"] = run_id
            captured["run_payload_extras"] = run_payload_extras
            return WritebackResult(
                run_id=run_id,
                status=status,
                stage_rows=int(len(df)),
                merge_attempted=merge_attempted,
                merge_authorized=merge_authorized,
                merge_inserted_rows=inserted,
                merge_updated_rows=updated,
                validation_metrics={
                    "row_count": int(len(df)),
                    "null_merge_key": 0,
                    "dup_merge_key": 0,
                    "merge_key_column": cfg.merge_key_column,
                },
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                error_text=None,
            )

        import miaproc.biomass as bm
        monkeypatch.setattr(bm, "run_writeback", _fake_run)
        return captured

    def test_stage_only_writeback_records_in_run_json(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        captured = self._patch_writeback_success(monkeypatch)

        argv = _make_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_biomass_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        # run_writeback was called with the right config + extras.
        cfg = captured["cfg"]
        assert cfg.output_project == "manglaria-staging"
        assert cfg.stage_table == "cf_biomass_stage_test"
        assert cfg.allow_final_merge is False
        assert cfg.merge_key_column == "primary_key"  # default
        extras = captured["run_payload_extras"]
        assert extras["bq_input_project"] == "manglaria"
        assert extras["bq_input_table"] == "forest_structure_biomass"
        assert extras["read_input_rows"] == 4
        assert extras["estimated_rows"] == 4
        assert extras["skipped_rows"] == 0

        import json
        run = json.loads((tmp_path / "run.json").read_text(encoding="utf-8"))
        assert run["outputs"]["bigquery_writeback"] is True
        assert run["writeback"]["status"] == "stage_only_succeeded"
        assert run["writeback"]["stage_rows"] == 4
        assert run["writeback"]["merge_attempted"] is False
        assert run["writeback"]["merge_authorized"] is False
        assert run["exit_code"] == cli.SUCCESS_EXIT

    def test_explicit_merge_writeback_passes_allow_flag(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())
        captured = self._patch_writeback_success(
            monkeypatch,
            status="succeeded",
            merge_attempted=True,
            merge_authorized=True,
            inserted=4,
        )

        argv = _make_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_biomass_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
                "--bq-final-table": "forest_structure_with_biomass",
            },
        )
        argv.append("--bq-allow-final-merge")
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT

        cfg = captured["cfg"]
        assert cfg.allow_final_merge is True
        assert cfg.final_table == "forest_structure_with_biomass"

        import json
        run = json.loads((tmp_path / "run.json").read_text(encoding="utf-8"))
        assert run["writeback"]["status"] == "succeeded"
        assert run["writeback"]["merge_attempted"] is True
        assert run["writeback"]["merge_authorized"] is True
        assert run["writeback"]["merge_inserted_rows"] == 4

    def test_overridden_merge_key_propagates(self, tmp_path, monkeypatch):
        # Source frame must carry the override column for
        # prepare_stage_dataframe (the live, non-stubbed code path on
        # the CLI side) to accept it.
        df = _stub_adult_mangroves().rename(
            columns={"primary_key": "tree_id"}
        )
        _patch_read(monkeypatch, df)
        captured = self._patch_writeback_success(monkeypatch)

        argv = _make_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_biomass_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
                "--bq-merge-key": "tree_id",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert captured["cfg"].merge_key_column == "tree_id"
        # The frame actually passed to run_writeback carries tree_id.
        assert "tree_id" in captured["df"].columns

    def test_writeback_failure_records_failed_in_run_json_and_returns_four(
        self, tmp_path, monkeypatch
    ):
        _patch_read(monkeypatch, _stub_adult_mangroves())

        def _fake_run_fail(df, cfg, *, run_id, started_at,
                            run_payload_extras=None, client=None):
            exc = RuntimeError("simulated writeback failure")
            exc.miaproc_writeback_state = {  # type: ignore[attr-defined]
                "status": "failed",
                "merge_attempted": False,
                "merge_authorized": False,
                "stage_rows": 0,
            }
            raise exc

        import miaproc.biomass as bm
        monkeypatch.setattr(bm, "run_writeback", _fake_run_fail)

        argv = _make_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_biomass_stage_test",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.RUNTIME_EXIT

        # Local artifacts still written.
        assert (tmp_path / "out.csv").exists()
        # run.json carries the failure record.
        import json
        run = json.loads((tmp_path / "run.json").read_text(encoding="utf-8"))
        assert run["outputs"]["bigquery_writeback"] is True
        assert run["writeback"]["status"] == "failed"
        assert run["writeback"]["error_text"] == "simulated writeback failure"
        assert run["exit_code"] == cli.RUNTIME_EXIT
