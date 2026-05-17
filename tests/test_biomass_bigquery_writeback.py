"""Tests for the M20 biomass BigQuery writeback module.

Covers the M20 contract:

- ``BigQueryBiomassWritebackConfig.validate()`` rejects bad input;
- DDL renders the runs control table (and **only** the runs table —
  biomass M20 has no watermark concept);
- validation SQL queries the configured merge-key column;
- merge SQL parameterizes on the merge-key column;
- ``prepare_stage_dataframe`` validates the merge-key column is
  present;
- ``run_writeback`` honors stage-only default vs. opt-in MERGE,
  records run rows on success / validation failure / generic
  failure paths, and attaches ``miaproc_writeback_state`` to
  re-raised exceptions for the CLI's failure-path JSON shape;
- the merge-key column is configurable.

All tests use stubbed BigQuery clients; no live credentials required.
"""
from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from miaproc.biomass import (
    BigQueryBiomassWritebackConfig,
    WritebackResult,
    WritebackValidationError,
    build_merge_statement,
    build_validation_query,
    prepare_stage_dataframe,
    render_runs_table_ddl,
    run_writeback,
)
from miaproc.biomass import bigquery_writeback as wb


def _make_cfg(**overrides) -> BigQueryBiomassWritebackConfig:
    base = dict(
        output_project="manglaria-staging",
        output_dataset="manglaria_lakehouse_ds",
        stage_table="cf_biomass_stage_smoke",
        control_dataset="_orch",
    )
    base.update(overrides)
    return BigQueryBiomassWritebackConfig(**base)


def _stub_enriched(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "primary_key": f"pk_{i}",
                "species": "Avicennia germinans",
                "dbh_cm": 10.0 + i,
                "tree_height_m": None,
                "life_stage": "Adult",
                "biomass_estimate": 27.0 + i,
                "equation_used": "dina_001",
            }
            for i in range(n)
        ]
    )


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    def test_billing_project_falls_back_to_output(self):
        cfg = _make_cfg()
        assert cfg.billing_project_or_output() == "manglaria-staging"

    def test_explicit_billing_project_overrides(self):
        cfg = _make_cfg(billing_project="manglaria-billing")
        assert cfg.billing_project_or_output() == "manglaria-billing"

    def test_no_watermark_table_attribute(self):
        # Decision rationale: biomass has no watermark concept.
        cfg = _make_cfg()
        assert not hasattr(cfg, "watermark_table")
        assert not hasattr(cfg, "watermark_table_fqn")

    def test_default_merge_key_is_primary_key(self):
        assert _make_cfg().merge_key_column == "primary_key"

    def test_runs_table_default_is_cf_biomass_runs(self):
        assert _make_cfg().runs_table == "cf_biomass_runs"

    def test_validate_passes_for_minimal_stage_only_config(self):
        _make_cfg().validate()  # no raise

    def test_validate_rejects_forbidden_output_project(self):
        with pytest.raises(ValueError) as exc:
            _make_cfg(output_project="manglaria").validate()
        assert "manglaria" in str(exc.value)
        assert "read-only" in str(exc.value).lower()

    def test_validate_rejects_empty_output_dataset(self):
        with pytest.raises(ValueError):
            _make_cfg(output_dataset="").validate()

    def test_validate_rejects_empty_stage_table(self):
        with pytest.raises(ValueError):
            _make_cfg(stage_table="").validate()

    def test_validate_rejects_empty_control_dataset(self):
        with pytest.raises(ValueError):
            _make_cfg(control_dataset="").validate()

    def test_validate_rejects_empty_merge_key(self):
        with pytest.raises(ValueError):
            _make_cfg(merge_key_column="").validate()

    def test_validate_rejects_allow_merge_without_final_table(self):
        with pytest.raises(ValueError) as exc:
            _make_cfg(allow_final_merge=True).validate()
        assert "final_table" in str(exc.value)

    def test_validate_passes_for_explicit_merge_config(self):
        _make_cfg(
            allow_final_merge=True,
            final_table="forest_structure_with_biomass",
        ).validate()

    def test_fqn_helpers_render_backticks(self):
        cfg = _make_cfg(final_table="forest_structure_with_biomass")
        assert cfg.stage_table_fqn() == (
            "`manglaria-staging.manglaria_lakehouse_ds.cf_biomass_stage_smoke`"
        )
        assert cfg.final_table_fqn() == (
            "`manglaria-staging.manglaria_lakehouse_ds."
            "forest_structure_with_biomass`"
        )
        assert cfg.runs_table_fqn() == (
            "`manglaria-staging._orch.cf_biomass_runs`"
        )

    def test_final_table_fqn_returns_none_when_unset(self):
        assert _make_cfg().final_table_fqn() is None


# ---------------------------------------------------------------------------
# DDL + SQL builders
# ---------------------------------------------------------------------------


class TestDDLAndSQL:
    def test_runs_table_ddl_creates_only_one_table(self):
        ddl = render_runs_table_ddl(_make_cfg())
        # Only one CREATE TABLE statement; biomass has no watermark.
        assert ddl.count("CREATE TABLE IF NOT EXISTS") == 1
        assert "cf_biomass_runs" in ddl
        assert "watermark" not in ddl.lower()

    def test_runs_ddl_contains_biomass_specific_columns(self):
        ddl = render_runs_table_ddl(_make_cfg())
        for col in (
            "run_id",
            "started_at",
            "finished_at",
            "status",
            "bq_input_table",
            "bq_stage_table",
            "bq_final_table",
            "merge_key_column",
            "stage_rows",
            "estimated_rows",
            "skipped_rows",
            "merge_attempted",
            "merge_authorized",
            "merge_inserted_rows",
            "merge_updated_rows",
        ):
            assert col in ddl, col
        # No eddy-specific columns leaked over.
        assert "watermark_value" not in ddl
        assert "watermark_advanced" not in ddl
        assert "site_id" not in ddl  # biomass runs are not per-site
        assert "bq_flux_table" not in ddl
        assert "bq_biomet_table" not in ddl

    def test_validation_query_uses_configured_merge_key(self):
        cfg = _make_cfg(merge_key_column="primary_key")
        sql = build_validation_query(cfg)
        assert "COUNTIF(primary_key IS NULL) AS null_merge_key" in sql
        assert (
            "COUNT(*) - COUNT(DISTINCT primary_key) AS dup_merge_key"
        ) in sql
        assert "cf_biomass_stage_smoke" in sql

    def test_validation_query_honors_overridden_merge_key(self):
        cfg = _make_cfg(merge_key_column="tree_id")
        sql = build_validation_query(cfg)
        assert "COUNTIF(tree_id IS NULL)" in sql
        assert "DISTINCT tree_id" in sql

    def test_merge_statement_uses_merge_key(self):
        cfg = _make_cfg(
            allow_final_merge=True,
            final_table="forest_structure_with_biomass",
        )
        sql = build_merge_statement(
            cfg,
            columns=["primary_key", "species", "biomass_estimate", "equation_used"],
        )
        assert "ON T.primary_key = S.primary_key" in sql
        # Non-key cols updated; key not in the SET clause.
        assert "species = S.species" in sql
        assert "biomass_estimate = S.biomass_estimate" in sql
        assert "primary_key = S.primary_key" not in sql.split("WHEN MATCHED")[1]

    def test_merge_statement_rejects_missing_final_table(self):
        with pytest.raises(ValueError):
            build_merge_statement(_make_cfg(), columns=["primary_key", "x"])

    def test_merge_statement_rejects_missing_merge_key_in_columns(self):
        cfg = _make_cfg(
            allow_final_merge=True,
            final_table="forest_structure_with_biomass",
        )
        with pytest.raises(ValueError) as exc:
            build_merge_statement(cfg, columns=["species", "biomass_estimate"])
        assert "primary_key" in str(exc.value)

    def test_merge_statement_with_overridden_key(self):
        cfg = _make_cfg(
            allow_final_merge=True,
            final_table="forest_structure_with_biomass",
            merge_key_column="tree_id",
        )
        sql = build_merge_statement(
            cfg,
            columns=["tree_id", "biomass_estimate", "equation_used"],
        )
        assert "ON T.tree_id = S.tree_id" in sql


# ---------------------------------------------------------------------------
# prepare_stage_dataframe
# ---------------------------------------------------------------------------


class TestPrepareStage:
    def test_passes_through_when_merge_key_present(self):
        df = _stub_enriched()
        cfg = _make_cfg()
        out = prepare_stage_dataframe(df, cfg=cfg)
        assert list(out.columns) == list(df.columns)
        assert len(out) == len(df)

    def test_does_not_mutate_input(self):
        df = _stub_enriched()
        original_cols = list(df.columns)
        prepare_stage_dataframe(df, cfg=_make_cfg())
        assert list(df.columns) == original_cols

    def test_raises_when_merge_key_missing(self):
        df = _stub_enriched().drop(columns=["primary_key"])
        with pytest.raises(ValueError) as exc:
            prepare_stage_dataframe(df, cfg=_make_cfg())
        assert "primary_key" in str(exc.value)
        assert "--bq-merge-key" in str(exc.value)

    def test_passes_through_when_overridden_merge_key_present(self):
        df = _stub_enriched().rename(columns={"primary_key": "tree_id"})
        cfg = _make_cfg(merge_key_column="tree_id")
        out = prepare_stage_dataframe(df, cfg=cfg)
        assert "tree_id" in out.columns


# ---------------------------------------------------------------------------
# run_writeback orchestration with a stubbed client
# ---------------------------------------------------------------------------


class _FakeQueryJob:
    """Minimal job stub: result() is a no-op; supports validation rows
    + MERGE affected-rows attribute set by the test."""

    def __init__(
        self,
        rows: list[dict] | None = None,
        num_dml_affected_rows: int | None = None,
    ) -> None:
        self._rows = rows or []
        self.num_dml_affected_rows = num_dml_affected_rows

    def result(self):
        return iter(self._rows)


class _FakeStageTable:
    def __init__(self, columns: list[str]) -> None:
        self.schema = [type("F", (), {"name": c})() for c in columns]


class _FakeBigQueryClient:
    """Stubs the minimal BQ surface ``run_writeback`` calls.

    Tracks which sql statements were issued in order so tests can
    assert on the orchestration sequence.
    """

    def __init__(
        self,
        validation_metrics: dict[str, int] | None = None,
        stage_columns: list[str] | None = None,
        merge_affected: int | None = 4,
        runs_schema_columns: list[str] | None = None,
        load_should_fail: bool = False,
    ) -> None:
        self.queries: list[str] = []
        self.loaded_dataframes: list[pd.DataFrame] = []
        self.inserted_rows: list[list[dict]] = []
        self._validation_metrics = validation_metrics or {
            "row_count": 4,
            "null_merge_key": 0,
            "dup_merge_key": 0,
        }
        self._stage_columns = stage_columns or [
            "primary_key",
            "species",
            "biomass_estimate",
            "equation_used",
        ]
        self._merge_affected = merge_affected
        self._runs_schema_columns = runs_schema_columns or [
            "run_id",
            "started_at",
            "finished_at",
            "status",
            "bq_input_project",
            "bq_input_dataset",
            "bq_input_table",
            "bq_output_project",
            "bq_output_dataset",
            "bq_stage_table",
            "bq_final_table",
            "merge_key_column",
            "read_input_rows",
            "stage_rows",
            "estimated_rows",
            "skipped_rows",
            "dataset",
            "equations_source",
            "merge_attempted",
            "merge_authorized",
            "merge_inserted_rows",
            "merge_updated_rows",
            "miaproc_version",
            "bigquery_client_version",
            "error_text",
        ]
        self._load_should_fail = load_should_fail

    def query(self, sql: str, *, job_config: Any = None):  # type: ignore[no-untyped-def]
        self.queries.append(sql)
        # Validation query is the only one that returns rows.
        if "AS row_count" in sql and "AS null_merge_key" in sql:
            return _FakeQueryJob(rows=[self._validation_metrics])
        # MERGE query — return a job carrying num_dml_affected_rows.
        if sql.lstrip().startswith("MERGE "):
            return _FakeQueryJob(num_dml_affected_rows=self._merge_affected)
        # DDL / other no-op queries.
        return _FakeQueryJob()

    def load_table_from_dataframe(self, df, table_ref, *, job_config):
        if self._load_should_fail:
            raise RuntimeError("simulated load failure")
        self.loaded_dataframes.append(df)
        return _FakeQueryJob()

    def get_table(self, table_ref):
        # Stage table lookup vs runs table lookup: dispatch by tail.
        if table_ref.endswith("cf_biomass_runs"):
            return _FakeStageTable(self._runs_schema_columns)
        return _FakeStageTable(self._stage_columns)

    def insert_rows_json(self, table, rows):
        self.inserted_rows.extend(rows)
        return []  # no errors


class TestRunWritebackStageOnly:
    def test_stage_only_default_records_stage_only_succeeded(self):
        df = _stub_enriched()
        cfg = _make_cfg()
        client = _FakeBigQueryClient()
        result = run_writeback(
            df, cfg, run_id="local-test", started_at="2026-05-06T00:00:00",
            client=client,
        )
        assert isinstance(result, WritebackResult)
        assert result.status == "stage_only_succeeded"
        assert result.stage_rows == 4
        assert result.merge_attempted is False
        assert result.merge_authorized is False
        assert result.merge_inserted_rows is None
        assert result.merge_updated_rows is None
        # Validation metrics propagated.
        assert result.validation_metrics["row_count"] == 4
        # One row inserted into runs table.
        assert len(client.inserted_rows) == 1
        run_row = client.inserted_rows[0]
        assert run_row["status"] == "stage_only_succeeded"
        assert run_row["merge_attempted"] is False
        assert run_row["merge_authorized"] is False
        assert run_row["merge_key_column"] == "primary_key"
        # Stage frame was loaded.
        assert len(client.loaded_dataframes) == 1


class TestRunWritebackMerge:
    def test_explicit_merge_records_succeeded_and_merge_counts(self):
        df = _stub_enriched()
        cfg = _make_cfg(
            allow_final_merge=True,
            final_table="forest_structure_with_biomass",
        )
        client = _FakeBigQueryClient(merge_affected=4)
        result = run_writeback(
            df, cfg, run_id="local-test-merge",
            started_at="2026-05-06T00:00:00", client=client,
        )
        assert result.status == "succeeded"
        assert result.merge_attempted is True
        assert result.merge_authorized is True
        assert result.merge_inserted_rows == 4
        assert result.stage_rows == 4
        # Runs row has the merge counts.
        run_row = client.inserted_rows[0]
        assert run_row["merge_inserted_rows"] == 4
        assert run_row["status"] == "succeeded"


class TestRunWritebackValidationFailure:
    def test_validation_failure_raises_and_records_status(self):
        df = _stub_enriched()
        cfg = _make_cfg(
            allow_final_merge=True,
            final_table="forest_structure_with_biomass",
        )
        # Stage validation reports duplicate primary_keys → must fail.
        client = _FakeBigQueryClient(
            validation_metrics={
                "row_count": 4,
                "null_merge_key": 0,
                "dup_merge_key": 2,
            }
        )
        with pytest.raises(WritebackValidationError) as exc:
            run_writeback(
                df, cfg, run_id="local-fail",
                started_at="2026-05-06T00:00:00", client=client,
            )
        assert exc.value.metrics["dup_merge_key"] == 2
        # State attached for CLI failure-path JSON.
        state = getattr(exc.value, "miaproc_writeback_state", None)
        assert state is not None
        assert state["status"] == "validation_failed"
        assert state["merge_attempted"] is False
        # MERGE statement must NOT have been issued.
        assert not any(
            q.lstrip().startswith("MERGE ") for q in client.queries
        )
        # Runs row recorded with validation_failed status.
        assert len(client.inserted_rows) == 1
        assert client.inserted_rows[0]["status"] == "validation_failed"


class TestRunWritebackGenericFailure:
    def test_load_failure_records_failed_and_reraises(self):
        df = _stub_enriched()
        cfg = _make_cfg()
        client = _FakeBigQueryClient(load_should_fail=True)
        with pytest.raises(RuntimeError) as exc:
            run_writeback(
                df, cfg, run_id="local-fail-2",
                started_at="2026-05-06T00:00:00", client=client,
            )
        assert "simulated load failure" in str(exc.value)
        state = getattr(exc.value, "miaproc_writeback_state", None)
        assert state is not None
        assert state["status"] == "failed"
        assert state["merge_attempted"] is False
        # Runs row recorded with failed status (best-effort).
        if client.inserted_rows:
            assert client.inserted_rows[0]["status"] == "failed"


# ---------------------------------------------------------------------------
# Module-level absence checks (the watermark-omission contract)
# ---------------------------------------------------------------------------


class TestNoWatermarkSurface:
    """The biomass M20 design omits watermarks. These tests guard against
    accidental re-introduction of watermark plumbing copied from the eddy
    parallel."""

    def test_module_exposes_no_watermark_symbols(self):
        for name in (
            "render_watermark_table_ddl",
            "build_watermark_merge",
            "read_watermark",
            "advance_watermark",
        ):
            assert not hasattr(wb, name), (
                f"biomass writeback should NOT expose {name!r} — biomass "
                "M20 is identity-keyed (per-tree primary_key), not "
                "time-series. See run-summary M20 block for rationale."
            )

    def test_writeback_result_has_no_watermark_fields(self):
        df = _stub_enriched()
        cfg = _make_cfg()
        client = _FakeBigQueryClient()
        result = run_writeback(
            df, cfg, run_id="local-test",
            started_at="2026-05-06T00:00:00", client=client,
        )
        d = result.to_dict()
        assert "watermark_advanced" not in d
        assert "watermark_value" not in d
        assert "watermark_table_fqn" not in d
