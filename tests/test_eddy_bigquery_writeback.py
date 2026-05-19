"""Tests for the BigQuery writeback / merge-control layer (M8).

The ``google-cloud-bigquery`` extra may not be installed in the
default CI slice, so these tests:

- exercise the pure SQL/DDL builders without any BigQuery import;
- inject a fake client that satisfies the small subset of the
  ``Client`` API the orchestration uses (``query``,
  ``load_table_from_dataframe``, ``get_table``, ``insert_rows_json``).

The fake client also records every operation in submission order so
tests can assert that the writeback orchestration ran the steps in
the right sequence and never attempted a forbidden write.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from miaproc.eddy import bigquery_writeback as wb
from miaproc.eddy.bigquery_writeback import (
    BigQueryWritebackConfig,
    COLUMN_COLLISION_ATTRS_KEY,
    DuplicateStageColumnsError,
    HUMIDITY_DERIVED_RENAME,
    HUMIDITY_SOURCE_COLUMN,
    WritebackResult,
    WritebackValidationError,
    bigquery_field_key,
    build_merge_statement,
    build_validation_query,
    ensure_unique_stage_columns,
    prepare_silver_stage_payload,
    prepare_stage_dataframe,
    render_runs_table_ddl,
    render_watermark_table_ddl,
    run_writeback,
    validate_source_columns_unique,
)


# ---------------------------------------------------------------------------
# Helpers / fakes
# ---------------------------------------------------------------------------


def _make_cfg(**overrides: Any) -> BigQueryWritebackConfig:
    base = dict(
        output_project="manglaria-staging",
        output_dataset="manglaria_lakehouse_ds",
        stage_table="cf_s2_stage_test",
        control_dataset="_orch",
        final_table="carbon_flux_eddycovariance_s2_filt_1",
        allow_final_merge=False,
        run_id="local-20260426T200000Z-1234",
        site_id="RBRL",
    )
    base.update(overrides)
    return BigQueryWritebackConfig(**base)


def _make_processed_df(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "primary_key": [f"pk-{i}" for i in range(n)],
            "site_id": ["RBRL"] * n,
            "timestamp": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "NEE_f": [0.1, 0.2, 0.3, 0.4],
            "Reco": [1.0, 1.1, 1.2, 1.3],
            "GPP": [0.0, 0.5, 1.0, 1.5],
        }
    )


class _FakeQueryJob:
    def __init__(self, rows: list[Any] | None = None,
                 num_dml_affected_rows: int | None = None):
        self._rows = rows or []
        self.num_dml_affected_rows = num_dml_affected_rows

    def result(self):
        return iter(self._rows)


class _FakeLoadJob:
    def result(self):
        return None


class _FakeTable:
    def __init__(self, schema: list[str]):
        self.schema = [SimpleNamespace(name=name) for name in schema]


class _FakeBQClient:
    """Minimal in-memory fake of ``google.cloud.bigquery.Client``.

    Records every call in ``self.operations`` so tests can assert
    sequence + arguments. Configurable per-test via:

    - ``query_handler(sql, job_config)`` -> ``_FakeQueryJob``
    - ``load_handler(df, table_ref, job_config)`` -> ``_FakeLoadJob``
    - ``schema_for(table_ref)`` -> list[str]
    - ``insert_handler(table, rows)`` -> list (errors)
    """

    def __init__(self):
        self.operations: list[tuple[str, Any]] = []
        self._inserted_rows: list[dict[str, Any]] = []
        self._schema_for: dict[str, list[str]] = {}
        self.query_handler = self._default_query_handler
        self.load_handler = lambda df, table_ref, job_config=None: _FakeLoadJob()
        self.insert_handler = lambda table, rows: []

    def set_schema(self, table_ref: str, columns: list[str]) -> None:
        self._schema_for[table_ref] = columns

    @property
    def inserted_rows(self) -> list[dict[str, Any]]:
        return list(self._inserted_rows)

    def _default_query_handler(self, sql: str, job_config: Any = None):
        return _FakeQueryJob()

    # --- BQ Client surface used by writeback ---------------------------------
    def query(self, sql: str, job_config: Any = None):
        self.operations.append(("query", (sql, job_config)))
        return self.query_handler(sql, job_config)

    def load_table_from_dataframe(self, df, table_ref, job_config=None):
        self.operations.append(
            ("load_table_from_dataframe", (table_ref, len(df), job_config))
        )
        return self.load_handler(df, table_ref, job_config)

    def get_table(self, table_ref):
        self.operations.append(("get_table", table_ref))
        cols = self._schema_for.get(table_ref, ["primary_key", "site_id",
                                                "timestamp", "NEE_f"])
        return _FakeTable(cols)

    def insert_rows_json(self, table, rows):
        self.operations.append(("insert_rows_json", len(rows)))
        self._inserted_rows.extend(rows)
        return self.insert_handler(table, rows)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestPrepareStageDataframe:
    """M10 schema-mapping helper that aligns the M6 backend output with
    the live ``_s2_filt_1`` extended source-flux schema (guide 001 §2.1)."""

    def _processed(self, n: int = 3) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=n, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
                "NEE_f": [0.11, 0.21, 0.31],
                "NEE_fqc": [0, 0, 0],
                "GPP": [0.0, 1.0, 2.0],
                "Reco": [1.0, 1.0, 1.0],
                "Tair": [20.0, 21.0, 22.0],
                "Tair_f": [20.5, 21.5, 22.5],
                "Rg": [0.0, 100.0, 200.0],
                "Rg_f": [0.0, 100.0, 200.0],
                "VPD": [5.0, 6.0, 7.0],
                "VPD_f": [5.1, 6.1, 7.1],
                "USTAR": [0.2, 0.3, 0.4],
            }
        )

    def _source_flux(self) -> pd.DataFrame:
        # Two of three processed timestamps have a source row; the third
        # is a regularized insert with no source primary_key.
        return pd.DataFrame(
            {
                "site_id": ["RBRL", "RBRL"],
                "timestamp": pd.to_datetime(
                    ["2025-08-01 00:00:00+00:00", "2025-08-01 00:30:00+00:00"]
                ),
                "primary_key": ["src-pk-A", "src-pk-B"],
                "co2_flux": [0.1, 0.2],
                "air_temperature": [293.15, 294.15],
                "u_star": [0.21, 0.22],
            }
        )

    def test_lowercase_analytical_columns_and_dateAndTime(self):
        out = prepare_stage_dataframe(self._processed(), site_id="RBRL")
        for col in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f", "dateAndTime"):
            assert col in out.columns, f"missing column {col}"
        assert out["nee_f"].iloc[0] == 0.11  # from NEE_f
        assert out["sw_in_f"].iloc[1] == 100.0  # from Rg_f
        assert out["ta_f"].iloc[2] == 22.5  # from Tair_f
        # dateAndTime is the ISO-style STRING form.
        assert out["dateAndTime"].iloc[0] == "2025-08-01 00:00:00"
        # Identity is materialized.
        assert (out["site_id"] == "RBRL").all()
        assert out["primary_key"].is_unique

    def test_source_primary_key_preferred_over_synthesized(self):
        out = prepare_stage_dataframe(
            self._processed(),
            site_id="RBRL",
            source_flux_df=self._source_flux(),
        )
        # First two rows have source primary_key; third falls back.
        assert out["primary_key"].iloc[0] == "src-pk-A"
        assert out["primary_key"].iloc[1] == "src-pk-B"
        assert out["primary_key"].iloc[2].startswith("RBRL|2025-08-01T01:00:00")
        # Source pass-through columns are present.
        assert "co2_flux" in out.columns
        assert out["co2_flux"].iloc[0] == 0.1
        # Regularized insert has NaN for source columns.
        assert pd.isna(out["co2_flux"].iloc[2])

    def test_target_columns_filter_drops_extras_keeps_identity(self):
        target = [
            "primary_key",
            "site_id",
            "timestamp",
            "dateAndTime",
            "nee_f",
            "nee_fqc",
            "sw_in_f",
            "ta_f",
            "vpd_f",
            "co2_flux",
            "air_temperature",
        ]
        out = prepare_stage_dataframe(
            self._processed(),
            site_id="RBRL",
            source_flux_df=self._source_flux(),
            target_columns=target,
        )
        # Engine-only outputs not in the target schema are dropped.
        for dropped in ("DateTime", "NEE", "NEE_f", "GPP", "Reco", "USTAR"):
            assert dropped not in out.columns
        # Target-listed columns that were sourced are kept.
        for kept in ("nee_f", "ta_f", "co2_flux", "air_temperature"):
            assert kept in out.columns
        # Identity triple always survives.
        for ident in ("primary_key", "site_id", "timestamp"):
            assert ident in out.columns

    def test_caller_frames_are_not_mutated(self):
        proc = self._processed()
        src = self._source_flux()
        proc_before = proc.copy(deep=True)
        src_before = src.copy(deep=True)
        _ = prepare_stage_dataframe(
            proc, site_id="RBRL", source_flux_df=src,
        )
        pd.testing.assert_frame_equal(proc, proc_before)
        pd.testing.assert_frame_equal(src, src_before)

    def test_missing_datetime_raises(self):
        with pytest.raises(ValueError, match="missing the 'DateTime' column"):
            prepare_stage_dataframe(
                pd.DataFrame({"NEE_f": [0.1]}), site_id="RBRL"
            )

    def test_target_types_cast_float_to_nullable_int(self):
        """The M7 nullable-Int -> float64 normalization on the read side
        leaves integer-typed source columns as floats. When the target
        table has those columns as INT64, prepare_stage_dataframe must
        coerce them back to pandas ``Int64`` before the stage write so
        the live MERGE doesn't reject FLOAT64 -> INT64 implicit casts
        (the actual M10 live blocker that surfaced as
        ``Value of type FLOAT64 cannot be assigned to daytime``)."""
        proc = self._processed()
        # Build a source-flux frame with an "INT64-from-source" column
        # that the M7 normalizer would have left as float64.
        src = pd.DataFrame(
            {
                "site_id": ["RBRL", "RBRL"],
                "timestamp": pd.to_datetime(
                    ["2025-08-01 00:00:00+00:00",
                     "2025-08-01 00:30:00+00:00"]
                ),
                "primary_key": ["src-A", "src-B"],
                "daytime": [1.0, float("nan")],  # came from nullable Int64
            }
        )
        target_types = {
            "primary_key": "STRING",
            "site_id": "STRING",
            "timestamp": "TIMESTAMP",
            "nee_fqc": "INT64",
            "daytime": "INT64",
        }
        out = prepare_stage_dataframe(
            proc, site_id="RBRL", source_flux_df=src,
            target_columns=list(target_types) + ["dateAndTime", "nee_f"],
            target_types=target_types,
        )
        assert str(out["daytime"].dtype) == "Int64"
        assert str(out["nee_fqc"].dtype) == "Int64"
        # NaN survives as pd.NA after the float->Int64 cast.
        assert out["daytime"].iloc[0] == 1
        assert pd.isna(out["daytime"].iloc[1])


class TestConfigValidation:
    def test_valid_config_passes_validate(self):
        _make_cfg().validate()  # no raise

    def test_forbidden_production_project_rejected(self):
        cfg = _make_cfg(output_project="manglaria")
        with pytest.raises(ValueError, match="forbidden project"):
            cfg.validate()

    def test_allow_final_merge_requires_final_table(self):
        cfg = _make_cfg(final_table=None, allow_final_merge=True)
        with pytest.raises(ValueError, match="allow_final_merge=True requires"):
            cfg.validate()

    def test_empty_required_fields_rejected(self):
        cfg = _make_cfg(stage_table="")
        with pytest.raises(ValueError, match="stage_table must be"):
            cfg.validate()


# ---------------------------------------------------------------------------
# DDL + SQL builders
# ---------------------------------------------------------------------------


class TestSQLBuilders:
    def test_runs_ddl_includes_required_columns(self):
        cfg = _make_cfg()
        ddl = render_runs_table_ddl(cfg)
        for col in (
            "run_id",
            "started_at",
            "finished_at",
            "status",
            "merge_attempted",
            "merge_authorized",
            "watermark_advanced",
        ):
            assert col in ddl
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "manglaria-staging" in ddl

    def test_watermark_ddl_uses_site_id_pk(self):
        cfg = _make_cfg()
        ddl = render_watermark_table_ddl(cfg)
        assert "site_id STRING NOT NULL" in ddl
        assert "last_processed_timestamp TIMESTAMP NOT NULL" in ddl

    def test_validation_query_covers_required_metrics(self):
        cfg = _make_cfg()
        sql = build_validation_query(cfg)
        for piece in (
            "COUNT(*) AS row_count",
            "COUNTIF(site_id IS NULL) AS null_site_id",
            "COUNTIF(timestamp IS NULL) AS null_timestamp",
            "COUNTIF(primary_key IS NULL) AS null_primary_key",
            "dup_site_timestamp",
            "dup_primary_key",
        ):
            assert piece in sql

    def test_merge_statement_keys_on_site_id_and_timestamp(self):
        cfg = _make_cfg()
        cols = ["primary_key", "site_id", "timestamp", "NEE_f", "Reco"]
        sql = build_merge_statement(cfg, columns=cols)
        assert "ON T.site_id = S.site_id AND T.timestamp = S.timestamp" in sql
        # Non-key updates only.
        assert "primary_key = S.primary_key" in sql
        assert "NEE_f = S.NEE_f" in sql
        assert "Reco = S.Reco" in sql
        assert "site_id = S.site_id" not in sql.split("UPDATE SET")[1].split(
            "WHEN NOT MATCHED"
        )[0]
        # No DELETE.
        assert "DELETE" not in sql

    def test_merge_requires_final_table(self):
        cfg = _make_cfg(final_table=None)
        with pytest.raises(ValueError, match="final_table must be set"):
            build_merge_statement(cfg, columns=["site_id", "timestamp", "x"])

    def test_merge_requires_non_key_columns(self):
        cfg = _make_cfg()
        with pytest.raises(ValueError, match="no non-key columns"):
            build_merge_statement(cfg, columns=["site_id", "timestamp"])


# ---------------------------------------------------------------------------
# validate_stage_table
# ---------------------------------------------------------------------------


class TestValidateStageTable:
    def _client_with_metrics(self, metrics: dict[str, Any]) -> _FakeBQClient:
        client = _FakeBQClient()
        row = SimpleNamespace(**metrics)

        def _handler(sql, job_config=None):
            return _FakeQueryJob(rows=[row])

        client.query_handler = _handler
        return client

    def test_clean_metrics_pass(self):
        cfg = _make_cfg()
        client = self._client_with_metrics(
            {
                "row_count": 4813,
                "null_site_id": 0,
                "null_timestamp": 0,
                "null_primary_key": 0,
                "dup_site_timestamp": 0,
                "dup_primary_key": 0,
            }
        )
        metrics = wb.validate_stage_table(cfg, client=client)
        assert metrics["row_count"] == 4813

    def test_zero_rows_raises(self):
        cfg = _make_cfg()
        client = self._client_with_metrics(
            {
                "row_count": 0,
                "null_site_id": 0,
                "null_timestamp": 0,
                "null_primary_key": 0,
                "dup_site_timestamp": 0,
                "dup_primary_key": 0,
            }
        )
        with pytest.raises(WritebackValidationError) as exc:
            wb.validate_stage_table(cfg, client=client)
        assert exc.value.metrics["row_count"] == 0

    def test_duplicate_keys_raise(self):
        cfg = _make_cfg()
        client = self._client_with_metrics(
            {
                "row_count": 100,
                "null_site_id": 0,
                "null_timestamp": 0,
                "null_primary_key": 0,
                "dup_site_timestamp": 2,
                "dup_primary_key": 5,
            }
        )
        with pytest.raises(WritebackValidationError) as exc:
            wb.validate_stage_table(cfg, client=client)
        assert "dup_site_timestamp=2" in str(exc.value)
        assert "dup_primary_key=5" in str(exc.value)


# ---------------------------------------------------------------------------
# merge_stage_into_final
# ---------------------------------------------------------------------------


class TestMergeStageIntoFinal:
    def test_merge_refused_without_explicit_authorization(self):
        cfg = _make_cfg(allow_final_merge=False)
        client = _FakeBQClient()
        with pytest.raises(RuntimeError, match="allow_final_merge=True"):
            wb.merge_stage_into_final(cfg, client=client)
        # And the client must NOT have been asked to run any SQL.
        assert all(op[0] != "query" for op in client.operations)

    def test_merge_uses_stage_schema_and_returns_counts(self):
        cfg = _make_cfg(allow_final_merge=True)
        client = _FakeBQClient()
        client.set_schema(
            "manglaria-staging.manglaria_lakehouse_ds.cf_s2_stage_test",
            ["primary_key", "site_id", "timestamp", "NEE_f", "Reco"],
        )
        client.query_handler = lambda sql, job_config=None: _FakeQueryJob(
            num_dml_affected_rows=42
        )
        out = wb.merge_stage_into_final(cfg, client=client)
        assert out["inserted"] == 42
        # MERGE SQL was issued exactly once.
        merge_calls = [op for op in client.operations if op[0] == "query"]
        assert len(merge_calls) == 1
        merge_sql = merge_calls[0][1][0]
        assert merge_sql.startswith("MERGE")
        assert "ON T.site_id = S.site_id AND T.timestamp = S.timestamp" in merge_sql


# ---------------------------------------------------------------------------
# run_writeback orchestrator
# ---------------------------------------------------------------------------


class TestRunWriteback:
    def _patch_validate_pass(self, monkeypatch, metrics=None):
        metrics = metrics or {
            "row_count": 4,
            "null_site_id": 0,
            "null_timestamp": 0,
            "null_primary_key": 0,
            "dup_site_timestamp": 0,
            "dup_primary_key": 0,
        }
        monkeypatch.setattr(
            wb, "validate_stage_table", lambda cfg, client=None: metrics
        )
        return metrics

    def test_stage_only_default_does_not_merge_or_advance_watermark(
        self, monkeypatch
    ):
        cfg = _make_cfg(allow_final_merge=False)
        client = _FakeBQClient()
        client.set_schema(
            "manglaria-staging._orch.cf_s2_runs",
            ["run_id", "started_at", "finished_at", "status",
             "stage_rows", "merge_attempted", "merge_authorized",
             "watermark_advanced"],
        )
        self._patch_validate_pass(monkeypatch)

        df = _make_processed_df()
        result = run_writeback(
            df, cfg, run_id="local-x", site_id="RBRL",
            started_at="2026-04-26T20:00:00+00:00",
            client=client,
        )
        assert isinstance(result, WritebackResult)
        assert result.status == wb.RUN_STATUS_STAGE_ONLY_SUCCEEDED
        assert result.merge_attempted is False
        assert result.merge_authorized is False
        assert result.watermark_advanced is False
        # The runs row was inserted with the stage-only-succeeded status.
        assert client.inserted_rows
        assert client.inserted_rows[0]["status"] == wb.RUN_STATUS_STAGE_ONLY_SUCCEEDED

    def test_explicit_merge_runs_merge_and_advances_watermark(
        self, monkeypatch
    ):
        cfg = _make_cfg(allow_final_merge=True)
        client = _FakeBQClient()
        client.set_schema(
            "manglaria-staging.manglaria_lakehouse_ds.cf_s2_stage_test",
            ["primary_key", "site_id", "timestamp", "NEE_f"],
        )
        client.set_schema(
            "manglaria-staging._orch.cf_s2_runs",
            ["run_id", "started_at", "finished_at", "status",
             "stage_rows", "merge_attempted", "merge_authorized",
             "watermark_advanced", "watermark_value"],
        )
        client.query_handler = lambda sql, job_config=None: _FakeQueryJob(
            num_dml_affected_rows=10
        )
        self._patch_validate_pass(monkeypatch)

        df = _make_processed_df()
        result = run_writeback(
            df, cfg, run_id="local-x", site_id="RBRL",
            started_at="2026-04-26T20:00:00+00:00",
            client=client,
        )
        assert result.status == wb.RUN_STATUS_SUCCEEDED
        assert result.merge_attempted is True
        assert result.merge_authorized is True
        assert result.watermark_advanced is True
        assert result.watermark_value is not None
        assert client.inserted_rows[0]["status"] == wb.RUN_STATUS_SUCCEEDED

    def test_validation_failure_records_validation_failed_and_raises(
        self, monkeypatch
    ):
        cfg = _make_cfg(allow_final_merge=True)
        client = _FakeBQClient()
        client.set_schema(
            "manglaria-staging._orch.cf_s2_runs",
            ["run_id", "status", "started_at", "finished_at",
             "stage_rows", "merge_attempted", "merge_authorized",
             "watermark_advanced", "error_text"],
        )

        def _bad_validate(cfg, client=None):
            raise WritebackValidationError(
                "Stage validation failed: dup_primary_key=3",
                metrics={"row_count": 100, "dup_primary_key": 3},
            )

        monkeypatch.setattr(wb, "validate_stage_table", _bad_validate)

        with pytest.raises(WritebackValidationError):
            run_writeback(
                _make_processed_df(), cfg, run_id="local-x",
                site_id="RBRL",
                started_at="2026-04-26T20:00:00+00:00",
                client=client,
            )
        assert client.inserted_rows
        assert (
            client.inserted_rows[0]["status"] == wb.RUN_STATUS_VALIDATION_FAILED
        )
        # Even on validation failure, MERGE was never attempted.
        assert all(
            not (
                op[0] == "query"
                and isinstance(op[1][0], str)
                and op[1][0].startswith("MERGE ")
                and "_s2_filt_1" in op[1][0]
            )
            for op in client.operations
        )

    def test_failed_merge_attaches_writeback_state_to_exception(
        self, monkeypatch
    ):
        """M10 failure-path drift fix: when MERGE itself raises, the
        re-raised exception must carry a ``miaproc_writeback_state``
        attribute with ``merge_attempted=True`` so the CLI's best-effort
        local ``run.json`` agrees with the authoritative
        ``cf_s2_runs`` row."""
        cfg = _make_cfg(allow_final_merge=True)
        client = _FakeBQClient()
        client.set_schema(
            "manglaria-staging._orch.cf_s2_runs",
            ["run_id", "status", "started_at", "finished_at",
             "stage_rows", "merge_attempted", "merge_authorized",
             "watermark_advanced", "error_text"],
        )
        self._patch_validate_pass(monkeypatch)

        def _bad_merge(cfg, client=None):
            raise RuntimeError("simulated MERGE failure")

        monkeypatch.setattr(wb, "merge_stage_into_final", _bad_merge)

        with pytest.raises(RuntimeError) as exc_info:
            run_writeback(
                _make_processed_df(), cfg, run_id="local-x",
                site_id="RBRL",
                started_at="2026-04-27T00:00:00+00:00",
                client=client,
            )
        state = getattr(exc_info.value, "miaproc_writeback_state", None)
        assert state is not None
        assert state["merge_attempted"] is True
        assert state["merge_authorized"] is True
        assert state["status"] == wb.RUN_STATUS_FAILED

    def test_validation_failure_state_records_no_merge_attempt(
        self, monkeypatch
    ):
        """Counterpart: the validation-failure path must attach state
        with ``merge_attempted=False`` (validation runs before merge)."""
        cfg = _make_cfg(allow_final_merge=True)
        client = _FakeBQClient()
        client.set_schema(
            "manglaria-staging._orch.cf_s2_runs",
            ["run_id", "status", "started_at", "finished_at",
             "stage_rows", "merge_attempted", "merge_authorized",
             "watermark_advanced", "error_text"],
        )

        def _bad_validate(cfg, client=None):
            raise WritebackValidationError(
                "stage validation failed",
                metrics={"row_count": 100, "dup_primary_key": 3},
            )

        monkeypatch.setattr(wb, "validate_stage_table", _bad_validate)
        with pytest.raises(WritebackValidationError) as exc_info:
            run_writeback(
                _make_processed_df(), cfg, run_id="local-x",
                site_id="RBRL",
                started_at="2026-04-27T00:00:00+00:00",
                client=client,
            )
        state = getattr(exc_info.value, "miaproc_writeback_state", None)
        assert state is not None
        assert state["merge_attempted"] is False
        assert state["status"] == wb.RUN_STATUS_VALIDATION_FAILED

    def test_forbidden_project_rejected_before_any_write(self, monkeypatch):
        cfg = _make_cfg(output_project="manglaria")
        client = _FakeBQClient()
        with pytest.raises(ValueError, match="forbidden project"):
            run_writeback(
                _make_processed_df(), cfg, run_id="local-x",
                site_id="RBRL",
                started_at="2026-04-26T20:00:00+00:00",
                client=client,
            )
        # Nothing was sent to the client.
        assert client.operations == []


# ---------------------------------------------------------------------------
# M28: stage-payload column-uniqueness guard, silver preservation,
# gold preservation, and rH / rH_norm_s humidity collision policy.
# ---------------------------------------------------------------------------


def _silver_with_bronze_sentinel(n: int = 3) -> pd.DataFrame:
    """A silver-shaped frame that carries an EddyPro source-only
    column ``bronze_only_flag`` so the M28 bronze->silver preservation
    contract can be exercised end-to-end."""
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, 0.3],
            "USTAR": [0.2, 0.3, 0.4],
            "Tair": [20.0, 21.0, 22.0],
            "VPD": [5.0, 6.0, 7.0],
            "Rg": [0.0, 100.0, 200.0],
            "QC_NEE": [0, 0, 0],
            "H": [10.0, 20.0, 30.0],
            "LE": [50.0, 60.0, 70.0],
            "P_RAIN": [0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0],
            # Source-only sentinel: not produced by stage1, but
            # preserved through the bronze->silver pipeline.
            "bronze_only_flag": [1, 0, 1],
        }
    )


def _silver_with_duplicate_rh() -> pd.DataFrame:
    """Stage-1-shaped frame carrying two columns literally named
    ``rH``. Pandas allows this even though BigQuery rejects it; the
    M28 dedup helper must resolve the collision before any
    ``load_table_from_dataframe`` call."""
    df = pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, 0.3],
            "USTAR": [0.2, 0.3, 0.4],
            "Tair": [20.0, 21.0, 22.0],
            "VPD": [5.0, 6.0, 7.0],
            "Rg": [0.0, 100.0, 200.0],
            "QC_NEE": [0, 0, 0],
            "H": [10.0, 20.0, 30.0],
            "LE": [50.0, 60.0, 70.0],
            "P_RAIN": [0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0],  # source
        }
    )
    # Append a second ``rH`` column with diverging values.
    extra = pd.DataFrame({"rH": [55.0, 75.0, 85.0]})
    df = pd.concat([df, extra], axis=1)
    # Sanity check: the fixture must produce a literal duplicate.
    assert list(df.columns).count("rH") == 2
    return df


class TestEnsureUniqueStageColumns:
    def test_unique_input_is_no_op(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        out, actions = ensure_unique_stage_columns(df)
        assert list(out.columns) == ["a", "b"]
        assert actions == []
        # Marker still attached for downstream introspection.
        assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == []

    def test_caller_frame_not_mutated(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        before = df.copy(deep=True)
        _ = ensure_unique_stage_columns(df)
        pd.testing.assert_frame_equal(df, before)

    def test_equivalent_rH_duplicate_is_suppressed(self):
        base = pd.DataFrame(
            {"DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            )}
        )
        rh = pd.Series([60.0, 70.0, 80.0])
        # Two columns literally named ``rH`` with identical values.
        df = pd.concat(
            [base, rh.rename("rH"), rh.copy().rename("rH")], axis=1
        )
        out, actions = ensure_unique_stage_columns(df)
        assert list(out.columns).count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME not in out.columns
        assert any(
            a["action"] == "suppressed_equivalent_duplicate" for a in actions
        )

    def test_divergent_rH_duplicate_renamed_to_rH_norm_s(self):
        df = _silver_with_duplicate_rh()
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        assert cols.count("rH") == 1, cols
        assert HUMIDITY_DERIVED_RENAME in cols
        # Source values stay under ``rH``; derived under ``rH_norm_s``.
        assert list(out["rH"]) == [60.0, 70.0, 80.0]
        assert list(out[HUMIDITY_DERIVED_RENAME]) == [55.0, 75.0, 85.0]
        assert any(
            a["action"] == "renamed_divergent_duplicate" for a in actions
        )

    def test_divergent_rH_with_existing_rH_norm_s_uses_suffix(self):
        df = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                HUMIDITY_DERIVED_RENAME: [10.0, 20.0, 30.0],
            }
        )
        df = pd.concat(
            [
                df,
                pd.Series([60.0, 70.0, 80.0]).rename("rH"),
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),
            ],
            axis=1,
        )
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        assert cols.count("rH") == 1
        # Existing rH_norm_s is preserved; new derived gets a suffix.
        assert HUMIDITY_DERIVED_RENAME in cols
        assert f"{HUMIDITY_DERIVED_RENAME}_2" in cols
        assert any(
            a["renamed_to"] == f"{HUMIDITY_DERIVED_RENAME}_2"
            for a in actions
        )

    def test_other_duplicate_raises_duplicate_stage_columns_error(self):
        base = pd.DataFrame(
            {"DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            )}
        )
        df = pd.concat(
            [
                base,
                pd.Series([1, 2, 3]).rename("NEE_f"),
                pd.Series([4, 5, 6]).rename("NEE_f"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            ensure_unique_stage_columns(df)
        assert "NEE_f" in str(exc_info.value)
        assert exc_info.value.duplicate_columns == ("NEE_f",)

    def test_nan_aware_equivalence_treats_paired_nans_as_equal(self):
        base = pd.DataFrame({"x": [1, 2, 3]})
        a = pd.Series([1.0, float("nan"), 3.0]).rename("rH")
        b = pd.Series([1.0, float("nan"), 3.0]).rename("rH")
        df = pd.concat([base, a, b], axis=1)
        out, actions = ensure_unique_stage_columns(df)
        assert list(out.columns).count("rH") == 1
        assert any(
            a_["action"] == "suppressed_equivalent_duplicate"
            for a_ in actions
        )


class TestEnsureUniqueStageColumnsCaseInsensitiveM31:
    """M31: BigQuery field keys are case-insensitive.

    Pandas considered ``RH`` and ``rH`` distinct, so the M28 guard
    let them through and ``load_table_from_dataframe`` failed with
    ``Field rH already exists in schema``. M31 closes that hole by
    operating on ``casefold``-d field keys for both the duplicate
    detection and the reserved-name search for the divergent-derived
    rename. The humidity family is normalized to the canonical
    source name ``rH``; non-humidity case collisions still raise.
    """

    @staticmethod
    def _two_humidity_variants_diverge() -> pd.DataFrame:
        base = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
                "USTAR": [0.2, 0.3, 0.4],
            }
        )
        return pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),
            ],
            axis=1,
        )

    @staticmethod
    def _two_humidity_variants_equivalent() -> pd.DataFrame:
        base = pd.DataFrame({"x": [1, 2, 3]})
        return pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([60.0, 70.0, 80.0]).rename("rH"),
            ],
            axis=1,
        )

    def test_bigquery_field_key_is_casefold(self):
        assert bigquery_field_key("rH") == "rh"
        assert bigquery_field_key("RH") == "rh"
        assert bigquery_field_key("Rh") == "rh"
        assert bigquery_field_key("rH_norm_s") == "rh_norm_s"
        # Idempotent for already-lowercase / non-letter content.
        assert bigquery_field_key("foo_bar_1") == "foo_bar_1"

    def test_rh_case_collision_equivalent_collapses_to_canonical_rH(self):
        df = self._two_humidity_variants_equivalent()
        out, actions = ensure_unique_stage_columns(df)
        # Final payload has BigQuery-unique field keys.
        keys = [bigquery_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)
        # The kept humidity column is canonicalized to ``rH``.
        assert "rH" in out.columns
        assert "RH" not in out.columns
        assert HUMIDITY_DERIVED_RENAME not in out.columns
        # Two actions: canonicalization + suppression of equivalent dup.
        assert any(
            a["action"] == "renamed_to_canonical_humidity"
            and a["renamed_to"] == HUMIDITY_SOURCE_COLUMN
            for a in actions
        )
        assert any(
            a["action"] == "suppressed_equivalent_duplicate"
            for a in actions
        )

    def test_rh_case_collision_divergent_keeps_rH_and_rH_norm_s(self):
        df = self._two_humidity_variants_diverge()
        out, actions = ensure_unique_stage_columns(df)
        keys = [bigquery_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)
        # Source ``RH`` is canonicalized to ``rH`` (the first
        # occurrence wins); the divergent derived ``rH`` is renamed.
        assert "rH" in out.columns
        assert HUMIDITY_DERIVED_RENAME in out.columns
        assert "RH" not in out.columns
        # Source values are preserved under the canonical name.
        assert list(out["rH"]) == [60.0, 70.0, 80.0]
        assert list(out[HUMIDITY_DERIVED_RENAME]) == [55.0, 75.0, 85.0]
        assert any(
            a["action"] == "renamed_to_canonical_humidity"
            for a in actions
        )
        assert any(
            a["action"] == "renamed_divergent_duplicate"
            and a["renamed_to"] == HUMIDITY_DERIVED_RENAME
            for a in actions
        )

    def test_payload_has_no_case_insensitive_duplicate_keys(self):
        """Stronger than ``payload.columns.is_unique``: cover the
        BigQuery uniqueness semantics that surfaced as the cloud
        failure mode."""
        df = self._two_humidity_variants_diverge()
        out, _ = ensure_unique_stage_columns(df)
        # The pandas-level uniqueness check passes both before and
        # after the fix; the case-insensitive check did not pass
        # before M31.
        assert out.columns.is_unique
        keys = [bigquery_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)

    def test_existing_rH_norm_s_still_uses_suffix_under_case_insensitivity(
        self,
    ):
        """The deterministic suffix logic must also consult
        case-folded keys when picking the next available rename
        target, so an existing ``rH_norm_s`` (or a hypothetical
        ``RH_norm_s``) cannot collide with the new derived rename."""
        base = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                HUMIDITY_DERIVED_RENAME: [10.0, 20.0, 30.0],
            }
        )
        df = pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),
            ],
            axis=1,
        )
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        # Source ``RH`` becomes canonical ``rH``; existing
        # ``rH_norm_s`` is preserved; divergent derived gets the
        # numeric suffix.
        assert cols.count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME in cols
        assert f"{HUMIDITY_DERIVED_RENAME}_2" in cols
        keys = [bigquery_field_key(c) for c in cols]
        assert len(set(keys)) == len(keys), cols
        assert any(
            a["renamed_to"] == f"{HUMIDITY_DERIVED_RENAME}_2"
            for a in actions
        )

    def test_non_humidity_case_insensitive_collision_raises(self):
        """``NEE`` + ``nee`` is a case-insensitive duplicate but
        not in the humidity family: the helper must raise
        ``DuplicateStageColumnsError`` so the upstream pipeline is
        fixed before any BigQuery write."""
        base = pd.DataFrame(
            {"DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            )}
        )
        df = pd.concat(
            [
                base,
                pd.Series([0.1, 0.2, 0.3]).rename("NEE"),
                pd.Series([0.11, 0.22, 0.33]).rename("nee"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            ensure_unique_stage_columns(df)
        msg = str(exc_info.value)
        # Both the affected logical key and the source variants are
        # named so the operator can locate the upstream defect.
        assert "nee" in msg
        assert "NEE" in msg
        # The error preserves the source column names for tooling.
        assert set(exc_info.value.duplicate_columns) >= {"NEE", "nee"}

    def test_multiple_non_humidity_case_collisions_all_reported(self):
        base = pd.DataFrame(
            {"x": [1, 2, 3]}
        )
        df = pd.concat(
            [
                base,
                pd.Series([0.1, 0.2, 0.3]).rename("NEE"),
                pd.Series([0.11, 0.22, 0.33]).rename("nee"),
                pd.Series([1, 2, 3]).rename("Tair"),
                pd.Series([4, 5, 6]).rename("tair"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            ensure_unique_stage_columns(df)
        names = set(exc_info.value.duplicate_columns)
        assert {"NEE", "nee", "Tair", "tair"} <= names

    def test_three_way_humidity_collision_canonicalizes_and_suffixes(self):
        """``RH`` + ``Rh`` + ``rH`` should collapse to ``rH`` with the
        right policy applied to each subsequent occurrence."""
        base = pd.DataFrame({"x": [1, 2, 3]})
        df = pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([60.0, 70.0, 80.0]).rename("Rh"),  # equiv
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),  # diverges
            ],
            axis=1,
        )
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        keys = [bigquery_field_key(c) for c in cols]
        assert len(set(keys)) == len(keys), cols
        assert "rH" in cols
        assert HUMIDITY_DERIVED_RENAME in cols
        # First variant renamed to canonical; second equivalent
        # suppressed; third divergent renamed to rH_norm_s.
        actions_by_kind = [a["action"] for a in actions]
        assert "renamed_to_canonical_humidity" in actions_by_kind
        assert "suppressed_equivalent_duplicate" in actions_by_kind
        assert "renamed_divergent_duplicate" in actions_by_kind


class TestPrepareSilverStagePayloadCaseInsensitiveM31:
    """M31: ``prepare_silver_stage_payload`` must inherit the
    case-insensitive humidity policy because cloud silver writeback
    failed with case-only-different ``RH`` + ``rH`` columns."""

    @staticmethod
    def _silver_with_RH_and_rH(*, diverge: bool) -> pd.DataFrame:
        rh = [55.0, 75.0, 85.0] if diverge else [60.0, 70.0, 80.0]
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
                "QC_NEE": [0, 0, 0],
                "Tair": [20.0, 21.0, 22.0],
                "USTAR": [0.2, 0.3, 0.4],
                "RH": [60.0, 70.0, 80.0],
                "rH": rh,
            }
        )

    def test_equivalent_RH_rH_collapses_to_canonical_rH(self):
        silver = self._silver_with_RH_and_rH(diverge=False)
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        keys = [bigquery_field_key(c) for c in payload.columns]
        assert len(set(keys)) == len(keys), list(payload.columns)
        assert "rH" in payload.columns
        assert "RH" not in payload.columns
        assert HUMIDITY_DERIVED_RENAME not in payload.columns
        # Kept ``rH`` carries source values (60, 70, 80) — both
        # variants were equivalent so the first wins.
        assert list(payload["rH"]) == [60.0, 70.0, 80.0]
        assert any(
            a["action"] == "renamed_to_canonical_humidity" for a in actions
        )

    def test_divergent_RH_rH_preserves_both_as_rH_and_rH_norm_s(self):
        silver = self._silver_with_RH_and_rH(diverge=True)
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        keys = [bigquery_field_key(c) for c in payload.columns]
        assert len(set(keys)) == len(keys), list(payload.columns)
        assert "rH" in payload.columns
        assert HUMIDITY_DERIVED_RENAME in payload.columns
        assert "RH" not in payload.columns
        assert list(payload["rH"]) == [60.0, 70.0, 80.0]
        assert list(payload[HUMIDITY_DERIVED_RENAME]) == [55.0, 75.0, 85.0]


class TestPrepareStageDataframeS2FiltOneMappingM31:
    """M31: gold ``prepare_stage_dataframe`` renames source -> target
    rather than duplicating because BigQuery field keys are
    case-insensitive. Keeping both ``NEE_f`` and ``nee_f`` would
    surface as ``Field nee_f already exists in schema`` at the live
    stage load."""

    @staticmethod
    def _gold_with_canonical_backend_outputs() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE_f": [0.11, 0.21, 0.31],
                "NEE_fqc": [0, 0, 0],
                "Rg_f": [0.0, 100.0, 200.0],
                "Tair_f": [20.5, 21.5, 22.5],
                "VPD_f": [5.1, 6.1, 7.1],
            }
        )

    def test_lowercase_targets_present_uppercase_backend_names_dropped(
        self,
    ):
        out = prepare_stage_dataframe(
            self._gold_with_canonical_backend_outputs(),
            site_id="RBRL",
        )
        for col in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f"):
            assert col in out.columns, col
        for col in ("NEE_f", "NEE_fqc", "Rg_f", "Tair_f", "VPD_f"):
            assert col not in out.columns, col
        keys = [bigquery_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)

    def test_values_carry_through_rename(self):
        out = prepare_stage_dataframe(
            self._gold_with_canonical_backend_outputs(),
            site_id="RBRL",
        )
        assert out["nee_f"].iloc[0] == 0.11
        assert out["sw_in_f"].iloc[1] == 100.0
        assert out["ta_f"].iloc[2] == 22.5


class TestValidateSourceColumnsUnique:
    def test_none_is_noop(self):
        validate_source_columns_unique(None, side="bronze")  # no raise

    def test_empty_is_noop(self):
        validate_source_columns_unique(pd.DataFrame(), side="bronze")

    def test_unique_columns_pass(self):
        validate_source_columns_unique(
            pd.DataFrame({"a": [1], "b": [2]}), side="bronze"
        )

    def test_duplicate_source_columns_raise(self):
        a = pd.Series([1, 2]).rename("rH")
        b = pd.Series([3, 4]).rename("rH")
        df = pd.concat([a, b], axis=1)
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            validate_source_columns_unique(df, side="bronze flux source")
        assert "bronze flux source" in str(exc_info.value)
        assert "rH" in str(exc_info.value)
        assert exc_info.value.duplicate_columns == ("rH",)


class TestPrepareSilverStagePayload:
    def test_preserves_bronze_sentinel_column(self):
        silver = _silver_with_bronze_sentinel()
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        assert "bronze_only_flag" in payload.columns
        assert actions == []
        # Identity triple is present and ordered first.
        assert list(payload.columns)[:3] == [
            "primary_key", "site_id", "timestamp"
        ]
        # Source values preserved.
        assert list(payload["bronze_only_flag"]) == [1, 0, 1]
        # Silver columns still present.
        for col in ("NEE", "USTAR", "Tair", "VPD", "Rg", "rH", "H", "LE"):
            assert col in payload.columns, col

    def test_payload_columns_are_unique(self):
        silver = _silver_with_duplicate_rh()
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        # The whole point of M28: the BigQuery client must never see
        # duplicate column names.
        assert payload.columns.is_unique, list(payload.columns)
        assert any(
            a["column"] == HUMIDITY_SOURCE_COLUMN for a in actions
        )

    def test_equivalent_rH_duplicate_suppressed(self):
        silver = _silver_with_bronze_sentinel()
        extra = pd.DataFrame({"rH": silver["rH"].copy()})
        silver_dup = pd.concat([silver, extra], axis=1)
        payload, actions = prepare_silver_stage_payload(
            silver_dup, site_id="RBRL"
        )
        assert list(payload.columns).count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME not in payload.columns
        assert any(
            a["action"] == "suppressed_equivalent_duplicate" for a in actions
        )

    def test_divergent_rH_duplicate_renamed_to_rH_norm_s(self):
        silver = _silver_with_duplicate_rh()
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        cols = list(payload.columns)
        assert cols.count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME in cols
        assert list(payload["rH"]) == [60.0, 70.0, 80.0]
        assert list(payload[HUMIDITY_DERIVED_RENAME]) == [55.0, 75.0, 85.0]
        assert any(
            a["action"] == "renamed_divergent_duplicate"
            and a["renamed_to"] == HUMIDITY_DERIVED_RENAME
            for a in actions
        )

    def test_source_flux_with_duplicate_columns_raises(self):
        silver = _silver_with_bronze_sentinel()
        bad_src = pd.concat(
            [
                pd.Series([1, 2, 3]).rename("rH"),
                pd.Series([4, 5, 6]).rename("rH"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError):
            prepare_silver_stage_payload(
                silver, site_id="RBRL", source_flux_df=bad_src,
            )

    def test_missing_datetime_raises(self):
        with pytest.raises(ValueError, match="missing the 'DateTime'"):
            prepare_silver_stage_payload(
                pd.DataFrame({"x": [1]}), site_id="RBRL"
            )

    def test_caller_frame_not_mutated(self):
        silver = _silver_with_bronze_sentinel()
        before = silver.copy(deep=True)
        _ = prepare_silver_stage_payload(silver, site_id="RBRL")
        pd.testing.assert_frame_equal(silver, before)

    def test_identity_triple_overwrite_recorded_in_actions(self):
        silver = _silver_with_bronze_sentinel()
        silver_with_ids = silver.copy()
        silver_with_ids["site_id"] = "STALE"
        silver_with_ids["timestamp"] = "stale"
        payload, actions = prepare_silver_stage_payload(
            silver_with_ids, site_id="RBRL"
        )
        assert (payload["site_id"] == "RBRL").all()
        # Both identity overwrites are recorded.
        names = {a["column"] for a in actions if a["action"] == "identity_overwrite"}
        assert "site_id" in names
        assert "timestamp" in names


class TestPrepareStageDataframeM28Preservation:
    """M28: when ``preserve_payload_columns=True``, every silver
    column survives into the gold stage payload even when the live
    final-table schema is narrower."""

    def _gold_with_silver(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE_f": [0.11, 0.21, 0.31],
                "NEE_fqc": [0, 0, 0],
                "Rg_f": [0.0, 100.0, 200.0],
                "Tair_f": [20.5, 21.5, 22.5],
                "VPD_f": [5.1, 6.1, 7.1],
                "GPP": [0.0, 1.0, 2.0],
                "Reco": [1.0, 1.0, 1.0],
                # Silver-only sentinel — must survive into the stage
                # payload under the M28 preservation contract.
                "silver_only_flag": [True, False, True],
                "H": [10.0, 20.0, 30.0],
                "LE": [50.0, 60.0, 70.0],
                "rH": [60.0, 70.0, 80.0],
            }
        )

    def test_preserves_silver_only_columns_when_preserve_true(self):
        narrow_target = [
            "primary_key", "site_id", "timestamp",
            "dateAndTime", "nee_f", "nee_fqc",
            "sw_in_f", "ta_f", "vpd_f",
        ]
        out = prepare_stage_dataframe(
            self._gold_with_silver(),
            site_id="RBRL",
            target_columns=narrow_target,
            preserve_payload_columns=True,
        )
        # Silver-only sentinel is preserved even though it's not in
        # the narrow final-table target.
        assert "silver_only_flag" in out.columns
        assert "rH" in out.columns
        assert "H" in out.columns
        # Gold analytical outputs are still there.
        for col in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f"):
            assert col in out.columns, col
        # Identity is preserved.
        for col in ("primary_key", "site_id", "timestamp"):
            assert col in out.columns, col

    def test_legacy_filter_still_works_when_preserve_false(self):
        """Existing M10 callers that pass ``target_columns`` and rely
        on it for filtering keep their behavior so long as they don't
        opt into M28 preservation."""
        narrow_target = [
            "primary_key", "site_id", "timestamp",
            "dateAndTime", "nee_f", "nee_fqc",
            "sw_in_f", "ta_f", "vpd_f",
        ]
        out = prepare_stage_dataframe(
            self._gold_with_silver(),
            site_id="RBRL",
            target_columns=narrow_target,
            preserve_payload_columns=False,
        )
        # Silver-only sentinel is dropped under legacy filtering.
        assert "silver_only_flag" not in out.columns
        assert "rH" not in out.columns

    def test_collision_actions_recorded_in_attrs(self):
        # Build a gold-with-silver frame that carries a duplicate rH.
        df = self._gold_with_silver()
        df = pd.concat(
            [df, pd.Series([55.0, 75.0, 85.0]).rename("rH")], axis=1
        )
        out = prepare_stage_dataframe(
            df,
            site_id="RBRL",
            preserve_payload_columns=True,
        )
        assert out.columns.is_unique
        assert HUMIDITY_DERIVED_RENAME in out.columns
        actions = out.attrs.get(COLUMN_COLLISION_ATTRS_KEY, [])
        assert any(
            a["column"] == HUMIDITY_SOURCE_COLUMN for a in actions
        )

    def test_other_duplicate_raises_before_load(self):
        df = self._gold_with_silver()
        # ``GPP`` is not in the lowercase rename map and not part of
        # the identity triple, so a literal duplicate survives all the
        # internal mutation and reaches ``ensure_unique_stage_columns``.
        df = pd.concat(
            [df, pd.Series([4.0, 5.0, 6.0]).rename("GPP")], axis=1
        )
        with pytest.raises(DuplicateStageColumnsError):
            prepare_stage_dataframe(
                df, site_id="RBRL", preserve_payload_columns=True,
            )
