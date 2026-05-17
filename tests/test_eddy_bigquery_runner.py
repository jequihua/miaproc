"""Tests for the BigQuery-native runner (M7).

The ``google-cloud-bigquery`` extra may not be installed in the default
CI slice, so these tests:

- exercise the pure SQL-building helpers without any BQ import;
- inject a fake client + monkey-patched parameter builder + monkey-
  patched ``_run_query_to_dataframe`` so ``read_bigquery_inputs`` can
  be tested without the real client.
"""
from __future__ import annotations

import pandas as pd
import pytest

from miaproc.eddy import bigquery_runner as br
from miaproc.eddy import (
    BigQueryEddyConfig,
    BigQueryReadResult,
    read_bigquery_inputs,
)


def _make_cfg(**overrides) -> BigQueryEddyConfig:
    base = dict(
        input_project="manglaria",
        input_dataset="manglaria_lakehouse_ds",
        flux_table="carbon_flux_eddycovariance",
        biomet_table="carbon_flux_biomet",
        site_id="RBRL",
    )
    base.update(overrides)
    return BigQueryEddyConfig(**base)


class TestQueryBuilders:
    def test_flux_query_uses_fully_qualified_name_and_site_filter(self):
        cfg = _make_cfg()
        sql = br.build_flux_query(cfg)
        assert (
            "`manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance`"
            in sql
        )
        assert "site_id = @site_id" in sql
        assert "ORDER BY timestamp" in sql
        # No window bounds when none configured.
        assert "@start_ts" not in sql
        assert "@end_ts" not in sql

    def test_biomet_query_uses_biomet_table(self):
        cfg = _make_cfg()
        sql = br.build_biomet_query(cfg)
        assert "`manglaria.manglaria_lakehouse_ds.carbon_flux_biomet`" in sql
        assert "site_id = @site_id" in sql

    def test_window_clauses_added_when_set(self):
        cfg = _make_cfg(
            start_timestamp="2025-01-01T00:00:00Z",
            end_timestamp="2025-02-01T00:00:00Z",
        )
        sql = br.build_flux_query(cfg)
        assert "@start_ts" in sql
        assert "@end_ts" in sql

    def test_only_start_window_renders_only_start_clause(self):
        cfg = _make_cfg(start_timestamp="2025-01-01T00:00:00Z")
        sql = br.build_flux_query(cfg)
        assert "@start_ts" in sql
        assert "@end_ts" not in sql

    def test_billing_project_falls_back_to_input(self):
        assert _make_cfg().billing_project_or_input() == "manglaria"
        assert (
            _make_cfg(billing_project="manglaria-staging").billing_project_or_input()
            == "manglaria-staging"
        )


class _FakeQueryJob:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def to_dataframe(self, create_bqstorage_client: bool = False) -> pd.DataFrame:
        return self._df


class _FakeBQClient:
    """Minimal stand-in for ``google.cloud.bigquery.Client``.

    Records the SQL strings + job configs the runner submits, and
    returns canned DataFrames in submission order.
    """

    def __init__(self, frames: list[pd.DataFrame]):
        self._frames = list(frames)
        self.calls: list[tuple[str, object]] = []

    def query(self, sql: str, job_config=None):
        self.calls.append((sql, job_config))
        if not self._frames:
            raise AssertionError("FakeBQClient ran out of canned frames")
        return _FakeQueryJob(self._frames.pop(0))


class TestReadBigQueryInputs:
    def test_returns_dataframes_and_records_queries(self, monkeypatch):
        flux_df = pd.DataFrame({"timestamp": ["2025-08-01"], "site_id": ["RBRL"]})
        biomet_df = pd.DataFrame({"timestamp": ["2025-08-01"], "site_id": ["RBRL"]})

        # Bypass the lazy ``google.cloud.bigquery`` import inside
        # ``_build_query_parameters`` and ``_run_query_to_dataframe``.
        monkeypatch.setattr(
            br, "_build_query_parameters", lambda cfg: ["site_id=RBRL"]
        )

        def _fake_run(client, sql, parameters, *, use_storage_api):
            return client.query(sql, job_config={"params": parameters}).to_dataframe(
                create_bqstorage_client=use_storage_api
            )

        monkeypatch.setattr(br, "_run_query_to_dataframe", _fake_run)

        client = _FakeBQClient([flux_df, biomet_df])
        cfg = _make_cfg()
        result = read_bigquery_inputs(cfg, client=client)

        assert isinstance(result, BigQueryReadResult)
        assert result.flux_rows == 1
        assert result.biomet_rows == 1
        assert "carbon_flux_eddycovariance" in result.flux_query
        assert "carbon_flux_biomet" in result.biomet_query
        assert result.query_parameters == {"site_id": "RBRL"}
        # Two BQ submissions: flux then biomet.
        assert len(client.calls) == 2

    def test_window_parameters_propagate_into_metadata(self, monkeypatch):
        flux_df = pd.DataFrame({"timestamp": ["2025-08-01"]})
        biomet_df = pd.DataFrame({"timestamp": ["2025-08-01"]})

        monkeypatch.setattr(br, "_build_query_parameters", lambda cfg: [])
        monkeypatch.setattr(
            br,
            "_run_query_to_dataframe",
            lambda client, sql, parameters, *, use_storage_api: client.query(
                sql
            ).to_dataframe(),
        )

        cfg = _make_cfg(
            start_timestamp="2025-01-01T00:00:00Z",
            end_timestamp="2025-02-01T00:00:00Z",
        )
        client = _FakeBQClient([flux_df, biomet_df])
        result = read_bigquery_inputs(cfg, client=client)
        assert result.query_parameters == {
            "site_id": "RBRL",
            "start_ts": "2025-01-01T00:00:00Z",
            "end_ts": "2025-02-01T00:00:00Z",
        }

    def test_normalize_converts_nullable_dtypes_to_numpy(self):
        """Nullable extension dtypes from ``to_dataframe()`` would
        choke ``np.where`` in ``qc.apply_qc_flags``. The runner must
        downcast them to numpy-backed dtypes (``float64`` / ``object``)
        so the file-mode and BigQuery-mode pipelines stay
        shape-compatible."""
        df = pd.DataFrame(
            {
                "qc_co2_flux": pd.array([0, 2, pd.NA, 1], dtype="Int64"),
                "co2_flux": pd.array([0.1, 0.2, pd.NA, 0.3], dtype="Float64"),
                "site_id": pd.array(["RBRL", "RBRL", "RBRL", "RBRL"], dtype="string"),
                "is_night": pd.array([True, False, pd.NA, True], dtype="boolean"),
            }
        )
        out = br.normalize_bigquery_dataframe(df)
        assert str(out["qc_co2_flux"].dtype) == "float64"
        assert str(out["co2_flux"].dtype) == "float64"
        assert str(out["site_id"].dtype) == "object"
        assert str(out["is_night"].dtype) == "object"
        # Ensure NA -> NaN survives (np.isnan should work on the floats).
        import numpy as np

        assert np.isnan(out["qc_co2_flux"].iloc[2])
        assert np.isnan(out["co2_flux"].iloc[2])

    def test_missing_bq_dependency_raises_actionable_error(self, monkeypatch):
        """When no client is injected and the BigQuery extras are not
        installed, the lazy import path must raise
        ``MissingBigQueryDependencyError`` with the install hint."""

        def _boom(cfg, client):
            raise br.MissingBigQueryDependencyError(br._INSTALL_HINT)

        monkeypatch.setattr(br, "_resolve_client", _boom)
        with pytest.raises(br.MissingBigQueryDependencyError, match="bigquery"):
            read_bigquery_inputs(_make_cfg())
