"""Tests for the M19 biomass BigQuery runner.

The ``google-cloud-bigquery`` extra may not be installed in the
default CI slice, so these tests:

- exercise the pure SQL-building helper without any BQ import;
- inject a fake client + monkey-patched ``_run_query_to_dataframe`` so
  ``read_bigquery_input`` can be tested without the real client.
"""
from __future__ import annotations

import pandas as pd

from miaproc.biomass import (
    BigQueryBiomassConfig,
    BigQueryBiomassReadResult,
    build_input_query,
    read_bigquery_input,
)
from miaproc.biomass import bigquery_runner as br


def _make_cfg(**overrides) -> BigQueryBiomassConfig:
    base = dict(
        input_project="manglaria",
        input_dataset="manglaria_lakehouse_ds",
        input_table="forest_structure_biomass",
    )
    base.update(overrides)
    return BigQueryBiomassConfig(**base)


def _stub_input_df(n: int = 4) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "primary_key": [f"pk_{i}" for i in range(n)],
            "species": ["Avicennia germinans"] * n,
            "dbh_cm": [10.0 + i for i in range(n)],
            "tree_height_m": [None] * n,
            "life_stage": ["Adult"] * n,
        }
    )


# ---------------------------------------------------------------------------
# BigQueryBiomassConfig + query builder
# ---------------------------------------------------------------------------


class TestBigQueryBiomassConfig:
    def test_default_billing_project_falls_back_to_input(self):
        assert _make_cfg().billing_project_or_input() == "manglaria"

    def test_explicit_billing_project_overrides_input(self):
        cfg = _make_cfg(billing_project="manglaria-staging")
        assert cfg.billing_project_or_input() == "manglaria-staging"

    def test_input_table_fqn_uses_backticks(self):
        cfg = _make_cfg()
        assert (
            cfg.input_table_fqn()
            == "`manglaria.manglaria_lakehouse_ds.forest_structure_biomass`"
        )

    def test_default_storage_api_enabled(self):
        assert _make_cfg().bq_storage_api is True

    def test_default_row_limit_is_none(self):
        assert _make_cfg().row_limit is None


class TestQueryBuilder:
    def test_build_input_query_uses_fully_qualified_name(self):
        cfg = _make_cfg()
        sql = build_input_query(cfg)
        assert (
            "`manglaria.manglaria_lakehouse_ds.forest_structure_biomass`"
            in sql
        )
        assert sql.startswith("SELECT * FROM ")

    def test_build_input_query_no_limit_by_default(self):
        sql = build_input_query(_make_cfg())
        assert "LIMIT" not in sql

    def test_build_input_query_appends_limit_when_set(self):
        sql = build_input_query(_make_cfg(row_limit=500))
        assert "LIMIT 500" in sql

    def test_no_site_id_filter_at_query_layer(self):
        # Biomass tables are per-row individual-tree records; the M19
        # contract deliberately does not filter on site_id at the
        # query layer (filtering / sub-selection is the cloud
        # wrapper's responsibility).
        sql = build_input_query(_make_cfg())
        assert "site_id" not in sql
        assert "@" not in sql  # no parameterized predicates


# ---------------------------------------------------------------------------
# read_bigquery_input with a stubbed client
# ---------------------------------------------------------------------------


class _FakeQueryJob:
    def __init__(self, df: pd.DataFrame):
        self._df = df

    def to_dataframe(self, *, create_bqstorage_client: bool = False) -> pd.DataFrame:
        return self._df


class _FakeBigQueryClient:
    def __init__(self, df: pd.DataFrame):
        self._df = df
        self.query_calls: list[tuple[str, object]] = []

    def query(self, sql: str, *, job_config=None):
        self.query_calls.append((sql, job_config))
        return _FakeQueryJob(self._df)


class TestReadBigQueryInput:
    def test_returns_read_result_with_expected_shape(self, monkeypatch):
        # Bypass the real google.cloud.bigquery import by patching the
        # private _run_query_to_dataframe helper. This exercises the
        # full read_bigquery_input pipeline without requiring the
        # bigquery extras at test time.
        df = _stub_input_df(5)

        def _fake_run(client, sql, *, use_storage_api):
            assert "forest_structure_biomass" in sql
            return df

        monkeypatch.setattr(br, "_run_query_to_dataframe", _fake_run)

        cfg = _make_cfg()
        result = read_bigquery_input(cfg, client=_FakeBigQueryClient(df))
        assert isinstance(result, BigQueryBiomassReadResult)
        assert result.input_rows == 5
        assert "forest_structure_biomass" in result.input_query
        assert result.query_parameters == {}
        assert list(result.input_df.columns) == [
            "primary_key",
            "species",
            "dbh_cm",
            "tree_height_m",
            "life_stage",
        ]

    def test_row_limit_propagates_to_sql(self, monkeypatch):
        captured: dict[str, str] = {"sql": ""}

        def _fake_run(client, sql, *, use_storage_api):
            captured["sql"] = sql
            return _stub_input_df(2)

        monkeypatch.setattr(br, "_run_query_to_dataframe", _fake_run)

        cfg = _make_cfg(row_limit=10)
        read_bigquery_input(cfg, client=_FakeBigQueryClient(_stub_input_df(2)))
        assert "LIMIT 10" in captured["sql"]

    def test_normalize_dataframe_coerces_extension_dtypes(self, monkeypatch):
        # BigQuery's to_dataframe() returns nullable extension dtypes
        # (Int64/Float64/string) for nullable columns. The biomass
        # runner must normalize those back to numpy-backed dtypes
        # before downstream matching code sees them, since the M16
        # matcher was written against CSV-parsed frames.
        df_with_extensions = pd.DataFrame(
            {
                "species": pd.array(
                    ["Avicennia germinans", None], dtype="string"
                ),
                "dbh_cm": pd.array([10.0, None], dtype="Float64"),
                "life_stage": pd.array(["Adult", None], dtype="string"),
            }
        )

        def _fake_run(client, sql, *, use_storage_api):
            return df_with_extensions

        monkeypatch.setattr(br, "_run_query_to_dataframe", _fake_run)

        result = read_bigquery_input(
            _make_cfg(), client=_FakeBigQueryClient(df_with_extensions)
        )
        assert str(result.input_df["species"].dtype) == "object"
        assert str(result.input_df["dbh_cm"].dtype) == "float64"
        assert str(result.input_df["life_stage"].dtype) == "object"
