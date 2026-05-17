"""BigQuery-native biomass ingestion (Milestone 19).

Reads an individual-tree forest-structure source table directly from
BigQuery and materializes it as a pandas DataFrame. The DataFrame is
then handed off to the accepted M16 / M17 / M17A enrichment path
through ``miaproc.biomass.enrich_table`` (and the parallel CLI
projection in :func:`miaproc.cli._run_biomass_run_bigquery_command`),
so the new mode shares the exact same scientific contract as the
file-based ``miaproc biomass enrich-table`` path.

Hard scope (M19 first pass):

- read-only on the input project; **no BigQuery write-back here**;
- one source table at a time;
- output handling is owned by ``miaproc.cli`` (local-disk only for
  this first pass; staging-table writes / MERGE are explicitly out
  of scope and would land in a separate milestone).

Decision 010 / risk R11 do not apply to biomass: this module never
imports ``rpy2``, never references the project-scoped R preflight,
and never uses ``--repo-root``. Biomass is pure-Python.

The ``google-cloud-bigquery`` dependency is **lazy-imported** so
importing ``miaproc.biomass`` (or running the file-based
``enrich-table`` CLI path) does not require the BigQuery extras.

The dtype-normalization helper is reused from the eddy BigQuery
runner (``miaproc.eddy.bigquery_runner.normalize_bigquery_dataframe``)
since the BigQuery-to-pandas dtype quirk is identical regardless of
domain — pandas extension dtypes (``Int64``/``Float64``/``boolean``/
``string``) get coerced back to numpy-backed dtypes so downstream
matching code that was written against CSV-parsed frames keeps
working without ``pd.NA`` ambiguity errors.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

# Reuse the eddy-side dtype helper rather than duplicating it.
# The function is BigQuery-shape-only, not eddy-specific.
from miaproc.eddy.bigquery_runner import normalize_bigquery_dataframe


__all__ = [
    "BigQueryBiomassConfig",
    "BigQueryBiomassReadResult",
    "MissingBigQueryDependencyError",
    "build_input_query",
    "read_bigquery_input",
]


_INSTALL_HINT = (
    "BigQuery-native miaproc mode requires the google-cloud-bigquery "
    "client. Install with: pip install 'miaproc[bigquery]'."
)


class MissingBigQueryDependencyError(ImportError):
    """Raised when ``google-cloud-bigquery`` cannot be imported."""


@dataclass(frozen=True)
class BigQueryBiomassConfig:
    """Configuration for a single BigQuery-native biomass read.

    Mirrors the M19 command-line surface: one input table, optional
    row limit, optional billing-project override. The default
    ``billing_project`` is the input project; in environments where
    operators must bill jobs to a different project (for example
    reading from ``manglaria`` while billing to ``manglaria-staging``)
    they can override it explicitly.

    There is no ``site_id`` filter at this layer because biomass
    forest-structure tables are per-row individual-tree records and
    do not share the eddy pattern of one row per (site, timestamp).
    Per-row filtering / sub-selection is the colleagues' / cloud
    wrapper's responsibility.
    """

    input_project: str
    input_dataset: str
    input_table: str
    billing_project: Optional[str] = None
    row_limit: Optional[int] = None
    bq_storage_api: bool = True

    def billing_project_or_input(self) -> str:
        return self.billing_project or self.input_project

    def input_table_fqn(self) -> str:
        return f"`{self.input_project}.{self.input_dataset}.{self.input_table}`"


@dataclass(frozen=True)
class BigQueryBiomassReadResult:
    """Outcome of a single BigQuery-native biomass read.

    ``input_df`` is the in-memory DataFrame the caller passes to
    :func:`miaproc.biomass.enrich_table` (or to ``estimate_trees``
    for the diagnostic-richer path). ``input_query`` is the rendered
    SQL string for transparency and run-metadata logging.
    ``input_rows`` is the post-read row count.
    ``query_parameters`` is a JSON-safe view of the parameters used
    (currently always empty, since the M19 query has no
    parameterized predicates by default; reserved for future
    optional filters).
    """

    input_df: pd.DataFrame
    input_rows: int
    input_query: str
    query_parameters: dict[str, Any] = field(default_factory=dict)


def build_input_query(cfg: BigQueryBiomassConfig) -> str:
    """Build the SELECT SQL for ``cfg``.

    Default shape is ``SELECT * FROM <fqn>`` with an optional
    ``LIMIT`` clause when ``cfg.row_limit`` is set. Keeping
    ``SELECT *`` matches the eddy pattern: the downstream
    biomass enrichment code expects whatever columns the source
    table provides (``species``, ``dbh_cm``, ``tree_height_m``,
    ``life_stage``, plus whatever original columns the table
    carries — all preserved verbatim by the row-preservation
    contract from M17).
    """
    sql = f"SELECT * FROM {cfg.input_table_fqn()}"
    if cfg.row_limit is not None:
        sql = f"{sql}\nLIMIT {int(cfg.row_limit)}"
    return sql


def _resolve_client(cfg: BigQueryBiomassConfig, client: Any) -> Any:
    if client is not None:
        return client
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise MissingBigQueryDependencyError(_INSTALL_HINT) from exc
    return bigquery.Client(project=cfg.billing_project_or_input())


def _run_query_to_dataframe(
    client: Any,
    sql: str,
    *,
    use_storage_api: bool,
) -> pd.DataFrame:
    """Execute ``sql`` and return the result as a pandas DataFrame.

    Tries the BigQuery Storage Read API first (faster, columnar);
    falls back to the REST-only ``to_dataframe()`` if the storage
    extra is not installed at runtime.
    """
    try:
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover
        raise MissingBigQueryDependencyError(_INSTALL_HINT) from exc

    job_config = bigquery.QueryJobConfig()
    job = client.query(sql, job_config=job_config)

    if use_storage_api:
        try:
            return job.to_dataframe(create_bqstorage_client=True)
        except Exception:
            import warnings

            warnings.warn(
                "BigQuery Storage Read API unavailable; falling back to "
                "the REST-based to_dataframe() path."
            )
    return job.to_dataframe(create_bqstorage_client=False)


def read_bigquery_input(
    cfg: BigQueryBiomassConfig,
    *,
    client: Any = None,
) -> BigQueryBiomassReadResult:
    """Read the biomass forest-structure slice for ``cfg`` from BigQuery.

    The returned :class:`BigQueryBiomassReadResult` carries the
    in-memory DataFrame (ready for ``estimate_trees`` /
    ``enrich_table``), the rendered SQL string, and the parameter
    values for run-metadata logging. **No BigQuery write happens
    here.**

    Parameters
    ----------
    cfg
        :class:`BigQueryBiomassConfig` describing project, dataset,
        table name, optional row limit, and optional billing project.
    client
        Optional pre-built ``google.cloud.bigquery.Client``. Tests
        inject a stub client that satisfies the minimal
        ``client.query(sql, job_config=...).to_dataframe(...)`` shape.
    """
    client = _resolve_client(cfg, client)
    sql = build_input_query(cfg)

    input_df = normalize_bigquery_dataframe(
        _run_query_to_dataframe(
            client, sql, use_storage_api=cfg.bq_storage_api
        )
    )

    return BigQueryBiomassReadResult(
        input_df=input_df,
        input_rows=int(len(input_df)),
        input_query=sql,
        query_parameters={},
    )
