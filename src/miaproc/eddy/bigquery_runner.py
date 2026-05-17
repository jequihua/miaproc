"""BigQuery-native eddy ingestion (Milestone 7).

Reads the production carbon-flux source tables directly with the
BigQuery Python client and materializes them as pandas DataFrames.
The DataFrames are then handed off to the existing in-memory stage-1
path (:func:`miaproc.eddy.load_stage1_from_dataframes`), so the new
mode shares the same scientific contract as the file-based path.

Hard scope (M7 first pass, per ``docs/guides/002_carbon_flux_bq_orchestration_guide.md``):

- read-only on the input project; no BigQuery write-back here;
- the first live target site is ``RBRL``;
- the engine is the caller's choice (``reddyproc-reference`` for the
  first live local test);
- output handling is owned by ``miaproc.cli`` (local-disk only for the
  first pass; staging-table writes / MERGE are explicitly out of scope).

Decision 010 / risk R11 are unaffected by this module: it never imports
``rpy2`` and never bypasses the project-scoped preflight gate. The CLI
runs the preflight before this module is asked to read anything.

The ``google-cloud-bigquery`` dependency is **lazy-imported** so
importing ``miaproc.eddy`` (or running the file-based CLI path) does not
require the BigQuery extras.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd


__all__ = [
    "BigQueryEddyConfig",
    "BigQueryReadResult",
    "BigQuerySilverInputConfig",
    "BigQuerySilverReadResult",
    "MissingBigQueryDependencyError",
    "build_flux_query",
    "build_biomet_query",
    "build_silver_query",
    "normalize_bigquery_dataframe",
    "read_bigquery_inputs",
    "read_bigquery_silver_input",
]


_INSTALL_HINT = (
    "BigQuery-native miaproc mode requires the google-cloud-bigquery "
    "client. Install with: pip install 'miaproc[bigquery]'."
)


class MissingBigQueryDependencyError(ImportError):
    """Raised when ``google-cloud-bigquery`` cannot be imported."""


@dataclass(frozen=True)
class BigQueryEddyConfig:
    """Configuration for a single BigQuery-native eddy read.

    Parameters mirror the M7 command-line surface (one flux table, one
    biomet table, optional site filter, optional time window). The
    default ``billing_project`` is the input project; in environments
    where operators must bill jobs to a different project (for example
    reading from ``manglaria`` while billing to ``manglaria-staging``)
    they can override it explicitly.

    ``site_id`` is optional (M24). When ``None``, the read is
    all-categories: no ``WHERE site_id = @site_id`` filter is
    appended to the rendered SQL, and the BigQuery scalar parameter
    is omitted. The eddy CLI never passes a user-selected site value
    here; programmatic callers may still pass ``site_id=<value>`` to
    fetch one site without grouping.
    """

    input_project: str
    input_dataset: str
    flux_table: str
    biomet_table: str
    site_id: Optional[str] = None
    start_timestamp: Optional[str] = None
    end_timestamp: Optional[str] = None
    billing_project: Optional[str] = None
    bq_storage_api: bool = True

    def billing_project_or_input(self) -> str:
        return self.billing_project or self.input_project

    def flux_table_fqn(self) -> str:
        return f"`{self.input_project}.{self.input_dataset}.{self.flux_table}`"

    def biomet_table_fqn(self) -> str:
        return f"`{self.input_project}.{self.input_dataset}.{self.biomet_table}`"


@dataclass(frozen=True)
class BigQueryReadResult:
    """Outcome of a single BigQuery-native read pair.

    ``flux_df`` and ``biomet_df`` are the in-memory DataFrames the
    caller passes to :func:`miaproc.eddy.load_stage1_from_dataframes`.
    ``flux_query`` / ``biomet_query`` are the rendered SQL strings (with
    parameter placeholders) for transparency and run-metadata logging.
    ``flux_rows`` / ``biomet_rows`` are post-read row counts.
    ``query_parameters`` is a JSON-safe view of the parameters used.
    """

    flux_df: pd.DataFrame
    biomet_df: pd.DataFrame
    flux_rows: int
    biomet_rows: int
    flux_query: str
    biomet_query: str
    query_parameters: dict[str, Any] = field(default_factory=dict)


def _build_select_with_window(
    table_fqn: str,
    *,
    site_id: Optional[str],
    start_timestamp: Optional[str],
    end_timestamp: Optional[str],
) -> str:
    """Build the parameterized ``SELECT`` query for one input table.

    Filters are appended only when the corresponding value is set. In
    particular the ``WHERE site_id = @site_id`` clause is omitted
    entirely when ``site_id is None`` (M24 all-categories read), so
    the rendered SQL pulls every category present in the source slice
    and grouping happens in Python instead of in the query. Timestamp
    bounds compose the same way. ``SELECT *`` keeps the case-study
    column shape intact because the in-memory stage-1 pipeline relies
    on auxiliary columns such as ``VPD``, ``QC_*``, ``site_id``, and
    ``timestamp``.
    """
    where: list[str] = []
    if site_id is not None:
        where.append("site_id = @site_id")
    if start_timestamp is not None:
        where.append("timestamp >= @start_ts")
    if end_timestamp is not None:
        where.append("timestamp < @end_ts")
    sql = f"SELECT * FROM {table_fqn}"
    if where:
        sql = sql + "\nWHERE " + " AND ".join(where)
    sql = sql + "\nORDER BY timestamp"
    return sql


def build_flux_query(cfg: BigQueryEddyConfig) -> str:
    """Render the flux SELECT for ``cfg`` (parameter placeholders only)."""
    return _build_select_with_window(
        cfg.flux_table_fqn(),
        site_id=cfg.site_id,
        start_timestamp=cfg.start_timestamp,
        end_timestamp=cfg.end_timestamp,
    )


def build_biomet_query(cfg: BigQueryEddyConfig) -> str:
    """Render the biomet SELECT for ``cfg`` (parameter placeholders only)."""
    return _build_select_with_window(
        cfg.biomet_table_fqn(),
        site_id=cfg.site_id,
        start_timestamp=cfg.start_timestamp,
        end_timestamp=cfg.end_timestamp,
    )


def normalize_bigquery_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas extension dtypes back to numpy-backed dtypes.

    The downstream stage-1 pipeline was written against CSV-parsed
    frames, where integer columns containing missing values come back
    as ``float64`` (with ``NaN`` for missing) and string columns come
    back as ``object``. ``google-cloud-bigquery``'s ``to_dataframe()``
    instead produces nullable extension dtypes
    (``Int64``/``Float64``/``boolean``/``string``) so missing values
    are ``pd.NA``. ``pd.NA`` doesn't compose with several ``numpy``
    primitives the legacy pipeline relies on (e.g. ``np.where`` on a
    ``BooleanArray`` raises ``"boolean value of NA is ambiguous"``).

    Normalizing on the BigQuery read boundary keeps the file-mode and
    BigQuery-mode ingestion paths shape-compatible without changing
    the downstream contract, and keeps ``qc.py`` / ``time.py`` /
    ``core.py`` blissfully ignorant of the ingestion source.

    Returns a new DataFrame; the input is not mutated.
    """
    out = df.copy()
    for col in out.columns:
        dtype_name = str(out[col].dtype)
        # Nullable integer / unsigned integer extension dtypes.
        if dtype_name.startswith("Int") or dtype_name.startswith("UInt"):
            out[col] = out[col].astype("float64")
            continue
        # Nullable float extension dtype.
        if dtype_name == "Float64" or dtype_name == "Float32":
            out[col] = out[col].astype("float64")
            continue
        # Nullable boolean extension dtype.
        if dtype_name == "boolean":
            out[col] = out[col].astype("object")
            continue
        # Pandas StringDtype (string[python] / string[pyarrow]).
        if dtype_name.startswith("string"):
            out[col] = out[col].astype("object")
            continue
    return out


def _build_query_parameters(cfg: BigQueryEddyConfig) -> list[Any]:
    """Construct ``ScalarQueryParameter`` objects for ``cfg``.

    Lazy-imports the BigQuery client; raises
    :class:`MissingBigQueryDependencyError` if the extras are absent.
    """
    try:
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover - exercised in tests via stub
        raise MissingBigQueryDependencyError(_INSTALL_HINT) from exc

    params: list[Any] = []
    if cfg.site_id is not None:
        params.append(
            bigquery.ScalarQueryParameter("site_id", "STRING", cfg.site_id)
        )
    if cfg.start_timestamp is not None:
        params.append(
            bigquery.ScalarQueryParameter("start_ts", "TIMESTAMP", cfg.start_timestamp)
        )
    if cfg.end_timestamp is not None:
        params.append(
            bigquery.ScalarQueryParameter("end_ts", "TIMESTAMP", cfg.end_timestamp)
        )
    return params


def _resolve_client(cfg: BigQueryEddyConfig, client: Any) -> Any:
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
    parameters: list[Any],
    *,
    use_storage_api: bool,
) -> pd.DataFrame:
    """Execute ``sql`` and return the result as a pandas DataFrame.

    Tries the BigQuery Storage Read API first (faster, columnar); falls
    back to the REST-only ``to_dataframe()`` if the storage extra is
    not installed at runtime. The Storage path is opt-in via
    ``cfg.bq_storage_api``.
    """
    try:
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover
        raise MissingBigQueryDependencyError(_INSTALL_HINT) from exc

    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    job = client.query(sql, job_config=job_config)

    if use_storage_api:
        try:
            return job.to_dataframe(create_bqstorage_client=True)
        except Exception:
            # Storage Read API isn't installed or isn't reachable from
            # this runtime; the REST path still produces a correct
            # DataFrame. Loud-but-not-fatal: log via warning, keep going.
            import warnings

            warnings.warn(
                "BigQuery Storage Read API unavailable; falling back to "
                "the REST-based to_dataframe() path."
            )
    return job.to_dataframe(create_bqstorage_client=False)


def read_bigquery_inputs(
    cfg: BigQueryEddyConfig,
    *,
    client: Any = None,
) -> BigQueryReadResult:
    """Read the flux and biomet slices for ``cfg`` from BigQuery.

    The returned :class:`BigQueryReadResult` carries the in-memory
    DataFrames (ready for ``load_stage1_from_dataframes``), the
    rendered SQL strings, and the parameter values for run-metadata
    logging. No BigQuery write happens here.

    Parameters
    ----------
    cfg
        :class:`BigQueryEddyConfig` describing project, dataset, table
        names, the site filter, and (optionally) a timestamp window.
    client
        Optional pre-built ``google.cloud.bigquery.Client`` instance.
        Tests inject a stub client that satisfies the minimal
        ``client.query(sql, job_config=...).to_dataframe(...)`` shape.
    """
    client = _resolve_client(cfg, client)
    parameters = _build_query_parameters(cfg)

    flux_sql = build_flux_query(cfg)
    biomet_sql = build_biomet_query(cfg)

    flux_df = normalize_bigquery_dataframe(
        _run_query_to_dataframe(
            client, flux_sql, parameters, use_storage_api=cfg.bq_storage_api
        )
    )
    biomet_df = normalize_bigquery_dataframe(
        _run_query_to_dataframe(
            client, biomet_sql, parameters, use_storage_api=cfg.bq_storage_api
        )
    )

    json_safe_params: dict[str, Any] = {}
    if cfg.site_id is not None:
        json_safe_params["site_id"] = cfg.site_id
    if cfg.start_timestamp is not None:
        json_safe_params["start_ts"] = cfg.start_timestamp
    if cfg.end_timestamp is not None:
        json_safe_params["end_ts"] = cfg.end_timestamp

    return BigQueryReadResult(
        flux_df=flux_df,
        biomet_df=biomet_df,
        flux_rows=int(len(flux_df)),
        biomet_rows=int(len(biomet_df)),
        flux_query=flux_sql,
        biomet_query=biomet_sql,
        query_parameters=json_safe_params,
    )


# ----------------------------------------------------------------------
# M22: silver-table BigQuery reader (input to run-bigquery-gold)
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class BigQuerySilverInputConfig:
    """Configuration for reading one BigQuery silver-stage table.

    M22 ``run-bigquery-gold`` consumes a silver-stage table that was
    written by ``run-bigquery-silver`` (or by an equivalent
    operator-staged path). The input is read read-only; the gold-side
    writeback is configured separately through
    :class:`BigQueryWritebackConfig`.

    The ``site_id`` filter and timestamp window are optional. They
    mirror the source-table reader so cloud orchestration can ask gold
    to consume only the slice of silver it needs without first
    materializing a separate per-site silver table.
    """

    input_project: str
    input_dataset: str
    silver_table: str
    site_id: Optional[str] = None
    start_timestamp: Optional[str] = None
    end_timestamp: Optional[str] = None
    billing_project: Optional[str] = None
    bq_storage_api: bool = True

    def billing_project_or_input(self) -> str:
        return self.billing_project or self.input_project

    def silver_table_fqn(self) -> str:
        return (
            f"`{self.input_project}.{self.input_dataset}.{self.silver_table}`"
        )


@dataclass(frozen=True)
class BigQuerySilverReadResult:
    """Outcome of a single BigQuery silver-table read.

    ``silver_df`` is the in-memory DataFrame the caller passes to the
    accepted gold engine dispatch. ``silver_query`` is the rendered
    SQL string and ``query_parameters`` the JSON-safe parameter view
    for run-metadata logging.
    """

    silver_df: pd.DataFrame
    silver_rows: int
    silver_query: str
    query_parameters: dict[str, Any] = field(default_factory=dict)


def build_silver_query(cfg: BigQuerySilverInputConfig) -> str:
    """Render the silver-stage SELECT for ``cfg`` (parameter placeholders only).

    ``SELECT *`` matches the eddy bronze/source pattern: gold dispatch
    expects whatever stage-1-derived columns silver carries, so we do
    not project a fixed subset at this layer. Filters on ``site_id``
    and the optional timestamp window are added only when the
    corresponding values are supplied.
    """
    where: list[str] = []
    if cfg.site_id is not None:
        where.append("site_id = @site_id")
    if cfg.start_timestamp is not None:
        where.append("timestamp >= @start_ts")
    if cfg.end_timestamp is not None:
        where.append("timestamp < @end_ts")
    sql = f"SELECT * FROM {cfg.silver_table_fqn()}"
    if where:
        sql = sql + "\nWHERE " + " AND ".join(where)
    sql = sql + "\nORDER BY timestamp"
    return sql


def _build_silver_query_parameters(cfg: BigQuerySilverInputConfig) -> list[Any]:
    try:
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover - exercised via stub
        raise MissingBigQueryDependencyError(_INSTALL_HINT) from exc

    params: list[Any] = []
    if cfg.site_id is not None:
        params.append(
            bigquery.ScalarQueryParameter("site_id", "STRING", cfg.site_id)
        )
    if cfg.start_timestamp is not None:
        params.append(
            bigquery.ScalarQueryParameter(
                "start_ts", "TIMESTAMP", cfg.start_timestamp
            )
        )
    if cfg.end_timestamp is not None:
        params.append(
            bigquery.ScalarQueryParameter(
                "end_ts", "TIMESTAMP", cfg.end_timestamp
            )
        )
    return params


def _resolve_silver_client(
    cfg: BigQuerySilverInputConfig, client: Any
) -> Any:
    if client is not None:
        return client
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise MissingBigQueryDependencyError(_INSTALL_HINT) from exc
    return bigquery.Client(project=cfg.billing_project_or_input())


def read_bigquery_silver_input(
    cfg: BigQuerySilverInputConfig,
    *,
    client: Any = None,
) -> BigQuerySilverReadResult:
    """Read the silver-stage slice for ``cfg`` from BigQuery.

    Returns a :class:`BigQuerySilverReadResult` carrying the in-memory
    silver DataFrame (ready for the accepted gold engine dispatch),
    the rendered SQL string, and the parameter values for run
    metadata. **No BigQuery write happens here.**

    The ``silver_df`` is normalized through
    :func:`normalize_bigquery_dataframe` so nullable extension dtypes
    (``Int64``/``Float64``/``boolean``/``string``) are converted back
    to numpy-backed dtypes the legacy stage-2 backend code expects.
    """
    client = _resolve_silver_client(cfg, client)
    parameters = _build_silver_query_parameters(cfg)
    sql = build_silver_query(cfg)

    silver_df = normalize_bigquery_dataframe(
        _run_query_to_dataframe(
            client, sql, parameters, use_storage_api=cfg.bq_storage_api
        )
    )

    json_safe_params: dict[str, Any] = {}
    if cfg.site_id is not None:
        json_safe_params["site_id"] = cfg.site_id
    if cfg.start_timestamp is not None:
        json_safe_params["start_ts"] = cfg.start_timestamp
    if cfg.end_timestamp is not None:
        json_safe_params["end_ts"] = cfg.end_timestamp

    return BigQuerySilverReadResult(
        silver_df=silver_df,
        silver_rows=int(len(silver_df)),
        silver_query=sql,
        query_parameters=json_safe_params,
    )
