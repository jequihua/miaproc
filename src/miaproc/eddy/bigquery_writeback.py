"""BigQuery writeback + merge control for processed eddy outputs (M8).

This module owns the **write side** of the BigQuery-native eddy path:

1. ensure control tables exist in the operator-owned orchestration
   dataset (idempotent ``CREATE TABLE IF NOT EXISTS``);
2. load the processed output DataFrame to a BigQuery **staging** table
   (always ``WRITE_TRUNCATE``: each run replaces the stage table state);
3. run explicit validation SQL against the stage table (row count,
   required-column non-null, ``(site_id, timestamp)`` and
   ``primary_key`` uniqueness);
4. **only when explicitly authorized**, MERGE the staged rows into the
   final target table on ``(site_id, timestamp)`` (no deletes; non-key
   columns including ``primary_key`` are updated from the stage row);
5. record run metadata into ``<control>.cf_s2_runs`` and advance the
   per-site watermark in ``<control>.cf_s2_watermark`` only after a
   successful merge.

Hard scope (M8 first writeback pass; per
``docs/guides/002_carbon_flux_bq_orchestration_guide.md``):

- production project ``manglaria`` is read-only; **all writes go to
  ``manglaria-staging`` (or whatever ``--bq-output-project`` the
  operator passes)**;
- final-table MERGE is gated by an explicit operator opt-in
  (``allow_final_merge=True``); the default is **stage-only**;
- watermark advances only on a successful merge — never on a
  stage-write failure, validation failure, or a stage-only run;
- Cloud Run Job deployment, scheduling, and IAM rollout are
  **deferred** to a follow-up pass.

Decision 010 / risk R11 are unaffected: this module never imports
``rpy2``, never reads or sets ``MIAPROC_ALLOW_GLOBAL_R``, and never
bypasses the project-scoped preflight. The CLI runs the preflight
before this module is asked to do anything.

The ``google-cloud-bigquery`` dependency is **lazy-imported** so
importing ``miaproc.eddy`` (or running the file-based CLI path) does
not require the BigQuery extras.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional

import pandas as pd


__all__ = [
    "BigQueryWritebackConfig",
    "WritebackResult",
    "WritebackValidationError",
    "DuplicateStageColumnsError",
    "build_validation_query",
    "build_merge_statement",
    "render_runs_table_ddl",
    "render_watermark_table_ddl",
    "ensure_control_tables_exist",
    "write_processed_to_stage",
    "validate_stage_table",
    "merge_stage_into_final",
    "record_run_row",
    "read_watermark",
    "advance_watermark",
    "max_timestamps_by_site",
    "run_writeback",
    "prepare_stage_dataframe",
    "prepare_silver_stage_payload",
    "ensure_unique_stage_columns",
    "validate_source_columns_unique",
    "bigquery_field_key",
    "read_final_table_columns",
    "apply_silver_source_truth_rename",
    "silver_to_internal_calc_frame",
    "S2_FILT_1_RENAME_MAP",
    "GROUPED_RUN_ROW_SITE_LABEL",
    "HUMIDITY_SOURCE_COLUMN",
    "HUMIDITY_DERIVED_RENAME",
    "COLUMN_COLLISION_ATTRS_KEY",
]


# Label written to ``cf_s2_runs.site_id`` when one writeback invocation
# stages the stacked output of more than one categorical group (M24
# all-data grouped CLI execution). Single-site writeback runs continue
# to record the actual ``site_id`` value, so legacy queries against
# the runs table see no behavioural change for single-site execution.
GROUPED_RUN_ROW_SITE_LABEL = "<grouped>"


# Mapping from the M6 backend output column names (the 13-column
# scientific contract on which `postproc(...)` returns) to the live
# `_s2_filt_1` extended source-flux schema described in
# ``docs/guides/001_carbon_flux_bq_orchestration_guide.md`` §2.1.
# Per guide 001 §2.1, ``_s2_filt_1`` adds six lowercase analytical
# columns on top of the EddyPro source-flux schema:
# ``dateAndTime``, ``nee_f``, ``nee_fqc``, ``sw_in_f``, ``ta_f``,
# ``vpd_f``. ``dateAndTime`` is built directly from ``DateTime`` and
# is not part of this rename map.
S2_FILT_1_RENAME_MAP: dict[str, str] = {
    "NEE_f": "nee_f",
    "NEE_fqc": "nee_fqc",
    "Rg_f": "sw_in_f",
    "Tair_f": "ta_f",
    "VPD_f": "vpd_f",
}


# These names are inlined from M7's ``bigquery_runner._INSTALL_HINT`` /
# ``MissingBigQueryDependencyError`` rather than imported, so that a
# downstream operator who only uses the writeback module gets the same
# actionable error without extra coupling.
_INSTALL_HINT = (
    "BigQuery writeback mode requires the google-cloud-bigquery "
    "client. Install with: pip install 'miaproc[bigquery]'."
)


# Operational merge identity, per guide 001 §11 / guide 002 §11.3.
# These names match the existing miaproc post-processing schema.
MERGE_KEY_COLUMNS: tuple[str, ...] = ("site_id", "timestamp")
REQUIRED_NON_NULL_COLUMNS: tuple[str, ...] = (
    "primary_key",
    "site_id",
    "timestamp",
)


# Status values written to the runs control table. Kept as a small
# closed set so downstream dashboards / queries can rely on it.
RUN_STATUS_RUNNING = "running"
RUN_STATUS_SUCCEEDED = "succeeded"
RUN_STATUS_STAGE_ONLY_SUCCEEDED = "stage_only_succeeded"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_VALIDATION_FAILED = "validation_failed"

_VALID_RUN_STATUSES: frozenset[str] = frozenset(
    {
        RUN_STATUS_RUNNING,
        RUN_STATUS_SUCCEEDED,
        RUN_STATUS_STAGE_ONLY_SUCCEEDED,
        RUN_STATUS_FAILED,
        RUN_STATUS_VALIDATION_FAILED,
    }
)


class WritebackValidationError(RuntimeError):
    """Raised when the stage-table validation SQL reports any failure.

    Carries the validation row as ``.metrics`` so the caller can
    record it into run metadata even when the merge is aborted.
    """

    def __init__(self, message: str, metrics: dict[str, Any]) -> None:
        super().__init__(message)
        self.metrics = metrics


# M28: humidity-collision policy. The bronze/source flux column
# carries the source-side humidity record under the canonical name
# ``rH``; if a stage-1 / silver derivation produces a second column
# also named ``rH`` and it diverges from source, it is renamed to
# ``rH_norm_s`` rather than being dropped or fused. The rH-equivalent
# duplicate is suppressed in favor of the source. Any duplicate column
# name that is not humidity has no deterministic policy and is
# surfaced as ``DuplicateStageColumnsError`` so the operator can fix
# the upstream pipeline before any BigQuery write.
HUMIDITY_SOURCE_COLUMN = "rH"
HUMIDITY_DERIVED_RENAME = "rH_norm_s"
# Key used to attach the M28 collision-action list to ``df.attrs`` so
# downstream callers (the CLI, in particular) can surface the actions
# in run-metadata JSON without re-derivation.
COLUMN_COLLISION_ATTRS_KEY = "miaproc_column_collision_actions"


class DuplicateStageColumnsError(ValueError):
    """Raised when a stage payload has unresolvable duplicate column names.

    Pandas allows duplicate column names; BigQuery rejects schemas with
    duplicate field names. ``ensure_unique_stage_columns`` handles the
    known ``rH`` source-vs-derived case deterministically; any other
    duplicate is a real upstream defect and raises this error so the
    package fails loudly before the BigQuery client load call.
    """

    def __init__(
        self,
        message: str,
        *,
        duplicate_columns: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.duplicate_columns = tuple(duplicate_columns)


# ----------------------------------------------------------------------
# Config + result dataclasses
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class BigQueryWritebackConfig:
    """Configuration for a single writeback orchestration run.

    All writes happen against ``output_project`` (which **must not** be
    the production input project). The runs/watermark control tables
    live in ``control_dataset`` under the same output project so the
    operator can audit them without crossing project boundaries.

    ``allow_final_merge`` defaults to ``False``: stage-only is the
    safe default for local operator use. Final-table MERGE only
    happens when this flag is explicitly ``True`` (typically wired to
    the CLI ``--bq-allow-final-merge`` switch).
    """

    output_project: str
    output_dataset: str
    stage_table: str
    control_dataset: str
    final_table: Optional[str] = None
    allow_final_merge: bool = False
    run_id: Optional[str] = None
    site_id: Optional[str] = None
    runs_table: str = "cf_s2_runs"
    watermark_table: str = "cf_s2_watermark"
    billing_project: Optional[str] = None
    forbidden_write_projects: tuple[str, ...] = ("manglaria",)

    def billing_project_or_output(self) -> str:
        return self.billing_project or self.output_project

    def stage_table_fqn(self) -> str:
        return (
            f"`{self.output_project}.{self.output_dataset}.{self.stage_table}`"
        )

    def final_table_fqn(self) -> Optional[str]:
        if not self.final_table:
            return None
        return (
            f"`{self.output_project}.{self.output_dataset}.{self.final_table}`"
        )

    def runs_table_fqn(self) -> str:
        return (
            f"`{self.output_project}.{self.control_dataset}.{self.runs_table}`"
        )

    def watermark_table_fqn(self) -> str:
        return (
            f"`{self.output_project}.{self.control_dataset}."
            f"{self.watermark_table}`"
        )

    def validate(self) -> None:
        """Cheap structural validation (raises ``ValueError`` on bad input)."""
        if not self.output_project:
            raise ValueError("output_project must be a non-empty string.")
        if self.output_project in self.forbidden_write_projects:
            raise ValueError(
                f"Refusing to configure writes against forbidden project "
                f"{self.output_project!r}. Production input projects must "
                "remain read-only; route writes to the staging project."
            )
        for name, value in (
            ("output_dataset", self.output_dataset),
            ("stage_table", self.stage_table),
            ("control_dataset", self.control_dataset),
        ):
            if not value:
                raise ValueError(f"{name} must be a non-empty string.")
        if self.allow_final_merge and not self.final_table:
            raise ValueError(
                "allow_final_merge=True requires final_table to be set."
            )


@dataclass(frozen=True)
class WritebackResult:
    """Structured outcome of a writeback run; JSON-safe via :meth:`to_dict`.

    ``watermark_value`` keeps the legacy single-site contract for
    callers that staged exactly one site (it carries that site's
    advanced watermark). For grouped multi-site stacked runs
    (M24), ``watermark_value`` carries the maximum timestamp across
    all advanced site watermarks and ``watermark_values_by_site``
    records the per-site advance map ``{site_id: ISO-8601 timestamp}``
    so a multi-site final MERGE is auditable from the local artifact
    alone. For single-site runs ``watermark_values_by_site`` carries
    one entry; for stage-only / failed runs it is empty.
    """

    run_id: str
    status: str
    stage_rows: int
    merge_attempted: bool
    merge_authorized: bool
    merge_inserted_rows: Optional[int]
    merge_updated_rows: Optional[int]
    watermark_advanced: bool
    watermark_value: Optional[str]
    validation_metrics: dict[str, Any] = field(default_factory=dict)
    stage_table_fqn: str = ""
    final_table_fqn: Optional[str] = None
    runs_table_fqn: str = ""
    watermark_table_fqn: str = ""
    error_text: Optional[str] = None
    watermark_values_by_site: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ----------------------------------------------------------------------
# DDL + SQL builders (pure; reviewable; no BQ client dependency)
# ----------------------------------------------------------------------


def render_runs_table_ddl(cfg: BigQueryWritebackConfig) -> str:
    """``CREATE TABLE IF NOT EXISTS`` for ``<control>.cf_s2_runs``.

    Schema is intentionally narrow: enough to audit a run, not a full
    cloud orchestration record. Cloud-job-specific columns (image
    digest, deployment label, scheduler trigger id, etc.) can be
    added in a follow-up pass without breaking this baseline.
    """
    return f"""\
CREATE TABLE IF NOT EXISTS {cfg.runs_table_fqn()} (
  run_id STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  status STRING NOT NULL,
  site_id STRING,
  engine STRING,
  bq_input_project STRING,
  bq_input_dataset STRING,
  bq_flux_table STRING,
  bq_biomet_table STRING,
  bq_output_project STRING,
  bq_output_dataset STRING,
  bq_stage_table STRING,
  bq_final_table STRING,
  read_flux_rows INT64,
  read_biomet_rows INT64,
  stage_rows INT64,
  merge_attempted BOOL,
  merge_authorized BOOL,
  merge_inserted_rows INT64,
  merge_updated_rows INT64,
  watermark_advanced BOOL,
  watermark_value TIMESTAMP,
  miaproc_version STRING,
  bigquery_client_version STRING,
  error_text STRING
)
"""


def render_watermark_table_ddl(cfg: BigQueryWritebackConfig) -> str:
    """``CREATE TABLE IF NOT EXISTS`` for ``<control>.cf_s2_watermark``."""
    return f"""\
CREATE TABLE IF NOT EXISTS {cfg.watermark_table_fqn()} (
  site_id STRING NOT NULL,
  last_processed_timestamp TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  last_run_id STRING
)
"""


def build_validation_query(cfg: BigQueryWritebackConfig) -> str:
    """Single-row validation SQL against the stage table.

    Returns one row with the metrics the writeback layer must check
    before any MERGE: row count, required-column null counts, and
    ``(site_id, timestamp)`` / ``primary_key`` duplicate counts.
    """
    return f"""\
SELECT
  COUNT(*) AS row_count,
  COUNTIF(site_id IS NULL) AS null_site_id,
  COUNTIF(timestamp IS NULL) AS null_timestamp,
  COUNTIF(primary_key IS NULL) AS null_primary_key,
  COUNT(*) - COUNT(DISTINCT FORMAT('%T|%T', site_id, timestamp))
    AS dup_site_timestamp,
  COUNT(*) - COUNT(DISTINCT primary_key) AS dup_primary_key
FROM {cfg.stage_table_fqn()}
"""


def build_merge_statement(
    cfg: BigQueryWritebackConfig,
    *,
    columns: list[str],
) -> str:
    """Render the deterministic MERGE on ``(site_id, timestamp)``.

    ``columns`` is the full column list of the stage table (must
    include the merge keys). Non-key columns are updated from the
    stage row; ``WHEN NOT MATCHED THEN INSERT`` writes the full row.
    No deletes — guide 001 §11 / guide 002 §11.3.
    """
    if not cfg.final_table:
        raise ValueError("final_table must be set to render a MERGE statement.")
    final_fqn = cfg.final_table_fqn()
    stage_fqn = cfg.stage_table_fqn()
    merge_keys = set(MERGE_KEY_COLUMNS)
    update_cols = [c for c in columns if c not in merge_keys]
    if not update_cols:
        raise ValueError(
            "Refusing to render MERGE: stage table has no non-key columns "
            "to update. Expected at least 'primary_key' and the post-"
            "processing payload."
        )
    update_set = ",\n  ".join(f"{c} = S.{c}" for c in update_cols)
    insert_cols = ", ".join(columns)
    insert_values = ", ".join(f"S.{c}" for c in columns)
    on_clause = " AND ".join(
        f"T.{k} = S.{k}" for k in MERGE_KEY_COLUMNS
    )
    return f"""\
MERGE {final_fqn} T
USING {stage_fqn} S
ON {on_clause}
WHEN MATCHED THEN UPDATE SET
  {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_cols})
VALUES ({insert_values})
"""


def build_watermark_merge(cfg: BigQueryWritebackConfig) -> str:
    """Upsert the per-site watermark row.

    The watermark merge runs only after a successful final-table MERGE
    (callers must enforce this); here we render the SQL only.
    """
    return f"""\
MERGE {cfg.watermark_table_fqn()} T
USING (
  SELECT
    @site_id AS site_id,
    @last_processed_timestamp AS last_processed_timestamp,
    CURRENT_TIMESTAMP() AS updated_at,
    @last_run_id AS last_run_id
) S
ON T.site_id = S.site_id
WHEN MATCHED THEN UPDATE SET
  last_processed_timestamp = S.last_processed_timestamp,
  updated_at = S.updated_at,
  last_run_id = S.last_run_id
WHEN NOT MATCHED THEN INSERT
  (site_id, last_processed_timestamp, updated_at, last_run_id)
VALUES
  (S.site_id, S.last_processed_timestamp, S.updated_at, S.last_run_id)
"""


# ----------------------------------------------------------------------
# Lazy bigquery client + low-level helpers
# ----------------------------------------------------------------------


def _import_bigquery() -> Any:
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise ImportError(_INSTALL_HINT) from exc
    return bigquery


def _resolve_client(cfg: BigQueryWritebackConfig, client: Any) -> Any:
    if client is not None:
        return client
    bigquery = _import_bigquery()
    return bigquery.Client(project=cfg.billing_project_or_output())


def _run_simple_sql(client: Any, sql: str) -> Any:
    """Submit ``sql`` and block until the BigQuery job finishes."""
    job = client.query(sql)
    # Fully-managed clients expose ``result()`` to wait for completion.
    job.result()
    return job


# ----------------------------------------------------------------------
# Public operations
# ----------------------------------------------------------------------


def ensure_control_tables_exist(
    cfg: BigQueryWritebackConfig,
    *,
    client: Any = None,
) -> None:
    """Idempotently create the runs + watermark control tables."""
    client = _resolve_client(cfg, client)
    _run_simple_sql(client, render_runs_table_ddl(cfg))
    _run_simple_sql(client, render_watermark_table_ddl(cfg))


def write_processed_to_stage(
    df: pd.DataFrame,
    cfg: BigQueryWritebackConfig,
    *,
    client: Any = None,
) -> int:
    """Load ``df`` into the stage table with ``WRITE_TRUNCATE``.

    Returns the row count loaded. Schema is auto-detected from the
    DataFrame; column ordering is preserved. Each writeback run fully
    replaces the stage table content so the validation + MERGE step
    only ever sees this run's output.
    """
    bigquery = _import_bigquery()
    client = _resolve_client(cfg, client)
    table_ref = (
        f"{cfg.output_project}.{cfg.output_dataset}.{cfg.stage_table}"
    )
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Schema is inferred from the DataFrame; the engine outputs are
        # numeric/text/timestamp scalars and round-trip cleanly.
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return int(len(df))


def validate_stage_table(
    cfg: BigQueryWritebackConfig,
    *,
    client: Any = None,
) -> dict[str, Any]:
    """Run the validation SQL against the stage table.

    Returns the metrics dict (always JSON-safe). Raises
    :class:`WritebackValidationError` if any check fails (zero rows,
    nulls in REQUIRED columns, or duplicates on either uniqueness key).
    """
    client = _resolve_client(cfg, client)
    sql = build_validation_query(cfg)
    job = client.query(sql)
    rows = list(job.result())
    if not rows:
        metrics = {"row_count": 0}
        raise WritebackValidationError(
            "Validation query returned no rows; stage table appears empty.",
            metrics=metrics,
        )
    row = rows[0]
    metrics = {
        "row_count": int(_row_field(row, "row_count") or 0),
        "null_site_id": int(_row_field(row, "null_site_id") or 0),
        "null_timestamp": int(_row_field(row, "null_timestamp") or 0),
        "null_primary_key": int(_row_field(row, "null_primary_key") or 0),
        "dup_site_timestamp": int(_row_field(row, "dup_site_timestamp") or 0),
        "dup_primary_key": int(_row_field(row, "dup_primary_key") or 0),
    }
    failures: list[str] = []
    if metrics["row_count"] <= 0:
        failures.append("row_count <= 0")
    for key in (
        "null_site_id",
        "null_timestamp",
        "null_primary_key",
        "dup_site_timestamp",
        "dup_primary_key",
    ):
        if metrics[key] > 0:
            failures.append(f"{key}={metrics[key]}")
    if failures:
        raise WritebackValidationError(
            "Stage validation failed: " + ", ".join(failures),
            metrics=metrics,
        )
    return metrics


def _row_field(row: Any, name: str) -> Any:
    """Read a field from a BigQuery ``Row`` or any mapping/sequence
    that mocks one. Tests that don't carry the full ``Row`` API can
    pass dicts; the helper accepts both shapes."""
    if hasattr(row, name):
        return getattr(row, name)
    if isinstance(row, dict):
        return row.get(name)
    try:
        return row[name]
    except Exception:
        return None


def merge_stage_into_final(
    cfg: BigQueryWritebackConfig,
    *,
    client: Any = None,
) -> dict[str, Any]:
    """Run the deterministic MERGE; return ``{"inserted": N, "updated": M}``.

    The caller must have:

    - written the stage table (see :func:`write_processed_to_stage`),
    - validated it (:func:`validate_stage_table`),
    - and confirmed ``cfg.allow_final_merge is True``.

    The merge target's column list is read from the stage table (not
    the final table) so a stage-side schema drift cannot silently
    insert a NULL column that the operator did not intend to write.
    """
    if not cfg.allow_final_merge:
        raise RuntimeError(
            "merge_stage_into_final called without "
            "allow_final_merge=True. Refusing to mutate the final table."
        )
    if not cfg.final_table:
        raise RuntimeError(
            "merge_stage_into_final called without a configured final_table."
        )
    client = _resolve_client(cfg, client)
    stage_ref = (
        f"{cfg.output_project}.{cfg.output_dataset}.{cfg.stage_table}"
    )
    stage_table = client.get_table(stage_ref)
    columns = [field.name for field in stage_table.schema]
    sql = build_merge_statement(cfg, columns=columns)
    job = client.query(sql)
    job.result()
    # ``num_dml_affected_rows`` is the total of inserted+updated for
    # MERGE on most BQ client versions; some versions expose
    # ``inserted_rows`` / ``updated_rows`` separately. Use the
    # available shape and degrade gracefully for tests / older clients.
    inserted = getattr(job, "num_dml_affected_rows", None)
    updated = None
    if hasattr(job, "merge_inserted_rows"):
        inserted = int(job.merge_inserted_rows)
    if hasattr(job, "merge_updated_rows"):
        updated = int(job.merge_updated_rows)
    return {
        "inserted": int(inserted) if inserted is not None else None,
        "updated": int(updated) if updated is not None else None,
    }


def record_run_row(
    cfg: BigQueryWritebackConfig,
    payload: dict[str, Any],
    *,
    client: Any = None,
) -> None:
    """Append one row to the runs control table.

    ``payload`` keys must match the ``cf_s2_runs`` schema rendered
    by :func:`render_runs_table_ddl`. The helper coerces unknown keys
    to ``NULL`` and validates the ``status`` value.
    """
    status = payload.get("status")
    if status not in _VALID_RUN_STATUSES:
        raise ValueError(
            f"record_run_row: unknown status {status!r}. "
            f"Allowed: {sorted(_VALID_RUN_STATUSES)}."
        )
    client = _resolve_client(cfg, client)
    table_ref = (
        f"{cfg.output_project}.{cfg.control_dataset}.{cfg.runs_table}"
    )
    table = client.get_table(table_ref)
    schema_cols = [f.name for f in table.schema]
    row = {col: payload.get(col) for col in schema_cols}
    errors = client.insert_rows_json(table, [row])
    if errors:
        raise RuntimeError(f"record_run_row: BigQuery insert errors: {errors}")


def read_watermark(
    cfg: BigQueryWritebackConfig,
    site_id: str,
    *,
    client: Any = None,
) -> Optional[str]:
    """Return the ISO-8601 ``last_processed_timestamp`` for ``site_id``,
    or ``None`` if no row exists."""
    bigquery = _import_bigquery()
    client = _resolve_client(cfg, client)
    sql = (
        f"SELECT last_processed_timestamp FROM {cfg.watermark_table_fqn()} "
        "WHERE site_id = @site_id"
    )
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
            ]
        ),
    )
    rows = list(job.result())
    if not rows:
        return None
    ts = _row_field(rows[0], "last_processed_timestamp")
    return None if ts is None else str(ts)


def advance_watermark(
    cfg: BigQueryWritebackConfig,
    *,
    site_id: str,
    last_processed_timestamp: str,
    last_run_id: str,
    client: Any = None,
) -> None:
    """Upsert the per-site watermark row; called only after merge success."""
    bigquery = _import_bigquery()
    client = _resolve_client(cfg, client)
    sql = build_watermark_merge(cfg)
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("site_id", "STRING", site_id),
                bigquery.ScalarQueryParameter(
                    "last_processed_timestamp",
                    "TIMESTAMP",
                    last_processed_timestamp,
                ),
                bigquery.ScalarQueryParameter(
                    "last_run_id", "STRING", last_run_id
                ),
            ]
        ),
    )
    job.result()


# ----------------------------------------------------------------------
# Schema mapping (M6 output -> live `_s2_filt_1` shape)
# ----------------------------------------------------------------------


def read_final_table_columns(
    cfg: BigQueryWritebackConfig,
    *,
    client: Any = None,
) -> Optional[list[str]]:
    """Return the final-table column names, or ``None`` if no final table
    is configured.

    Used by :func:`prepare_stage_dataframe` to filter the staged frame
    to the intersection with the live target schema, so the rendered
    MERGE never references columns the final table does not have. The
    cost is a single ``client.get_table(...)`` round-trip per writeback
    invocation.
    """
    if not cfg.final_table:
        return None
    client = _resolve_client(cfg, client)
    table_ref = (
        f"{cfg.output_project}.{cfg.output_dataset}.{cfg.final_table}"
    )
    table = client.get_table(table_ref)
    return [field.name for field in table.schema]


def read_final_table_schema(
    cfg: BigQueryWritebackConfig,
    *,
    client: Any = None,
) -> Optional[dict[str, str]]:
    """Return ``{column_name: bigquery_field_type}`` for the final table,
    or ``None`` if no final table is configured.

    Used by :func:`prepare_stage_dataframe` to coerce the staged
    DataFrame's column dtypes to match the live target table's BigQuery
    types. Without this, the M7 ``normalize_bigquery_dataframe`` step
    (which casts nullable ``Int64`` → ``float64`` so ``np.where`` in
    ``qc.apply_qc_flags`` doesn't choke on ``pd.NA``) leaves integer
    columns as floats, which BigQuery then refuses to MERGE into INT64
    target columns.
    """
    if not cfg.final_table:
        return None
    client = _resolve_client(cfg, client)
    table_ref = (
        f"{cfg.output_project}.{cfg.output_dataset}.{cfg.final_table}"
    )
    table = client.get_table(table_ref)
    return {field.name: field.field_type for field in table.schema}


_BQ_TYPE_TO_PANDAS_DTYPE: dict[str, str] = {
    # BigQuery INT64 columns must round-trip through pandas nullable
    # ``Int64`` so missing values stay representable; the live MERGE
    # rejects FLOAT64 → INT64 implicit casts.
    "INT64": "Int64",
    "INTEGER": "Int64",
    # FLOAT64 stays as numpy float64 (no nullable wrapper needed; NaN
    # is the canonical missing value).
    "FLOAT64": "float64",
    "FLOAT": "float64",
    "NUMERIC": "float64",
    "BIGNUMERIC": "float64",
    "STRING": "object",
    "BOOL": "boolean",
    "BOOLEAN": "boolean",
    # TIMESTAMP / DATE / DATETIME columns are left alone — pandas
    # already produces the right datetime dtype from the BigQuery
    # read or from this module's stage-identity construction.
}


def _cast_to_target_types(
    df: pd.DataFrame,
    target_types: dict[str, str],
) -> pd.DataFrame:
    """Coerce ``df`` columns to dtypes compatible with ``target_types``.

    Skips columns absent from either side. Failures during coercion
    are surfaced as ``ValueError`` so the caller (and the operator)
    sees the problematic column name; silent fallback would re-create
    the M9 → M10 schema-drift class of bug.
    """
    out = df.copy()
    for col, bq_type in target_types.items():
        if col not in out.columns:
            continue
        pandas_dtype = _BQ_TYPE_TO_PANDAS_DTYPE.get(bq_type.upper())
        if pandas_dtype is None:
            continue
        if pandas_dtype == "Int64":
            try:
                # Two-step: float -> nullable Int (drops .0 from
                # whole-number floats; NaN -> pd.NA).
                out[col] = pd.to_numeric(out[col], errors="coerce").astype(
                    "Int64"
                )
            except Exception as exc:
                raise ValueError(
                    f"_cast_to_target_types: column {col!r} could not be "
                    f"cast to Int64 (target BigQuery type {bq_type!r}): {exc}"
                ) from exc
        elif pandas_dtype == "float64":
            try:
                out[col] = pd.to_numeric(out[col], errors="coerce")
            except Exception as exc:
                raise ValueError(
                    f"_cast_to_target_types: column {col!r} could not be "
                    f"cast to float64 (target BigQuery type {bq_type!r}): {exc}"
                ) from exc
        elif pandas_dtype == "object":
            out[col] = out[col].astype("object")
        elif pandas_dtype == "boolean":
            out[col] = out[col].astype("boolean")
    return out


# ----------------------------------------------------------------------
# M28: stage-payload column-uniqueness guard + silver payload builder
# ----------------------------------------------------------------------


def validate_source_columns_unique(
    df: Optional[pd.DataFrame], *, side: str
) -> None:
    """Raise :class:`DuplicateStageColumnsError` if ``df`` has duplicate names.

    Used to guard the M28 contract that the bronze/source flux frame
    handed to silver payload preparation has unique column names. If a
    source itself carries duplicates we cannot guess which one is
    authoritative; the caller must fix the upstream pipeline.

    A ``None`` or empty frame is a no-op so the helper is safe for
    paths that may not always pass a source frame.
    """
    if df is None:
        return
    columns_attr = getattr(df, "columns", None)
    if columns_attr is None:
        return
    cols = list(columns_attr)
    if not cols:
        return
    seen: dict[str, int] = {}
    for c in cols:
        seen[c] = seen.get(c, 0) + 1
    dups = sorted(c for c, n in seen.items() if n > 1)
    if dups:
        raise DuplicateStageColumnsError(
            f"{side} input has duplicate column names: {dups}. "
            "Stage payload preparation cannot guess which one is "
            "authoritative; fix the upstream pipeline before staging.",
            duplicate_columns=tuple(dups),
        )


def _series_equivalent_nan_aware(a: pd.Series, b: pd.Series) -> bool:
    """Return True iff ``a`` and ``b`` carry the same values, NaN ≡ NaN.

    Conservative: returns False on any shape mismatch, any null-mask
    mismatch, or any non-null value mismatch. Used by
    :func:`ensure_unique_stage_columns` to decide whether two same-named
    columns can be merged (one suppressed) or must be preserved as
    ``rH`` + ``rH_norm_s``.
    """
    if a is None or b is None:
        return False
    if len(a) != len(b):
        return False
    try:
        a_na = a.isna().to_numpy()
        b_na = b.isna().to_numpy()
    except Exception:
        return False
    if not bool((a_na == b_na).all()):
        return False
    mask = ~a_na
    if not bool(mask.any()):
        return True
    try:
        return bool((a.to_numpy()[mask] == b.to_numpy()[mask]).all())
    except Exception:
        return False


def bigquery_field_key(name: Any) -> str:
    """Return the BigQuery logical field key for a column name.

    BigQuery treats schema field names case-insensitively, so
    ``RH`` and ``rH`` collide as the same logical field even though
    pandas considers them distinct (M31). The package-level
    duplicate-column policy operates on this key, not the literal
    pandas column name, so the staged payload is BigQuery-compatible.
    """
    return str(name).casefold()


def ensure_unique_stage_columns(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Return ``(df_unique, collision_actions)`` deterministically.

    Pandas allows duplicate column names; BigQuery does not, and
    BigQuery's schema field uniqueness is **case-insensitive** (M31).
    So columns named ``RH`` and ``rH`` are unique to pandas but
    collide on BigQuery key ``rh``. The M28 + M31 contract is:

    - For every collision on BigQuery key ``rh`` (the known humidity
      family), the first occurrence is canonicalized to ``rH`` and
      becomes the source-side kept column. Subsequent occurrences are
      compared NaN-aware against the kept series:

      * If equivalent, the duplicate is dropped and a
        ``suppressed_equivalent_duplicate`` action is recorded.
      * Otherwise the duplicate is renamed to ``rH_norm_s`` (with
        deterministic numeric suffixes ``rH_norm_s_2`` etc. if
        ``rH_norm_s`` is already taken) and a
        ``renamed_divergent_duplicate`` action is recorded.

      If the first humidity variant arrives under a non-canonical
      case (``RH``, ``Rh``, ...) it is renamed to ``rH`` and a
      ``renamed_to_canonical_humidity`` action is recorded so the
      operator can audit the canonicalization.

    - Every other case-insensitive duplicate raises
      :class:`DuplicateStageColumnsError` naming the affected logical
      field keys and source column names.

    The returned frame is a new object; the input is not mutated. The
    actions list records every collision the helper resolved so
    downstream run-metadata JSON can surface non-silent handling.

    The ``COLUMN_COLLISION_ATTRS_KEY`` attribute on the returned frame
    carries the same actions list for callers that prefer
    ``df.attrs``-based introspection.
    """
    if df is None:
        return df, []
    cols = list(df.columns)
    keys = [bigquery_field_key(c) for c in cols]
    if len(set(keys)) == len(keys):
        out = df.copy()
        out.attrs[COLUMN_COLLISION_ATTRS_KEY] = []
        return out, []

    # Bucket positional indices by BigQuery logical key (case-insensitive).
    key_to_indices: dict[str, list[int]] = {}
    for i, k in enumerate(keys):
        key_to_indices.setdefault(k, []).append(i)

    humidity_key = bigquery_field_key(HUMIDITY_SOURCE_COLUMN)

    # Reject any case-insensitive duplicate we have no policy for
    # (anything other than the humidity family).
    other_dup_keys = sorted(
        k
        for k, idxs in key_to_indices.items()
        if len(idxs) > 1 and k != humidity_key
    )
    if other_dup_keys:
        affected_names = sorted(
            {
                str(cols[i])
                for k in other_dup_keys
                for i in key_to_indices[k]
            }
        )
        raise DuplicateStageColumnsError(
            "Stage payload has duplicate column names with no "
            "deterministic resolution policy (BigQuery field keys are "
            f"case-insensitive). Affected logical keys: {other_dup_keys}. "
            f"Affected column names: {affected_names}. The package "
            "refuses to send a duplicate-column schema to BigQuery; the "
            "upstream pipeline must be fixed before the writeback runs.",
            duplicate_columns=tuple(affected_names),
        )

    actions: list[dict[str, Any]] = []
    rh_idxs = key_to_indices.get(humidity_key, [])
    keep_mask = [True] * len(cols)
    rename_at: dict[int, str] = {}

    if len(rh_idxs) >= 2:
        first_idx = rh_idxs[0]
        first_series = df.iloc[:, first_idx]
        first_name = str(cols[first_idx])
        # Canonicalize the kept humidity column to ``rH`` (the
        # source-side canonical name from
        # ``constants.BIOMET_OUT_RENAME``) so the BigQuery schema
        # field is deterministic regardless of which case variant
        # arrived first from upstream.
        if first_name != HUMIDITY_SOURCE_COLUMN:
            rename_at[first_idx] = HUMIDITY_SOURCE_COLUMN
            actions.append(
                {
                    "column": first_name,
                    "action": "renamed_to_canonical_humidity",
                    "renamed_to": HUMIDITY_SOURCE_COLUMN,
                    "reason": (
                        "BigQuery field keys are case-insensitive; "
                        f"renamed humidity column {first_name!r} to "
                        f"{HUMIDITY_SOURCE_COLUMN!r} to match the "
                        "canonical source name."
                    ),
                }
            )
        # Reserved keys: case-insensitive view of every column name
        # already in the frame or already chosen as a rename target,
        # so a new derived name cannot collide with anything.
        reserved_keys: set[str] = {
            bigquery_field_key(c) for c in cols
        }
        for new_name in rename_at.values():
            reserved_keys.add(bigquery_field_key(new_name))
        for dup_idx in rh_idxs[1:]:
            dup_series = df.iloc[:, dup_idx]
            dup_name = str(cols[dup_idx])
            if _series_equivalent_nan_aware(first_series, dup_series):
                keep_mask[dup_idx] = False
                actions.append(
                    {
                        "column": HUMIDITY_SOURCE_COLUMN,
                        "action": "suppressed_equivalent_duplicate",
                        "renamed_to": None,
                        "source_column_name": dup_name,
                        "reason": (
                            f"derived duplicate {dup_name!r} equivalent "
                            "to source; kept the first occurrence as "
                            f"{HUMIDITY_SOURCE_COLUMN!r}."
                        ),
                    }
                )
            else:
                new_name = HUMIDITY_DERIVED_RENAME
                n = 2
                while bigquery_field_key(new_name) in reserved_keys:
                    new_name = f"{HUMIDITY_DERIVED_RENAME}_{n}"
                    n += 1
                reserved_keys.add(bigquery_field_key(new_name))
                rename_at[dup_idx] = new_name
                actions.append(
                    {
                        "column": HUMIDITY_SOURCE_COLUMN,
                        "action": "renamed_divergent_duplicate",
                        "renamed_to": new_name,
                        "source_column_name": dup_name,
                        "reason": (
                            f"derived duplicate {dup_name!r} diverges "
                            "from source; preserved both as "
                            f"{HUMIDITY_SOURCE_COLUMN!r} and "
                            f"{new_name!r}."
                        ),
                    }
                )

    # Materialize the deduplicated frame. Use iloc-based positional
    # selection so the duplicate-named columns can be addressed
    # individually without ambiguity.
    keep_positions = [i for i, keep in enumerate(keep_mask) if keep]
    new_cols = [rename_at.get(i, cols[i]) for i in keep_positions]
    new_data = df.iloc[:, keep_positions].copy()
    new_data.columns = new_cols
    new_data.attrs[COLUMN_COLLISION_ATTRS_KEY] = list(actions)
    # Final paranoia check: case-insensitive uniqueness. If something
    # slipped past the deterministic policy, surface it loudly so the
    # BigQuery client never sees a duplicate-key schema.
    final_keys = [bigquery_field_key(c) for c in new_cols]
    if len(set(final_keys)) != len(final_keys):
        seen: dict[str, int] = {}
        for k in final_keys:
            seen[k] = seen.get(k, 0) + 1
        post = sorted(k for k, v in seen.items() if v > 1)
        raise DuplicateStageColumnsError(
            "ensure_unique_stage_columns failed to resolve duplicates "
            f"(BigQuery logical keys): {post}. This is a package bug; "
            "please report it.",
            duplicate_columns=tuple(post),
        )
    return new_data, actions


def _rename_unique_columns(
    df: pd.DataFrame, rename_map: dict[str, str]
) -> pd.DataFrame:
    """Return ``df`` with every column whose name is a unique key in
    ``rename_map`` renamed to its mapped value.

    A column is renamed only when its internal name appears exactly
    once in the frame. If upstream produced duplicates of an internal
    name (e.g. two literal ``rH`` columns from a malformed silver
    construction), those duplicates are left untouched so the
    existing :func:`ensure_unique_stage_columns` humidity policy can
    resolve them deterministically (``rH`` + ``rH_norm_s``) rather
    than collapsing into a literal-duplicate source-truth name and
    losing the second series silently.

    The caller's frame is not mutated.
    """
    cols = list(df.columns)
    if not cols:
        return df
    counts: dict[str, int] = {}
    for c in cols:
        counts[c] = counts.get(c, 0) + 1
    new_cols = list(cols)
    changed = False
    for i, c in enumerate(cols):
        if c in rename_map and counts[c] == 1:
            new_cols[i] = rename_map[c]
            changed = True
    if not changed:
        return df
    out = df.copy()
    out.columns = new_cols
    return out


def apply_silver_source_truth_rename(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Rename internal eddy column names to their M32 / M32A source-
    truth final names.

    Maps the backend/canonical names used by stage 1 (``DateTime``,
    ``NEE``, ``QC_NEE``, ``Tair``, ``USTAR``, ``VPD``, ``Rg``,
    ``P_RAIN``, ``rH``) to the inherited final names exposed in
    silver / gold payloads (``timestamp``, ``co2_flux``,
    ``qc_co2_flux``, ``air_temperature_c``, ``u_star``, ``VPD_hpa``,
    ``SWIN_1_1_1``, ``P_RAIN_1_1_1``, ``RH_1_1_1``). Columns not in
    :data:`~miaproc.eddy.constants.SILVER_INTERNAL_TO_FINAL_RENAME`
    are returned unchanged, so passing an already-source-truth frame
    is a no-op. The caller's frame is not mutated.

    When an internal name appears more than once in ``df`` (an
    upstream defect — stage 1 emits at most one of each internal
    column), the rename is skipped for that name so the existing
    :func:`ensure_unique_stage_columns` humidity policy can still
    resolve the M28 ``rH`` / ``rH_norm_s`` fallback.

    M32A: if both internal ``DateTime`` and source-truth
    ``timestamp`` are present, the internal ``DateTime`` is dropped
    so the rename does not synthesize a duplicate ``timestamp``
    field. The source-truth ``timestamp`` value wins at the silver
    output boundary by contract; internal ``DateTime`` is recomputed
    on the gold side via :func:`silver_to_internal_calc_frame`.
    """
    from .constants import SILVER_INTERNAL_TO_FINAL_RENAME

    out = df
    # M32A: drop the internal ``DateTime`` when the source-truth
    # ``timestamp`` is already attached to the frame so the rename
    # cannot create a literal-duplicate ``timestamp`` column.
    if (
        out is not None
        and "DateTime" in out.columns
        and "timestamp" in out.columns
    ):
        out = out.drop(columns=["DateTime"])
    return _rename_unique_columns(out, SILVER_INTERNAL_TO_FINAL_RENAME)


def silver_to_internal_calc_frame(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Map a source-truth silver frame back to the internal eddy
    backend names so :func:`postproc` / :func:`prepare_reddyproc_input`
    can run unchanged.

    Used by the gold CLI after reading a source-truth silver table
    (``timestamp``, ``co2_flux``, ``qc_co2_flux``,
    ``air_temperature_c``, ...) to rebuild the calculation frame
    (``DateTime``, ``NEE``, ``QC_NEE``, ``Tair``, ...) the
    hesseflux / REddyProc backends expect. Columns absent from
    :data:`~miaproc.eddy.constants.FINAL_TO_INTERNAL_RENAME` are
    returned unchanged. The caller's frame is not mutated.

    Like :func:`apply_silver_source_truth_rename`, only unique
    source-truth column names are renamed so a hypothetical upstream
    duplicate is preserved rather than fused.

    M32A: if both source-truth ``timestamp`` and a leaked internal
    ``DateTime`` are present in the same frame (upstream defect or a
    legacy non-source-truth silver table), the leaked ``DateTime``
    is dropped so the source-truth ``timestamp`` value wins on the
    rename. The BigQuery silver table the gold CLI reads back under
    the M32A contract will only carry ``timestamp``, so the
    source-truth value is the authoritative one to feed into the
    backend.
    """
    from .constants import FINAL_TO_INTERNAL_RENAME

    out = df
    # M32A: prefer the source-truth ``timestamp`` over any leaked
    # internal ``DateTime`` so the reconstructed calc frame carries
    # the source-truth time series.
    if (
        out is not None
        and "timestamp" in out.columns
        and "DateTime" in out.columns
    ):
        out = out.drop(columns=["DateTime"])
    return _rename_unique_columns(out, FINAL_TO_INTERNAL_RENAME)


def prepare_silver_stage_payload(
    silver_df: pd.DataFrame,
    *,
    site_id: str,
    source_flux_df: Optional[pd.DataFrame] = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Build a BigQuery-ready silver stage payload (M28).

    Contract:

    - Validate that ``source_flux_df`` (when provided) has unique
      column names; fail loudly via
      :class:`DuplicateStageColumnsError` if not.
    - Preserve every column the silver frame carries. ``silver_df`` is
      the output of ``stage1_from_raw_frames(...)`` and already
      contains the joined bronze/source flux columns plus the
      biomet-derived stage-1 additions, in source-first order.
    - Materialize the operational identity triple
      ``(primary_key, site_id, timestamp)`` from
      ``silver_df['DateTime']`` so the writeback validation SQL
      (``site_id``/``timestamp`` non-null + uniqueness on
      ``primary_key`` and ``(site_id, timestamp)``) can succeed
      without renaming source columns.
    - Run :func:`ensure_unique_stage_columns` so the payload sent to
      ``load_table_from_dataframe`` has unique column names. Resolves
      the M28 ``rH`` / ``rH_norm_s`` collision deterministically.

    Returns ``(payload, collision_actions)``. ``payload.attrs`` also
    carries the actions under :data:`COLUMN_COLLISION_ATTRS_KEY` so
    callers can recover them without holding the tuple.

    The caller's frames are not mutated. ``source_flux_df`` is only
    consulted for the uniqueness guard — silver column order/values
    are taken from ``silver_df`` because stage-1 has already done the
    bronze-preserving join.
    """
    # M32A: accept either the internal ``DateTime`` column (stage 1
    # output, before the source-truth rename) or the source-truth
    # ``timestamp`` column (silver frame that has already passed
    # through :func:`apply_silver_source_truth_rename`, or a silver
    # table read back from BigQuery under the M32A contract). Prefer
    # ``timestamp`` if both are present so the source-truth value
    # wins at the silver output boundary.
    if "timestamp" in silver_df.columns:
        ts_source_col = "timestamp"
    elif "DateTime" in silver_df.columns:
        ts_source_col = "DateTime"
    else:
        raise ValueError(
            "prepare_silver_stage_payload: silver frame missing both "
            "'DateTime' and 'timestamp' columns required for stage-"
            "identity derivation."
        )
    validate_source_columns_unique(source_flux_df, side="bronze flux source")

    out = silver_df.copy()
    dt = pd.to_datetime(out[ts_source_col], utc=True, errors="coerce")
    iso = dt.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    # M32A: drop the legacy internal ``DateTime`` column at the silver
    # output boundary. The source-truth payload exposes only
    # ``timestamp`` per the lineage CSV; the canonical ``timestamp``
    # is reinjected from ``dt`` below.
    if "DateTime" in out.columns:
        out = out.drop(columns=["DateTime"])

    # Identity-triple assignment via DataFrame insertion would create
    # a duplicate when the silver frame already carries one of those
    # names; we resolve it explicitly to avoid silently overwriting a
    # source-side column with a synthesized value.
    #
    # M32A: when the silver frame provided ``timestamp`` as the
    # time source (``ts_source_col == "timestamp"``), the synthesized
    # ``timestamp`` is derived from that exact column. Re-injecting
    # it is not a substitution and should not surface as an
    # ``identity_overwrite`` action — the audit signal is reserved
    # for the case where the caller supplied a *stale* identity
    # column we replaced.
    identity_cols = ("primary_key", "site_id", "timestamp")
    pre_existing = [c for c in identity_cols if c in out.columns]
    if ts_source_col == "timestamp" and "timestamp" in pre_existing:
        pre_existing = [c for c in pre_existing if c != "timestamp"]
    if "timestamp" in out.columns:
        out = out.drop(columns=["timestamp"])
    if pre_existing:
        # Drop pre-existing identity columns so the freshly synthesized
        # values are the only ones present. Record this in actions so
        # the CLI run-metadata makes the substitution visible.
        out = out.drop(columns=pre_existing)

    # M32: rename internal eddy aliases (NEE, USTAR, Tair, VPD, Rg,
    # P_RAIN, rH, QC_NEE) to source-truth final names (co2_flux,
    # u_star, air_temperature_c, VPD_hpa, SWIN_1_1_1, P_RAIN_1_1_1,
    # RH_1_1_1, qc_co2_flux). Applied here, before the unique-column
    # guard, so a preserved flux-side ``RH`` and the renamed biomet
    # ``rH`` -> ``RH_1_1_1`` no longer collide on the case-insensitive
    # BigQuery field key. A no-op when the silver frame already
    # carries source-truth names. Under M32A the rename map also
    # includes ``DateTime -> timestamp``, but ``DateTime`` is dropped
    # above so the rename never synthesizes a literal-duplicate
    # ``timestamp``.
    out = apply_silver_source_truth_rename(out)

    out["timestamp"] = dt
    out["site_id"] = site_id
    out["primary_key"] = site_id + "|" + iso

    # Order: identity triple first, then every preserved silver column
    # in its original order. This puts the BigQuery merge keys at the
    # front of the staged table which matches the M10 convention.
    ordered = list(identity_cols) + [
        c for c in out.columns if c not in identity_cols
    ]
    out = out[ordered]

    payload, actions = ensure_unique_stage_columns(out)

    if pre_existing:
        for ident in pre_existing:
            actions.append(
                {
                    "column": ident,
                    "action": "identity_overwrite",
                    "renamed_to": None,
                    "reason": (
                        "silver frame carried a pre-existing "
                        f"{ident!r} column; replaced with the "
                        "writeback-synthesized identity value"
                    ),
                }
            )
        payload.attrs[COLUMN_COLLISION_ATTRS_KEY] = list(actions)

    return payload, actions


# ----------------------------------------------------------------------
# Gold-side stage-mapping helper (M10) extended with M28 dedup hook
# ----------------------------------------------------------------------


def prepare_stage_dataframe(
    processed: pd.DataFrame,
    *,
    site_id: str,
    source_flux_df: Optional[pd.DataFrame] = None,
    target_columns: Optional[list[str]] = None,
    target_types: Optional[dict[str, str]] = None,
    preserve_payload_columns: bool = False,
) -> pd.DataFrame:
    """Map the M6 backend output to the live ``_s2_filt_1`` stage shape.

    The M6 backend returns a 13-column scientific contract keyed on
    ``DateTime`` (regularized 30-minute grid). The live target table
    ``manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1``
    extends the EddyPro source-flux schema with six lowercase
    analytical columns plus a ``dateAndTime`` STRING (guide 001 §2.1):

    - ``dateAndTime``  (STRING, ``"YYYY-MM-DD HH:MM:SS"``)
    - ``nee_f``        (FLOAT, from ``NEE_f``)
    - ``nee_fqc``      (INTEGER, from ``NEE_fqc``)
    - ``sw_in_f``      (FLOAT, from ``Rg_f``)
    - ``ta_f``         (FLOAT, from ``Tair_f``)
    - ``vpd_f``        (FLOAT, from ``VPD_f``)

    The required identity columns (``primary_key``, ``site_id``,
    ``timestamp``) and the EddyPro source-flux pass-through columns
    are sourced from ``source_flux_df`` (the BigQuery flux read) via
    a LEFT JOIN on ``timestamp``. Where a source row exists for a
    processed timestamp the source ``primary_key`` is preferred;
    where the processed timestamp is a regularized insert (no
    matching source row), a deterministic
    ``primary_key = "<site_id>|<iso_utc_timestamp>"`` is synthesized
    so ``primary_key`` uniqueness stays equivalent to
    ``(site_id, timestamp)`` uniqueness by construction.

    When ``target_columns`` is provided (typically from
    :func:`read_final_table_columns`) and ``preserve_payload_columns``
    is ``False``, the returned frame is filtered to the intersection
    of its columns with that list. The identity triple
    ``(primary_key, site_id, timestamp)`` is always kept so the
    validation SQL contract is preserved. ``DateTime`` and the extra
    engine outputs (``GPP``, ``Reco``, raw ``NEE``) are dropped when
    filtering because the live target does not carry them.

    M28: when ``preserve_payload_columns=True`` the ``target_columns``
    filter is **not** applied. ``target_columns`` / ``target_types``
    still drive dtype coercion (so INT64 target columns coming back
    as float64 from the M7 normalization are cast back to nullable
    ``Int64`` for the MERGE), but every column the caller passed
    through ``processed`` survives into the stage payload. This is the
    silver-to-gold preservation contract; final-table MERGE
    compatibility with payload columns the target does not yet carry
    is a follow-up concern outside M28.

    On the way out the frame is passed through
    :func:`ensure_unique_stage_columns` so the M28 ``rH`` /
    ``rH_norm_s`` collision policy applies and BigQuery never sees a
    duplicate-named schema. The resolved collision actions are
    attached to ``df.attrs[COLUMN_COLLISION_ATTRS_KEY]`` so callers
    can surface them in run-metadata JSON.

    The caller's frames are not mutated.
    """
    if "DateTime" not in processed.columns:
        raise ValueError(
            "prepare_stage_dataframe: processed output is missing the "
            "'DateTime' column required for stage-identity derivation."
        )

    out = processed.copy()
    dt = pd.to_datetime(out["DateTime"], utc=True, errors="coerce")
    out["timestamp"] = dt
    out["site_id"] = site_id
    iso_utc = dt.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Synthesized fallback; may be overwritten by source primary_key below.
    out["primary_key"] = site_id + "|" + iso_utc

    # Lowercase analytical mapping (guide 001 §2.1).
    #
    # M31: rename source -> target rather than duplicating. BigQuery
    # field keys are case-insensitive, so keeping both ``NEE_f`` and
    # ``nee_f`` would fail at ``load_table_from_dataframe`` with
    # ``Field nee_f already exists in schema``. The lowercase
    # analytical names are the user-facing canonical names in the
    # ``_s2_filt_1`` schema; the uppercase backend-output names are
    # an internal scientific convention that does not need to survive
    # into the staged BigQuery payload.
    for src_col, dst_col in S2_FILT_1_RENAME_MAP.items():
        if src_col in out.columns:
            if dst_col in out.columns and dst_col != src_col:
                out = out.drop(columns=[dst_col])
            out = out.rename(columns={src_col: dst_col})

    # dateAndTime STRING column.
    out["dateAndTime"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    # Source-flux pass-through (carries the EddyPro raw columns + the
    # source-side primary_key for non-regularized timestamps).
    if source_flux_df is not None and len(source_flux_df) > 0:
        src = source_flux_df.copy()
        if "timestamp" not in src.columns:
            raise ValueError(
                "prepare_stage_dataframe: source_flux_df is missing the "
                "'timestamp' column required to align with the processed "
                "output."
            )
        src["timestamp"] = pd.to_datetime(
            src["timestamp"], utc=True, errors="coerce"
        )
        if "site_id" in src.columns:
            src = src.loc[src["site_id"] == site_id].copy()
        # Drop columns we have already filled on `out` to avoid
        # silently overwriting them with stale source values.
        already_in_out = set(out.columns) - {"timestamp", "primary_key"}
        keep = [c for c in src.columns if c not in already_in_out]
        src_subset = src[keep].copy()
        # Rename source primary_key to defer the choice of which value wins.
        if "primary_key" in src_subset.columns:
            src_subset = src_subset.rename(
                columns={"primary_key": "_src_primary_key"}
            )
        # Drop site_id from source if it slipped through; we already have it.
        if "site_id" in src_subset.columns:
            src_subset = src_subset.drop(columns=["site_id"])
        out = out.merge(
            src_subset, on="timestamp", how="left", suffixes=("", "_src_dup")
        )
        if "_src_primary_key" in out.columns:
            out["primary_key"] = (
                out["_src_primary_key"].astype("object")
                .where(out["_src_primary_key"].notna(), out["primary_key"])
            )
            out = out.drop(columns=["_src_primary_key"])
        # Defensive: drop any suffix-collision artefacts.
        out = out.loc[
            :, [c for c in out.columns if not c.endswith("_src_dup")]
        ]

    # Filter to target columns when the caller has opted into the
    # legacy M10 narrowing. Identity triple is always kept. M28's
    # preservation default skips this branch.
    identity = ("primary_key", "site_id", "timestamp")
    if target_columns is not None and not preserve_payload_columns:
        keep_set = set(target_columns) | set(identity)
        out = out.loc[:, [c for c in out.columns if c in keep_set]]

    # Cast to target dtypes before the stage write so the live MERGE's
    # implicit type-conversion rules accept the values (e.g. INT64
    # target columns must come from a pandas Int64 column, not from
    # the M7-normalized float64).
    if target_types is not None:
        out = _cast_to_target_types(out, target_types)

    # M32: drop redundant internal-name passthroughs in the gold
    # stage payload whenever the corresponding source-truth final
    # column is also present. The hesseflux/REddyProc backends emit
    # raw NEE/Tair/USTAR/VPD/Rg passthroughs alongside the genuinely
    # new gap-filled outputs (NEE_f, Tair_f, ...); under the M32
    # contract those passthroughs duplicate the source-truth silver
    # columns (co2_flux, air_temperature_c, u_star, VPD_hpa,
    # SWIN_1_1_1) that ``_attach_silver_columns_to_gold`` carries
    # forward. We keep the new processing outputs and drop only the
    # redundant passthroughs to avoid ambiguous backend-vs-source-truth
    # semantics in the staged payload.
    from .constants import SILVER_INTERNAL_TO_FINAL_RENAME

    cols_to_drop = [
        internal
        for internal, final in SILVER_INTERNAL_TO_FINAL_RENAME.items()
        if internal in out.columns and final in out.columns
    ]
    if cols_to_drop:
        out = out.drop(columns=cols_to_drop)

    # Order: identity first, then everything else in stable alphabetical order.
    rest = sorted(c for c in out.columns if c not in identity)
    out = out[list(identity) + rest]

    # M28: enforce unique column names before any caller can hand the
    # frame to ``write_processed_to_stage``. Resolves the known ``rH``
    # source-vs-derived collision deterministically and raises
    # ``DuplicateStageColumnsError`` for anything else.
    out, _actions = ensure_unique_stage_columns(out)
    return out


# ----------------------------------------------------------------------
# Top-level orchestration
# ----------------------------------------------------------------------


def run_writeback(
    df: pd.DataFrame,
    cfg: BigQueryWritebackConfig,
    *,
    run_id: str,
    started_at: str,
    site_id: Optional[str] = None,
    run_payload_extras: Optional[dict[str, Any]] = None,
    client: Any = None,
) -> WritebackResult:
    """Execute the full writeback orchestration for one processed run.

    Sequence:

    1. ``cfg.validate()``;
    2. ``ensure_control_tables_exist(...)``;
    3. ``write_processed_to_stage(df, cfg)``;
    4. ``validate_stage_table(cfg)``;
    5. if ``cfg.allow_final_merge`` and ``cfg.final_table``:
       ``merge_stage_into_final(cfg)`` then advance per-site
       watermarks using
       :func:`max_timestamps_by_site` (M24: one watermark row per
       distinct ``site_id`` present in the staged frame); single-site
       runs naturally collapse to one ``advance_watermark`` call;
    6. ``record_run_row(cfg, ...)`` regardless of merge decision;
    7. return a :class:`WritebackResult`.

    Multi-site grouped runs (M24): when ``site_id`` is omitted (the
    grouped CLI path always omits it) the runs-table ``site_id``
    field is set to the unique stage-side site value when there is
    only one, otherwise to :data:`GROUPED_RUN_ROW_SITE_LABEL`.

    Validation failure aborts merge but is still recorded as a
    `validation_failed` run row. Any other exception during stage write
    or merge is recorded as ``failed`` and re-raised so the CLI can
    surface a non-zero exit code.
    """
    cfg.validate()
    client = _resolve_client(cfg, client)

    extras = dict(run_payload_extras or {})

    # Resolve a label for the runs control row's `site_id` field.
    # Grouped multi-site stacked runs (no caller-supplied site_id and
    # the staged frame carries more than one site value) collapse to
    # the GROUPED_RUN_ROW_SITE_LABEL sentinel so a downstream auditor
    # can tell the row covers more than one site without inventing a
    # composite identifier.
    if site_id is None:
        unique_sites: list[str] = []
        if "site_id" in df.columns:
            unique_sites = sorted(
                str(v)
                for v in df["site_id"].dropna().unique().tolist()
            )
        if len(unique_sites) == 1:
            run_row_site_label: Optional[str] = unique_sites[0]
        elif len(unique_sites) >= 2:
            run_row_site_label = GROUPED_RUN_ROW_SITE_LABEL
        else:
            run_row_site_label = None
    else:
        run_row_site_label = site_id

    base_payload: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "site_id": run_row_site_label,
        "bq_output_project": cfg.output_project,
        "bq_output_dataset": cfg.output_dataset,
        "bq_stage_table": cfg.stage_table,
        "bq_final_table": cfg.final_table,
        "merge_authorized": bool(cfg.allow_final_merge),
        **extras,
    }

    stage_rows = 0
    merge_attempted = False
    merge_inserted: Optional[int] = None
    merge_updated: Optional[int] = None
    watermark_advanced = False
    watermark_value: Optional[str] = None
    watermark_values_by_site: dict[str, str] = {}
    validation_metrics: dict[str, Any] = {}
    error_text: Optional[str] = None

    try:
        ensure_control_tables_exist(cfg, client=client)
        stage_rows = write_processed_to_stage(df, cfg, client=client)
        validation_metrics = validate_stage_table(cfg, client=client)
    except WritebackValidationError as exc:
        validation_metrics = exc.metrics
        error_text = str(exc)
        finished_payload = {
            **base_payload,
            "finished_at": _utc_now_iso(),
            "status": RUN_STATUS_VALIDATION_FAILED,
            "stage_rows": stage_rows,
            "merge_attempted": False,
            "merge_inserted_rows": None,
            "merge_updated_rows": None,
            "watermark_advanced": False,
            "watermark_value": None,
            "error_text": error_text,
        }
        record_run_row(cfg, finished_payload, client=client)
        _attach_writeback_state(
            exc,
            merge_attempted=False,
            merge_authorized=bool(cfg.allow_final_merge),
            stage_rows=stage_rows,
            status=RUN_STATUS_VALIDATION_FAILED,
        )
        raise
    except Exception as exc:
        error_text = str(exc)
        finished_payload = {
            **base_payload,
            "finished_at": _utc_now_iso(),
            "status": RUN_STATUS_FAILED,
            "stage_rows": stage_rows,
            "merge_attempted": False,
            "merge_inserted_rows": None,
            "merge_updated_rows": None,
            "watermark_advanced": False,
            "watermark_value": None,
            "error_text": error_text,
        }
        try:
            record_run_row(cfg, finished_payload, client=client)
        except Exception:
            pass
        _attach_writeback_state(
            exc,
            merge_attempted=False,
            merge_authorized=bool(cfg.allow_final_merge),
            stage_rows=stage_rows,
            status=RUN_STATUS_FAILED,
        )
        raise

    if cfg.allow_final_merge and cfg.final_table:
        try:
            merge_attempted = True
            merge_counts = merge_stage_into_final(cfg, client=client)
            merge_inserted = merge_counts.get("inserted")
            merge_updated = merge_counts.get("updated")
            # Per-site watermark advancement (M24): one watermark row
            # per distinct stacked site. Falls back to the legacy
            # caller-supplied ``site_id`` + global max timestamp when
            # the staged frame lacks a usable ``site_id`` column.
            per_site = max_timestamps_by_site(df)
            if per_site:
                for site_key in sorted(per_site):
                    ts_iso = per_site[site_key]
                    advance_watermark(
                        cfg,
                        site_id=site_key,
                        last_processed_timestamp=ts_iso,
                        last_run_id=run_id,
                        client=client,
                    )
                watermark_values_by_site = dict(per_site)
                watermark_advanced = True
                watermark_value = max(per_site.values())
            elif site_id is not None:
                ts_max = _max_timestamp(df)
                if ts_max is not None:
                    advance_watermark(
                        cfg,
                        site_id=site_id,
                        last_processed_timestamp=ts_max,
                        last_run_id=run_id,
                        client=client,
                    )
                    watermark_values_by_site = {site_id: ts_max}
                    watermark_advanced = True
                    watermark_value = ts_max
            status = RUN_STATUS_SUCCEEDED
        except Exception as exc:
            error_text = str(exc)
            finished_payload = {
                **base_payload,
                "finished_at": _utc_now_iso(),
                "status": RUN_STATUS_FAILED,
                "stage_rows": stage_rows,
                "merge_attempted": True,
                "merge_inserted_rows": merge_inserted,
                "merge_updated_rows": merge_updated,
                "watermark_advanced": False,
                "watermark_value": None,
                "error_text": error_text,
            }
            try:
                record_run_row(cfg, finished_payload, client=client)
            except Exception:
                pass
            _attach_writeback_state(
                exc,
                merge_attempted=True,
                merge_authorized=bool(cfg.allow_final_merge),
                stage_rows=stage_rows,
                status=RUN_STATUS_FAILED,
            )
            raise
    else:
        status = RUN_STATUS_STAGE_ONLY_SUCCEEDED

    finished_payload = {
        **base_payload,
        "finished_at": _utc_now_iso(),
        "status": status,
        "stage_rows": stage_rows,
        "merge_attempted": merge_attempted,
        "merge_inserted_rows": merge_inserted,
        "merge_updated_rows": merge_updated,
        "watermark_advanced": watermark_advanced,
        "watermark_value": watermark_value,
        "error_text": None,
    }
    record_run_row(cfg, finished_payload, client=client)

    return WritebackResult(
        run_id=run_id,
        status=status,
        stage_rows=stage_rows,
        merge_attempted=merge_attempted,
        merge_authorized=bool(cfg.allow_final_merge),
        merge_inserted_rows=merge_inserted,
        merge_updated_rows=merge_updated,
        watermark_advanced=watermark_advanced,
        watermark_value=watermark_value,
        validation_metrics=validation_metrics,
        stage_table_fqn=cfg.stage_table_fqn(),
        final_table_fqn=cfg.final_table_fqn(),
        runs_table_fqn=cfg.runs_table_fqn(),
        watermark_table_fqn=cfg.watermark_table_fqn(),
        error_text=None,
        watermark_values_by_site=watermark_values_by_site,
    )


# ----------------------------------------------------------------------
# Small private helpers
# ----------------------------------------------------------------------


def _utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _attach_writeback_state(
    exc: BaseException,
    *,
    merge_attempted: bool,
    merge_authorized: bool,
    stage_rows: int,
    status: str,
) -> None:
    """Annotate ``exc`` with a ``miaproc_writeback_state`` payload so the
    CLI's best-effort failure-path ``run.json`` writer can reflect the
    real progress of the writeback orchestration (in particular,
    whether MERGE was attempted) without re-deriving it from the
    exception text.

    Best-effort: silently swallows any failure to set the attribute
    (e.g. on builtin exception types that disallow attributes), since
    the authoritative record is the ``cf_s2_runs`` row that
    ``run_writeback`` already inserted before re-raising.
    """
    try:
        exc.miaproc_writeback_state = {  # type: ignore[attr-defined]
            "merge_attempted": bool(merge_attempted),
            "merge_authorized": bool(merge_authorized),
            "stage_rows": int(stage_rows),
            "status": status,
        }
    except Exception:
        pass


def _max_timestamp(df: pd.DataFrame) -> Optional[str]:
    """Return the max ``timestamp`` (or ``DateTime``) in ``df`` as ISO."""
    for col in ("timestamp", "DateTime"):
        if col in df.columns:
            ts_max = pd.to_datetime(df[col], errors="coerce").max()
            if pd.notna(ts_max):
                return ts_max.isoformat()
    return None


def max_timestamps_by_site(df: pd.DataFrame) -> dict[str, str]:
    """Return ``{site_id: ISO-8601 max-timestamp}`` for the staged frame.

    Group by the staged ``site_id`` column, take the max of the
    ``timestamp`` column (or ``DateTime`` as a fallback), and emit
    one entry per non-null site value present. Used by
    :func:`run_writeback` after a successful MERGE to drive per-site
    watermark advancement for stacked multi-site grouped runs (M24).
    Empty when ``df`` has neither ``site_id`` nor a usable timestamp
    column; callers must treat that as a refusal-to-advance signal.
    """
    if "site_id" not in df.columns:
        return {}
    ts_col: Optional[str] = None
    for cand in ("timestamp", "DateTime"):
        if cand in df.columns:
            ts_col = cand
            break
    if ts_col is None:
        return {}
    out: dict[str, str] = {}
    work = df[["site_id", ts_col]].copy()
    work[ts_col] = pd.to_datetime(work[ts_col], errors="coerce")
    work = work.dropna(subset=["site_id", ts_col])
    for site, group in work.groupby("site_id"):
        ts_max = group[ts_col].max()
        if pd.notna(ts_max):
            out[str(site)] = ts_max.isoformat()
    return out
