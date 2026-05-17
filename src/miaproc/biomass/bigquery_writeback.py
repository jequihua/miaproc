"""BigQuery writeback + merge control for biomass enrichment outputs (M20).

This module owns the **write side** of the BigQuery-native biomass path,
parallel to ``miaproc.eddy.bigquery_writeback`` but with the
biomass-specific design choices the M20 prompt asked us to resolve
honestly:

1. ensure the runs control table exists in the operator-owned
   orchestration dataset (idempotent ``CREATE TABLE IF NOT EXISTS``);
2. load the enriched DataFrame to a BigQuery **staging** table
   (always ``WRITE_TRUNCATE``: each run replaces the stage table);
3. run validation SQL against the stage table (row count,
   ``primary_key`` non-null, ``primary_key`` uniqueness — single
   merge identity by default);
4. **only when explicitly authorized**, MERGE the staged rows into
   the final target table on the configured merge key (default
   ``primary_key``; non-key columns updated from the stage row;
   inserts on ``WHEN NOT MATCHED``);
5. record run metadata into ``<control>.cf_biomass_runs``.

**Watermark omitted by design.** The eddy parallel uses a per-site
``last_processed_timestamp`` watermark because eddy is time-series
append-only (process new timestamps as they arrive). Biomass is
per-tree identity-keyed enrichment — every tree has a stable
``primary_key`` and re-running biomass simply re-MERGEs on that key,
so there is no "next batch by time" to checkmark. Adding a watermark
table would create misleading semantics (what would
"last processed timestamp" even mean for static tree records?). M20
therefore keeps a **runs/control table only**, no watermark table.

Merge identity is configurable via ``cfg.merge_key_column`` (default
``"primary_key"``), so a deployment whose forest-structure table uses a
different stable identifier can override without code edits.

Hard scope (M20 first writeback pass):

- output project must not equal a forbidden production input project
  (default forbidden set: ``("manglaria",)``);
- final-table MERGE is gated by ``allow_final_merge=True``; default is
  **stage-only**;
- Cloud Run / IAM / Scheduler / Terraform are deferred to colleagues;
- the M16 / M17 / M17A / M19 contracts are preserved exactly: row
  preservation, exactly two appended columns
  (``biomass_estimate``, ``equation_used``),
  ``equation_used``-masked-when-NaN semantics, M17A alias recovery,
  no R / no preflight.

The ``google-cloud-bigquery`` dependency is **lazy-imported** so
importing ``miaproc.biomass`` does not require the BigQuery extras.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional

import pandas as pd


__all__ = [
    "BigQueryBiomassWritebackConfig",
    "WritebackResult",
    "WritebackValidationError",
    "build_validation_query",
    "build_merge_statement",
    "render_runs_table_ddl",
    "ensure_control_tables_exist",
    "write_enriched_to_stage",
    "validate_stage_table",
    "merge_stage_into_final",
    "record_run_row",
    "run_writeback",
    "prepare_stage_dataframe",
]


_INSTALL_HINT = (
    "BigQuery writeback mode requires the google-cloud-bigquery "
    "client. Install with: pip install 'miaproc[bigquery]'."
)


# Default biomass merge identity. Overridable via
# ``BigQueryBiomassWritebackConfig.merge_key_column`` so deployments
# whose forest-structure tables key on a different stable identifier
# can override without code edits. Non-null + unique on this key are
# the load-bearing validation checks before any MERGE runs.
DEFAULT_MERGE_KEY_COLUMN: str = "primary_key"


# Status values written to the runs control table. Same closed set as
# the eddy M8 parallel so downstream dashboards can share status
# vocabulary across modules.
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
    """Raised when the stage-table validation SQL reports any failure."""

    def __init__(self, message: str, metrics: dict[str, Any]) -> None:
        super().__init__(message)
        self.metrics = metrics


# ----------------------------------------------------------------------
# Config + result dataclasses
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class BigQueryBiomassWritebackConfig:
    """Configuration for a single biomass writeback orchestration run.

    All writes happen against ``output_project`` (which **must not** be
    a production read-only project). The runs control table lives in
    ``control_dataset`` under the same output project so the operator
    can audit it without crossing project boundaries.

    ``allow_final_merge`` defaults to ``False``: stage-only is the
    safe default. Final-table MERGE only happens when this flag is
    explicitly ``True`` (typically wired to the CLI
    ``--bq-allow-final-merge`` switch) **and** ``final_table`` is set.

    ``merge_key_column`` defaults to ``"primary_key"`` — biomass
    forest-structure tables conventionally carry this column as a
    stable per-tree identifier. Override for deployments that key on
    a different stable column.

    No ``watermark_table`` field: biomass M20 deliberately omits the
    watermark concept. See the module docstring for the design
    rationale.
    """

    output_project: str
    output_dataset: str
    stage_table: str
    control_dataset: str
    final_table: Optional[str] = None
    allow_final_merge: bool = False
    run_id: Optional[str] = None
    merge_key_column: str = DEFAULT_MERGE_KEY_COLUMN
    runs_table: str = "cf_biomass_runs"
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
            ("merge_key_column", self.merge_key_column),
        ):
            if not value:
                raise ValueError(f"{name} must be a non-empty string.")
        if self.allow_final_merge and not self.final_table:
            raise ValueError(
                "allow_final_merge=True requires final_table to be set."
            )


@dataclass(frozen=True)
class WritebackResult:
    """Structured outcome of a biomass writeback run; JSON-safe via :meth:`to_dict`.

    No watermark fields — biomass M20 has no watermark concept. The
    run_id, status, stage_rows, and merge counts are sufficient for
    audit; the runs control table is the authoritative trail.
    """

    run_id: str
    status: str
    stage_rows: int
    merge_attempted: bool
    merge_authorized: bool
    merge_inserted_rows: Optional[int]
    merge_updated_rows: Optional[int]
    validation_metrics: dict[str, Any] = field(default_factory=dict)
    stage_table_fqn: str = ""
    final_table_fqn: Optional[str] = None
    runs_table_fqn: str = ""
    error_text: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ----------------------------------------------------------------------
# DDL + SQL builders (pure; reviewable; no BQ client dependency)
# ----------------------------------------------------------------------


def render_runs_table_ddl(cfg: BigQueryBiomassWritebackConfig) -> str:
    """``CREATE TABLE IF NOT EXISTS`` for ``<control>.cf_biomass_runs``.

    Schema is biomass-shaped (single input table, no flux+biomet pair;
    no watermark fields). Biomass-specific provenance lets a downstream
    auditor reconstruct the read source from the runs row alone.
    """
    return f"""\
CREATE TABLE IF NOT EXISTS {cfg.runs_table_fqn()} (
  run_id STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  status STRING NOT NULL,
  bq_input_project STRING,
  bq_input_dataset STRING,
  bq_input_table STRING,
  bq_output_project STRING,
  bq_output_dataset STRING,
  bq_stage_table STRING,
  bq_final_table STRING,
  merge_key_column STRING,
  read_input_rows INT64,
  stage_rows INT64,
  estimated_rows INT64,
  skipped_rows INT64,
  dataset STRING,
  equations_source STRING,
  merge_attempted BOOL,
  merge_authorized BOOL,
  merge_inserted_rows INT64,
  merge_updated_rows INT64,
  miaproc_version STRING,
  bigquery_client_version STRING,
  error_text STRING
)
"""


def build_validation_query(cfg: BigQueryBiomassWritebackConfig) -> str:
    """Single-row validation SQL against the stage table.

    Returns one row with the metrics the writeback layer must check
    before any MERGE: row count, ``merge_key_column`` null count, and
    ``merge_key_column`` duplicate count. Single-key uniqueness check
    is the biomass parallel to eddy's composite-key check; biomass
    tables conventionally carry a stable single-column identifier
    per tree.
    """
    key = cfg.merge_key_column
    return f"""\
SELECT
  COUNT(*) AS row_count,
  COUNTIF({key} IS NULL) AS null_merge_key,
  COUNT(*) - COUNT(DISTINCT {key}) AS dup_merge_key
FROM {cfg.stage_table_fqn()}
"""


def build_merge_statement(
    cfg: BigQueryBiomassWritebackConfig,
    *,
    columns: list[str],
) -> str:
    """Render the deterministic MERGE on ``cfg.merge_key_column``.

    ``columns`` is the full column list of the stage table (must
    include the merge key). Non-key columns are updated from the
    stage row; ``WHEN NOT MATCHED THEN INSERT`` writes the full row.
    No deletes — same posture as the eddy M8 parallel.
    """
    if not cfg.final_table:
        raise ValueError("final_table must be set to render a MERGE statement.")
    key = cfg.merge_key_column
    if key not in columns:
        raise ValueError(
            f"Refusing to render MERGE: stage table has no '{key}' "
            "column. Either include the merge key in the staged frame "
            "or override --bq-merge-key to point at a column the table "
            "actually has."
        )
    final_fqn = cfg.final_table_fqn()
    stage_fqn = cfg.stage_table_fqn()
    update_cols = [c for c in columns if c != key]
    if not update_cols:
        raise ValueError(
            "Refusing to render MERGE: stage table has no non-key columns "
            "to update."
        )
    update_set = ",\n  ".join(f"{c} = S.{c}" for c in update_cols)
    insert_cols = ", ".join(columns)
    insert_values = ", ".join(f"S.{c}" for c in columns)
    return f"""\
MERGE {final_fqn} T
USING {stage_fqn} S
ON T.{key} = S.{key}
WHEN MATCHED THEN UPDATE SET
  {update_set}
WHEN NOT MATCHED THEN INSERT ({insert_cols})
VALUES ({insert_values})
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


def _resolve_client(
    cfg: BigQueryBiomassWritebackConfig, client: Any
) -> Any:
    if client is not None:
        return client
    bigquery = _import_bigquery()
    return bigquery.Client(project=cfg.billing_project_or_output())


def _run_simple_sql(client: Any, sql: str) -> Any:
    """Submit ``sql`` and block until the BigQuery job finishes."""
    job = client.query(sql)
    job.result()
    return job


def _row_field(row: Any, name: str) -> Any:
    """Read a field from a BigQuery ``Row`` or any mapping/sequence
    that mocks one."""
    if hasattr(row, name):
        return getattr(row, name)
    if isinstance(row, dict):
        return row.get(name)
    try:
        return row[name]
    except Exception:
        return None


# ----------------------------------------------------------------------
# Public operations
# ----------------------------------------------------------------------


def ensure_control_tables_exist(
    cfg: BigQueryBiomassWritebackConfig,
    *,
    client: Any = None,
) -> None:
    """Idempotently create the runs control table.

    Single-table version of the eddy parallel: biomass M20 has no
    watermark table, so this only ensures ``cf_biomass_runs``.
    """
    client = _resolve_client(cfg, client)
    _run_simple_sql(client, render_runs_table_ddl(cfg))


def write_enriched_to_stage(
    df: pd.DataFrame,
    cfg: BigQueryBiomassWritebackConfig,
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
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    return int(len(df))


def validate_stage_table(
    cfg: BigQueryBiomassWritebackConfig,
    *,
    client: Any = None,
) -> dict[str, Any]:
    """Run the validation SQL against the stage table.

    Returns the metrics dict. Raises :class:`WritebackValidationError`
    if any check fails (zero rows, nulls in the merge key, or
    duplicates on the merge key).
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
        "null_merge_key": int(_row_field(row, "null_merge_key") or 0),
        "dup_merge_key": int(_row_field(row, "dup_merge_key") or 0),
        "merge_key_column": cfg.merge_key_column,
    }
    failures: list[str] = []
    if metrics["row_count"] <= 0:
        failures.append("row_count <= 0")
    for key in ("null_merge_key", "dup_merge_key"):
        if metrics[key] > 0:
            failures.append(f"{key}={metrics[key]}")
    if failures:
        raise WritebackValidationError(
            "Stage validation failed: " + ", ".join(failures),
            metrics=metrics,
        )
    return metrics


def merge_stage_into_final(
    cfg: BigQueryBiomassWritebackConfig,
    *,
    client: Any = None,
) -> dict[str, Any]:
    """Run the deterministic MERGE; return ``{"inserted": N, "updated": M}``.

    The caller must have:

    - written the stage table (see :func:`write_enriched_to_stage`),
    - validated it (:func:`validate_stage_table`),
    - and confirmed ``cfg.allow_final_merge is True``.

    The merge target's column list is read from the stage table (not
    the final table) so a stage-side schema drift cannot silently
    insert a NULL column the operator did not intend to write.
    """
    if not cfg.allow_final_merge:
        raise RuntimeError(
            "merge_stage_into_final called without allow_final_merge=True. "
            "Refusing to mutate the final table."
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
    cfg: BigQueryBiomassWritebackConfig,
    payload: dict[str, Any],
    *,
    client: Any = None,
) -> None:
    """Append one row to the runs control table.

    ``payload`` keys must match the ``cf_biomass_runs`` schema rendered
    by :func:`render_runs_table_ddl`. Unknown keys are coerced to
    ``NULL``; the ``status`` value is validated against the closed
    set of run statuses.
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


# ----------------------------------------------------------------------
# Stage preparation (minimal — biomass output is already stage-shaped)
# ----------------------------------------------------------------------


def prepare_stage_dataframe(
    enriched: pd.DataFrame,
    *,
    cfg: BigQueryBiomassWritebackConfig,
) -> pd.DataFrame:
    """Validate that ``enriched`` is suitable for the stage table.

    Biomass M20 keeps stage prep deliberately minimal: the enriched
    output from ``estimate_trees`` / the M19 CLI projection IS the
    stage shape (input rows + 2 appended columns). We only confirm
    that the merge-key column exists, since a missing merge key would
    cause the validation step to fail loudly; surfacing it here gives
    a more actionable error before the BigQuery round-trip.

    Returns a copy of the input frame; the caller's frame is not
    mutated.
    """
    if cfg.merge_key_column not in enriched.columns:
        raise ValueError(
            f"prepare_stage_dataframe: merge-key column "
            f"{cfg.merge_key_column!r} is not present in the enriched "
            "frame. Either include it in the source forest-structure "
            "table or override --bq-merge-key to point at a column the "
            "table does have."
        )
    return enriched.copy()


# ----------------------------------------------------------------------
# Top-level orchestration
# ----------------------------------------------------------------------


def run_writeback(
    df: pd.DataFrame,
    cfg: BigQueryBiomassWritebackConfig,
    *,
    run_id: str,
    started_at: str,
    run_payload_extras: Optional[dict[str, Any]] = None,
    client: Any = None,
) -> WritebackResult:
    """Execute the full writeback orchestration for one biomass run.

    Sequence:

    1. ``cfg.validate()``;
    2. ``ensure_control_tables_exist(...)`` (runs table only);
    3. ``write_enriched_to_stage(df, cfg)``;
    4. ``validate_stage_table(cfg)``;
    5. if ``cfg.allow_final_merge`` and ``cfg.final_table``:
       ``merge_stage_into_final(cfg)``;
       (no watermark step — biomass M20 omits the watermark concept);
    6. ``record_run_row(cfg, ...)`` regardless of merge decision;
    7. return a :class:`WritebackResult`.

    Validation failure aborts merge but is recorded as
    ``validation_failed``. Any other exception during stage write or
    merge is recorded as ``failed`` and re-raised so the CLI can
    surface a non-zero exit code.
    """
    cfg.validate()
    client = _resolve_client(cfg, client)

    extras = dict(run_payload_extras or {})
    base_payload: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "bq_output_project": cfg.output_project,
        "bq_output_dataset": cfg.output_dataset,
        "bq_stage_table": cfg.stage_table,
        "bq_final_table": cfg.final_table,
        "merge_key_column": cfg.merge_key_column,
        "merge_authorized": bool(cfg.allow_final_merge),
        **extras,
    }

    stage_rows = 0
    merge_attempted = False
    merge_inserted: Optional[int] = None
    merge_updated: Optional[int] = None
    validation_metrics: dict[str, Any] = {}
    error_text: Optional[str] = None

    try:
        ensure_control_tables_exist(cfg, client=client)
        stage_rows = write_enriched_to_stage(df, cfg, client=client)
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
        validation_metrics=validation_metrics,
        stage_table_fqn=cfg.stage_table_fqn(),
        final_table_fqn=cfg.final_table_fqn(),
        runs_table_fqn=cfg.runs_table_fqn(),
        error_text=None,
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
    """Annotate ``exc`` with ``miaproc_writeback_state`` so the CLI's
    failure-path ``run.json`` writer can reflect real progress without
    re-deriving from the exception text."""
    try:
        exc.miaproc_writeback_state = {  # type: ignore[attr-defined]
            "merge_attempted": bool(merge_attempted),
            "merge_authorized": bool(merge_authorized),
            "stage_rows": int(stage_rows),
            "status": status,
        }
    except Exception:
        pass
