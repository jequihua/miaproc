"""Production CLI for long-series miaproc processing.

Non-interactive job-friendly entrypoint that wraps stage-1 ingestion +
``postproc`` for routine and cloud-job execution.

CLI shape (M7 introduces a module-aware namespace; M14 adds the
silver/gold split):

- ``miaproc run ...`` — file-based eddy run (M6 contract; unchanged).
- ``miaproc eddy run-bigquery ...`` — BigQuery-native eddy run (new in
  M7). Reads flux + biomet directly from BigQuery, runs the standard
  scientific path in memory, writes the same three artifacts to local
  disk. Cloud staging-table writes / MERGE are out of scope here.
- ``miaproc eddy run-silver ...`` — silver-stage only (new in M14).
  Stage-1 ingest + clean + regularize -> silver table. No engine, no R
  preflight; silver is the input contract for ``run-gold`` and any
  future cloud orchestrator.
- ``miaproc eddy run-gold ...`` — gold-stage only (new in M14). Reads
  a silver-stage table from disk, dispatches the selected engine
  (default ``reddyproc-reference``), and writes a gold table that
  preserves silver columns and appends backend outputs.
- ``miaproc eddy run-bigquery-silver ...`` — BigQuery-native silver
  (new in M22). Reads bronze/source flux + biomet directly from
  BigQuery, runs the same accepted stage-1 pipeline as
  ``run-silver``, and writes the silver table + run JSON to local
  disk. Optionally stages the silver output back into a BigQuery
  silver stage table; **stage-only is the only supported writeback
  for silver** (M22 keeps the safe default narrow). No engine, no R
  preflight.
- ``miaproc eddy run-bigquery-gold ...`` — BigQuery-native gold
  (new in M22). Reads a silver-stage table from BigQuery, dispatches
  the selected engine (default ``reddyproc-reference``), and writes
  the gold table + diagnostics + run JSON to local disk. Optionally
  stages the gold output back into BigQuery and MERGEs into a final
  gold target table under explicit ``--bq-allow-final-merge`` opt-in
  (same M8/M10 writeback safety posture as the one-shot path).
- ``miaproc biomass enrich-table ...`` — biomass table enrichment
  (new in M17). Reads an individual-tree table from CSV/parquet and
  writes the same table back with exactly two appended columns:
  the biomass estimate and the equation-used identifier (sourced
  from ``source_record_id``). Default dataset is ``"dina"`` (M16
  mangrove direct-biomass equations).
- ``miaproc biomass run-bigquery ...`` — BigQuery-native biomass
  enrichment (new in M19). Reads one individual-tree forest-structure
  source table directly from BigQuery and writes the enriched table +
  run JSON to local disk. Same row-preserving + exactly-two-appended
  contract as ``enrich-table``. M20 optionally stages the enriched
  output back to BigQuery and can MERGE into a final table only with
  explicit ``--bq-allow-final-merge``.

Both paths share the three supported run modes (M6 Task 2):

- ``hesseflux-native`` (Decision 011 closure-track default)
- ``hesseflux-ltwrapper`` (opt-in REddyProc-comparability mode)
- ``reddyproc-reference`` (R-backed reference; preflight-gated per
  Decision 010 / risk R11)

Every successful run writes three artifacts:

1. processed table (csv or parquet, by extension);
2. backend diagnostics JSON (the ``df.attrs["miaproc_diagnostics"]``
   payload, JSON-cleaned);
3. run metadata JSON (engine, key config, timestamps, row counts,
   input paths/identifiers, output paths, versions where available).

Exit codes are stable for batch orchestrators:

- ``0``: success.
- ``2``: ``reddyproc-reference`` preflight not project-scoped approved.
- ``3``: input or config validation failure.
- ``4``: runtime processing failure.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import math
import os
import platform
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger("miaproc.cli")


SUCCESS_EXIT = 0
PREFLIGHT_NOT_APPROVED_EXIT = 2
VALIDATION_EXIT = 3
RUNTIME_EXIT = 4


ENGINES = (
    "hesseflux-native",
    "hesseflux-ltwrapper",
    "reddyproc-reference",
)


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _add_common_engine_args(
    p: argparse.ArgumentParser,
    *,
    engine_default: Optional[str] = None,
) -> None:
    """Engine + output-artifact + reddyproc-site flags shared by the
    file-based, BigQuery-native, and silver/gold split run commands.

    By default ``--engine`` is required. Pass ``engine_default`` to make
    it optional with a stable default — used by ``miaproc eddy run-gold``
    where ``reddyproc-reference`` is the documented Docker-runtime
    default per Decision 010 and the M14 silver/gold split.
    """
    if engine_default is None:
        p.add_argument(
            "--engine",
            required=True,
            choices=list(ENGINES),
            help="Run mode. See miaproc.cli docstring for closure-track defaults.",
        )
    else:
        p.add_argument(
            "--engine",
            default=engine_default,
            choices=list(ENGINES),
            help=(
                "Run mode (default: %(default)s). See miaproc.cli docstring "
                "for closure-track defaults."
            ),
        )
    p.add_argument(
        "--output-table",
        required=True,
        type=Path,
        help="Output table path. Format inferred from extension (.csv or .parquet).",
    )
    p.add_argument(
        "--output-diagnostics-json",
        required=True,
        type=Path,
        help="Output path for backend diagnostics JSON.",
    )
    p.add_argument(
        "--output-run-json",
        required=True,
        type=Path,
        help="Output path for run-metadata JSON (engine, config, timestamps, paths).",
    )
    p.add_argument(
        "--repo-root",
        type=Path,
        default=None,
        help=(
            "Repository root containing renv.lock for the project-scoped R "
            "preflight. Required for --engine reddyproc-reference."
        ),
    )
    p.add_argument(
        "--lt-min-night-samples",
        type=int,
        default=100,
        help=(
            "Minimum nighttime fqc==0 samples required by the Lloyd-Taylor "
            "wrapper fit (--engine hesseflux-ltwrapper)."
        ),
    )
    # Optional reddyproc site metadata, mirrored from example 03.
    p.add_argument("--site-name", default="Marismas_Nacionales")
    p.add_argument("--latitude", type=float, default=22.25)
    p.add_argument("--longitude", type=float, default=-105.50)
    p.add_argument("--timezone-hour", type=float, default=-7.0)
    p.add_argument("--local-tz", default="America/Mazatlan")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="miaproc",
        description=(
            "miaproc production CLI — non-interactive long-series "
            "post-processing for batch and cloud-job execution. "
            "Module-aware: file-based eddy work uses 'miaproc run'; "
            "BigQuery-native eddy work uses 'miaproc eddy run-bigquery'."
        ),
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")
    sub.required = True

    # --- File-based eddy run (M6 contract; unchanged) ---
    run = sub.add_parser(
        "run",
        help="File-based eddy run (stage-1 + stage-2 in one job).",
        description=(
            "Run a single non-interactive file-based post-processing job "
            "and write machine-readable artifacts (processed table, "
            "diagnostics JSON, run metadata JSON)."
        ),
    )
    run.add_argument("--flux-dir", required=True, type=Path)
    run.add_argument("--biomet-dir", required=True, type=Path)
    run.add_argument(
        "--group-column",
        default=None,
        help=(
            "Optional categorical column name used to partition the "
            "all-data input into per-category runs (e.g. 'site_id'). "
            "When set, every non-null category present in the input "
            "is processed independently and the final --output-table "
            "is the deterministic stack of all per-category outputs. "
            "When omitted, the whole input is processed once. M24: "
            "single-value site/category selection is no longer a CLI "
            "option; pre-filter the input or call package functions "
            "programmatically to process exactly one subset."
        ),
    )
    run.add_argument(
        "--output-groups-dir",
        type=Path,
        default=None,
        help=(
            "Optional directory for per-category artifacts written "
            "under grouped execution. Defaults to "
            "'<output-table-parent>/<output-table-stem>__groups'."
        ),
    )
    run.add_argument(
        "--skip-flux",
        type=int,
        default=0,
        help="Rows to skip at the top of each flux CSV (1 if a units row is present).",
    )
    run.add_argument(
        "--skip-biomet",
        type=int,
        default=0,
        help="Rows to skip at the top of each biomet CSV.",
    )
    _add_common_engine_args(run)

    # --- Module-aware namespace: 'eddy' ---
    eddy = sub.add_parser(
        "eddy",
        help="Eddy-covariance domain commands (M7+).",
        description=(
            "Module-aware namespace for eddy-covariance commands. "
            "Future modules (e.g. biomass) will live under their own "
            "siblings to keep the top-level CLI free of single-domain "
            "assumptions (see docs/guides/002_carbon_flux_bq_orchestration_guide.md)."
        ),
    )
    eddy_sub = eddy.add_subparsers(dest="eddy_command", metavar="<eddy_command>")
    eddy_sub.required = True

    eddy_bq = eddy_sub.add_parser(
        "run-bigquery",
        help=(
            "BigQuery-native eddy run (read + optional stage write + "
            "opt-in MERGE)."
        ),
        description=(
            "Read flux + biomet inputs directly from BigQuery, run the "
            "selected eddy engine in memory, and write the standard "
            "three artifacts to local disk. Optionally stage processed "
            "output back into BigQuery (when --bq-stage-table is set) "
            "and MERGE it into the staging final table (only when "
            "--bq-allow-final-merge is also passed). Stage-only is the "
            "safe default; the production input project remains "
            "read-only at all times."
        ),
    )
    eddy_bq.add_argument(
        "--bq-input-project",
        required=True,
        help="GCP project that owns the source flux + biomet tables.",
    )
    eddy_bq.add_argument(
        "--bq-input-dataset",
        required=True,
        help="BigQuery dataset that contains the source tables.",
    )
    eddy_bq.add_argument(
        "--bq-flux-table",
        required=True,
        help="Source flux table name (e.g. carbon_flux_eddycovariance).",
    )
    eddy_bq.add_argument(
        "--bq-biomet-table",
        required=True,
        help="Source biomet table name (e.g. carbon_flux_biomet).",
    )
    eddy_bq.add_argument(
        "--bq-billing-project",
        default=None,
        help=(
            "Optional GCP project to bill query jobs to. Defaults to "
            "--bq-input-project."
        ),
    )
    eddy_bq.add_argument(
        "--bq-start-timestamp",
        default=None,
        help=(
            "Optional inclusive lower bound for the timestamp window "
            "(ISO-8601, e.g. '2025-01-01T00:00:00Z')."
        ),
    )
    eddy_bq.add_argument(
        "--bq-end-timestamp",
        default=None,
        help=(
            "Optional exclusive upper bound for the timestamp window "
            "(ISO-8601, e.g. '2025-02-01T00:00:00Z')."
        ),
    )
    eddy_bq.add_argument(
        "--bq-no-storage-api",
        action="store_true",
        help=(
            "Disable the BigQuery Storage Read API path; use REST-only "
            "to_dataframe() instead. Slower; useful when the storage "
            "extra is not installed in the runtime."
        ),
    )
    # --- M8 writeback / merge-control flags ---
    # All writeback flags are optional individually; if --bq-stage-table
    # is set, the writeback path is engaged and the rest of the
    # output-side flags become required (validated in
    # _validate_bigquery_args). Stage-only is the default; final-table
    # MERGE requires --bq-allow-final-merge to opt in explicitly.
    eddy_bq.add_argument(
        "--bq-output-project",
        default=None,
        help=(
            "GCP project to write the staging table, control tables, and "
            "(optionally) final-table MERGE into. Must NOT be the input "
            "production project. Required when --bq-stage-table is set."
        ),
    )
    eddy_bq.add_argument(
        "--bq-output-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the staging "
            "and final tables. Required when --bq-stage-table is set."
        ),
    )
    eddy_bq.add_argument(
        "--bq-stage-table",
        default=None,
        help=(
            "Stage table name under --bq-output-dataset. Setting this "
            "engages the writeback path (write + validate + optional "
            "merge). Each run replaces the stage table content "
            "(WRITE_TRUNCATE)."
        ),
    )
    eddy_bq.add_argument(
        "--bq-final-table",
        default=None,
        help=(
            "Final target table name under --bq-output-dataset for the "
            "MERGE step (e.g. carbon_flux_eddycovariance_s2_filt_1). "
            "Required when --bq-allow-final-merge is set."
        ),
    )
    eddy_bq.add_argument(
        "--bq-control-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the "
            "orchestration control tables (cf_s2_runs, cf_s2_watermark). "
            "Required when --bq-stage-table is set. Idempotent CREATE "
            "TABLE IF NOT EXISTS is run on each writeback invocation."
        ),
    )
    eddy_bq.add_argument(
        "--bq-allow-final-merge",
        action="store_true",
        help=(
            "Explicit operator opt-in to MERGE the staged rows into the "
            "final target table (and to advance the per-site watermark). "
            "Default is stage-only — final-table mutation never happens "
            "without this flag."
        ),
    )
    eddy_bq.add_argument(
        "--bq-run-id",
        default=None,
        help=(
            "Optional run identifier recorded in the control tables. "
            "Defaults to a generated 'local-<UTC-stamp>-<pid>' string so "
            "operator runs are namespaced away from any future "
            "scheduled-cloud run identifiers."
        ),
    )
    eddy_bq.add_argument(
        "--group-column",
        default=None,
        help=(
            "Optional categorical column name used to partition the "
            "all-data BigQuery read into per-category runs (e.g. "
            "'site_id'). When set, every non-null category present in "
            "the read window is processed independently and the final "
            "stacked output is the concatenation of all per-category "
            "outputs in deterministic order. When omitted, the whole "
            "read is processed once. M24: single-site selection is no "
            "longer a CLI option; the BigQuery read is all-categories "
            "and does not inject a WHERE site_id = @site_id filter "
            "from the CLI. BigQuery writeback (when engaged) writes "
            "the stacked all-category output once into the shared "
            "stage table, so cloud orchestration can use shared stage "
            "table names such as cf_s2_gold_stage rather than "
            "per-site names."
        ),
    )
    eddy_bq.add_argument(
        "--output-groups-dir",
        type=Path,
        default=None,
        help=(
            "Optional directory for per-category local artifacts. "
            "Defaults to '<output-table-parent>/"
            "<output-table-stem>__groups'."
        ),
    )
    _add_common_engine_args(eddy_bq)

    # --- M14 silver/gold split: file-first two-stage eddy contract ---
    eddy_silver = eddy_sub.add_parser(
        "run-silver",
        help=(
            "Run silver-stage eddy processing (flux + biomet -> silver "
            "table; stage-1 only, no engine, no R)."
        ),
        description=(
            "Run the stage-1 silver eddy path: load + clean + regularize "
            "the joined flux + biomet slice, then write the resulting "
            "silver table plus a run-metadata JSON. No engine dispatch "
            "and no R preflight — silver is the input contract for "
            "'miaproc eddy run-gold' and for any future cloud "
            "orchestrator that wraps the same module logic."
        ),
    )
    eddy_silver.add_argument("--flux-dir", required=True, type=Path)
    eddy_silver.add_argument("--biomet-dir", required=True, type=Path)
    eddy_silver.add_argument(
        "--group-column",
        default=None,
        help=(
            "Optional categorical column to partition the all-data "
            "input into per-category silver runs (e.g. 'site_id'). "
            "When set, every non-null category present in the input "
            "is processed independently and the final --output-table "
            "is the deterministic stack of all per-category silver "
            "outputs. When omitted, the whole input becomes one "
            "silver table. M24: single-value site selection is no "
            "longer a CLI option."
        ),
    )
    eddy_silver.add_argument(
        "--output-groups-dir",
        type=Path,
        default=None,
        help=(
            "Optional directory for per-category silver artifacts. "
            "Defaults to '<output-table-parent>/"
            "<output-table-stem>__groups'."
        ),
    )
    eddy_silver.add_argument(
        "--skip-flux",
        type=int,
        default=0,
        help="Rows to skip at the top of each flux CSV (1 if a units row is present).",
    )
    eddy_silver.add_argument(
        "--skip-biomet",
        type=int,
        default=0,
        help="Rows to skip at the top of each biomet CSV.",
    )
    eddy_silver.add_argument(
        "--tz-in",
        default="UTC",
        help="Timezone of the input CSV timestamps (default: UTC).",
    )
    eddy_silver.add_argument(
        "--tz-out",
        default="UTC",
        help="Timezone for the regularized DateTime output column (default: UTC).",
    )
    eddy_silver.add_argument(
        "--output-table",
        required=True,
        type=Path,
        help="Output silver table path. Format inferred from extension (.csv or .parquet).",
    )
    eddy_silver.add_argument(
        "--output-run-json",
        required=True,
        type=Path,
        help="Output path for silver run-metadata JSON.",
    )

    eddy_gold = eddy_sub.add_parser(
        "run-gold",
        help=(
            "Run gold-stage eddy processing (silver table -> gold table; "
            "default engine reddyproc-reference)."
        ),
        description=(
            "Run the stage-2 gold eddy path: read a silver-stage table "
            "from disk, dispatch the selected engine through "
            "miaproc.eddy.postproc(...), and write the gold table plus "
            "diagnostics + run metadata. Silver columns the backend did "
            "not produce are LEFT-joined onto the gold output keyed on "
            "DateTime so the silver-to-gold column-preservation contract "
            "is visible in one file. The default engine is "
            "'reddyproc-reference' per Decision 010 and the M14 "
            "Docker-runtime story; pass --engine hesseflux-native for "
            "Python-only local CSV testing without R."
        ),
    )
    eddy_gold.add_argument(
        "--silver-table",
        required=True,
        type=Path,
        help=(
            "Path to a silver-stage table written by 'miaproc eddy "
            "run-silver' (.csv or .parquet)."
        ),
    )
    eddy_gold.add_argument(
        "--group-column",
        default=None,
        help=(
            "Optional categorical column on the silver table used to "
            "partition gold execution into per-category runs (e.g. "
            "'site_id'). When set, every non-null category in the "
            "silver table is processed independently and the final "
            "--output-table is the deterministic stack of all "
            "per-category gold outputs (with M14 silver-column "
            "preservation applied per-group). When omitted, the "
            "whole silver table is processed once. M24: single-site "
            "selection is no longer a CLI option."
        ),
    )
    eddy_gold.add_argument(
        "--output-groups-dir",
        type=Path,
        default=None,
        help=(
            "Optional directory for per-category gold artifacts. "
            "Defaults to '<output-table-parent>/"
            "<output-table-stem>__groups'."
        ),
    )
    _add_common_engine_args(eddy_gold, engine_default="reddyproc-reference")

    # --- M22 BigQuery-native silver/gold split ---
    eddy_bq_silver = eddy_sub.add_parser(
        "run-bigquery-silver",
        help=(
            "BigQuery-native silver-stage eddy run "
            "(bronze/source BigQuery -> silver; optional silver stage write)."
        ),
        description=(
            "Read bronze/source flux + biomet inputs directly from "
            "BigQuery, run the same accepted stage-1 pipeline as "
            "'miaproc eddy run-silver', and write the silver table + "
            "run-metadata JSON to local disk. Optionally stage the "
            "silver output back to BigQuery (when --bq-stage-table is "
            "set). M22 keeps silver writeback stage-only by design — "
            "there is no --bq-final-table / --bq-allow-final-merge for "
            "silver. No engine dispatch, no R, no project-scoped "
            "preflight: silver is the input contract for "
            "'miaproc eddy run-bigquery-gold'."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-input-project",
        required=True,
        help="GCP project that owns the source flux + biomet tables.",
    )
    eddy_bq_silver.add_argument(
        "--bq-input-dataset",
        required=True,
        help="BigQuery dataset that contains the source tables.",
    )
    eddy_bq_silver.add_argument(
        "--bq-flux-table",
        required=True,
        help="Source flux table name (e.g. carbon_flux_eddycovariance).",
    )
    eddy_bq_silver.add_argument(
        "--bq-biomet-table",
        required=True,
        help="Source biomet table name (e.g. carbon_flux_biomet).",
    )
    eddy_bq_silver.add_argument(
        "--bq-billing-project",
        default=None,
        help=(
            "Optional GCP project to bill query jobs to. Defaults to "
            "--bq-input-project."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-start-timestamp",
        default=None,
        help=(
            "Optional inclusive lower bound for the timestamp window "
            "(ISO-8601, e.g. '2025-01-01T00:00:00Z')."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-end-timestamp",
        default=None,
        help=(
            "Optional exclusive upper bound for the timestamp window "
            "(ISO-8601, e.g. '2025-02-01T00:00:00Z')."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-no-storage-api",
        action="store_true",
        help=(
            "Disable the BigQuery Storage Read API path; use REST-only "
            "to_dataframe() instead. Slower; useful when the storage "
            "extra is not installed in the runtime."
        ),
    )
    eddy_bq_silver.add_argument(
        "--group-column",
        default=None,
        help=(
            "Optional categorical column to partition the all-data "
            "BigQuery silver run into per-category runs (e.g. "
            "'site_id'). When set, every non-null category present "
            "in the read window is processed independently; the "
            "stacked silver output is staged once into the BigQuery "
            "silver stage table when writeback is engaged. When "
            "omitted, the whole read becomes one silver table. "
            "M24: single-site selection is no longer a CLI option; "
            "the BigQuery read is all-categories and does not inject "
            "a WHERE site_id = @site_id filter from the CLI."
        ),
    )
    eddy_bq_silver.add_argument(
        "--output-groups-dir",
        type=Path,
        default=None,
        help=(
            "Optional directory for per-category local silver "
            "artifacts. Defaults to '<output-table-parent>/"
            "<output-table-stem>__groups'."
        ),
    )
    eddy_bq_silver.add_argument(
        "--tz-in",
        default="UTC",
        help="Timezone of the input timestamps (default: UTC).",
    )
    eddy_bq_silver.add_argument(
        "--tz-out",
        default="UTC",
        help="Timezone for the regularized DateTime output column (default: UTC).",
    )
    eddy_bq_silver.add_argument(
        "--output-table",
        required=True,
        type=Path,
        help="Output silver table path. Format inferred from extension (.csv or .parquet).",
    )
    eddy_bq_silver.add_argument(
        "--output-run-json",
        required=True,
        type=Path,
        help="Output path for silver run-metadata JSON.",
    )
    # Silver writeback (stage-only by design — no final/merge for silver in M22).
    eddy_bq_silver.add_argument(
        "--bq-output-project",
        default=None,
        help=(
            "GCP project to write the silver staging table and the "
            "runs control table into. Must NOT be the input production "
            "project. Required when --bq-stage-table is set."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-output-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the silver "
            "stage table. Required when --bq-stage-table is set."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-stage-table",
        default=None,
        help=(
            "Silver stage table name under --bq-output-dataset. Setting "
            "this engages the writeback path (write + validate). Each "
            "run replaces the stage table content (WRITE_TRUNCATE). M22 "
            "silver is stage-only: no MERGE flag exists."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-control-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the "
            "orchestration runs control table (cf_s2_runs). Required "
            "when --bq-stage-table is set."
        ),
    )
    eddy_bq_silver.add_argument(
        "--bq-run-id",
        default=None,
        help=(
            "Optional run identifier recorded in the runs control "
            "table. Defaults to a generated 'local-<UTC-stamp>-<pid>' "
            "string so operator runs are namespaced away from any "
            "future scheduled-cloud run identifiers."
        ),
    )
    eddy_bq_silver.add_argument(
        "--stage-payload-dry-run-dir",
        type=Path,
        default=None,
        help=(
            "M29: build the exact silver stage payload that would be "
            "sent to BigQuery and write it to this directory as "
            "'stage_payload.csv' + 'stage_payload_metadata.json' "
            "instead of performing any BigQuery write. The BigQuery "
            "read still happens (this is a read -> local-payload "
            "validation mode), but no stage write, validation SQL, "
            "MERGE, or watermark advancement runs. Wins over "
            "--bq-stage-table; intended for cloud engineers to "
            "inspect the unique-column payload before authorizing a "
            "real writeback."
        ),
    )

    eddy_bq_gold = eddy_sub.add_parser(
        "run-bigquery-gold",
        help=(
            "BigQuery-native gold-stage eddy run (silver BigQuery -> "
            "gold; optional gold stage write + opt-in MERGE)."
        ),
        description=(
            "Read a silver-stage table directly from BigQuery and run "
            "the selected eddy backend through the same accepted gold "
            "path as 'miaproc eddy run-gold'. Writes the gold table + "
            "backend diagnostics JSON + run-metadata JSON to local "
            "disk. Optionally stages the gold output back to BigQuery "
            "(when --bq-stage-table is set) and MERGEs into a final "
            "gold table under explicit --bq-allow-final-merge opt-in "
            "(same M8/M10 writeback safety posture as the one-shot "
            "'miaproc eddy run-bigquery'). The default engine is "
            "'reddyproc-reference' (Decision 010 / R11): pass "
            "--repo-root /app so the project-scoped R preflight can "
            "approve the runtime."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-input-project",
        required=True,
        help="GCP project that owns the silver input table.",
    )
    eddy_bq_gold.add_argument(
        "--bq-input-dataset",
        required=True,
        help="BigQuery dataset that contains the silver input table.",
    )
    eddy_bq_gold.add_argument(
        "--bq-silver-table",
        required=True,
        help=(
            "Silver-stage table name (typically written by "
            "'miaproc eddy run-bigquery-silver')."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-billing-project",
        default=None,
        help=(
            "Optional GCP project to bill query jobs to. Defaults to "
            "--bq-input-project."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-start-timestamp",
        default=None,
        help=(
            "Optional inclusive lower bound for the silver timestamp "
            "window (ISO-8601)."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-end-timestamp",
        default=None,
        help=(
            "Optional exclusive upper bound for the silver timestamp "
            "window (ISO-8601)."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-no-storage-api",
        action="store_true",
        help=(
            "Disable the BigQuery Storage Read API path; use REST-only "
            "to_dataframe() instead."
        ),
    )
    eddy_bq_gold.add_argument(
        "--group-column",
        default=None,
        help=(
            "Optional categorical column on the silver table used to "
            "partition gold execution into per-category runs (e.g. "
            "'site_id'). When set, every non-null category present "
            "in the silver-table read window is processed "
            "independently; the stacked gold output is staged once "
            "into the BigQuery gold stage table when writeback is "
            "engaged, and a final MERGE then advances per-site "
            "watermarks for every stacked site. When omitted, the "
            "whole silver read becomes one gold run. M24: single-site "
            "selection is no longer a CLI option; the silver BigQuery "
            "read is all-categories and does not inject a "
            "WHERE site_id = @site_id filter from the CLI."
        ),
    )
    eddy_bq_gold.add_argument(
        "--output-groups-dir",
        type=Path,
        default=None,
        help=(
            "Optional directory for per-category local gold "
            "artifacts. Defaults to '<output-table-parent>/"
            "<output-table-stem>__groups'."
        ),
    )
    # Gold writeback (full M8/M10 surface mirrored from run-bigquery).
    eddy_bq_gold.add_argument(
        "--bq-output-project",
        default=None,
        help=(
            "GCP project to write the gold staging table, control "
            "tables, and (optionally) final-table MERGE into. Must NOT "
            "be the input production project. Required when "
            "--bq-stage-table is set."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-output-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the gold "
            "stage and final tables. Required when --bq-stage-table "
            "is set."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-stage-table",
        default=None,
        help=(
            "Gold stage table name under --bq-output-dataset. Setting "
            "this engages the writeback path (write + validate + "
            "optional merge). Each run replaces the stage table "
            "content (WRITE_TRUNCATE)."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-final-table",
        default=None,
        help=(
            "Final gold target table name under --bq-output-dataset for "
            "the MERGE step. Required when --bq-allow-final-merge is "
            "set."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-control-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the "
            "orchestration control tables (cf_s2_runs, cf_s2_watermark). "
            "Required when --bq-stage-table is set."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-allow-final-merge",
        action="store_true",
        help=(
            "Explicit operator opt-in to MERGE the staged gold rows "
            "into the final target table (and to advance the per-site "
            "watermark). Default is stage-only — final-table mutation "
            "never happens without this flag."
        ),
    )
    eddy_bq_gold.add_argument(
        "--bq-run-id",
        default=None,
        help=(
            "Optional run identifier recorded in the control tables. "
            "Defaults to a generated 'local-<UTC-stamp>-<pid>' string."
        ),
    )
    eddy_bq_gold.add_argument(
        "--stage-payload-dry-run-dir",
        type=Path,
        default=None,
        help=(
            "M29: build the exact gold stage payload that would be "
            "sent to BigQuery and write it to this directory as "
            "'stage_payload.csv' + 'stage_payload_metadata.json' "
            "instead of performing any BigQuery write. The silver "
            "BigQuery read and the gold engine dispatch still run "
            "(the engine's R preflight gate is unchanged for "
            "reddyproc-reference), but no stage write, validation "
            "SQL, MERGE, or watermark advancement happens. Wins "
            "over --bq-stage-table and --bq-allow-final-merge."
        ),
    )
    _add_common_engine_args(eddy_bq_gold, engine_default="reddyproc-reference")

    # --- M17 biomass: module-aware namespace + enrich-table subcommand ---
    biomass = sub.add_parser(
        "biomass",
        help="Biomass-domain commands (M17+).",
        description=(
            "Module-aware namespace for biomass commands. The current "
            "entry point is 'enrich-table' (M17): row-preserving "
            "enrichment of an individual-tree table with biomass "
            "estimate + equation-used identifier (sourced from "
            "source_record_id). Cloud orchestration is owned by infra "
            "colleagues (M18+ handoff drafts)."
        ),
    )
    biomass_sub = biomass.add_subparsers(
        dest="biomass_command", metavar="<biomass_command>"
    )
    biomass_sub.required = True

    biomass_enrich = biomass_sub.add_parser(
        "enrich-table",
        help=(
            "Enrich a tree table with biomass estimate + "
            "equation-used identifier (exactly 2 appended columns)."
        ),
        description=(
            "Read an individual-tree table from CSV/parquet, enrich it "
            "row-preservingly through the accepted M16 biomass API, and "
            "write the same table back with exactly two appended "
            "columns: a numeric biomass estimate and the "
            "equation-used identifier (the matched equation's "
            "source_record_id). Original rows + columns are preserved "
            "verbatim; ineligible rows (missing dbh_cm, non-adult "
            "life_stage for direct biomass, species not matched) are "
            "kept with NaN/None in the two appended columns. Default "
            "dataset is 'dina' (mangrove direct-biomass equations from "
            "the M16 packaged parquet)."
        ),
    )
    biomass_enrich.add_argument(
        "--input-table",
        required=True,
        type=Path,
        help="Path to the input tree table (.csv or .parquet).",
    )
    biomass_enrich.add_argument(
        "--output-table",
        required=True,
        type=Path,
        help=(
            "Path to write the enriched table. Format inferred from "
            "extension (.csv or .parquet)."
        ),
    )
    biomass_enrich.add_argument(
        "--output-run-json",
        required=True,
        type=Path,
        help="Path to write the run-metadata JSON.",
    )
    biomass_enrich.add_argument(
        "--dataset",
        default="dina",
        help=(
            "source_dataset filter for the M16 packaged parquet. "
            "Default 'dina' (mangrove direct-biomass equations); use "
            "'infys' for the Mexican volume workbook; pass an empty "
            "string to disable the dataset filter."
        ),
    )
    biomass_enrich.add_argument(
        "--equations-path",
        type=Path,
        default=None,
        help=(
            "Optional explicit path to a non-default equations parquet. "
            "Default (omit the flag) loads the packaged M16 unified "
            "parquet."
        ),
    )
    biomass_enrich.add_argument(
        "--state",
        default=None,
        help=(
            "Optional state filter (Mexican state for 'infys' rows; "
            "'dina' rows have no state and match via the any-state "
            "fallback)."
        ),
    )
    biomass_enrich.add_argument(
        "--response-variable",
        default=None,
        help=(
            "Optional response_variable filter (e.g. 'B' for biomass "
            "kg, 'V' for volume m3). Defaults to no filter."
        ),
    )
    # BiomassColumns overrides — keep aligned with the BiomassColumns
    # constructor so the CLI mirrors the library defaults.
    biomass_enrich.add_argument(
        "--species-col",
        default="species",
        help="Input column name for species (default: 'species').",
    )
    biomass_enrich.add_argument(
        "--dbh-col",
        default="dbh_cm",
        help="Input column name for DBH in cm (default: 'dbh_cm').",
    )
    biomass_enrich.add_argument(
        "--height-col",
        default="tree_height_m",
        help=(
            "Input column name for total height in metres (default: "
            "'tree_height_m'). Optional for direct-biomass 'dina' rows."
        ),
    )
    biomass_enrich.add_argument(
        "--life-stage-col",
        default="life_stage",
        help=(
            "Input column name for life-stage classification (default: "
            "'life_stage'). Direct-biomass requires the value to "
            "normalize to 'Adult'."
        ),
    )
    # Output column names — stable defaults, but overridable.
    biomass_enrich.add_argument(
        "--biomass-estimate-col",
        default="biomass_estimate",
        help=(
            "Name of the appended numeric estimate column "
            "(default: 'biomass_estimate')."
        ),
    )
    biomass_enrich.add_argument(
        "--equation-used-col",
        default="equation_used",
        help=(
            "Name of the appended equation-used identifier column, "
            "sourced from the matched equation's source_record_id "
            "(default: 'equation_used')."
        ),
    )

    # --- M19/M20 biomass run-bigquery: BQ read + optional writeback ---
    biomass_bq = biomass_sub.add_parser(
        "run-bigquery",
        help=(
            "Read an individual-tree table directly from BigQuery, "
            "enrich row-preservingly with the M16/M17/M17A contract, "
            "write local output, and optionally stage/merge to BigQuery."
        ),
        description=(
            "BigQuery-native biomass enrichment (M19 read + M20 optional "
            "writeback). Reads one "
            "individual-tree forest-structure source table from "
            "BigQuery, runs the same accepted enrichment pipeline as "
            "'miaproc biomass enrich-table' in memory, and writes the "
            "enriched table + run-metadata JSON to local disk. "
            "When --bq-stage-table is set, it also WRITE_TRUNCATEs the "
            "enriched output to a BigQuery stage table, validates the "
            "stage, and records a biomass run row. Stage-only is the "
            "safe default; final-table mutation requires both "
            "--bq-final-table and explicit --bq-allow-final-merge. The "
            "output contract is identical to 'enrich-table': original "
            "rows + columns preserved, exactly two appended columns by "
            "default ('biomass_estimate', 'equation_used'). Default "
            "dataset is 'dina'. Biomass needs no R, so this command "
            "never invokes the project-scoped preflight."
        ),
    )
    # BigQuery read flags.
    biomass_bq.add_argument(
        "--bq-input-project",
        required=True,
        help="GCP project that owns the source forest-structure table.",
    )
    biomass_bq.add_argument(
        "--bq-input-dataset",
        required=True,
        help="BigQuery dataset that contains the source table.",
    )
    biomass_bq.add_argument(
        "--bq-input-table",
        required=True,
        help=(
            "Source forest-structure / individual-tree table name. "
            "The table is read with SELECT * so any original columns "
            "the table carries pass through to the enriched output."
        ),
    )
    biomass_bq.add_argument(
        "--bq-billing-project",
        default=None,
        help=(
            "Optional GCP project to bill query jobs to. Defaults to "
            "--bq-input-project."
        ),
    )
    biomass_bq.add_argument(
        "--bq-row-limit",
        type=int,
        default=None,
        help=(
            "Optional LIMIT clause on the BigQuery read. Useful for "
            "local-evidence smokes against a real table without "
            "pulling the whole slice."
        ),
    )
    biomass_bq.add_argument(
        "--bq-no-storage-api",
        action="store_true",
        help=(
            "Disable the BigQuery Storage Read API path; use REST-only "
            "to_dataframe() instead. Slower; useful when the storage "
            "extra is not installed in the runtime."
        ),
    )
    # Output flags.
    biomass_bq.add_argument(
        "--output-table",
        required=True,
        type=Path,
        help=(
            "Path to write the enriched table. Format inferred from "
            "extension (.csv or .parquet)."
        ),
    )
    biomass_bq.add_argument(
        "--output-run-json",
        required=True,
        type=Path,
        help="Path to write the run-metadata JSON.",
    )
    # Enrichment flags (parallel to enrich-table).
    biomass_bq.add_argument(
        "--dataset",
        default="dina",
        help=(
            "source_dataset filter for the M16 packaged parquet. "
            "Default 'dina' (mangrove direct-biomass equations); use "
            "'infys' for the Mexican volume workbook; pass an empty "
            "string to disable the dataset filter."
        ),
    )
    biomass_bq.add_argument(
        "--equations-path",
        type=Path,
        default=None,
        help=(
            "Optional explicit path to a non-default equations parquet. "
            "Default (omit the flag) loads the packaged M16 unified "
            "parquet."
        ),
    )
    biomass_bq.add_argument(
        "--state",
        default=None,
        help=(
            "Optional state filter (Mexican state for 'infys' rows; "
            "'dina' rows have no state and match via the any-state "
            "fallback)."
        ),
    )
    biomass_bq.add_argument(
        "--response-variable",
        default=None,
        help=(
            "Optional response_variable filter (e.g. 'B' for biomass "
            "kg, 'V' for volume m3). Defaults to no filter."
        ),
    )
    biomass_bq.add_argument(
        "--species-col",
        default="species",
        help="Input column name for species (default: 'species').",
    )
    biomass_bq.add_argument(
        "--dbh-col",
        default="dbh_cm",
        help="Input column name for DBH in cm (default: 'dbh_cm').",
    )
    biomass_bq.add_argument(
        "--height-col",
        default="tree_height_m",
        help=(
            "Input column name for total height in metres (default: "
            "'tree_height_m'). Optional for direct-biomass 'dina' rows."
        ),
    )
    biomass_bq.add_argument(
        "--life-stage-col",
        default="life_stage",
        help=(
            "Input column name for life-stage classification (default: "
            "'life_stage'). Direct-biomass requires the value to "
            "normalize to 'Adult'."
        ),
    )
    biomass_bq.add_argument(
        "--biomass-estimate-col",
        default="biomass_estimate",
        help=(
            "Name of the appended numeric estimate column "
            "(default: 'biomass_estimate')."
        ),
    )
    biomass_bq.add_argument(
        "--equation-used-col",
        default="equation_used",
        help=(
            "Name of the appended equation-used identifier column, "
            "sourced from the matched equation's source_record_id "
            "(default: 'equation_used')."
        ),
    )

    # --- M20 biomass writeback / merge-control flag set ---
    # All writeback flags are optional individually; if --bq-stage-table
    # is set, the writeback path is engaged and the rest of the
    # output-side flags become required (validated in
    # _validate_biomass_bigquery_args). Stage-only is the default;
    # final-table MERGE requires --bq-allow-final-merge to opt in
    # explicitly. Symmetric with the eddy M8 flag set.
    biomass_bq.add_argument(
        "--bq-output-project",
        default=None,
        help=(
            "GCP project to write the staging table, control table, "
            "and (optionally) final-table MERGE into. Must NOT be the "
            "input production project. Required when --bq-stage-table "
            "is set."
        ),
    )
    biomass_bq.add_argument(
        "--bq-output-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the "
            "staging and final tables. Required when --bq-stage-table "
            "is set."
        ),
    )
    biomass_bq.add_argument(
        "--bq-stage-table",
        default=None,
        help=(
            "Stage table name under --bq-output-dataset. Setting this "
            "engages the writeback path (write + validate + optional "
            "merge). Each run replaces the stage table content "
            "(WRITE_TRUNCATE)."
        ),
    )
    biomass_bq.add_argument(
        "--bq-final-table",
        default=None,
        help=(
            "Final target table name under --bq-output-dataset for "
            "the MERGE step. Required when --bq-allow-final-merge "
            "is set."
        ),
    )
    biomass_bq.add_argument(
        "--bq-control-dataset",
        default=None,
        help=(
            "BigQuery dataset under --bq-output-project for the "
            "biomass orchestration runs control table "
            "(cf_biomass_runs). Required when --bq-stage-table is "
            "set. Idempotent CREATE TABLE IF NOT EXISTS is run on "
            "each writeback invocation. **No watermark table** for "
            "biomass - the M20 design omits the watermark concept; "
            "see the run-summary M20 block for the rationale."
        ),
    )
    biomass_bq.add_argument(
        "--bq-allow-final-merge",
        action="store_true",
        help=(
            "Explicit operator opt-in to MERGE the staged rows into "
            "the final target table. Default is stage-only; final-"
            "table mutation never happens without this flag."
        ),
    )
    biomass_bq.add_argument(
        "--bq-merge-key",
        default="primary_key",
        help=(
            "Column name to MERGE on (default: 'primary_key'). The "
            "stage table must carry this column non-null and unique "
            "per row, or stage validation aborts the merge."
        ),
    )
    biomass_bq.add_argument(
        "--bq-run-id",
        default=None,
        help=(
            "Optional run identifier recorded in the runs control "
            "table. Defaults to a generated 'local-<UTC-stamp>-<pid>' "
            "string so operator runs are namespaced away from any "
            "future scheduled-cloud run identifiers."
        ),
    )

    return parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _json_safe(obj: Any) -> Any:
    """Recursively coerce diagnostics/config values to JSON-safe scalars.

    Diagnostics payloads contain numpy scalars, tuples, dataclasses, and
    nested dicts. Standard ``json.dumps`` rejects most of those. This
    helper does a best-effort conversion without losing information.
    """
    if obj is None or isinstance(obj, (bool, int, float, str)):
        if isinstance(obj, float) and not math.isfinite(obj):
            return None if math.isnan(obj) else str(obj)
        return obj
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return _json_safe(dataclasses.asdict(obj))
    if hasattr(obj, "item") and callable(obj.item):
        try:
            return _json_safe(obj.item())
        except Exception:  # pragma: no cover - defensive
            return str(obj)
    if isinstance(obj, Path):
        return str(obj)
    return str(obj)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(_json_safe(payload), fh, indent=2, sort_keys=True)
        fh.write("\n")


def _write_table(path: Path, df: Any) -> str:
    """Write the processed table to ``path`` using the format implied
    by its extension. Returns the format name written (``csv`` or
    ``parquet``).

    Raises a ``ValueError`` (treated as validation failure by the
    caller) when the extension is unsupported.
    """
    suffix = path.suffix.lower()
    path.parent.mkdir(parents=True, exist_ok=True)
    if suffix == ".csv":
        df.to_csv(path, index=False)
        return "csv"
    if suffix in (".parquet", ".pq"):
        df.to_parquet(path, index=False)
        return "parquet"
    raise ValueError(
        f"Unsupported --output-table extension {suffix!r}. "
        "Use .csv or .parquet."
    )


def _package_versions() -> dict[str, Optional[str]]:
    """Best-effort version capture for the run metadata JSON."""
    versions: dict[str, Optional[str]] = {
        "python": platform.python_version(),
        "miaproc": None,
        "pandas": None,
        "numpy": None,
        "hesseflux": None,
        "rpy2": None,
        "google-cloud-bigquery": None,
    }
    try:
        from importlib.metadata import PackageNotFoundError, version

        for name in (
            "miaproc",
            "pandas",
            "numpy",
            "hesseflux",
            "rpy2",
            "google-cloud-bigquery",
        ):
            try:
                versions[name] = version(name)
            except PackageNotFoundError:
                versions[name] = None
            except Exception:
                versions[name] = None
    except Exception:  # pragma: no cover - defensive
        pass
    return versions


def _check_output_extension(path: Path) -> None:
    if path.suffix.lower() not in (".csv", ".parquet", ".pq"):
        raise ValueError(
            f"Unsupported --output-table extension {path.suffix!r}. "
            "Use .csv or .parquet."
        )


# ---------------------------------------------------------------------------
# M24: All-data grouped CLI execution helpers
# ---------------------------------------------------------------------------


def _sanitize_for_filename(value: Any) -> str:
    """Conservative sanitization for using a category value as a filename.

    Lowercases, replaces any non-alphanumeric character with ``_``, and
    collapses runs of underscores. Preserves the original category
    value separately in run JSON so the on-disk name is never the
    authoritative identifier. Empty results fall back to ``"group"``.
    """
    text = str(value)
    out_chars: list[str] = []
    for ch in text:
        if ch.isalnum():
            out_chars.append(ch.lower())
        else:
            out_chars.append("_")
    sanitized = "".join(out_chars).strip("_")
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    return sanitized or "group"


def _resolve_groups_dir(
    output_table: Path, groups_dir_arg: Optional[Path]
) -> Path:
    """Resolve the per-group artifact directory for grouped CLI runs.

    Defaults to ``<output-table-parent>/<output-table-stem>__groups``
    so per-category artifacts sit alongside the stacked output table
    in a predictable, easy-to-clean location. Operators can override
    via ``--output-groups-dir``.
    """
    if groups_dir_arg is not None:
        return groups_dir_arg
    parent = output_table.parent if output_table.parent != Path("") else Path(".")
    return parent / f"{output_table.stem}__groups"


def _per_group_table_path(
    groups_dir: Path,
    *,
    output_table: Path,
    category: Any,
    role: str,
) -> Path:
    """Return the per-category table path under ``groups_dir``.

    ``role`` is a short kind label (``"silver"``, ``"gold"``,
    ``"processed"``) that disambiguates the per-group artefact when
    the same group directory holds more than one stage's output.
    """
    suffix = output_table.suffix.lower()
    return groups_dir / f"{_sanitize_for_filename(category)}__{role}{suffix}"


def _per_group_diagnostics_path(
    groups_dir: Path, *, category: Any
) -> Path:
    return groups_dir / f"{_sanitize_for_filename(category)}__diag.json"


def _validate_group_column(
    df: Any, col: Optional[str], *, side: str
) -> None:
    """Raise a clear ValueError when ``col`` is not present in ``df``."""
    if col is None:
        return
    if col not in df.columns:
        raise ValueError(
            f"--group-column {col!r} not present in {side} input "
            f"(columns: {sorted(map(str, df.columns))})."
        )


def _iter_categories(df: Any, col: str) -> tuple[list[Any], int]:
    """Return the deterministic sorted list of non-null categories
    in ``df[col]`` and the count of null-category rows skipped.

    Empty input frames or columns with only null values produce an
    empty category list and a non-zero null-count; the caller is
    responsible for surfacing that as an empty-output-but-not-error
    condition or as a runtime failure depending on context.
    """
    series = df[col]
    null_count = int(series.isna().sum())
    categories = sorted(
        v for v in series.dropna().unique().tolist()
    )
    return categories, null_count


# ---------------------------------------------------------------------------
# Engine dispatch
# ---------------------------------------------------------------------------


def _build_hesseflux_config(
    *,
    reco_fit_mode: str,
    lt_min_night_samples: int,
):
    """Construct the closure-track ``HessefluxConfig`` (Decision 011)."""
    from miaproc.eddy import HessefluxConfig

    return HessefluxConfig(
        ustar_mode="dynamic",
        ustar_probs=(0.05, 0.5, 0.95),
        ustar_scenario="U50",
        ustar_min_night_samples=500,
        ustar_temp_bins=4,
        ustar_bins=20,
        ustar_plateau_fraction=0.95,
        partition_method="lasslop",
        swthr=20.0,
        nogppnight=False,
        reco_fit_mode=reco_fit_mode,
        lt_min_night_samples=lt_min_night_samples,
    )


def _run_preflight_or_exit(repo_root: Path) -> dict[str, Any]:
    """Run the project-scoped R preflight; return its dict.

    Aborts the process with ``PREFLIGHT_NOT_APPROVED_EXIT`` (2) when
    the discovered R runtime is anything less than project-scoped
    approved. Decision 010 / risk R11: no silent fallback to a global
    R installation.
    """
    from miaproc.eddy import (
        RRuntimePreflightPolicy,
        preflight_reddyproc_r_environment,
        render_r_preflight_report,
    )

    logger.info(
        "Preflight: repo_root=%s R_HOME=%r",
        repo_root,
        os.environ.get("R_HOME"),
    )
    policy = RRuntimePreflightPolicy(repo_root=str(repo_root))
    result = preflight_reddyproc_r_environment(policy=policy)
    sys.stdout.write(render_r_preflight_report(result) + "\n")

    approval_source = str(result.approval_source or "")
    project_scoped = (
        result.status == "ok"
        and result.approved
        and approval_source.startswith("project-scoped")
    )
    if not project_scoped:
        logger.error(
            "Preflight not project-scoped approved (status=%s, approved=%s, "
            "approval_source=%r). Refusing to run reddyproc-reference.",
            result.status,
            result.approved,
            approval_source,
        )
        sys.exit(PREFLIGHT_NOT_APPROVED_EXIT)
    logger.info("Preflight approved (project-scoped).")
    return result.to_dict()


def _dispatch_engine(
    engine: str,
    df_stage1: Any,
    args: argparse.Namespace,
) -> tuple[Any, dict[str, Any]]:
    """Run ``postproc`` for the chosen engine and return ``(df, config_record)``.

    ``config_record`` captures the resolved key configuration for the
    run-metadata JSON; it is intentionally compact and stable.
    """
    from miaproc.eddy import postproc

    if engine == "hesseflux-native":
        cfg = _build_hesseflux_config(
            reco_fit_mode="native",
            lt_min_night_samples=args.lt_min_night_samples,
        )
        out = postproc(df_stage1, engine="hesseflux", hesseflux_config=cfg)
        return out, {
            "backend": "hesseflux",
            "ustar_mode": cfg.ustar_mode,
            "ustar_scenario": cfg.ustar_scenario,
            "partition_method": cfg.partition_method,
            "swthr": cfg.swthr,
            "nogppnight": cfg.nogppnight,
            "reco_fit_mode": cfg.reco_fit_mode,
        }

    if engine == "hesseflux-ltwrapper":
        cfg = _build_hesseflux_config(
            reco_fit_mode="lt_reddyproc_aligned",
            lt_min_night_samples=args.lt_min_night_samples,
        )
        out = postproc(df_stage1, engine="hesseflux", hesseflux_config=cfg)
        return out, {
            "backend": "hesseflux",
            "ustar_mode": cfg.ustar_mode,
            "ustar_scenario": cfg.ustar_scenario,
            "partition_method": cfg.partition_method,
            "swthr": cfg.swthr,
            "nogppnight": cfg.nogppnight,
            "reco_fit_mode": cfg.reco_fit_mode,
            "lt_min_night_samples": cfg.lt_min_night_samples,
        }

    if engine == "reddyproc-reference":
        from miaproc.eddy import ReddyProcConfig

        cfg = ReddyProcConfig(
            site_name=args.site_name,
            latitude=args.latitude,
            longitude=args.longitude,
            timezone_hour=args.timezone_hour,
            local_tz=args.local_tz,
            ustar_n_sample=200,
            ustar_probs=(0.05, 0.5, 0.95),
            ustar_scenario="U50",
        )
        out = postproc(df_stage1, engine="reddyproc-rpy2", reddyproc_config=cfg)
        return out, {
            "backend": "reddyproc-rpy2",
            "site_name": cfg.site_name,
            "latitude": cfg.latitude,
            "longitude": cfg.longitude,
            "timezone_hour": cfg.timezone_hour,
            "local_tz": cfg.local_tz,
            "ustar_scenario": cfg.ustar_scenario,
        }

    raise ValueError(f"Unknown engine {engine!r}")


# ---------------------------------------------------------------------------
# File-based run command (M6 contract; unchanged)
# ---------------------------------------------------------------------------


def _validate_args(args: argparse.Namespace) -> None:
    """Validate cross-cutting CLI arguments. Raises ``ValueError`` on failure."""
    if not args.flux_dir.exists():
        raise ValueError(f"--flux-dir does not exist: {args.flux_dir}")
    if not args.biomet_dir.exists():
        raise ValueError(f"--biomet-dir does not exist: {args.biomet_dir}")
    if args.engine == "reddyproc-reference" and args.repo_root is None:
        raise ValueError(
            "--repo-root is required for --engine reddyproc-reference "
            "(project-scoped preflight gate, Decision 010)."
        )
    _check_output_extension(args.output_table)


def _stack_dataframes(frames: list[Any]) -> Any:
    """Concatenate per-group output frames in deterministic call order.

    Preserves ``df.attrs`` from the first non-empty frame so backend
    diagnostics on a single-group run are not lost when grouping is
    inactive. For multi-group runs the per-group diagnostics are
    written to the per-group artefact directory; the aggregated
    diagnostics JSON exposes them under ``groups``.
    """
    import pandas as pd

    if not frames:
        return pd.DataFrame()
    saved_attrs: dict[str, Any] = {}
    for f in frames:
        attrs = getattr(f, "attrs", None) or {}
        if attrs:
            saved_attrs = dict(attrs)
            break
    stacked = pd.concat(frames, ignore_index=True, copy=False)
    if saved_attrs:
        stacked.attrs.update(saved_attrs)
    return stacked


def _run_command(args: argparse.Namespace) -> int:
    started_at = _utc_now_iso()

    try:
        _validate_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    preflight_record: Optional[dict[str, Any]] = None
    if args.engine == "reddyproc-reference":
        # Preflight first; aborts the process with exit 2 if not approved.
        preflight_record = _run_preflight_or_exit(args.repo_root)

    group_column: Optional[str] = args.group_column
    groups_dir = (
        _resolve_groups_dir(args.output_table, args.output_groups_dir)
        if group_column is not None
        else None
    )

    per_group_records: list[dict[str, Any]] = []
    null_category_rows = 0
    stage1_total = 0
    out_frames: list[Any] = []
    config_record: dict[str, Any] = {}

    try:
        from miaproc.eddy import (
            load_stage1,
            read_and_combine_csv,
            stage1_from_raw_frames,
        )

        if group_column is None:
            logger.info(
                "Stage-1 load (ungrouped): flux=%s biomet=%s",
                args.flux_dir,
                args.biomet_dir,
            )
            df_stage1 = load_stage1(
                path_full_output=str(args.flux_dir),
                path_biomet=str(args.biomet_dir),
                skip_full_output=args.skip_flux,
                skip_biomet=args.skip_biomet,
                site_id=None,
                drop_rain_rows=False,
            )
            logger.info("Stage-1 rows: %d", len(df_stage1))
            stage1_total = int(len(df_stage1))
            logger.info("Running engine=%s ...", args.engine)
            out, config_record = _dispatch_engine(
                args.engine, df_stage1, args
            )
            out_frames.append(out)
        else:
            logger.info(
                "Stage-1 grouped run: flux=%s biomet=%s group_column=%s",
                args.flux_dir,
                args.biomet_dir,
                group_column,
            )
            full_output = read_and_combine_csv(
                Path(str(args.flux_dir)), skip_rows=args.skip_flux
            )
            biomet = read_and_combine_csv(
                Path(str(args.biomet_dir)), skip_rows=args.skip_biomet
            )
            _validate_group_column(full_output, group_column, side="flux")
            categories, null_category_rows = _iter_categories(
                full_output, group_column
            )
            if not categories:
                raise ValueError(
                    f"No non-null values for --group-column {group_column!r} "
                    f"in flux input."
                )
            assert groups_dir is not None
            groups_dir.mkdir(parents=True, exist_ok=True)
            for category in categories:
                logger.info("Group %s=%r: stage-1 + engine", group_column, category)
                flux_group = full_output.loc[
                    full_output[group_column] == category
                ].reset_index(drop=True)
                if group_column in biomet.columns:
                    biomet_group = biomet.loc[
                        biomet[group_column] == category
                    ].reset_index(drop=True)
                else:
                    biomet_group = biomet
                df_stage1_group = stage1_from_raw_frames(
                    flux_group,
                    biomet_group,
                    site_id=None,
                    drop_rain_rows=False,
                )
                stage1_total += int(len(df_stage1_group))
                out_group, config_record = _dispatch_engine(
                    args.engine, df_stage1_group, args
                )
                out_frames.append(out_group)
                # Per-group artefacts.
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="processed",
                )
                _write_table(per_group_table_path, out_group)
                per_group_diag_path = _per_group_diagnostics_path(
                    groups_dir, category=category
                )
                per_group_diag = (
                    out_group.attrs.get("miaproc_diagnostics") or {}
                )
                _write_json(per_group_diag_path, dict(per_group_diag))
                per_group_records.append(
                    {
                        "category_value": category,
                        "stage1_rows": int(len(df_stage1_group)),
                        "output_rows": int(len(out_group)),
                        "table_path": str(per_group_table_path),
                        "diagnostics_path": str(per_group_diag_path),
                    }
                )

        out = _stack_dataframes(out_frames)
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    # Write artifacts.
    try:
        table_format = _write_table(args.output_table, out)
        diagnostics_payload: dict[str, Any]
        if group_column is None:
            diagnostics_payload = dict(
                out.attrs.get("miaproc_diagnostics") or {}
            )
        else:
            diagnostics_payload = {
                "group_column": group_column,
                "groups": [
                    {
                        "category_value": rec["category_value"],
                        "diagnostics_path": rec["diagnostics_path"],
                    }
                    for rec in per_group_records
                ],
            }
        _write_json(args.output_diagnostics_json, diagnostics_payload)

        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "engine": args.engine,
            "config": config_record,
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "stage1": stage1_total,
                "output": int(len(out)),
                "null_category_rows_skipped": null_category_rows,
            },
            "inputs": {
                "mode": "file",
                "flux_dir": str(args.flux_dir),
                "biomet_dir": str(args.biomet_dir),
                "group_column": group_column,
                "skip_flux": int(args.skip_flux),
                "skip_biomet": int(args.skip_biomet),
            },
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "diagnostics_json": str(args.output_diagnostics_json),
                "run_json": str(args.output_run_json),
                "groups_dir": str(groups_dir) if groups_dir else None,
            },
            "groups": per_group_records,
            "versions": _package_versions(),
            "exit_code": SUCCESS_EXIT,
        }
        if preflight_record is not None:
            run_metadata["preflight"] = preflight_record
        _write_json(args.output_run_json, run_metadata)
    except Exception as exc:
        logger.error("Failed to write output artifacts: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Done. engine=%s groups=%d rows=%d table=%s diagnostics=%s run=%s exit=%d",
        args.engine,
        len(per_group_records) if group_column else 1,
        len(out),
        args.output_table,
        args.output_diagnostics_json,
        args.output_run_json,
        SUCCESS_EXIT,
    )
    return SUCCESS_EXIT


# ---------------------------------------------------------------------------
# BigQuery-native eddy run (M7)
# ---------------------------------------------------------------------------


def _validate_bigquery_args(args: argparse.Namespace) -> None:
    """Validate the ``eddy run-bigquery`` arguments."""
    if args.engine == "reddyproc-reference" and args.repo_root is None:
        raise ValueError(
            "--repo-root is required for --engine reddyproc-reference "
            "(project-scoped preflight gate, Decision 010)."
        )
    _check_output_extension(args.output_table)
    # Writeback flag-set validation: --bq-stage-table is the trigger.
    writeback_engaged = bool(args.bq_stage_table)
    if writeback_engaged:
        if not args.bq_output_project:
            raise ValueError(
                "--bq-output-project is required when --bq-stage-table is set."
            )
        if args.bq_output_project == args.bq_input_project:
            raise ValueError(
                "--bq-output-project must differ from --bq-input-project. "
                "Production input projects must remain read-only; route "
                "writes to a staging project."
            )
        if not args.bq_output_dataset:
            raise ValueError(
                "--bq-output-dataset is required when --bq-stage-table is set."
            )
        if not args.bq_control_dataset:
            raise ValueError(
                "--bq-control-dataset is required when --bq-stage-table "
                "is set (control tables for run records + watermark)."
            )
        if args.bq_allow_final_merge and not args.bq_final_table:
            raise ValueError(
                "--bq-allow-final-merge requires --bq-final-table to name "
                "the MERGE target."
            )
    else:
        # If writeback is not engaged, surfacing accidental partial
        # configuration is a validation failure rather than silent
        # ignore — operators should know writeback was not applied.
        downstream_only_flags = (
            ("--bq-output-project", args.bq_output_project),
            ("--bq-output-dataset", args.bq_output_dataset),
            ("--bq-final-table", args.bq_final_table),
            ("--bq-control-dataset", args.bq_control_dataset),
        )
        set_without_stage = [name for name, val in downstream_only_flags if val]
        if set_without_stage:
            raise ValueError(
                f"Writeback flags {set_without_stage} require "
                "--bq-stage-table. Stage-only writeback is the safe "
                "default; final-table merge additionally requires "
                "--bq-allow-final-merge."
            )
        if args.bq_allow_final_merge:
            raise ValueError(
                "--bq-allow-final-merge requires --bq-stage-table "
                "and --bq-final-table to be set."
            )


def _run_bigquery_command(args: argparse.Namespace) -> int:
    """Execute the BigQuery-native eddy run with M24 grouped semantics.

    Reads flux + biomet from BigQuery (no CLI-injected
    ``WHERE site_id = @site_id`` filter — see Decision M24), runs the
    in-memory stage-1 pipeline per category when ``--group-column`` is
    set or once over the whole read otherwise, dispatches the engine
    per group, and writes local artefacts. With ``--bq-stage-table``
    set, BigQuery writeback runs **once** against the stacked
    all-category output so cloud orchestration can use shared stage
    tables (e.g. ``cf_s2_gold_stage``). Final MERGE remains gated on
    ``--bq-allow-final-merge``; per-site watermarks then advance for
    every site present in the stacked output.
    """
    started_at = _utc_now_iso()

    try:
        _validate_bigquery_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    preflight_record: Optional[dict[str, Any]] = None
    if args.engine == "reddyproc-reference":
        preflight_record = _run_preflight_or_exit(args.repo_root)

    group_column: Optional[str] = args.group_column
    groups_dir = (
        _resolve_groups_dir(args.output_table, args.output_groups_dir)
        if group_column is not None
        else None
    )

    bq_inputs_record: dict[str, Any] = {}
    per_group_records: list[dict[str, Any]] = []
    null_category_rows = 0
    stage1_total = 0
    out_frames: list[Any] = []
    stage_frames: list[Any] = []
    config_record: dict[str, Any] = {}
    bq_result = None
    df_stage1 = None
    out = None
    try:
        from miaproc.eddy import (
            BigQueryEddyConfig,
            load_stage1_from_dataframes,
            read_bigquery_inputs,
        )

        bq_cfg = BigQueryEddyConfig(
            input_project=args.bq_input_project,
            input_dataset=args.bq_input_dataset,
            flux_table=args.bq_flux_table,
            biomet_table=args.bq_biomet_table,
            # M24: CLI never injects a single-site filter into the
            # BigQuery read.
            site_id=None,
            start_timestamp=args.bq_start_timestamp,
            end_timestamp=args.bq_end_timestamp,
            billing_project=args.bq_billing_project,
            bq_storage_api=not args.bq_no_storage_api,
        )
        logger.info(
            "BigQuery read (all-categories): project=%s dataset=%s flux=%s biomet=%s",
            bq_cfg.input_project,
            bq_cfg.input_dataset,
            bq_cfg.flux_table,
            bq_cfg.biomet_table,
        )
        bq_result = read_bigquery_inputs(bq_cfg)
        logger.info(
            "BigQuery rows: flux=%d biomet=%d",
            bq_result.flux_rows,
            bq_result.biomet_rows,
        )

        bq_inputs_record = {
            "mode": "bigquery",
            "input_project": bq_cfg.input_project,
            "input_dataset": bq_cfg.input_dataset,
            "flux_table": bq_cfg.flux_table,
            "biomet_table": bq_cfg.biomet_table,
            "billing_project": bq_cfg.billing_project_or_input(),
            "group_column": group_column,
            "start_timestamp": bq_cfg.start_timestamp,
            "end_timestamp": bq_cfg.end_timestamp,
            "bq_storage_api": bq_cfg.bq_storage_api,
            "flux_query": bq_result.flux_query,
            "biomet_query": bq_result.biomet_query,
            "query_parameters": dict(bq_result.query_parameters),
            "read_row_counts": {
                "flux": int(bq_result.flux_rows),
                "biomet": int(bq_result.biomet_rows),
            },
        }

        if group_column is None:
            df_stage1 = load_stage1_from_dataframes(
                flux_df=bq_result.flux_df,
                biomet_df=bq_result.biomet_df,
                site_id=None,
                drop_rain_rows=False,
            )
            logger.info("Stage-1 rows: %d", len(df_stage1))
            stage1_total = int(len(df_stage1))
            logger.info("Running engine=%s ...", args.engine)
            out, config_record = _dispatch_engine(
                args.engine, df_stage1, args
            )
            out_frames.append(out)
        else:
            _validate_group_column(
                bq_result.flux_df, group_column, side="bigquery flux"
            )
            categories, null_category_rows = _iter_categories(
                bq_result.flux_df, group_column
            )
            if not categories:
                raise ValueError(
                    f"No non-null values for --group-column "
                    f"{group_column!r} in BigQuery flux read."
                )
            assert groups_dir is not None
            groups_dir.mkdir(parents=True, exist_ok=True)
            for category in categories:
                logger.info(
                    "Group %s=%r: stage-1 + engine", group_column, category
                )
                flux_group = bq_result.flux_df.loc[
                    bq_result.flux_df[group_column] == category
                ].reset_index(drop=True)
                if group_column in bq_result.biomet_df.columns:
                    biomet_group = bq_result.biomet_df.loc[
                        bq_result.biomet_df[group_column] == category
                    ].reset_index(drop=True)
                else:
                    biomet_group = bq_result.biomet_df
                df_stage1_group = load_stage1_from_dataframes(
                    flux_df=flux_group,
                    biomet_df=biomet_group,
                    site_id=None,
                    drop_rain_rows=False,
                )
                stage1_total += int(len(df_stage1_group))
                out_group, config_record = _dispatch_engine(
                    args.engine, df_stage1_group, args
                )
                out_frames.append(out_group)
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="processed",
                )
                _write_table(per_group_table_path, out_group)
                per_group_diag_path = _per_group_diagnostics_path(
                    groups_dir, category=category
                )
                per_group_diag = (
                    out_group.attrs.get("miaproc_diagnostics") or {}
                )
                _write_json(per_group_diag_path, dict(per_group_diag))
                per_group_records.append(
                    {
                        "category_value": category,
                        "stage1_rows": int(len(df_stage1_group)),
                        "output_rows": int(len(out_group)),
                        "table_path": str(per_group_table_path),
                        "diagnostics_path": str(per_group_diag_path),
                    }
                )

        out = _stack_dataframes(out_frames)
        # Aggregate stage1 frame for downstream row-count metadata.
        # We keep the per-group-summed scalar; df_stage1 is unused
        # downstream once the stack exists.
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    # Write local-disk artifacts first (always done, regardless of
    # whether BigQuery writeback is engaged).
    try:
        table_format = _write_table(args.output_table, out)
        if group_column is None:
            diagnostics_payload = dict(
                out.attrs.get("miaproc_diagnostics") or {}
            )
        else:
            diagnostics_payload = {
                "group_column": group_column,
                "groups": [
                    {
                        "category_value": rec["category_value"],
                        "diagnostics_path": rec["diagnostics_path"],
                    }
                    for rec in per_group_records
                ],
            }
        _write_json(args.output_diagnostics_json, diagnostics_payload)
    except Exception as exc:
        logger.error("Failed to write output artifacts: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    # BigQuery writeback (M8/M24): engaged only when --bq-stage-table is
    # set. Stacks per-group stage frames into one stage payload (so
    # shared stage tables such as cf_s2_gold_stage are valid). Stage-
    # only is the default; final-table MERGE requires
    # --bq-allow-final-merge and advances per-site watermarks for
    # every stacked site.
    writeback_record: Optional[dict[str, Any]] = None
    writeback_engaged = bool(args.bq_stage_table)
    if writeback_engaged:
        try:
            from miaproc.eddy import (
                BigQueryWritebackConfig,
                prepare_stage_dataframe,
                read_final_table_columns,
                read_final_table_schema,
                run_writeback,
            )

            run_id = args.bq_run_id or _default_run_id()
            wb_cfg = BigQueryWritebackConfig(
                output_project=args.bq_output_project,
                output_dataset=args.bq_output_dataset,
                stage_table=args.bq_stage_table,
                control_dataset=args.bq_control_dataset,
                final_table=args.bq_final_table,
                allow_final_merge=bool(args.bq_allow_final_merge),
                run_id=run_id,
                site_id=None,
                billing_project=args.bq_output_project,
            )
            logger.info(
                "BigQuery writeback engaged: stage=%s final=%s control=%s "
                "allow_final_merge=%s run_id=%s groups=%d",
                wb_cfg.stage_table_fqn(),
                wb_cfg.final_table_fqn(),
                wb_cfg.runs_table_fqn(),
                wb_cfg.allow_final_merge,
                run_id,
                len(per_group_records) if group_column else 1,
            )
            run_payload_extras = {
                "engine": args.engine,
                "bq_input_project": args.bq_input_project,
                "bq_input_dataset": args.bq_input_dataset,
                "bq_flux_table": args.bq_flux_table,
                "bq_biomet_table": args.bq_biomet_table,
                "read_flux_rows": int(
                    bq_inputs_record["read_row_counts"]["flux"]
                ),
                "read_biomet_rows": int(
                    bq_inputs_record["read_row_counts"]["biomet"]
                ),
                "miaproc_version": _package_versions().get("miaproc"),
                "bigquery_client_version": _package_versions().get(
                    "google-cloud-bigquery"
                ),
            }
            target_columns = read_final_table_columns(wb_cfg)
            target_types = read_final_table_schema(wb_cfg)
            # Per-group stage-frame preparation, then concat once.
            if group_column is None:
                # Ungrouped path: synthesize a single stage-frame
                # using the unique flux site_id (if present) as the
                # stage-identity site_id; falling back to a
                # placeholder when the source carries no site_id.
                resolved_site = _infer_single_site_id(bq_result.flux_df)
                stage_df = prepare_stage_dataframe(
                    out,
                    site_id=resolved_site or "<unknown>",
                    source_flux_df=bq_result.flux_df,
                    target_columns=target_columns,
                    target_types=target_types,
                )
            else:
                for rec, frame in zip(per_group_records, out_frames):
                    cat = rec["category_value"]
                    flux_group = bq_result.flux_df.loc[
                        bq_result.flux_df[group_column] == cat
                    ].reset_index(drop=True)
                    stage_part = prepare_stage_dataframe(
                        frame,
                        site_id=str(cat),
                        source_flux_df=flux_group,
                        target_columns=target_columns,
                        target_types=target_types,
                    )
                    stage_frames.append(stage_part)
                stage_df = _stack_dataframes(stage_frames)
            wb_result = run_writeback(
                stage_df,
                wb_cfg,
                run_id=run_id,
                started_at=started_at,
                run_payload_extras=run_payload_extras,
            )
            writeback_record = wb_result.to_dict()
            logger.info(
                "BigQuery writeback done: status=%s stage_rows=%d "
                "merge_attempted=%s merge_authorized=%s "
                "watermark_advanced=%s watermark_values_by_site=%s",
                wb_result.status,
                wb_result.stage_rows,
                wb_result.merge_attempted,
                wb_result.merge_authorized,
                wb_result.watermark_advanced,
                wb_result.watermark_values_by_site,
            )
        except SystemExit:
            raise
        except Exception as exc:
            logger.error("BigQuery writeback failed: %s", exc)
            traceback.print_exc()
            wb_state = getattr(exc, "miaproc_writeback_state", {}) or {}
            failed_writeback_record = {
                "status": wb_state.get("status", "failed"),
                "error_text": str(exc),
                "merge_attempted": bool(
                    wb_state.get("merge_attempted", False)
                ),
                "merge_authorized": bool(
                    wb_state.get(
                        "merge_authorized", bool(args.bq_allow_final_merge)
                    )
                ),
                "stage_rows": int(wb_state.get("stage_rows", 0)),
            }
            try:
                _write_run_json_bigquery(
                    args=args,
                    started_at=started_at,
                    bq_inputs_record=bq_inputs_record,
                    out=out,
                    stage1_rows=stage1_total,
                    config_record=config_record,
                    table_format=table_format,
                    preflight_record=preflight_record,
                    writeback_record=failed_writeback_record,
                    per_group_records=per_group_records,
                    group_column=group_column,
                    null_category_rows=null_category_rows,
                    groups_dir=groups_dir,
                    exit_code=RUNTIME_EXIT,
                )
            except Exception:
                pass
            return RUNTIME_EXIT

    # Run metadata JSON (always written, includes writeback record when engaged).
    try:
        _write_run_json_bigquery(
            args=args,
            started_at=started_at,
            bq_inputs_record=bq_inputs_record,
            out=out,
            stage1_rows=stage1_total,
            config_record=config_record,
            table_format=table_format,
            preflight_record=preflight_record,
            writeback_record=writeback_record,
            per_group_records=per_group_records,
            group_column=group_column,
            null_category_rows=null_category_rows,
            groups_dir=groups_dir,
            exit_code=SUCCESS_EXIT,
        )
    except Exception as exc:
        logger.error("Failed to write run metadata JSON: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Done. engine=%s mode=bigquery groups=%d rows=%d table=%s "
        "diagnostics=%s run=%s writeback=%s exit=%d",
        args.engine,
        len(per_group_records) if group_column else 1,
        len(out),
        args.output_table,
        args.output_diagnostics_json,
        args.output_run_json,
        "engaged" if writeback_engaged else "skipped",
        SUCCESS_EXIT,
    )
    return SUCCESS_EXIT


def _infer_single_site_id(flux_df: Any) -> Optional[str]:
    """Return the unique ``site_id`` value present in ``flux_df``.

    Returns ``None`` when the source has no ``site_id`` column or
    multiple distinct values; the caller is responsible for
    substituting a stable placeholder so the stage-identity columns
    (M10 contract) are still constructable.
    """
    if flux_df is None or "site_id" not in getattr(
        flux_df, "columns", []
    ):
        return None
    unique = sorted(
        str(v) for v in flux_df["site_id"].dropna().unique().tolist()
    )
    if len(unique) == 1:
        return unique[0]
    return None


def _default_run_id() -> str:
    """Generate a namespaced run id for operator-driven local runs."""
    import os
    from datetime import datetime, timezone

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"local-{stamp}-{os.getpid()}"


def _prepare_processed_for_stage(df: Any, *, site_id: str) -> Any:
    """Materialize the BigQuery stage identity columns on the processed output.

    The M6 backend contract returns the 13-column scientific output
    keyed on ``DateTime`` (regularized 30-minute grid). The M8
    BigQuery writeback layer needs three additional identity columns
    to satisfy the operational merge identity (guide 002 §11.3) and
    the stage validation contract:

    - ``site_id``: constant supplied by the caller as the
      stage-identity site label (the CLI passes the per-group
      category under M24; legacy single-site callers may pass an
      explicit value).
    - ``timestamp``: the regularized ``DateTime`` re-exposed under
      the BigQuery source-table name. Kept as a tz-aware
      ``datetime64[ns, UTC]`` so BigQuery loads it as ``TIMESTAMP``.
    - ``primary_key``: deterministic surrogate
      ``"<site_id>|<iso_utc_timestamp>"``. This makes ``primary_key``
      uniqueness equivalent to ``(site_id, timestamp)`` uniqueness
      by construction, which is the merge identity the writeback
      validation SQL enforces.

    The original ``DateTime`` column is preserved so downstream
    consumers that still read by it are unaffected. Returns a new
    DataFrame; the caller's frame is not mutated.
    """
    import pandas as pd

    out = df.copy()
    if "DateTime" not in out.columns:
        raise ValueError(
            "_prepare_processed_for_stage: processed output is missing "
            "the 'DateTime' column required for stage-identity derivation."
        )
    dt = pd.to_datetime(out["DateTime"], utc=True, errors="coerce")
    out["timestamp"] = dt
    out["site_id"] = site_id
    iso = dt.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    out["primary_key"] = site_id + "|" + iso
    # Order identity columns first for human readability of the staged table.
    identity = ["primary_key", "site_id", "timestamp"]
    rest = [c for c in out.columns if c not in identity]
    return out[identity + rest]


# ---------------------------------------------------------------------------
# M29 stage-payload dry-run helpers (silver + gold)
# ---------------------------------------------------------------------------


def _unique_source_columns(df: Any) -> list[str]:
    """Return the unique column names of ``df`` in source order.

    Used by the M29 dry-run metadata builder to compare the input
    (bronze flux for silver, silver for gold) against the resolved
    stage payload. Operates on column names only; the helper does not
    raise on duplicate names because the writeback-side
    ``validate_source_columns_unique`` /
    ``ensure_unique_stage_columns`` paths already enforce uniqueness
    where it matters. Returns names in first-seen order so the
    metadata stays deterministic.
    """
    seen: set[str] = set()
    out: list[str] = []
    columns_attr = getattr(df, "columns", None)
    if columns_attr is None:
        return out
    for c in list(columns_attr):
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _stage1_alias_map() -> dict[str, str]:
    """Return the raw-bronze -> source-truth-silver column alias map (M32).

    Stage 1 still uses backend/canonical names internally (``NEE``,
    ``Tair``, ``USTAR``, ``QC_NEE``, ``Rg``, ``P_RAIN``, ``rH``), but
    the M32 silver payload contract renames those to source-truth
    final names (``co2_flux``, ``air_temperature_c``, ``u_star``,
    ``qc_co2_flux``, ``SWIN_1_1_1``, ``P_RAIN_1_1_1``, ``RH_1_1_1``)
    before any writeback. Only the unit-transformed inherited
    variables and the legacy ``u.`` legacy flux name require an
    explicit alias under the M32 contract; the rest survive into the
    silver payload under their bronze names and are detected by exact
    match.

    Returned as a fresh dict so callers can mutate it locally without
    poisoning the package state.
    """
    from miaproc.eddy.constants import SILVER_BRONZE_TO_FINAL_ALIASES

    return dict(SILVER_BRONZE_TO_FINAL_ALIASES)


SILVER_STAGE1_INPUT_ALIASES: dict[str, str] = _stage1_alias_map()


# M32A: alias map used by the gold-side dry-run preservation check.
# The gold path runs ``prepare_stage_dataframe`` which (under the M32
# redundant-passthrough rule) drops the internal ``DateTime`` column
# in favor of the source-truth ``timestamp``. When the silver input
# still carries ``DateTime`` (legacy fixtures or pre-M32A silver
# tables), this alias map lets the dry-run check resolve it to
# ``timestamp`` instead of reporting a spurious missing column.
# Pure-source-truth silver inputs (``timestamp`` only) match by
# exact name and do not need an alias entry.
GOLD_SILVER_INPUT_ALIASES: dict[str, str] = {"DateTime": "timestamp"}


def _build_stage_payload_dry_run_metadata(
    payload_df: Any,
    *,
    stage: str,
    command: str,
    payload_path: Path,
    input_df: Any,
    collision_actions: list[dict[str, Any]],
    would_write: Optional[dict[str, Any]] = None,
    input_alias_map: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Build the JSON-safe metadata dict for the M29 dry-run artifact.

    ``stage`` is ``"silver"`` or ``"gold"``. ``input_df`` is the
    source frame the payload was derived from (bronze flux for
    silver; silver for gold) and is used to compute the
    ``preserved_input_columns`` / ``missing_input_columns`` /
    ``appended_payload_columns`` triples. ``collision_actions`` is
    the M28 action list (already returned by
    ``prepare_silver_stage_payload`` for silver and attached to
    ``df.attrs[COLUMN_COLLISION_ATTRS_KEY]`` for gold).

    ``would_write`` is included as-is when the operator supplied
    writeback flags alongside the dry-run flag, so the artifact
    records what BigQuery resource set the real writeback would have
    touched. ``None`` collapses to ``{}`` in the output so the
    artifact shape stays stable.

    ``input_alias_map`` (M31) maps raw bronze column names to their
    canonical silver names. When a raw input column is absent from
    the payload but its canonical alias is present, the column is
    treated as preserved (preserved_via_alias) rather than missing,
    and the resolution is recorded under
    ``input_column_payload_aliases``. ``None`` and ``{}`` disable
    aliasing (used by the gold dry-run, whose ``input_df`` is silver
    and already in canonical names). Strictness is preserved: a
    bronze column whose canonical alias is also absent still counts
    as missing and still raises the dry-run guard.

    Raises ``ValueError`` (callers translate to ``RUNTIME_EXIT``) if
    any unique input column truly fails to make it into the payload
    (neither under its source name nor under a configured alias) —
    that would be a silent regression of the M28 preservation
    contract and the operator should see it loudly rather than via a
    misleading JSON artifact.

    M31 also tightens the columns_unique invariant: ``columns_unique``
    is true only when the payload has no case-insensitive collisions
    on BigQuery field keys; ``duplicate_columns`` lists the
    case-insensitive duplicates that would have been rejected by
    ``load_table_from_dataframe``.
    """
    from miaproc.eddy.bigquery_writeback import bigquery_field_key

    columns = [str(c) for c in payload_df.columns]
    dtypes = [str(payload_df[c].dtype) for c in payload_df.columns]
    keys = [bigquery_field_key(c) for c in columns]
    duplicate_columns = sorted(
        {columns[i] for i, k in enumerate(keys) if keys.count(k) > 1}
    )
    columns_unique = len(set(keys)) == len(keys)
    identity_present = {
        ident: ident in columns
        for ident in ("primary_key", "site_id", "timestamp")
    }

    unique_inputs = _unique_source_columns(input_df)
    payload_set = set(columns)
    alias_map: dict[str, str] = dict(input_alias_map or {})

    # M31: humidity canonicalization (``RH`` -> ``rH``) happens inside
    # ``ensure_unique_stage_columns`` and is recorded as a
    # ``renamed_to_canonical_humidity`` action. Treat those renames as
    # honest aliases too so a bronze ``RH`` that was canonicalized in
    # the payload is reported as preserved-via-alias rather than as a
    # spurious missing column. Configured stage1 aliases win over
    # canonicalization-derived aliases.
    for action in collision_actions or []:
        if action.get("action") != "renamed_to_canonical_humidity":
            continue
        source_name = action.get("column")
        target_name = action.get("renamed_to")
        if (
            isinstance(source_name, str)
            and isinstance(target_name, str)
            and source_name not in alias_map
        ):
            alias_map[source_name] = target_name

    preserved: list[str] = []
    missing: list[str] = []
    alias_resolutions: dict[str, str] = {}
    for c in unique_inputs:
        if c in payload_set:
            preserved.append(c)
            continue
        aliased = alias_map.get(c)
        if aliased is not None and aliased in payload_set:
            preserved.append(c)
            alias_resolutions[c] = aliased
            continue
        missing.append(c)

    input_or_alias = set(unique_inputs) | set(alias_resolutions.values())
    appended = sorted(c for c in payload_set if c not in input_or_alias)

    if missing:
        raise ValueError(
            "stage payload dry-run: unique input columns missing from "
            f"the {stage} payload: {missing}. Refusing to write a "
            "misleading dry-run artifact; this would mean the "
            "bronze/silver preservation contract regressed."
        )

    return {
        "dry_run": True,
        "stage": stage,
        "command": command,
        "payload_path": str(payload_path),
        "payload_format": "csv",
        "row_count": int(len(payload_df)),
        "column_count": int(len(columns)),
        "columns": columns,
        "dtypes": dtypes,
        "columns_unique": columns_unique,
        "duplicate_columns": duplicate_columns,
        "identity_columns_present": identity_present,
        "column_collision_actions": list(collision_actions or []),
        "preserved_input_columns": preserved,
        "missing_input_columns": missing,
        "appended_payload_columns": appended,
        "input_column_payload_aliases": dict(alias_resolutions),
        "bigquery_write_attempted": False,
        "validation_sql_attempted": False,
        "merge_attempted": False,
        "watermark_advanced": False,
        "would_write": dict(would_write or {}),
    }


def _write_stage_payload_dry_run_artifacts(
    payload_df: Any,
    *,
    stage: str,
    command: str,
    dry_run_dir: Path,
    input_df: Any,
    collision_actions: list[dict[str, Any]],
    would_write: Optional[dict[str, Any]] = None,
    input_alias_map: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Write ``stage_payload.csv`` + ``stage_payload_metadata.json``.

    Creates ``dry_run_dir`` with parents as needed and overwrites the
    deterministic artifact names on rerun. Returns the dry-run
    ``writeback`` record the caller will embed in the command's
    run-metadata JSON. The record carries:

    - ``status = "stage_payload_dry_run_succeeded"``
    - ``stage_rows`` (the staged payload row count)
    - ``stage_payload_columns_unique`` (the M28 invariant)
    - ``column_collision_actions`` (M28 resolution log)
    - ``bigquery_write_attempted``, ``merge_attempted``,
      ``merge_authorized``, ``watermark_advanced`` all ``False``
    - ``payload_artifacts`` pointing to the CSV + metadata paths

    Any IO failure propagates so the caller can return
    ``RUNTIME_EXIT`` and avoid claiming success.
    """
    dry_run_dir.mkdir(parents=True, exist_ok=True)
    payload_path = dry_run_dir / "stage_payload.csv"
    metadata_path = dry_run_dir / "stage_payload_metadata.json"

    payload_df.to_csv(payload_path, index=False)

    metadata = _build_stage_payload_dry_run_metadata(
        payload_df,
        stage=stage,
        command=command,
        payload_path=payload_path,
        input_df=input_df,
        collision_actions=collision_actions,
        would_write=would_write,
        input_alias_map=input_alias_map,
    )
    _write_json(metadata_path, metadata)

    return {
        "status": "stage_payload_dry_run_succeeded",
        "stage_rows": int(len(payload_df)),
        "stage_payload_columns_unique": bool(metadata["columns_unique"]),
        "column_collision_actions": list(collision_actions or []),
        "bigquery_write_attempted": False,
        "merge_attempted": False,
        "merge_authorized": False,
        "watermark_advanced": False,
        "payload_artifacts": {
            "stage_payload_csv": str(payload_path),
            "stage_payload_metadata_json": str(metadata_path),
        },
    }


def _silver_would_write(args: argparse.Namespace) -> Optional[dict[str, Any]]:
    """Capture writeback target metadata for a silver dry-run.

    Returns ``None`` when no writeback flags were supplied alongside
    the dry-run flag; otherwise returns the FQN-ish target descriptors
    so the metadata artifact tells a complete operator story.
    """
    if not any(
        getattr(args, name, None)
        for name in (
            "bq_stage_table",
            "bq_output_project",
            "bq_output_dataset",
            "bq_control_dataset",
        )
    ):
        return None
    return {
        "bq_output_project": args.bq_output_project,
        "bq_output_dataset": args.bq_output_dataset,
        "bq_stage_table": args.bq_stage_table,
        "bq_control_dataset": args.bq_control_dataset,
        "bq_final_table": None,
        "bq_allow_final_merge": False,
        "bq_run_id": args.bq_run_id,
    }


def _gold_would_write(args: argparse.Namespace) -> Optional[dict[str, Any]]:
    """Capture writeback target metadata for a gold dry-run.

    Mirrors :func:`_silver_would_write` but also surfaces the gold-only
    ``bq_final_table`` / ``bq_allow_final_merge`` opt-in so the
    operator can see exactly which final MERGE was authorized (and
    skipped) under dry-run.
    """
    if not any(
        getattr(args, name, None)
        for name in (
            "bq_stage_table",
            "bq_output_project",
            "bq_output_dataset",
            "bq_control_dataset",
            "bq_final_table",
            "bq_allow_final_merge",
        )
    ):
        return None
    return {
        "bq_output_project": args.bq_output_project,
        "bq_output_dataset": args.bq_output_dataset,
        "bq_stage_table": args.bq_stage_table,
        "bq_control_dataset": args.bq_control_dataset,
        "bq_final_table": args.bq_final_table,
        "bq_allow_final_merge": bool(args.bq_allow_final_merge),
        "bq_run_id": args.bq_run_id,
    }


def _write_run_json_bigquery(
    *,
    args: argparse.Namespace,
    started_at: str,
    bq_inputs_record: dict[str, Any],
    out: Any,
    stage1_rows: int,
    config_record: dict[str, Any],
    table_format: str,
    preflight_record: Optional[dict[str, Any]],
    writeback_record: Optional[dict[str, Any]],
    per_group_records: Optional[list[dict[str, Any]]] = None,
    group_column: Optional[str] = None,
    null_category_rows: int = 0,
    groups_dir: Optional[Path] = None,
    exit_code: int = SUCCESS_EXIT,
) -> None:
    """Materialize the run-metadata JSON for the BigQuery-mode CLI."""
    ended_at = _utc_now_iso()
    writeback_engaged = bool(args.bq_stage_table)
    run_metadata: dict[str, Any] = {
        "engine": args.engine,
        "config": config_record,
        "timestamps": {"started_at": started_at, "ended_at": ended_at},
        "row_counts": {
            "bigquery_flux": int(bq_inputs_record["read_row_counts"]["flux"]),
            "bigquery_biomet": int(
                bq_inputs_record["read_row_counts"]["biomet"]
            ),
            "stage1": int(stage1_rows),
            "output": int(len(out)),
            "null_category_rows_skipped": int(null_category_rows),
        },
        "inputs": bq_inputs_record,
        "outputs": {
            "table": str(args.output_table),
            "table_format": table_format,
            "diagnostics_json": str(args.output_diagnostics_json),
            "run_json": str(args.output_run_json),
            "bigquery_writeback": writeback_engaged,
            "groups_dir": str(groups_dir) if groups_dir else None,
        },
        "groups": list(per_group_records or []),
        "group_column": group_column,
        "writeback": writeback_record,
        "versions": _package_versions(),
        "exit_code": exit_code,
    }
    if preflight_record is not None:
        run_metadata["preflight"] = preflight_record
    _write_json(args.output_run_json, run_metadata)


# ---------------------------------------------------------------------------
# M14 silver/gold split
# ---------------------------------------------------------------------------


def _read_silver_table(path: Path) -> Any:
    """Read a silver-stage table from CSV or parquet for gold consumption.

    M32A: silver outputs under the source-truth contract carry
    ``timestamp`` (no internal ``DateTime``). CSV round-trips lose
    timezone metadata, so the helper parses whichever of
    ``timestamp`` / ``DateTime`` is present back to a tz-aware
    ``datetime64[ns, UTC]`` column. Legacy silver files that still
    carry ``DateTime`` continue to work.
    """
    import pandas as pd

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
        for col in ("timestamp", "DateTime"):
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col], utc=True, errors="coerce"
                )
        return df
    if suffix in (".parquet", ".pq"):
        return pd.read_parquet(path)
    raise ValueError(
        f"Unsupported --silver-table extension {suffix!r}. Use .csv or .parquet."
    )


def _attach_silver_columns_to_gold(
    gold_df: Any, silver_df: Any
) -> tuple[Any, list[str]]:
    """Left-join silver-only columns onto the gold output, keyed on DateTime.

    Gold's accepted backend output (the 13-column contract from
    ``backend_contract.md``) is preserved verbatim — no renames, no
    drops. Silver columns that the backend did not already produce
    are appended after the gold columns. ``df.attrs`` is preserved
    across the merge so backend diagnostics survive.

    M32A: silver may carry the source-truth ``timestamp`` column
    instead of the internal ``DateTime``. When that is the case, the
    helper synthesizes a temporary ``DateTime`` on the silver side
    from ``timestamp`` so the merge still keys on the canonical
    backend-side ``DateTime``. The silver-side ``timestamp`` survives
    as a normal silver-extra (it is not present in ``gold_df``), so
    the merged gold frame ends up carrying both the backend-side
    ``DateTime`` and the source-truth ``timestamp``; the M32
    redundant-passthrough rule inside
    :func:`miaproc.eddy.bigquery_writeback.prepare_stage_dataframe`
    drops the internal ``DateTime`` from the staged payload.

    Returns ``(merged_df, silver_only_columns_appended)``. Falls
    back to ``(gold_df, [])`` if neither ``DateTime`` nor
    ``timestamp`` can be coalesced into a shared join key, so a
    malformed silver table still produces a gold output.
    """
    import pandas as pd

    if "DateTime" not in gold_df.columns:
        return gold_df, []
    if "DateTime" in silver_df.columns:
        silver_for_merge = silver_df
    elif "timestamp" in silver_df.columns:
        silver_for_merge = silver_df.copy()
        silver_for_merge["DateTime"] = pd.to_datetime(
            silver_df["timestamp"], utc=True, errors="coerce"
        )
    else:
        return gold_df, []
    silver_extras = [
        c
        for c in silver_for_merge.columns
        if c not in gold_df.columns and c != "DateTime"
    ]
    if not silver_extras:
        return gold_df, []
    saved_attrs = dict(getattr(gold_df, "attrs", {}) or {})
    extras_df = silver_for_merge[["DateTime"] + silver_extras]
    merged = gold_df.merge(extras_df, on="DateTime", how="left")
    merged.attrs = saved_attrs
    return merged, silver_extras


def _validate_eddy_silver_args(args: argparse.Namespace) -> None:
    if not args.flux_dir.exists():
        raise ValueError(f"--flux-dir does not exist: {args.flux_dir}")
    if not args.biomet_dir.exists():
        raise ValueError(f"--biomet-dir does not exist: {args.biomet_dir}")
    _check_output_extension(args.output_table)


def _run_eddy_silver_command(args: argparse.Namespace) -> int:
    """Run the silver-stage eddy command (M14, M24)."""
    started_at = _utc_now_iso()
    try:
        _validate_eddy_silver_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    group_column: Optional[str] = args.group_column
    groups_dir = (
        _resolve_groups_dir(args.output_table, args.output_groups_dir)
        if group_column is not None
        else None
    )
    per_group_records: list[dict[str, Any]] = []
    null_category_rows = 0
    silver_frames: list[Any] = []

    try:
        from miaproc.eddy import (
            apply_silver_source_truth_rename,
            load_stage1,
            read_and_combine_csv,
            stage1_from_raw_frames,
        )

        if group_column is None:
            logger.info(
                "Silver stage-1 load (ungrouped): flux=%s biomet=%s",
                args.flux_dir,
                args.biomet_dir,
            )
            silver = load_stage1(
                path_full_output=str(args.flux_dir),
                path_biomet=str(args.biomet_dir),
                tz_in=args.tz_in,
                tz_out=args.tz_out,
                skip_full_output=args.skip_flux,
                skip_biomet=args.skip_biomet,
                site_id=None,
                drop_rain_rows=False,
            )
            silver = apply_silver_source_truth_rename(silver)
            silver_frames.append(silver)
        else:
            logger.info(
                "Silver grouped run: flux=%s biomet=%s group_column=%s",
                args.flux_dir,
                args.biomet_dir,
                group_column,
            )
            full_output = read_and_combine_csv(
                Path(str(args.flux_dir)), skip_rows=args.skip_flux
            )
            biomet = read_and_combine_csv(
                Path(str(args.biomet_dir)), skip_rows=args.skip_biomet
            )
            _validate_group_column(full_output, group_column, side="flux")
            categories, null_category_rows = _iter_categories(
                full_output, group_column
            )
            if not categories:
                raise ValueError(
                    f"No non-null values for --group-column "
                    f"{group_column!r} in flux input."
                )
            assert groups_dir is not None
            groups_dir.mkdir(parents=True, exist_ok=True)
            for category in categories:
                logger.info(
                    "Group %s=%r: silver stage-1", group_column, category
                )
                flux_group = full_output.loc[
                    full_output[group_column] == category
                ].reset_index(drop=True)
                if group_column in biomet.columns:
                    biomet_group = biomet.loc[
                        biomet[group_column] == category
                    ].reset_index(drop=True)
                else:
                    biomet_group = biomet
                silver_group = stage1_from_raw_frames(
                    flux_group,
                    biomet_group,
                    tz_in=args.tz_in,
                    tz_out=args.tz_out,
                    site_id=None,
                    drop_rain_rows=False,
                )
                silver_group = apply_silver_source_truth_rename(silver_group)
                silver_frames.append(silver_group)
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="silver",
                )
                _write_table(per_group_table_path, silver_group)
                per_group_records.append(
                    {
                        "category_value": category,
                        "silver_rows": int(len(silver_group)),
                        "table_path": str(per_group_table_path),
                    }
                )

        silver = _stack_dataframes(silver_frames)
        logger.info(
            "Silver rows: %d cols: %d", len(silver), len(silver.columns)
        )
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    try:
        table_format = _write_table(args.output_table, silver)
        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "stage": "silver",
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "silver": int(len(silver)),
                "null_category_rows_skipped": int(null_category_rows),
            },
            "column_counts": {"silver": int(len(silver.columns))},
            "silver_columns": [str(c) for c in silver.columns],
            "inputs": {
                "mode": "file",
                "flux_dir": str(args.flux_dir),
                "biomet_dir": str(args.biomet_dir),
                "group_column": group_column,
                "skip_flux": int(args.skip_flux),
                "skip_biomet": int(args.skip_biomet),
                "tz_in": args.tz_in,
                "tz_out": args.tz_out,
            },
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "run_json": str(args.output_run_json),
                "groups_dir": str(groups_dir) if groups_dir else None,
            },
            "groups": per_group_records,
            "group_column": group_column,
            "versions": _package_versions(),
            "exit_code": SUCCESS_EXIT,
        }
        _write_json(args.output_run_json, run_metadata)
    except Exception as exc:
        logger.error("Failed to write silver artifacts: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Silver done. groups=%d rows=%d cols=%d table=%s exit=%d",
        len(per_group_records) if group_column else 1,
        len(silver),
        len(silver.columns),
        args.output_table,
        SUCCESS_EXIT,
    )
    return SUCCESS_EXIT


def _validate_eddy_gold_args(args: argparse.Namespace) -> None:
    if not args.silver_table.exists():
        raise ValueError(f"--silver-table does not exist: {args.silver_table}")
    if args.silver_table.suffix.lower() not in (".csv", ".parquet", ".pq"):
        raise ValueError(
            f"--silver-table must be .csv or .parquet "
            f"(got {args.silver_table.suffix!r})."
        )
    if args.engine == "reddyproc-reference" and args.repo_root is None:
        raise ValueError(
            "--repo-root is required for --engine reddyproc-reference "
            "(project-scoped preflight gate, Decision 010)."
        )
    _check_output_extension(args.output_table)


def _run_eddy_gold_command(args: argparse.Namespace) -> int:
    """Run the gold-stage eddy command (M14, M24)."""
    started_at = _utc_now_iso()
    try:
        _validate_eddy_gold_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    preflight_record: Optional[dict[str, Any]] = None
    if args.engine == "reddyproc-reference":
        preflight_record = _run_preflight_or_exit(args.repo_root)

    group_column: Optional[str] = args.group_column
    groups_dir = (
        _resolve_groups_dir(args.output_table, args.output_groups_dir)
        if group_column is not None
        else None
    )
    per_group_records: list[dict[str, Any]] = []
    null_category_rows = 0
    gold_frames: list[Any] = []
    silver_extras_union: list[str] = []
    config_record: dict[str, Any] = {}
    silver_total_rows = 0
    silver_total_cols = 0

    try:
        from miaproc.eddy import silver_to_internal_calc_frame

        logger.info("Gold reading silver: %s", args.silver_table)
        silver = _read_silver_table(args.silver_table)
        silver_total_rows = int(len(silver))
        silver_total_cols = int(len(silver.columns))
        logger.info(
            "Silver loaded: rows=%d cols=%d",
            silver_total_rows,
            silver_total_cols,
        )

        if group_column is None:
            logger.info("Running gold engine=%s (ungrouped)...", args.engine)
            # M32: silver carries source-truth final names; reconstruct
            # the internal calc frame so the hesseflux / REddyProc
            # backend dispatcher (which expects NEE / Tair / USTAR /
            # QC_NEE / Rg / VPD / rH) runs unchanged. Silver remains
            # source-truth for the gold-side preservation contract.
            internal_silver = silver_to_internal_calc_frame(silver)
            gold, config_record = _dispatch_engine(
                args.engine, internal_silver, args
            )
            gold_with_silver, silver_extras_union = (
                _attach_silver_columns_to_gold(gold, silver)
            )
            gold_frames.append(gold_with_silver)
        else:
            _validate_group_column(silver, group_column, side="silver")
            categories, null_category_rows = _iter_categories(
                silver, group_column
            )
            if not categories:
                raise ValueError(
                    f"No non-null values for --group-column "
                    f"{group_column!r} in silver table."
                )
            assert groups_dir is not None
            groups_dir.mkdir(parents=True, exist_ok=True)
            extras_set: set[str] = set()
            for category in categories:
                logger.info(
                    "Group %s=%r: gold engine=%s",
                    group_column,
                    category,
                    args.engine,
                )
                silver_group = silver.loc[
                    silver[group_column] == category
                ].reset_index(drop=True)
                internal_silver_group = silver_to_internal_calc_frame(
                    silver_group
                )
                gold_group, config_record = _dispatch_engine(
                    args.engine, internal_silver_group, args
                )
                gold_with_silver_group, silver_extras_g = (
                    _attach_silver_columns_to_gold(gold_group, silver_group)
                )
                gold_frames.append(gold_with_silver_group)
                extras_set.update(silver_extras_g)
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="gold",
                )
                _write_table(per_group_table_path, gold_with_silver_group)
                per_group_diag_path = _per_group_diagnostics_path(
                    groups_dir, category=category
                )
                per_group_diag = (
                    getattr(gold_with_silver_group, "attrs", {}) or {}
                ).get("miaproc_diagnostics") or {}
                _write_json(per_group_diag_path, dict(per_group_diag))
                per_group_records.append(
                    {
                        "category_value": category,
                        "silver_input_rows": int(len(silver_group)),
                        "gold_output_rows": int(
                            len(gold_with_silver_group)
                        ),
                        "table_path": str(per_group_table_path),
                        "diagnostics_path": str(per_group_diag_path),
                    }
                )
            silver_extras_union = sorted(extras_set)

        gold_with_silver = _stack_dataframes(gold_frames)
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    try:
        table_format = _write_table(args.output_table, gold_with_silver)
        if group_column is None:
            diagnostics = (
                getattr(gold_with_silver, "attrs", {}) or {}
            ).get("miaproc_diagnostics") or {}
            _write_json(args.output_diagnostics_json, dict(diagnostics))
        else:
            _write_json(
                args.output_diagnostics_json,
                {
                    "group_column": group_column,
                    "groups": [
                        {
                            "category_value": rec["category_value"],
                            "diagnostics_path": rec["diagnostics_path"],
                        }
                        for rec in per_group_records
                    ],
                },
            )

        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "stage": "gold",
            "engine": args.engine,
            "config": config_record,
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "silver_input": silver_total_rows,
                "gold_output": int(len(gold_with_silver)),
                "null_category_rows_skipped": int(null_category_rows),
            },
            "column_counts": {
                "silver_input": silver_total_cols,
                "gold_output": int(len(gold_with_silver.columns)),
                "silver_only_appended": len(silver_extras_union),
            },
            "silver_columns_appended": [str(c) for c in silver_extras_union],
            "inputs": {
                "mode": "file",
                "silver_table": str(args.silver_table),
                "group_column": group_column,
            },
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "diagnostics_json": str(args.output_diagnostics_json),
                "run_json": str(args.output_run_json),
                "groups_dir": str(groups_dir) if groups_dir else None,
            },
            "groups": per_group_records,
            "group_column": group_column,
            "versions": _package_versions(),
            "exit_code": SUCCESS_EXIT,
        }
        if preflight_record is not None:
            run_metadata["preflight"] = preflight_record
        _write_json(args.output_run_json, run_metadata)
    except Exception as exc:
        logger.error("Failed to write gold artifacts: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Gold done. engine=%s groups=%d rows=%d cols=%d "
        "(silver-only-appended=%d) table=%s exit=%d",
        args.engine,
        len(per_group_records) if group_column else 1,
        len(gold_with_silver),
        len(gold_with_silver.columns),
        len(silver_extras_union),
        args.output_table,
        SUCCESS_EXIT,
    )
    return SUCCESS_EXIT


# ---------------------------------------------------------------------------
# M22 BigQuery silver/gold split
# ---------------------------------------------------------------------------


def _validate_eddy_bq_silver_args(args: argparse.Namespace) -> None:
    """Validate ``eddy run-bigquery-silver`` arguments."""
    _check_output_extension(args.output_table)
    writeback_engaged = bool(args.bq_stage_table)
    if writeback_engaged:
        if not args.bq_output_project:
            raise ValueError(
                "--bq-output-project is required when --bq-stage-table is set."
            )
        # M26: same-project input/output is allowed when bronze/source
        # has been mirrored or staged into the staging project. The
        # hard production-read-only invariant is enforced by
        # ``BigQueryWritebackConfig.forbidden_write_projects`` at the
        # package layer; the CLI does not additionally require
        # output != input for silver.
        if not args.bq_output_dataset:
            raise ValueError(
                "--bq-output-dataset is required when --bq-stage-table is set."
            )
        if not args.bq_control_dataset:
            raise ValueError(
                "--bq-control-dataset is required when --bq-stage-table "
                "is set (runs control table)."
            )
    else:
        downstream_only_flags = (
            ("--bq-output-project", args.bq_output_project),
            ("--bq-output-dataset", args.bq_output_dataset),
            ("--bq-control-dataset", args.bq_control_dataset),
        )
        set_without_stage = [name for name, val in downstream_only_flags if val]
        if set_without_stage:
            raise ValueError(
                f"Writeback flags {set_without_stage} require "
                "--bq-stage-table. M22 silver writeback is stage-only."
            )


def _run_eddy_bigquery_silver_command(args: argparse.Namespace) -> int:
    """Run the M22/M24 BigQuery silver command (bronze/source -> silver).

    Reads flux + biomet from BigQuery without an injected
    ``WHERE site_id = @site_id`` filter (M24); when
    ``--group-column`` is set the read is partitioned in Python and
    every category present is processed independently. Optionally
    stages the stacked all-category silver output to BigQuery once
    (stage-only by design — M22 keeps the silver writeback surface
    narrow and does not introduce a final-merge path).
    """
    started_at = _utc_now_iso()
    try:
        _validate_eddy_bq_silver_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    group_column: Optional[str] = args.group_column
    groups_dir = (
        _resolve_groups_dir(args.output_table, args.output_groups_dir)
        if group_column is not None
        else None
    )
    per_group_records: list[dict[str, Any]] = []
    null_category_rows = 0
    silver_frames: list[Any] = []

    bq_inputs_record: dict[str, Any] = {}
    bq_result = None
    try:
        from miaproc.eddy import (
            BigQueryEddyConfig,
            load_stage1_from_dataframes,
            read_bigquery_inputs,
            stage1_from_raw_frames,
        )

        bq_cfg = BigQueryEddyConfig(
            input_project=args.bq_input_project,
            input_dataset=args.bq_input_dataset,
            flux_table=args.bq_flux_table,
            biomet_table=args.bq_biomet_table,
            site_id=None,
            start_timestamp=args.bq_start_timestamp,
            end_timestamp=args.bq_end_timestamp,
            billing_project=args.bq_billing_project,
            bq_storage_api=not args.bq_no_storage_api,
        )
        logger.info(
            "Silver BigQuery read (all-categories): project=%s dataset=%s "
            "flux=%s biomet=%s",
            bq_cfg.input_project,
            bq_cfg.input_dataset,
            bq_cfg.flux_table,
            bq_cfg.biomet_table,
        )
        bq_result = read_bigquery_inputs(bq_cfg)
        logger.info(
            "Silver BigQuery rows: flux=%d biomet=%d",
            bq_result.flux_rows,
            bq_result.biomet_rows,
        )
        bq_inputs_record = {
            "mode": "bigquery",
            "input_project": bq_cfg.input_project,
            "input_dataset": bq_cfg.input_dataset,
            "flux_table": bq_cfg.flux_table,
            "biomet_table": bq_cfg.biomet_table,
            "billing_project": bq_cfg.billing_project_or_input(),
            "group_column": group_column,
            "start_timestamp": bq_cfg.start_timestamp,
            "end_timestamp": bq_cfg.end_timestamp,
            "bq_storage_api": bq_cfg.bq_storage_api,
            "flux_query": bq_result.flux_query,
            "biomet_query": bq_result.biomet_query,
            "query_parameters": dict(bq_result.query_parameters),
            "read_row_counts": {
                "flux": int(bq_result.flux_rows),
                "biomet": int(bq_result.biomet_rows),
            },
            "tz_in": args.tz_in,
            "tz_out": args.tz_out,
        }

        # M32: stage 1 still produces backend/internal aliases
        # (``NEE``, ``Tair``, ``USTAR``, ``QC_NEE``, ``Rg``,
        # ``P_RAIN``, ``rH``, ``VPD``); the silver-output boundary
        # renames them to source-truth final names (``co2_flux``,
        # ``air_temperature_c``, ``u_star``, ``qc_co2_flux``,
        # ``SWIN_1_1_1``, ``P_RAIN_1_1_1``, ``RH_1_1_1``,
        # ``VPD_hpa``) so the local silver artifact, the BigQuery
        # silver stage payload, and any downstream gold reader all
        # see the same source-facing column shape.
        from miaproc.eddy import apply_silver_source_truth_rename

        if group_column is None:
            silver = load_stage1_from_dataframes(
                flux_df=bq_result.flux_df,
                biomet_df=bq_result.biomet_df,
                tz_in=args.tz_in,
                tz_out=args.tz_out,
                site_id=None,
                drop_rain_rows=False,
            )
            silver = apply_silver_source_truth_rename(silver)
            silver_frames.append(silver)
        else:
            _validate_group_column(
                bq_result.flux_df, group_column, side="bigquery flux"
            )
            categories, null_category_rows = _iter_categories(
                bq_result.flux_df, group_column
            )
            if not categories:
                raise ValueError(
                    f"No non-null values for --group-column "
                    f"{group_column!r} in BigQuery flux read."
                )
            assert groups_dir is not None
            groups_dir.mkdir(parents=True, exist_ok=True)
            for category in categories:
                logger.info(
                    "Group %s=%r: silver stage-1", group_column, category
                )
                flux_group = bq_result.flux_df.loc[
                    bq_result.flux_df[group_column] == category
                ].reset_index(drop=True)
                if group_column in bq_result.biomet_df.columns:
                    biomet_group = bq_result.biomet_df.loc[
                        bq_result.biomet_df[group_column] == category
                    ].reset_index(drop=True)
                else:
                    biomet_group = bq_result.biomet_df
                silver_group = stage1_from_raw_frames(
                    flux_group,
                    biomet_group,
                    tz_in=args.tz_in,
                    tz_out=args.tz_out,
                    site_id=None,
                    drop_rain_rows=False,
                )
                silver_group = apply_silver_source_truth_rename(silver_group)
                silver_frames.append(silver_group)
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="silver",
                )
                _write_table(per_group_table_path, silver_group)
                per_group_records.append(
                    {
                        "category_value": category,
                        "silver_rows": int(len(silver_group)),
                        "table_path": str(per_group_table_path),
                    }
                )

        silver = _stack_dataframes(silver_frames)
        logger.info(
            "Silver rows: %d cols: %d", len(silver), len(silver.columns)
        )
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    try:
        table_format = _write_table(args.output_table, silver)
    except Exception as exc:
        logger.error("Failed to write silver output table: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    writeback_record: Optional[dict[str, Any]] = None
    writeback_engaged = bool(args.bq_stage_table)
    writeback_exit_code = SUCCESS_EXIT

    dry_run_dir: Optional[Path] = args.stage_payload_dry_run_dir
    if dry_run_dir is not None:
        # M29: build the exact stage payload the writeback path would
        # produce, then write local artifacts and skip every BigQuery
        # write (load_table_from_dataframe / validation SQL / MERGE /
        # watermark advance). Wins over --bq-stage-table per the M29
        # contract; ``would_write`` records what targets were named so
        # the dry-run artifact still tells a complete operator story.
        try:
            from miaproc.eddy import prepare_silver_stage_payload

            silver_collision_actions: list[dict[str, Any]] = []
            if group_column is None:
                resolved_site = _infer_single_site_id(bq_result.flux_df)
                stage_df, silver_collision_actions = (
                    prepare_silver_stage_payload(
                        silver,
                        site_id=resolved_site or "<unknown>",
                        source_flux_df=bq_result.flux_df,
                    )
                )
            else:
                stage_parts: list[Any] = []
                for rec, frame in zip(per_group_records, silver_frames):
                    cat = rec["category_value"]
                    flux_group = bq_result.flux_df.loc[
                        bq_result.flux_df[group_column] == cat
                    ].reset_index(drop=True)
                    part, part_actions = prepare_silver_stage_payload(
                        frame,
                        site_id=str(cat),
                        source_flux_df=flux_group,
                    )
                    stage_parts.append(part)
                    silver_collision_actions.extend(part_actions)
                stage_df = _stack_dataframes(stage_parts)
            writeback_record = _write_stage_payload_dry_run_artifacts(
                stage_df,
                stage="silver",
                command="eddy run-bigquery-silver",
                dry_run_dir=dry_run_dir,
                input_df=bq_result.flux_df,
                collision_actions=silver_collision_actions,
                would_write=_silver_would_write(args),
                input_alias_map=SILVER_STAGE1_INPUT_ALIASES,
            )
            logger.info(
                "Silver stage-payload dry-run: rows=%d cols=%d dir=%s "
                "collision_actions=%d (no BigQuery write, no MERGE, "
                "no watermark advance)",
                int(len(stage_df)),
                int(len(stage_df.columns)),
                dry_run_dir,
                len(silver_collision_actions),
            )
        except SystemExit:
            raise
        except Exception as exc:
            logger.error("Silver dry-run artifact write failed: %s", exc)
            traceback.print_exc()
            writeback_record = {
                "status": "stage_payload_dry_run_failed",
                "error_text": str(exc),
                "bigquery_write_attempted": False,
                "merge_attempted": False,
                "merge_authorized": False,
                "watermark_advanced": False,
                "stage_payload_columns_unique": False,
                "column_collision_actions": [],
                "payload_artifacts": {
                    "stage_payload_csv": None,
                    "stage_payload_metadata_json": None,
                },
            }
            writeback_exit_code = RUNTIME_EXIT
    elif writeback_engaged:
        try:
            from miaproc.eddy import (
                BigQueryWritebackConfig,
                prepare_silver_stage_payload,
                run_writeback,
            )

            run_id = args.bq_run_id or _default_run_id()
            wb_cfg = BigQueryWritebackConfig(
                output_project=args.bq_output_project,
                output_dataset=args.bq_output_dataset,
                stage_table=args.bq_stage_table,
                control_dataset=args.bq_control_dataset,
                final_table=None,
                allow_final_merge=False,
                run_id=run_id,
                site_id=None,
                billing_project=args.bq_output_project,
            )
            logger.info(
                "Silver BigQuery writeback engaged (stage-only, stacked "
                "all-category): stage=%s control=%s run_id=%s groups=%d",
                wb_cfg.stage_table_fqn(),
                wb_cfg.runs_table_fqn(),
                run_id,
                len(per_group_records) if group_column else 1,
            )
            run_payload_extras = {
                "engine": None,
                "bq_input_project": args.bq_input_project,
                "bq_input_dataset": args.bq_input_dataset,
                "bq_flux_table": args.bq_flux_table,
                "bq_biomet_table": args.bq_biomet_table,
                "read_flux_rows": int(bq_result.flux_rows),
                "read_biomet_rows": int(bq_result.biomet_rows),
                "miaproc_version": _package_versions().get("miaproc"),
                "bigquery_client_version": _package_versions().get(
                    "google-cloud-bigquery"
                ),
            }
            # Build the stage frame from the stacked silver. For
            # ungrouped runs use the flux-side single site_id where
            # available; for grouped runs build per-group stage parts
            # using each group's category as the stage-identity site.
            # M28: route through prepare_silver_stage_payload so the
            # bronze-source uniqueness guard runs, source columns are
            # preserved, and the rH / rH_norm_s collision policy
            # resolves any duplicate column names before BigQuery sees
            # the schema.
            silver_collision_actions: list[dict[str, Any]] = []
            if group_column is None:
                resolved_site = _infer_single_site_id(bq_result.flux_df)
                stage_df, silver_collision_actions = (
                    prepare_silver_stage_payload(
                        silver,
                        site_id=resolved_site or "<unknown>",
                        source_flux_df=bq_result.flux_df,
                    )
                )
            else:
                stage_parts: list[Any] = []
                for rec, frame in zip(per_group_records, silver_frames):
                    cat = rec["category_value"]
                    flux_group = bq_result.flux_df.loc[
                        bq_result.flux_df[group_column] == cat
                    ].reset_index(drop=True)
                    part, part_actions = prepare_silver_stage_payload(
                        frame,
                        site_id=str(cat),
                        source_flux_df=flux_group,
                    )
                    stage_parts.append(part)
                    silver_collision_actions.extend(part_actions)
                stage_df = _stack_dataframes(stage_parts)
            wb_result = run_writeback(
                stage_df,
                wb_cfg,
                run_id=run_id,
                started_at=started_at,
                run_payload_extras=run_payload_extras,
            )
            writeback_record = wb_result.to_dict()
            writeback_record["stage_payload_columns_unique"] = True
            writeback_record["column_collision_actions"] = list(
                silver_collision_actions
            )
            logger.info(
                "Silver writeback done: status=%s stage_rows=%d "
                "collision_actions=%d",
                wb_result.status,
                wb_result.stage_rows,
                len(silver_collision_actions),
            )
        except SystemExit:
            raise
        except Exception as exc:
            logger.error("Silver BigQuery writeback failed: %s", exc)
            traceback.print_exc()
            wb_state = getattr(exc, "miaproc_writeback_state", {}) or {}
            writeback_record = {
                "status": wb_state.get("status", "failed"),
                "error_text": str(exc),
                "merge_attempted": False,
                "merge_authorized": False,
                "stage_rows": int(wb_state.get("stage_rows", 0)),
                "stage_payload_columns_unique": False,
                "column_collision_actions": [],
            }
            writeback_exit_code = RUNTIME_EXIT

    try:
        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "stage": "silver",
            "command": "eddy run-bigquery-silver",
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "silver": int(len(silver)),
                "null_category_rows_skipped": int(null_category_rows),
            },
            "column_counts": {"silver": int(len(silver.columns))},
            "silver_columns": [str(c) for c in silver.columns],
            "inputs": bq_inputs_record,
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "run_json": str(args.output_run_json),
                "bigquery_writeback": writeback_engaged,
                "stage_payload_dry_run": dry_run_dir is not None,
                "stage_payload_dry_run_dir": (
                    str(dry_run_dir) if dry_run_dir is not None else None
                ),
                "groups_dir": str(groups_dir) if groups_dir else None,
            },
            "groups": per_group_records,
            "group_column": group_column,
            "writeback": writeback_record,
            "versions": _package_versions(),
            "exit_code": writeback_exit_code,
        }
        _write_json(args.output_run_json, run_metadata)
    except Exception as exc:
        logger.error("Failed to write silver run-metadata JSON: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Silver BigQuery done. groups=%d rows=%d cols=%d writeback=%s "
        "table=%s exit=%d",
        len(per_group_records) if group_column else 1,
        len(silver),
        len(silver.columns),
        "engaged" if writeback_engaged else "skipped",
        args.output_table,
        writeback_exit_code,
    )
    return writeback_exit_code


def _validate_eddy_bq_gold_args(args: argparse.Namespace) -> None:
    """Validate ``eddy run-bigquery-gold`` arguments."""
    if args.engine == "reddyproc-reference" and args.repo_root is None:
        raise ValueError(
            "--repo-root is required for --engine reddyproc-reference "
            "(project-scoped preflight gate, Decision 010)."
        )
    _check_output_extension(args.output_table)
    writeback_engaged = bool(args.bq_stage_table)
    if writeback_engaged:
        if not args.bq_output_project:
            raise ValueError(
                "--bq-output-project is required when --bq-stage-table is set."
            )
        # Gold's silver input typically lives in the same staging
        # project as the gold writeback target (run-bigquery-silver
        # wrote it there). The hard production-read-only invariant is
        # enforced by ``BigQueryWritebackConfig.forbidden_write_projects``
        # at the package layer; the CLI does not additionally require
        # output != input for gold-from-silver.
        if not args.bq_output_dataset:
            raise ValueError(
                "--bq-output-dataset is required when --bq-stage-table is set."
            )
        if not args.bq_control_dataset:
            raise ValueError(
                "--bq-control-dataset is required when --bq-stage-table "
                "is set (control tables for run records + watermark)."
            )
        if args.bq_allow_final_merge and not args.bq_final_table:
            raise ValueError(
                "--bq-allow-final-merge requires --bq-final-table to name "
                "the MERGE target."
            )
    else:
        downstream_only_flags = (
            ("--bq-output-project", args.bq_output_project),
            ("--bq-output-dataset", args.bq_output_dataset),
            ("--bq-final-table", args.bq_final_table),
            ("--bq-control-dataset", args.bq_control_dataset),
        )
        set_without_stage = [name for name, val in downstream_only_flags if val]
        if set_without_stage:
            raise ValueError(
                f"Writeback flags {set_without_stage} require "
                "--bq-stage-table. Stage-only writeback is the safe "
                "default; final-table merge additionally requires "
                "--bq-allow-final-merge."
            )
        if args.bq_allow_final_merge:
            raise ValueError(
                "--bq-allow-final-merge requires --bq-stage-table "
                "and --bq-final-table to be set."
            )


def _run_eddy_bigquery_gold_command(args: argparse.Namespace) -> int:
    """Run the M22 BigQuery gold command (silver -> gold).

    Reads a silver-stage table from BigQuery, dispatches the selected
    engine through the same accepted gold path as ``miaproc eddy
    run-gold``, and writes the gold table + diagnostics + run JSON
    locally. Optionally stages the gold output back to BigQuery and
    MERGEs into a final gold target table under explicit
    ``--bq-allow-final-merge`` opt-in (same M8/M10 safety posture as
    the one-shot ``run-bigquery``).
    """
    started_at = _utc_now_iso()
    try:
        _validate_eddy_bq_gold_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    preflight_record: Optional[dict[str, Any]] = None
    if args.engine == "reddyproc-reference":
        preflight_record = _run_preflight_or_exit(args.repo_root)

    group_column: Optional[str] = args.group_column
    groups_dir = (
        _resolve_groups_dir(args.output_table, args.output_groups_dir)
        if group_column is not None
        else None
    )
    per_group_records: list[dict[str, Any]] = []
    null_category_rows = 0
    gold_frames: list[Any] = []
    silver_extras_union: list[str] = []
    config_record: dict[str, Any] = {}
    silver_result = None
    silver = None
    silver_total_cols = 0

    bq_inputs_record: dict[str, Any] = {}
    try:
        from miaproc.eddy import (
            BigQuerySilverInputConfig,
            read_bigquery_silver_input,
            silver_to_internal_calc_frame,
        )

        silver_cfg = BigQuerySilverInputConfig(
            input_project=args.bq_input_project,
            input_dataset=args.bq_input_dataset,
            silver_table=args.bq_silver_table,
            site_id=None,
            start_timestamp=args.bq_start_timestamp,
            end_timestamp=args.bq_end_timestamp,
            billing_project=args.bq_billing_project,
            bq_storage_api=not args.bq_no_storage_api,
        )
        logger.info(
            "Gold BigQuery silver read (all-categories): project=%s "
            "dataset=%s silver=%s",
            silver_cfg.input_project,
            silver_cfg.input_dataset,
            silver_cfg.silver_table,
        )
        silver_result = read_bigquery_silver_input(silver_cfg)
        silver = silver_result.silver_df
        silver_total_cols = int(len(silver.columns))
        logger.info(
            "Silver loaded from BigQuery: rows=%d cols=%d",
            len(silver),
            silver_total_cols,
        )
        bq_inputs_record = {
            "mode": "bigquery",
            "input_project": silver_cfg.input_project,
            "input_dataset": silver_cfg.input_dataset,
            "silver_table": silver_cfg.silver_table,
            "billing_project": silver_cfg.billing_project_or_input(),
            "group_column": group_column,
            "start_timestamp": silver_cfg.start_timestamp,
            "end_timestamp": silver_cfg.end_timestamp,
            "bq_storage_api": silver_cfg.bq_storage_api,
            "silver_query": silver_result.silver_query,
            "query_parameters": dict(silver_result.query_parameters),
            "read_row_counts": {"silver": int(silver_result.silver_rows)},
        }

        if group_column is None:
            logger.info("Running gold engine=%s (ungrouped)...", args.engine)
            # M32: silver carries source-truth final names; rebuild the
            # internal calc frame (NEE / Tair / USTAR / QC_NEE / Rg /
            # VPD / rH) before dispatching to the backend so hesseflux
            # / REddyProc / dynamic-u* / prepare_reddyproc_input keep
            # working unchanged. Silver retains source-truth names for
            # the gold-side preservation contract.
            internal_silver = silver_to_internal_calc_frame(silver)
            gold, config_record = _dispatch_engine(
                args.engine, internal_silver, args
            )
            gold_with_silver, silver_extras_union = (
                _attach_silver_columns_to_gold(gold, silver)
            )
            gold_frames.append(gold_with_silver)
        else:
            _validate_group_column(silver, group_column, side="silver")
            categories, null_category_rows = _iter_categories(
                silver, group_column
            )
            if not categories:
                raise ValueError(
                    f"No non-null values for --group-column "
                    f"{group_column!r} in BigQuery silver read."
                )
            assert groups_dir is not None
            groups_dir.mkdir(parents=True, exist_ok=True)
            extras_set: set[str] = set()
            for category in categories:
                logger.info(
                    "Group %s=%r: gold engine=%s",
                    group_column,
                    category,
                    args.engine,
                )
                silver_group = silver.loc[
                    silver[group_column] == category
                ].reset_index(drop=True)
                internal_silver_group = silver_to_internal_calc_frame(
                    silver_group
                )
                gold_group, config_record = _dispatch_engine(
                    args.engine, internal_silver_group, args
                )
                gold_with_silver_group, silver_extras_g = (
                    _attach_silver_columns_to_gold(gold_group, silver_group)
                )
                gold_frames.append(gold_with_silver_group)
                extras_set.update(silver_extras_g)
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="gold",
                )
                _write_table(per_group_table_path, gold_with_silver_group)
                per_group_diag_path = _per_group_diagnostics_path(
                    groups_dir, category=category
                )
                per_group_diag = (
                    getattr(gold_with_silver_group, "attrs", {}) or {}
                ).get("miaproc_diagnostics") or {}
                _write_json(per_group_diag_path, dict(per_group_diag))
                per_group_records.append(
                    {
                        "category_value": category,
                        "silver_input_rows": int(len(silver_group)),
                        "gold_output_rows": int(
                            len(gold_with_silver_group)
                        ),
                        "table_path": str(per_group_table_path),
                        "diagnostics_path": str(per_group_diag_path),
                    }
                )
            silver_extras_union = sorted(extras_set)

        gold_with_silver = _stack_dataframes(gold_frames)
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    # Local artifacts first so operators retain outputs if the
    # optional BigQuery writeback later fails.
    try:
        table_format = _write_table(args.output_table, gold_with_silver)
        if group_column is None:
            diagnostics = (
                getattr(gold_with_silver, "attrs", {}) or {}
            ).get("miaproc_diagnostics") or {}
            _write_json(args.output_diagnostics_json, dict(diagnostics))
        else:
            _write_json(
                args.output_diagnostics_json,
                {
                    "group_column": group_column,
                    "groups": [
                        {
                            "category_value": rec["category_value"],
                            "diagnostics_path": rec["diagnostics_path"],
                        }
                        for rec in per_group_records
                    ],
                },
            )
    except Exception as exc:
        logger.error("Failed to write gold output artifacts: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    writeback_record: Optional[dict[str, Any]] = None
    writeback_engaged = bool(args.bq_stage_table)
    writeback_exit_code = SUCCESS_EXIT

    dry_run_dir: Optional[Path] = args.stage_payload_dry_run_dir
    if dry_run_dir is not None:
        # M29: build the exact gold stage payload the writeback path
        # would produce, write local artifacts, and skip every
        # BigQuery write (no stage write, no validation SQL, no
        # MERGE, no watermark advance). The silver BigQuery read +
        # gold engine dispatch + Decision-010 R preflight already
        # ran upstream; M29 only short-circuits the BigQuery-write
        # side. Wins over --bq-stage-table / --bq-allow-final-merge.
        try:
            from miaproc.eddy import (
                COLUMN_COLLISION_ATTRS_KEY,
                prepare_stage_dataframe,
            )

            gold_collision_actions: list[dict[str, Any]] = []
            if group_column is None:
                resolved_site = _infer_single_site_id(silver)
                stage_df = prepare_stage_dataframe(
                    gold_with_silver,
                    site_id=resolved_site or "<unknown>",
                    source_flux_df=None,
                    target_columns=None,
                    target_types=None,
                    preserve_payload_columns=True,
                )
                gold_collision_actions = list(
                    stage_df.attrs.get(COLUMN_COLLISION_ATTRS_KEY, [])
                )
            else:
                stage_parts: list[Any] = []
                for rec, frame in zip(per_group_records, gold_frames):
                    cat = rec["category_value"]
                    part = prepare_stage_dataframe(
                        frame,
                        site_id=str(cat),
                        source_flux_df=None,
                        target_columns=None,
                        target_types=None,
                        preserve_payload_columns=True,
                    )
                    stage_parts.append(part)
                    gold_collision_actions.extend(
                        part.attrs.get(COLUMN_COLLISION_ATTRS_KEY, [])
                    )
                stage_df = _stack_dataframes(stage_parts)
            writeback_record = _write_stage_payload_dry_run_artifacts(
                stage_df,
                stage="gold",
                command="eddy run-bigquery-gold",
                dry_run_dir=dry_run_dir,
                input_df=silver,
                collision_actions=gold_collision_actions,
                would_write=_gold_would_write(args),
                input_alias_map=GOLD_SILVER_INPUT_ALIASES,
            )
            logger.info(
                "Gold stage-payload dry-run: rows=%d cols=%d dir=%s "
                "collision_actions=%d (no BigQuery write, no MERGE, "
                "no watermark advance)",
                int(len(stage_df)),
                int(len(stage_df.columns)),
                dry_run_dir,
                len(gold_collision_actions),
            )
        except SystemExit:
            raise
        except Exception as exc:
            logger.error("Gold dry-run artifact write failed: %s", exc)
            traceback.print_exc()
            writeback_record = {
                "status": "stage_payload_dry_run_failed",
                "error_text": str(exc),
                "bigquery_write_attempted": False,
                "merge_attempted": False,
                "merge_authorized": False,
                "watermark_advanced": False,
                "stage_payload_columns_unique": False,
                "column_collision_actions": [],
                "payload_artifacts": {
                    "stage_payload_csv": None,
                    "stage_payload_metadata_json": None,
                },
            }
            writeback_exit_code = RUNTIME_EXIT
    elif writeback_engaged:
        try:
            from miaproc.eddy import (
                BigQueryWritebackConfig,
                COLUMN_COLLISION_ATTRS_KEY,
                prepare_stage_dataframe,
                read_final_table_columns,
                read_final_table_schema,
                run_writeback,
            )

            run_id = args.bq_run_id or _default_run_id()
            wb_cfg = BigQueryWritebackConfig(
                output_project=args.bq_output_project,
                output_dataset=args.bq_output_dataset,
                stage_table=args.bq_stage_table,
                control_dataset=args.bq_control_dataset,
                final_table=args.bq_final_table,
                allow_final_merge=bool(args.bq_allow_final_merge),
                run_id=run_id,
                site_id=None,
                billing_project=args.bq_output_project,
            )
            logger.info(
                "Gold BigQuery writeback engaged: stage=%s final=%s "
                "control=%s allow_final_merge=%s run_id=%s groups=%d",
                wb_cfg.stage_table_fqn(),
                wb_cfg.final_table_fqn(),
                wb_cfg.runs_table_fqn(),
                wb_cfg.allow_final_merge,
                run_id,
                len(per_group_records) if group_column else 1,
            )
            run_payload_extras = {
                "engine": args.engine,
                "bq_input_project": args.bq_input_project,
                "bq_input_dataset": args.bq_input_dataset,
                "bq_flux_table": args.bq_silver_table,
                "bq_biomet_table": None,
                "read_flux_rows": int(silver_result.silver_rows),
                "read_biomet_rows": None,
                "miaproc_version": _package_versions().get("miaproc"),
                "bigquery_client_version": _package_versions().get(
                    "google-cloud-bigquery"
                ),
            }
            target_columns = read_final_table_columns(wb_cfg)
            target_types = read_final_table_schema(wb_cfg)
            # Build per-group stage frames using the group's category
            # as the stage-identity site_id; concat for a single
            # stacked stage write so shared stage tables are valid.
            # M28: preserve_payload_columns=True keeps every incoming
            # silver column in the gold stage payload (final-table
            # MERGE schema alignment is a follow-up concern).
            gold_collision_actions: list[dict[str, Any]] = []
            if group_column is None:
                resolved_site = _infer_single_site_id(silver)
                stage_df = prepare_stage_dataframe(
                    gold_with_silver,
                    site_id=resolved_site or "<unknown>",
                    source_flux_df=None,
                    target_columns=target_columns,
                    target_types=target_types,
                    preserve_payload_columns=True,
                )
                gold_collision_actions = list(
                    stage_df.attrs.get(COLUMN_COLLISION_ATTRS_KEY, [])
                )
            else:
                stage_parts: list[Any] = []
                for rec, frame in zip(per_group_records, gold_frames):
                    cat = rec["category_value"]
                    part = prepare_stage_dataframe(
                        frame,
                        site_id=str(cat),
                        source_flux_df=None,
                        target_columns=target_columns,
                        target_types=target_types,
                        preserve_payload_columns=True,
                    )
                    stage_parts.append(part)
                    gold_collision_actions.extend(
                        part.attrs.get(COLUMN_COLLISION_ATTRS_KEY, [])
                    )
                stage_df = _stack_dataframes(stage_parts)
            wb_result = run_writeback(
                stage_df,
                wb_cfg,
                run_id=run_id,
                started_at=started_at,
                run_payload_extras=run_payload_extras,
            )
            writeback_record = wb_result.to_dict()
            writeback_record["stage_payload_columns_unique"] = True
            writeback_record["column_collision_actions"] = list(
                gold_collision_actions
            )
            logger.info(
                "Gold writeback done: status=%s stage_rows=%d "
                "merge_attempted=%s merge_authorized=%s "
                "watermark_advanced=%s watermark_values_by_site=%s "
                "collision_actions=%d",
                wb_result.status,
                wb_result.stage_rows,
                wb_result.merge_attempted,
                wb_result.merge_authorized,
                wb_result.watermark_advanced,
                wb_result.watermark_values_by_site,
                len(gold_collision_actions),
            )
        except SystemExit:
            raise
        except Exception as exc:
            logger.error("Gold BigQuery writeback failed: %s", exc)
            traceback.print_exc()
            wb_state = getattr(exc, "miaproc_writeback_state", {}) or {}
            writeback_record = {
                "status": wb_state.get("status", "failed"),
                "error_text": str(exc),
                "merge_attempted": bool(
                    wb_state.get("merge_attempted", False)
                ),
                "merge_authorized": bool(
                    wb_state.get(
                        "merge_authorized", bool(args.bq_allow_final_merge)
                    )
                ),
                "stage_rows": int(wb_state.get("stage_rows", 0)),
                "stage_payload_columns_unique": False,
                "column_collision_actions": [],
            }
            writeback_exit_code = RUNTIME_EXIT

    try:
        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "stage": "gold",
            "command": "eddy run-bigquery-gold",
            "engine": args.engine,
            "config": config_record,
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "silver_input": int(len(silver)) if silver is not None else 0,
                "gold_output": int(len(gold_with_silver)),
                "null_category_rows_skipped": int(null_category_rows),
            },
            "column_counts": {
                "silver_input": silver_total_cols,
                "gold_output": int(len(gold_with_silver.columns)),
                "silver_only_appended": len(silver_extras_union),
            },
            "silver_columns_appended": [
                str(c) for c in silver_extras_union
            ],
            "inputs": bq_inputs_record,
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "diagnostics_json": str(args.output_diagnostics_json),
                "run_json": str(args.output_run_json),
                "bigquery_writeback": writeback_engaged,
                "stage_payload_dry_run": dry_run_dir is not None,
                "stage_payload_dry_run_dir": (
                    str(dry_run_dir) if dry_run_dir is not None else None
                ),
                "groups_dir": str(groups_dir) if groups_dir else None,
            },
            "groups": per_group_records,
            "group_column": group_column,
            "writeback": writeback_record,
            "versions": _package_versions(),
            "exit_code": writeback_exit_code,
        }
        if preflight_record is not None:
            run_metadata["preflight"] = preflight_record
        _write_json(args.output_run_json, run_metadata)
    except Exception as exc:
        logger.error("Failed to write gold run-metadata JSON: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Gold BigQuery done. engine=%s groups=%d rows=%d cols=%d "
        "(silver-only-appended=%d) writeback=%s table=%s exit=%d",
        args.engine,
        len(per_group_records) if group_column else 1,
        len(gold_with_silver),
        len(gold_with_silver.columns),
        len(silver_extras_union),
        "engaged" if writeback_engaged else "skipped",
        args.output_table,
        writeback_exit_code,
    )
    return writeback_exit_code


# ---------------------------------------------------------------------------
# M17 biomass: enrich-table command
# ---------------------------------------------------------------------------


def _read_biomass_table(path: Path) -> Any:
    """Read an input tree table from CSV or parquet (extension-driven).

    Mirrors ``_check_output_extension`` semantics: ``.csv`` /
    ``.parquet`` / ``.pq`` accepted; anything else is a validation
    failure. CSV reads use pandas' default parser; parquet preserves
    column dtypes cleanly.
    """
    import pandas as pd

    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in (".parquet", ".pq"):
        return pd.read_parquet(path)
    raise ValueError(
        f"Unsupported --input-table extension {suffix!r}. Use .csv or .parquet."
    )


def _validate_biomass_enrich_args(args: argparse.Namespace) -> None:
    if not args.input_table.exists():
        raise ValueError(f"--input-table does not exist: {args.input_table}")
    if args.input_table.suffix.lower() not in (".csv", ".parquet", ".pq"):
        raise ValueError(
            f"--input-table must be .csv or .parquet "
            f"(got {args.input_table.suffix!r})."
        )
    if args.equations_path is not None and not args.equations_path.exists():
        raise ValueError(
            f"--equations-path does not exist: {args.equations_path}"
        )
    if args.biomass_estimate_col == args.equation_used_col:
        raise ValueError(
            "--biomass-estimate-col and --equation-used-col must differ "
            f"(got {args.biomass_estimate_col!r} for both)."
        )
    _check_output_extension(args.output_table)


def _run_biomass_enrich_table_command(args: argparse.Namespace) -> int:
    """Run the M17 biomass table-enrichment command."""
    started_at = _utc_now_iso()
    try:
        _validate_biomass_enrich_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    try:
        from miaproc.biomass import BiomassColumns, enrich_table, load_equations
        from miaproc.biomass.api import estimate_trees

        logger.info("Reading input table: %s", args.input_table)
        df_in = _read_biomass_table(args.input_table)
        logger.info(
            "Input rows: %d cols: %d", len(df_in), len(df_in.columns)
        )

        equations_source = (
            "packaged_default"
            if args.equations_path is None
            else str(args.equations_path)
        )
        logger.info("Loading equations: %s", equations_source)
        equations = load_equations(args.equations_path)

        cols = BiomassColumns(
            species=args.species_col,
            dbh_cm=args.dbh_col,
            height_m=args.height_col,
            life_stage=args.life_stage_col,
        )

        # Normalize the dataset filter: empty string -> no filter.
        dataset = args.dataset if args.dataset else None
        logger.info(
            "Enriching table: dataset=%s state=%s estimate_col=%s "
            "equation_used_col=%s",
            dataset,
            args.state,
            args.biomass_estimate_col,
            args.equation_used_col,
        )

        # One pass through the M16 estimator gives us both the 2-col
        # output projection and the diagnostics (match_status counts)
        # for the run JSON. Calling enrich_table separately would do
        # the same work twice.
        full = estimate_trees(
            df_in,
            equations=equations,
            state=args.state,
            columns=cols,
            response_variable=args.response_variable,
            dataset=dataset,
        )
        # Validate output column-name policy here so a collision
        # surfaces before we write a half-shaped table.
        for col in (args.biomass_estimate_col, args.equation_used_col):
            if col in df_in.columns:
                raise ValueError(
                    f"Output column name {col!r} collides with an "
                    "existing input column. Pick a different name via "
                    "--biomass-estimate-col / --equation-used-col."
                )

        out = df_in.copy()
        estimate_series = full["estimate_response_variable"]
        out[args.biomass_estimate_col] = estimate_series.values
        # Match the ``enrich_table`` masking contract: ``equation_used``
        # is populated only when the estimate is actually non-NaN, so
        # rejected matches don't leak into the M17 output column even
        # though they are exposed in the M16 ``source_record_id`` audit
        # field on the full per-row API.
        used = full["source_record_id"].where(estimate_series.notna())
        out[args.equation_used_col] = used.where(used.notna(), None).values
        _ = enrich_table  # imported for re-export sanity, not invoked

        match_status_counts = (
            full["match_status"].value_counts(dropna=False).to_dict()
        )
        estimated_count = int(out[args.biomass_estimate_col].notna().sum())
        skipped_count = int(out[args.biomass_estimate_col].isna().sum())
        logger.info(
            "Enrichment done: rows=%d estimated=%d skipped=%d",
            len(out),
            estimated_count,
            skipped_count,
        )
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    try:
        table_format = _write_table(args.output_table, out)
        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "command": "biomass enrich-table",
            "stage": "biomass_enrich_table",
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "input": int(len(df_in)),
                "output": int(len(out)),
                "estimated": estimated_count,
                "skipped": skipped_count,
            },
            "match_status_counts": {
                str(k): int(v) for k, v in match_status_counts.items()
            },
            "config": {
                "dataset": dataset,
                "state": args.state,
                "response_variable": args.response_variable,
                "equations_source": equations_source,
                "columns": {
                    "species": args.species_col,
                    "dbh_cm": args.dbh_col,
                    "tree_height_m": args.height_col,
                    "life_stage": args.life_stage_col,
                },
            },
            "output_columns_appended": [
                args.biomass_estimate_col,
                args.equation_used_col,
            ],
            "inputs": {
                "mode": "file",
                "input_table": str(args.input_table),
            },
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "run_json": str(args.output_run_json),
            },
            "versions": _package_versions(),
            "exit_code": SUCCESS_EXIT,
        }
        _write_json(args.output_run_json, run_metadata)
    except Exception as exc:
        logger.error("Failed to write biomass artifacts: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Biomass enrich-table done. rows=%d estimated=%d skipped=%d "
        "table=%s exit=%d",
        len(out),
        estimated_count,
        skipped_count,
        args.output_table,
        SUCCESS_EXIT,
    )
    return SUCCESS_EXIT


# ---------------------------------------------------------------------------
# M19/M20 biomass run-bigquery: BigQuery-native enrichment + writeback
# ---------------------------------------------------------------------------


def _validate_biomass_bigquery_args(args: argparse.Namespace) -> None:
    if not args.bq_input_project:
        raise ValueError("--bq-input-project must be a non-empty string.")
    if not args.bq_input_dataset:
        raise ValueError("--bq-input-dataset must be a non-empty string.")
    if not args.bq_input_table:
        raise ValueError("--bq-input-table must be a non-empty string.")
    if args.bq_row_limit is not None and args.bq_row_limit <= 0:
        raise ValueError(
            f"--bq-row-limit must be a positive integer (got {args.bq_row_limit})."
        )
    if args.equations_path is not None and not args.equations_path.exists():
        raise ValueError(
            f"--equations-path does not exist: {args.equations_path}"
        )
    if args.biomass_estimate_col == args.equation_used_col:
        raise ValueError(
            "--biomass-estimate-col and --equation-used-col must differ "
            f"(got {args.biomass_estimate_col!r} for both)."
        )
    _check_output_extension(args.output_table)

    # M20 writeback flag-set validation: --bq-stage-table is the trigger.
    writeback_engaged = bool(args.bq_stage_table)
    if writeback_engaged:
        if not args.bq_output_project:
            raise ValueError(
                "--bq-output-project is required when --bq-stage-table is set."
            )
        if args.bq_output_project == args.bq_input_project:
            raise ValueError(
                "--bq-output-project must differ from --bq-input-project. "
                "Production input projects must remain read-only; route "
                "writes to a staging project."
            )
        if not args.bq_output_dataset:
            raise ValueError(
                "--bq-output-dataset is required when --bq-stage-table is set."
            )
        if not args.bq_control_dataset:
            raise ValueError(
                "--bq-control-dataset is required when --bq-stage-table "
                "is set (runs control table)."
            )
        if not args.bq_merge_key:
            raise ValueError(
                "--bq-merge-key must be a non-empty column name."
            )
        if args.bq_allow_final_merge and not args.bq_final_table:
            raise ValueError(
                "--bq-allow-final-merge requires --bq-final-table to name "
                "the MERGE target."
            )
    else:
        # Surface partial writeback config rather than silently
        # ignoring it.
        downstream_only_flags = (
            ("--bq-output-project", args.bq_output_project),
            ("--bq-output-dataset", args.bq_output_dataset),
            ("--bq-final-table", args.bq_final_table),
            ("--bq-control-dataset", args.bq_control_dataset),
        )
        set_without_stage = [name for name, val in downstream_only_flags if val]
        if set_without_stage:
            raise ValueError(
                f"Writeback flags {set_without_stage} require "
                "--bq-stage-table. Stage-only writeback is the safe "
                "default; final-table merge additionally requires "
                "--bq-allow-final-merge."
            )
        if args.bq_allow_final_merge:
            raise ValueError(
                "--bq-allow-final-merge requires --bq-stage-table "
                "and --bq-final-table to be set."
            )


def _run_biomass_run_bigquery_command(args: argparse.Namespace) -> int:
    """Run the biomass BigQuery-native command (M19 read + M20 writeback).

    Reads one tree table from BigQuery, enriches it with the
    accepted M16 / M17 / M17A contract, writes the enriched table +
    run JSON to local disk, and (M20) optionally stages the enriched
    output back into BigQuery + opt-in MERGE into the final table.
    Stage-only is the safe default; final-table mutation requires
    ``--bq-allow-final-merge``. Biomass M20 has no watermark concept
    by design (per-tree identity-keyed enrichment, not time-series).
    """
    started_at = _utc_now_iso()
    try:
        _validate_biomass_bigquery_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT

    bq_inputs_record: dict[str, Any] = {}
    try:
        from miaproc.biomass import (
            BigQueryBiomassConfig,
            BiomassColumns,
            load_equations,
            read_bigquery_input,
        )
        from miaproc.biomass.api import estimate_trees

        bq_cfg = BigQueryBiomassConfig(
            input_project=args.bq_input_project,
            input_dataset=args.bq_input_dataset,
            input_table=args.bq_input_table,
            billing_project=args.bq_billing_project,
            row_limit=args.bq_row_limit,
            bq_storage_api=not args.bq_no_storage_api,
        )
        logger.info(
            "BigQuery read: project=%s dataset=%s table=%s row_limit=%s",
            bq_cfg.input_project,
            bq_cfg.input_dataset,
            bq_cfg.input_table,
            bq_cfg.row_limit,
        )
        bq_result = read_bigquery_input(bq_cfg)
        df_in = bq_result.input_df
        logger.info(
            "BigQuery rows: %d cols: %d", len(df_in), len(df_in.columns)
        )

        bq_inputs_record = {
            "mode": "bigquery",
            "input_project": bq_cfg.input_project,
            "input_dataset": bq_cfg.input_dataset,
            "input_table": bq_cfg.input_table,
            "billing_project": bq_cfg.billing_project_or_input(),
            "row_limit": bq_cfg.row_limit,
            "bq_storage_api": bq_cfg.bq_storage_api,
            "input_query": bq_result.input_query,
            "query_parameters": dict(bq_result.query_parameters),
            "read_row_counts": {"input": int(bq_result.input_rows)},
        }

        equations_source = (
            "packaged_default"
            if args.equations_path is None
            else str(args.equations_path)
        )
        logger.info("Loading equations: %s", equations_source)
        equations = load_equations(args.equations_path)

        cols = BiomassColumns(
            species=args.species_col,
            dbh_cm=args.dbh_col,
            height_m=args.height_col,
            life_stage=args.life_stage_col,
        )

        dataset = args.dataset if args.dataset else None
        logger.info(
            "Enriching table: dataset=%s state=%s estimate_col=%s "
            "equation_used_col=%s",
            dataset,
            args.state,
            args.biomass_estimate_col,
            args.equation_used_col,
        )

        # Output column-name policy: surface collisions before write
        # so a half-shaped table is never produced.
        for col in (args.biomass_estimate_col, args.equation_used_col):
            if col in df_in.columns:
                raise ValueError(
                    f"Output column name {col!r} collides with an "
                    "existing input column. Pick a different name via "
                    "--biomass-estimate-col / --equation-used-col."
                )

        full = estimate_trees(
            df_in,
            equations=equations,
            state=args.state,
            columns=cols,
            response_variable=args.response_variable,
            dataset=dataset,
        )
        out = df_in.copy()
        estimate_series = full["estimate_response_variable"]
        out[args.biomass_estimate_col] = estimate_series.values
        # Same masking as the file-based enrich-table CLI: equation_used
        # is only populated when an estimate was actually applied.
        used = full["source_record_id"].where(estimate_series.notna())
        out[args.equation_used_col] = used.where(used.notna(), None).values

        match_status_counts = (
            full["match_status"].value_counts(dropna=False).to_dict()
        )
        estimated_count = int(out[args.biomass_estimate_col].notna().sum())
        skipped_count = int(out[args.biomass_estimate_col].isna().sum())
        logger.info(
            "Enrichment done: rows=%d estimated=%d skipped=%d",
            len(out),
            estimated_count,
            skipped_count,
        )
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    # Write local-disk artifacts first (always done, regardless of
    # whether BigQuery writeback is engaged — operators get the
    # enriched table + run JSON even when writeback later fails).
    try:
        table_format = _write_table(args.output_table, out)
    except Exception as exc:
        logger.error("Failed to write biomass output table: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    # M20 writeback (engaged only when --bq-stage-table is set).
    writeback_record: Optional[dict[str, Any]] = None
    writeback_engaged = bool(args.bq_stage_table)
    writeback_exit_code = SUCCESS_EXIT
    if writeback_engaged:
        try:
            from miaproc.biomass import (
                BigQueryBiomassWritebackConfig,
                prepare_stage_dataframe,
                run_writeback,
            )

            run_id = args.bq_run_id or _default_run_id()
            wb_cfg = BigQueryBiomassWritebackConfig(
                output_project=args.bq_output_project,
                output_dataset=args.bq_output_dataset,
                stage_table=args.bq_stage_table,
                control_dataset=args.bq_control_dataset,
                final_table=args.bq_final_table,
                allow_final_merge=bool(args.bq_allow_final_merge),
                run_id=run_id,
                merge_key_column=args.bq_merge_key,
                billing_project=args.bq_output_project,
            )
            logger.info(
                "BigQuery writeback engaged: stage=%s final=%s control=%s "
                "merge_key=%s allow_final_merge=%s run_id=%s",
                wb_cfg.stage_table_fqn(),
                wb_cfg.final_table_fqn(),
                wb_cfg.runs_table_fqn(),
                wb_cfg.merge_key_column,
                wb_cfg.allow_final_merge,
                run_id,
            )
            run_payload_extras = {
                "bq_input_project": args.bq_input_project,
                "bq_input_dataset": args.bq_input_dataset,
                "bq_input_table": args.bq_input_table,
                "read_input_rows": int(len(df_in)),
                "estimated_rows": estimated_count,
                "skipped_rows": skipped_count,
                "dataset": str(dataset) if dataset is not None else None,
                "equations_source": equations_source,
                "miaproc_version": _package_versions().get("miaproc"),
                "bigquery_client_version": _package_versions().get(
                    "google-cloud-bigquery"
                ),
            }
            stage_df = prepare_stage_dataframe(out, cfg=wb_cfg)
            wb_result = run_writeback(
                stage_df,
                wb_cfg,
                run_id=run_id,
                started_at=started_at,
                run_payload_extras=run_payload_extras,
            )
            writeback_record = wb_result.to_dict()
            logger.info(
                "BigQuery writeback done: status=%s stage_rows=%d "
                "merge_attempted=%s merge_authorized=%s",
                wb_result.status,
                wb_result.stage_rows,
                wb_result.merge_attempted,
                wb_result.merge_authorized,
            )
        except SystemExit:
            raise
        except Exception as exc:
            logger.error("BigQuery writeback failed: %s", exc)
            traceback.print_exc()
            wb_state = getattr(exc, "miaproc_writeback_state", {}) or {}
            writeback_record = {
                "status": wb_state.get("status", "failed"),
                "error_text": str(exc),
                "merge_attempted": bool(
                    wb_state.get("merge_attempted", False)
                ),
                "merge_authorized": bool(
                    wb_state.get(
                        "merge_authorized",
                        bool(args.bq_allow_final_merge),
                    )
                ),
                "stage_rows": int(wb_state.get("stage_rows", 0)),
            }
            writeback_exit_code = RUNTIME_EXIT

    # Run-metadata JSON (always written; includes writeback record
    # when engaged).
    try:
        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "command": "biomass run-bigquery",
            "stage": "biomass_run_bigquery",
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "input": int(len(df_in)),
                "output": int(len(out)),
                "estimated": estimated_count,
                "skipped": skipped_count,
            },
            "match_status_counts": {
                str(k): int(v) for k, v in match_status_counts.items()
            },
            "config": {
                "dataset": dataset,
                "state": args.state,
                "response_variable": args.response_variable,
                "equations_source": equations_source,
                "columns": {
                    "species": args.species_col,
                    "dbh_cm": args.dbh_col,
                    "tree_height_m": args.height_col,
                    "life_stage": args.life_stage_col,
                },
            },
            "output_columns_appended": [
                args.biomass_estimate_col,
                args.equation_used_col,
            ],
            "inputs": bq_inputs_record,
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "run_json": str(args.output_run_json),
                "bigquery_writeback": writeback_engaged,
            },
            "writeback": writeback_record,
            "versions": _package_versions(),
            "exit_code": writeback_exit_code,
        }
        _write_json(args.output_run_json, run_metadata)
    except Exception as exc:
        logger.error("Failed to write run metadata JSON: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    logger.info(
        "Biomass run-bigquery done. rows=%d estimated=%d skipped=%d "
        "writeback=%s table=%s exit=%d",
        len(out),
        estimated_count,
        skipped_count,
        "engaged" if writeback_engaged else "skipped",
        args.output_table,
        writeback_exit_code,
    )
    return writeback_exit_code


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def _configure_logging() -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(
        logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def main(argv: Optional[list[str]] = None) -> int:
    _configure_logging()
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        return _run_command(args)
    if args.command == "eddy":
        if args.eddy_command == "run-bigquery":
            return _run_bigquery_command(args)
        if args.eddy_command == "run-silver":
            return _run_eddy_silver_command(args)
        if args.eddy_command == "run-gold":
            return _run_eddy_gold_command(args)
        if args.eddy_command == "run-bigquery-silver":
            return _run_eddy_bigquery_silver_command(args)
        if args.eddy_command == "run-bigquery-gold":
            return _run_eddy_bigquery_gold_command(args)
        parser.error(f"Unknown eddy command: {args.eddy_command!r}")
        return VALIDATION_EXIT  # pragma: no cover - argparse exits first
    if args.command == "biomass":
        if args.biomass_command == "enrich-table":
            return _run_biomass_enrich_table_command(args)
        if args.biomass_command == "run-bigquery":
            return _run_biomass_run_bigquery_command(args)
        parser.error(f"Unknown biomass command: {args.biomass_command!r}")
        return VALIDATION_EXIT  # pragma: no cover - argparse exits first
    parser.error(f"Unknown command: {args.command!r}")
    return VALIDATION_EXIT  # pragma: no cover - argparse exits first


if __name__ == "__main__":
    sys.exit(main())
