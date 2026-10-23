"""Production CLI for long-series miaproc processing.

Non-interactive job-friendly entrypoint that wraps stage-1 ingestion +
``postproc`` for routine and cloud-job execution.

CLI shape (M7 introduces a module-aware namespace; M14 adds the
silver/gold split; M002 adds Parquet inputs and run-directory publication):

- ``miaproc run ...`` — file-based eddy run (M6 contract; unchanged).
- ``miaproc eddy run-silver ...`` — silver-stage only (new in M14).
  Stage-1 ingest + clean + regularize -> silver table. No engine, no R
  preflight; silver is the input contract for ``run-gold``. Inputs are
  explicit Parquet files (``--flux-parquet``/``--biomet-parquet``) or
  the documented CSV directories.
- ``miaproc eddy run-gold ...`` — gold-stage only (new in M14). Reads
  a silver-stage table from disk, dispatches the selected engine
  (default ``reddyproc-reference``), and writes a gold table that
  preserves silver columns and appends backend outputs.
- ``miaproc eddy validate-run ...`` — consumer/readback check of a
  published silver/gold run directory (completion manifest + matching
  artifact sizes and checksums).

The BigQuery transport (``eddy run-bigquery``, ``run-bigquery-silver``,
``run-bigquery-gold``, every ``--bq-*`` flag and
``--stage-payload-dry-run-dir``) was removed in M002-S04. Invoking any of
them exits ``3`` with a message pointing to the file workflow and
``docs/MIGRATING_FROM_BIGQUERY.md``; nothing falls back to another path.

miaproc is eddy-covariance only: the former ``miaproc biomass ...``
commands and the ``miaproc.biomass`` package were removed (M001-S02).

All run commands share the three supported run modes (M6 Task 2):

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
- ``3``: input or config validation failure (including a removed
  BigQuery command or flag, and ``validate-run`` on an incomplete,
  mismatched or malformed run).
- ``4``: runtime processing failure.
- ``5``: publication ambiguous — a valid completion manifest was written
  (the run is published and is never deleted) but the producer could not
  confirm a clean finish (ambiguous close acknowledgement or failed
  post-publication readback). Consumers validate the run.
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
# M002-S03 (D005/C4): a valid completion manifest was written, so the run IS
# published, but the producer could not confirm a clean finish (ambiguous
# close acknowledgement or failed post-publication readback). Never deleted.
PUBLICATION_AMBIGUOUS_EXIT = 5


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
    file-based ``run`` and the silver/gold split run commands.

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


def _add_publication_args(p: argparse.ArgumentParser) -> None:
    """M002-S03: fresh run directory + bounded local scratch (D005/C4)."""
    p.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help=(
            "Fresh run directory this invocation owns (must not exist; the "
            "caller/orchestrator provides an exclusive run name). Every "
            "--output-* path must lie inside it. Defaults to the parent "
            "directory of --output-table. A completion manifest "
            "'_MIAPROC_RUN_COMPLETE.json' is written last; only a run with a "
            "valid manifest is published (docs/RUN_PUBLICATION.md)."
        ),
    )
    p.add_argument(
        "--scratch-dir",
        type=Path,
        default=None,
        help=(
            "Local scratch base for staging/validating artifacts before "
            "publication (default: $MIAPROC_SCRATCH_DIR or the system temp "
            "directory). Only the invocation-owned subdirectory is removed."
        ),
    )
    p.add_argument(
        "--scratch-max-bytes",
        type=int,
        default=None,
        help=(
            "Positive limit on the bytes this invocation writes into its "
            "scratch (all staged tables, diagnostics, run metadata and the "
            "manifest), enforced while writing (default: "
            "$MIAPROC_SCRATCH_MAX_BYTES or 4 GiB). Exceeding it fails with "
            "exit 4 before publication. It does not cap Python/R/Arrow "
            "memory; on Cloud Run scratch may consume RAM, so budget "
            "Python, R, scratch and FUSE caches together."
        ),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="miaproc",
        description=(
            "miaproc production CLI — non-interactive long-series "
            "post-processing for batch and cloud-job execution. "
            "Module-aware: file-based eddy work uses 'miaproc run' or the "
            "'miaproc eddy run-silver' / 'run-gold' stages."
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
            "Module-aware namespace for eddy-covariance commands: "
            "file-based silver/gold stages and published-run validation "
            "(see docs/RUN_PUBLICATION.md)."
        ),
    )
    eddy_sub = eddy.add_subparsers(dest="eddy_command", metavar="<eddy_command>")
    eddy_sub.required = True

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
            "orchestrator that wraps the same module logic. Inputs are "
            "CSV directories (--flux-dir/--biomet-dir) or explicit "
            "single Parquet files (--flux-parquet/--biomet-parquet); "
            "exactly one source per side."
        ),
    )
    # M002-S02: each side takes exactly one source - a CSV directory
    # (documented legacy compatibility) or one explicit Parquet file.
    flux_src = eddy_silver.add_mutually_exclusive_group(required=True)
    flux_src.add_argument(
        "--flux-dir",
        type=Path,
        help="Directory of flux CSV files (every *.csv, combined).",
    )
    flux_src.add_argument(
        "--flux-parquet",
        type=Path,
        help=(
            "One flux Parquet file (.parquet/.pq) on a local or mounted "
            "path. Needs 'timestamp' (or 'date' + 'time'), co2_flux, "
            "qc_co2_flux, air_temperature, VPD and u_star (or 'u.'). "
            "URI schemes such as gs:// or s3:// are rejected."
        ),
    )
    biomet_src = eddy_silver.add_mutually_exclusive_group(required=True)
    biomet_src.add_argument(
        "--biomet-dir",
        type=Path,
        help="Directory of biomet CSV files (every *.csv, combined).",
    )
    biomet_src.add_argument(
        "--biomet-parquet",
        type=Path,
        help=(
            "One biomet Parquet file (.parquet/.pq). Needs 'timestamp' "
            "(or 'date' + 'time'), SWIN_1_1_1, P_RAIN_1_1_1 and RH_1_1_1."
        ),
    )
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
            "longer a CLI option. Labels whose per-group file names "
            "collide (e.g. 'S/1' and 's 1') are refused (exit 4)."
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
        help=(
            "Rows to skip at the top of each flux CSV (1 if a units row "
            "is present). CSV directories only; rejected with --flux-parquet."
        ),
    )
    eddy_silver.add_argument(
        "--skip-biomet",
        type=int,
        default=0,
        help=(
            "Rows to skip at the top of each biomet CSV. CSV directories "
            "only; rejected with --biomet-parquet."
        ),
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
        help=(
            "Output silver table path. .parquet/.pq writes the source-facing "
            "silver contract (one 'timestamp', no 'DateTime', source "
            "identity carried; docs/PARQUET_FILE_SCHEMA.md); .csv writes "
            "the legacy CSV shape."
        ),
    )
    eddy_silver.add_argument(
        "--output-run-json",
        required=True,
        type=Path,
        help="Output path for silver run-metadata JSON.",
    )
    _add_publication_args(eddy_silver)

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
            "Python-only local CSV testing without R. A .parquet/.pq "
            "--output-table writes the source-facing gold contract (one "
            "'timestamp', no 'DateTime', gold_derived names nee_f/"
            "nee_fqc/sw_in_f/ta_f/vpd_f + dateAndTime; docs/"
            "PARQUET_FILE_SCHEMA.md); .csv keeps the legacy CSV shape."
        ),
    )
    eddy_gold.add_argument(
        "--silver-table",
        required=True,
        type=Path,
        help=(
            "Path to a silver-stage table written by 'miaproc eddy "
            "run-silver' (.csv, or one .parquet/.pq file validated for a "
            "'timestamp' column, unique fields and at least one row)."
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
            "selection is no longer a CLI option. Labels whose per-group "
            "file names collide are refused (exit 4)."
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
    _add_publication_args(eddy_gold)
    eddy_gold.add_argument(
        "--allow-legacy-silver-csv",
        action="store_true",
        help=(
            "Explicit legacy compatibility: accept a CSV --silver-table that "
            "has no completion manifest (pre-M002-S03 output). Parquet silver "
            "and any silver inside a published run are always validated "
            "against the run manifest."
        ),
    )

    eddy_validate = eddy_sub.add_parser(
        "validate-run",
        help="Validate a published silver/gold run directory (manifest + checksums).",
        description=(
            "Readback/consumer check: the run directory must hold a valid "
            "completion manifest whose artifacts exist with matching sizes "
            "and SHA-256 checksums. Exit 0 prints a JSON summary; an "
            "incomplete or mismatched run exits 3."
        ),
    )
    eddy_validate.add_argument("--run-dir", required=True, type=Path)
    eddy_validate.add_argument("--stage", choices=("silver", "gold"), default=None)

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


def _write_json(path: Path, payload: dict[str, Any], *, publisher: Any = None) -> None:
    """Write ``payload`` as JSON; through ``publisher``'s byte-limited scratch when given."""
    path.parent.mkdir(parents=True, exist_ok=True)
    opened = publisher.open_staged_text(path) if publisher is not None else path.open("w", encoding="utf-8")
    with opened as fh:
        json.dump(_json_safe(payload), fh, indent=2, sort_keys=True)
        fh.write("\n")


def _write_table(path: Path, df: Any, *, publisher: Any = None) -> str:
    """Write the processed table to ``path`` using the format implied
    by its extension. Returns the format name written (``csv`` or
    ``parquet``). With ``publisher`` (M002-S03), ``path`` is a staged
    scratch file and every byte written is charged to its scratch limit.

    Raises a ``ValueError`` (treated as validation failure by the
    caller) when the extension is unsupported.
    """
    suffix = path.suffix.lower()
    path.parent.mkdir(parents=True, exist_ok=True)
    if suffix == ".csv":
        if publisher is None:
            df.to_csv(path, index=False)
        else:
            with publisher.open_staged_text(path, newline="") as fh:
                df.to_csv(fh, index=False)
        return "csv"
    if suffix in (".parquet", ".pq"):
        # PyArrow explicitly, no index column (M002-S02).
        from miaproc.eddy.io import write_parquet_table

        if publisher is None:
            write_parquet_table(path, df)
        else:
            with publisher.open_staged(path) as fh:
                write_parquet_table(path, df, sink=fh)
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
    }
    try:
        from importlib.metadata import PackageNotFoundError, version

        for name in (
            "miaproc",
            "pandas",
            "numpy",
            "hesseflux",
            "rpy2",
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


def _reject_colliding_group_filenames(categories: list[Any], group_column: str) -> None:
    """M002-S02: refuse categories whose sanitized per-group file names
    collide (e.g. ``'S/1'`` and ``'s 1'``), which would otherwise make two
    groups share and overwrite one per-group artefact."""
    by_name: dict[str, list[Any]] = {}
    for category in categories:
        by_name.setdefault(_sanitize_for_filename(category), []).append(category)
    clashes = {k: v for k, v in by_name.items() if len(v) > 1}
    if clashes:
        raise ValueError(
            f"--group-column {group_column!r} values map to the same per-group "
            f"file name: {clashes}. Relabel the categories so each is distinct "
            "after lower-casing and replacing non-alphanumerics with '_'."
        )


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
        # M002-S02: explicit PyArrow read with schema/row validation
        # (ParquetInputError is a ValueError -> exit 3).
        from miaproc.eddy.io import read_parquet_table

        return read_parquet_table(path, flag="--silver-table", role="silver")
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
    redundant-passthrough rule inside the source-facing payload helpers
    (:mod:`miaproc.eddy.payload`) drops the internal ``DateTime`` from the
    published gold payload.

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


def _is_parquet_path(path: Optional[Path]) -> bool:
    return path is not None and path.suffix.lower() in (".parquet", ".pq")


def _reject_uri_args(args: argparse.Namespace, flags: tuple[str, ...]) -> None:
    """M002-S02: path flags accept local/mounted paths only (exit 3 on URIs)."""
    from miaproc.eddy.io import reject_uri

    for flag in flags:
        value = getattr(args, flag.lstrip("-").replace("-", "_"), None)
        if value is not None:
            reject_uri(value, flag=flag)


def _group_rows(df: Any, col: str, category: Any) -> Any:
    """Rows of ``df`` whose ``col`` equals ``category`` (nulls never match)."""
    mask = df[col].eq(category).fillna(False).astype(bool)
    return df.loc[mask].reset_index(drop=True)


def _with_attrs(payload: Any, source: Any, actions: list) -> Any:
    """Keep the backend diagnostics of ``source`` on ``payload`` and add
    the collision actions (they are also persisted to the run JSON)."""
    from miaproc.eddy.payload import COLUMN_COLLISION_ATTRS_KEY

    payload.attrs = {**dict(getattr(source, "attrs", {}) or {}),
                     COLUMN_COLLISION_ATTRS_KEY: list(actions)}
    return payload


def _validate_eddy_silver_args(args: argparse.Namespace) -> None:
    _reject_uri_args(args, ("--flux-dir", "--flux-parquet", "--biomet-dir", "--biomet-parquet",
                            "--output-table", "--output-run-json", "--output-groups-dir",
                            "--run-dir", "--scratch-dir"))
    for flag, value in (("--flux-dir", args.flux_dir), ("--biomet-dir", args.biomet_dir),
                        ("--flux-parquet", args.flux_parquet),
                        ("--biomet-parquet", args.biomet_parquet)):
        if value is not None and not value.exists():
            raise ValueError(f"{flag} does not exist: {value}")
    if args.flux_parquet is not None and args.skip_flux:
        raise ValueError("--skip-flux applies to CSV directories only; it cannot be used with --flux-parquet.")
    if args.biomet_parquet is not None and args.skip_biomet:
        raise ValueError("--skip-biomet applies to CSV directories only; it cannot be used with --biomet-parquet.")
    _check_output_extension(args.output_table)


def _read_silver_inputs(args: argparse.Namespace) -> tuple[Any, Any]:
    """Read the raw flux and biomet frames (Parquet file or CSV directory)."""
    from miaproc.eddy import read_and_combine_csv
    from miaproc.eddy.io import read_parquet_table

    if args.flux_parquet is not None:
        flux = read_parquet_table(args.flux_parquet, flag="--flux-parquet", role="flux")
    else:
        flux = read_and_combine_csv(Path(str(args.flux_dir)), skip_rows=args.skip_flux)
    if args.biomet_parquet is not None:
        biomet = read_parquet_table(args.biomet_parquet, flag="--biomet-parquet", role="biomet")
    else:
        biomet = read_and_combine_csv(Path(str(args.biomet_dir)), skip_rows=args.skip_biomet)
    return flux, biomet


def _run_dir_arg(args: argparse.Namespace) -> Path:
    return args.run_dir if args.run_dir is not None else args.output_table.parent


def _publication_record(pub: Any) -> dict[str, Any]:
    from miaproc.eddy import publish

    return {"run_dir": str(pub.run_dir_arg), "manifest": publish.MANIFEST_NAME,
            "manifest_version": publish.MANIFEST_VERSION,
            "scratch_max_bytes": pub.scratch_max_bytes}


def _publish_or_exit_code(pub: Any, *, run_metadata: Path, diagnostics: Optional[Path] = None) -> int:
    """Publish staged artifacts (manifest last); map outcomes to exit codes."""
    from miaproc.eddy import publish

    try:
        pub.publish(run_metadata=run_metadata, diagnostics=diagnostics)
    except publish.AmbiguousPublicationError as exc:
        logger.error("PUBLICATION AMBIGUOUS (exit %d): %s", PUBLICATION_AMBIGUOUS_EXIT, exc)
        return PUBLICATION_AMBIGUOUS_EXIT
    logger.info("Published run %s (%s written last).", pub.run_dir_arg, publish.MANIFEST_NAME)
    return SUCCESS_EXIT


def _run_eddy_silver_command(args: argparse.Namespace) -> int:
    """Run the silver stage into a fresh, manifest-published run directory (M002-S03)."""
    from miaproc.eddy import publish

    try:
        _validate_eddy_silver_args(args)
        outputs = {"--output-table": args.output_table, "--output-run-json": args.output_run_json}
        if args.output_groups_dir is not None:
            outputs["--output-groups-dir"] = args.output_groups_dir
        inputs = [v for v in (args.flux_dir, args.biomet_dir, args.flux_parquet, args.biomet_parquet)
                  if v is not None]
        pub = publish.RunPublisher(stage="silver", run_dir=_run_dir_arg(args), outputs=outputs,
                                   inputs=inputs, scratch_dir=args.scratch_dir,
                                   scratch_max_bytes=args.scratch_max_bytes)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT
    try:
        return _run_eddy_silver_staged(args, pub)
    finally:
        pub.cleanup_scratch()


def _run_eddy_silver_staged(args: argparse.Namespace, pub: Any) -> int:
    """Run the silver-stage eddy command (M14, M24; Parquet inputs M002-S02).

    M002-S03: every artifact is written to ``pub``'s scratch and published
    to the fresh run directory only after the whole run succeeded.
    """
    started_at = _utc_now_iso()
    parquet_input = args.flux_parquet is not None or args.biomet_parquet is not None
    raw: Optional[tuple[Any, Any]] = None
    try:
        _validate_eddy_silver_args(args)
        if parquet_input:
            raw = _read_silver_inputs(args)
    except ValueError as exc:  # includes ParquetInputError
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT
    except (OSError, RuntimeError) as exc:
        # CSV-directory side of a mixed CSV/Parquet run (no *.csv, unreadable
        # file): the same runtime failure exit as the pure CSV-directory path.
        logger.error("Runtime processing failure: %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT

    source_facing = _is_parquet_path(args.output_table)
    group_column: Optional[str] = args.group_column
    groups_dir = (
        _resolve_groups_dir(args.output_table, args.output_groups_dir)
        if group_column is not None
        else None
    )
    per_group_records: list[dict[str, Any]] = []
    null_category_rows = 0
    silver_frames: list[Any] = []
    collision_actions: list[dict[str, Any]] = []
    input_rows: dict[str, Optional[int]] = {"flux": None, "biomet": None}
    # Rows stage 1 drops as repeated instants (keep-first) or as unparsed
    # times, per input side and counting each source row once; null when the
    # CLI does not hold the raw frames (ungrouped CSV-directory path).
    duplicates: dict[str, Optional[int]] = {"flux": None, "biomet": None}
    unparsed: dict[str, Optional[int]] = {"flux": None, "biomet": None}

    try:
        from miaproc.eddy import (
            apply_silver_source_truth_rename,
            load_stage1,
            load_stage1_from_dataframes,
            stage1_from_raw_frames,
        )
        from miaproc.eddy.payload import prepare_local_silver_payload
        from miaproc.eddy.time import instant_drop_counts

        def drop_counts(flux_df: Any, biomet_df: Any) -> dict[str, dict[str, int]]:
            """``{"duplicate": {"flux": n, "biomet": m}, "unparsed": {...}}``."""
            per_side = {side: instant_drop_counts(frame, tz_in=args.tz_in, tz_out=args.tz_out)
                        for side, frame in (("flux", flux_df), ("biomet", biomet_df))}
            return {kind: {side: c[kind] for side, c in per_side.items()} for kind in ("duplicate", "unparsed")}

        def finish(stage1: Any) -> tuple[Any, list[dict[str, Any]]]:
            # Parquet output: source-facing payload (M002-S02);
            # CSV output: the legacy M32/M32A CSV shape (unchanged).
            if source_facing:
                return prepare_local_silver_payload(stage1)
            return apply_silver_source_truth_rename(stage1), []

        stage1_kwargs = dict(tz_in=args.tz_in, tz_out=args.tz_out, site_id=None, drop_rain_rows=False)
        if raw is not None:
            input_rows = {"flux": int(len(raw[0])), "biomet": int(len(raw[1]))}

        if group_column is None:
            logger.info(
                "Silver stage-1 load (ungrouped): flux=%s biomet=%s",
                args.flux_parquet or args.flux_dir,
                args.biomet_parquet or args.biomet_dir,
            )
            if raw is None:
                silver = load_stage1(
                    path_full_output=str(args.flux_dir),
                    path_biomet=str(args.biomet_dir),
                    skip_full_output=args.skip_flux,
                    skip_biomet=args.skip_biomet,
                    **stage1_kwargs,
                )
            else:
                # M002-S02: Parquet frames reuse the in-memory entrypoint.
                counts = drop_counts(raw[0], raw[1])
                duplicates, unparsed = counts["duplicate"], counts["unparsed"]
                silver = load_stage1_from_dataframes(
                    flux_df=raw[0], biomet_df=raw[1], **stage1_kwargs
                )
            silver, collision_actions = finish(silver)
            silver_frames.append(silver)
        else:
            logger.info(
                "Silver grouped run: flux=%s biomet=%s group_column=%s",
                args.flux_parquet or args.flux_dir,
                args.biomet_parquet or args.biomet_dir,
                group_column,
            )
            if raw is None:
                full_output, biomet = _read_silver_inputs(args)
                run_stage1 = stage1_from_raw_frames
            else:
                full_output, biomet = raw

                def run_stage1(flux_df, biomet_df, **kw):
                    return load_stage1_from_dataframes(flux_df=flux_df, biomet_df=biomet_df, **kw)
            _validate_group_column(full_output, group_column, side="flux")
            categories, null_category_rows = _iter_categories(
                full_output, group_column
            )
            if not categories:
                raise ValueError(
                    f"No non-null values for --group-column "
                    f"{group_column!r} in flux input."
                )
            _reject_colliding_group_filenames(categories, group_column)
            assert groups_dir is not None
            duplicates = {"flux": 0, "biomet": 0}
            unparsed = {"flux": 0, "biomet": 0}
            shared_biomet = group_column not in biomet.columns
            if shared_biomet:
                # One biomet frame serves every group: count its rows once.
                shared = drop_counts(full_output.iloc[:0], biomet)
                duplicates["biomet"] = shared["duplicate"]["biomet"]
                unparsed["biomet"] = shared["unparsed"]["biomet"]
            for category in categories:
                logger.info(
                    "Group %s=%r: silver stage-1", group_column, category
                )
                flux_group = _group_rows(full_output, group_column, category)
                if group_column in biomet.columns:
                    biomet_group = _group_rows(biomet, group_column, category)
                else:
                    biomet_group = biomet
                group_counts = drop_counts(flux_group, biomet_group)
                group_duplicates = group_counts["duplicate"]
                for side in ("flux",) if shared_biomet else ("flux", "biomet"):
                    duplicates[side] += group_counts["duplicate"][side]
                    unparsed[side] += group_counts["unparsed"][side]
                silver_group = run_stage1(flux_group, biomet_group, **stage1_kwargs)
                silver_group, group_actions = finish(silver_group)
                collision_actions.extend(
                    {**a, "category_value": category} for a in group_actions
                )
                silver_frames.append(silver_group)
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="silver",
                )
                _write_table(pub.staged(per_group_table_path, role="group_table",
                                        category_value=str(category),
                                        expected_rows=int(len(silver_group))), silver_group,
                             publisher=pub)
                per_group_records.append(
                    {
                        "category_value": category,
                        "silver_rows": int(len(silver_group)),
                        "duplicate_instants_dropped": group_duplicates,
                        "unparsed_time_rows_dropped": group_counts["unparsed"],
                        "table_path": str(per_group_table_path),
                        "column_collision_actions": group_actions,
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
        table_format = _write_table(
            pub.staged(args.output_table, role="table", expected_rows=int(len(silver))), silver,
            publisher=pub)
        ended_at = _utc_now_iso()
        run_metadata: dict[str, Any] = {
            "stage": "silver",
            "timestamps": {"started_at": started_at, "ended_at": ended_at},
            "row_counts": {
                "silver": int(len(silver)),
                "null_category_rows_skipped": int(null_category_rows),
                "flux_input": input_rows["flux"],
                "biomet_input": input_rows["biomet"],
                "duplicate_instants_dropped": duplicates,
                "unparsed_time_rows_dropped": unparsed,
            },
            "column_counts": {"silver": int(len(silver.columns))},
            "silver_columns": [str(c) for c in silver.columns],
            "inputs": {
                "mode": "file",
                "flux_format": "parquet" if args.flux_parquet is not None else "csv_dir",
                "biomet_format": "parquet" if args.biomet_parquet is not None else "csv_dir",
                "flux_dir": str(args.flux_dir) if args.flux_dir is not None else None,
                "biomet_dir": str(args.biomet_dir) if args.biomet_dir is not None else None,
                "flux_parquet": str(args.flux_parquet) if args.flux_parquet is not None else None,
                "biomet_parquet": str(args.biomet_parquet) if args.biomet_parquet is not None else None,
                "group_column": group_column,
                "skip_flux": int(args.skip_flux),
                "skip_biomet": int(args.skip_biomet),
                "tz_in": args.tz_in,
                "tz_out": args.tz_out,
            },
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "contract": "source-facing" if source_facing else "legacy-csv",
                "run_json": str(args.output_run_json),
                "groups_dir": str(groups_dir) if groups_dir else None,
            },
            "publication": _publication_record(pub),
            "column_collision_actions": collision_actions,
            "groups": per_group_records,
            "group_column": group_column,
            "versions": _package_versions(),
            "exit_code": SUCCESS_EXIT,
        }
        _write_json(pub.staged(args.output_run_json, role="run_metadata"), run_metadata, publisher=pub)
        code = _publish_or_exit_code(pub, run_metadata=args.output_run_json)
    except Exception as exc:
        logger.error("Failed to write or publish silver artifacts (nothing published): %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT
    if code != SUCCESS_EXIT:
        return code

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
    _reject_uri_args(args, ("--silver-table", "--output-table", "--output-diagnostics-json",
                            "--output-run-json", "--output-groups-dir", "--run-dir", "--scratch-dir"))
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


def _validate_silver_source(args: argparse.Namespace) -> dict[str, Any]:
    """M002-S03 consumer check: silver must come from a valid published run.

    The nearest ancestor holding a completion manifest is the silver run;
    its manifest must validate (sizes + SHA-256) and list ``--silver-table``.
    Only an explicit ``--allow-legacy-silver-csv`` accepts an unpublished
    CSV silver (legacy compatibility path). Raises ``ValueError``.
    """
    from miaproc.eddy import publish

    run_dir = publish.find_run_dir(args.silver_table)
    if run_dir is None:
        if args.allow_legacy_silver_csv and args.silver_table.suffix.lower() == ".csv":
            logger.warning("Legacy CSV silver without a completion manifest accepted "
                           "(--allow-legacy-silver-csv): %s", args.silver_table)
            return {"mode": "legacy-csv", "run_dir": None}
        raise ValueError(
            f"--silver-table {args.silver_table} is not inside a published run (no "
            f"{publish.MANIFEST_NAME} found): the silver run is incomplete or was not produced "
            "by 'eddy run-silver'. Re-run silver into a fresh run directory; a pre-M002-S03 CSV "
            "silver needs the explicit --allow-legacy-silver-csv.")
    manifest = publish.validate_run(run_dir, stage="silver")
    record = publish.artifact_record(manifest, run_dir, args.silver_table)
    if record.get("role") != "table":
        raise ValueError(f"--silver-table {args.silver_table} is a {record.get('role')!r} artifact, "
                         "not the run's silver table.")
    return {"mode": "published-run", "run_dir": str(run_dir), "manifest": publish.MANIFEST_NAME,
            "table": record["path"], "sha256": record["sha256"], "rows": record.get("rows")}


def _run_eddy_gold_command(args: argparse.Namespace) -> int:
    """Run the gold stage into a fresh, manifest-published run directory (M002-S03)."""
    from miaproc.eddy import publish

    try:
        _validate_eddy_gold_args(args)
        silver_source = _validate_silver_source(args)
        outputs = {"--output-table": args.output_table,
                   "--output-diagnostics-json": args.output_diagnostics_json,
                   "--output-run-json": args.output_run_json}
        if args.output_groups_dir is not None:
            outputs["--output-groups-dir"] = args.output_groups_dir
        inputs = [args.silver_table] + ([Path(silver_source["run_dir"])] if silver_source["run_dir"] else [])
        pub = publish.RunPublisher(stage="gold", run_dir=_run_dir_arg(args), outputs=outputs,
                                   inputs=inputs, scratch_dir=args.scratch_dir,
                                   scratch_max_bytes=args.scratch_max_bytes)
    except ValueError as exc:  # includes RunValidationError
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT
    try:
        return _run_eddy_gold_staged(args, pub, silver_source)
    finally:
        pub.cleanup_scratch()


def _run_eddy_validate_command(args: argparse.Namespace) -> int:
    """``eddy validate-run``: consumer/readback validation of a published run."""
    from miaproc.eddy import publish

    try:
        manifest = publish.validate_run(args.run_dir, stage=args.stage)
        summary = {
            "ok": True, "run_dir": str(args.run_dir), "stage": manifest["stage"],
            "manifest_version": manifest["manifest_version"],
            "run_metadata": manifest["run_metadata"], "diagnostics": manifest["diagnostics"],
            "artifacts": [{k: rec.get(k) for k in ("path", "role", "rows", "size", "sha256")}
                          for rec in manifest["artifacts"]],
        }
    except KeyError as exc:
        # M002-S03-F8: a manifest without a required key is a malformed run
        # (validation failure, exit 3), not a crash.
        logger.error("Run validation failed: manifest lacks required key %s", exc)
        print(json.dumps({"ok": False, "run_dir": str(args.run_dir),
                          "error": f"manifest lacks required key {exc}"}))
        return VALIDATION_EXIT
    except (ValueError, OSError) as exc:
        logger.error("Run validation failed: %s", exc)
        print(json.dumps({"ok": False, "run_dir": str(args.run_dir), "error": str(exc)}))
        return VALIDATION_EXIT
    print(json.dumps(summary, sort_keys=True))
    return SUCCESS_EXIT


def _run_eddy_gold_staged(args: argparse.Namespace, pub: Any, silver_source: dict[str, Any]) -> int:
    """Run the gold-stage eddy command (M14, M24; Parquet contract M002-S02).

    M002-S03: artifacts are staged in ``pub``'s scratch and published only
    after the whole run succeeded (manifest last).
    """
    started_at = _utc_now_iso()
    try:
        _validate_eddy_gold_args(args)
    except ValueError as exc:
        logger.error("Argument validation failed: %s", exc)
        return VALIDATION_EXIT
    silver: Any = None
    if _is_parquet_path(args.silver_table):
        # M002-S02: a Parquet silver is read and validated (schema, rows)
        # before any preflight or engine work; failures are input errors.
        try:
            silver = _read_silver_table(args.silver_table)
        except ValueError as exc:
            logger.error("Silver input validation failed: %s", exc)
            return VALIDATION_EXIT

    preflight_record: Optional[dict[str, Any]] = None
    if args.engine == "reddyproc-reference":
        preflight_record = _run_preflight_or_exit(args.repo_root)

    source_facing = _is_parquet_path(args.output_table)
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
    collision_actions: list[dict[str, Any]] = []
    silver_total_rows = 0
    silver_total_cols = 0

    try:
        from miaproc.eddy import silver_to_internal_calc_frame
        from miaproc.eddy.payload import prepare_local_gold_payload

        def finish(gold_with_silver: Any) -> tuple[Any, list[dict[str, Any]]]:
            # Parquet output: source-facing gold payload (M002-S02);
            # CSV output: the legacy CSV shape (unchanged).
            if not source_facing:
                return gold_with_silver, []
            payload, actions = prepare_local_gold_payload(gold_with_silver)
            return _with_attrs(payload, gold_with_silver, actions), actions

        logger.info("Gold reading silver: %s", args.silver_table)
        if silver is None:
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
            gold_with_silver, collision_actions = finish(gold_with_silver)
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
            _reject_colliding_group_filenames(categories, group_column)
            assert groups_dir is not None
            extras_set: set[str] = set()
            for category in categories:
                logger.info(
                    "Group %s=%r: gold engine=%s",
                    group_column,
                    category,
                    args.engine,
                )
                silver_group = _group_rows(silver, group_column, category)
                internal_silver_group = silver_to_internal_calc_frame(
                    silver_group
                )
                gold_group, config_record = _dispatch_engine(
                    args.engine, internal_silver_group, args
                )
                gold_with_silver_group, silver_extras_g = (
                    _attach_silver_columns_to_gold(gold_group, silver_group)
                )
                gold_with_silver_group, group_actions = finish(gold_with_silver_group)
                collision_actions.extend(
                    {**a, "category_value": category} for a in group_actions
                )
                gold_frames.append(gold_with_silver_group)
                extras_set.update(silver_extras_g)
                per_group_table_path = _per_group_table_path(
                    groups_dir,
                    output_table=args.output_table,
                    category=category,
                    role="gold",
                )
                _write_table(pub.staged(per_group_table_path, role="group_table",
                                        category_value=str(category),
                                        expected_rows=int(len(gold_with_silver_group))),
                             gold_with_silver_group, publisher=pub)
                per_group_diag_path = _per_group_diagnostics_path(
                    groups_dir, category=category
                )
                per_group_diag = (
                    getattr(gold_with_silver_group, "attrs", {}) or {}
                ).get("miaproc_diagnostics") or {}
                _write_json(pub.staged(per_group_diag_path, role="group_diagnostics",
                                       category_value=str(category)), dict(per_group_diag),
                            publisher=pub)
                per_group_records.append(
                    {
                        "category_value": category,
                        "silver_input_rows": int(len(silver_group)),
                        "gold_output_rows": int(
                            len(gold_with_silver_group)
                        ),
                        "table_path": str(per_group_table_path),
                        "diagnostics_path": str(per_group_diag_path),
                        "column_collision_actions": group_actions,
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
        table_format = _write_table(
            pub.staged(args.output_table, role="table", expected_rows=int(len(gold_with_silver))),
            gold_with_silver, publisher=pub)
        diag_stage = pub.staged(args.output_diagnostics_json, role="diagnostics")
        if group_column is None:
            diagnostics = (
                getattr(gold_with_silver, "attrs", {}) or {}
            ).get("miaproc_diagnostics") or {}
            _write_json(diag_stage, dict(diagnostics), publisher=pub)
        else:
            _write_json(
                diag_stage,
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
                publisher=pub,
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
            "gold_columns": [str(c) for c in gold_with_silver.columns],
            "inputs": {
                "mode": "file",
                "silver_table": str(args.silver_table),
                "silver_format": "parquet" if _is_parquet_path(args.silver_table) else "csv",
                "silver_publication": silver_source,
                "group_column": group_column,
            },
            "outputs": {
                "table": str(args.output_table),
                "table_format": table_format,
                "contract": "source-facing" if source_facing else "legacy-csv",
                "diagnostics_json": str(args.output_diagnostics_json),
                "run_json": str(args.output_run_json),
                "groups_dir": str(groups_dir) if groups_dir else None,
            },
            "publication": _publication_record(pub),
            "column_collision_actions": collision_actions,
            "groups": per_group_records,
            "group_column": group_column,
            "versions": _package_versions(),
            "exit_code": SUCCESS_EXIT,
        }
        if preflight_record is not None:
            run_metadata["preflight"] = preflight_record
        _write_json(pub.staged(args.output_run_json, role="run_metadata"), run_metadata, publisher=pub)
        code = _publish_or_exit_code(pub, run_metadata=args.output_run_json,
                                     diagnostics=args.output_diagnostics_json)
    except Exception as exc:
        logger.error("Failed to write or publish gold artifacts (nothing published): %s", exc)
        traceback.print_exc()
        return RUNTIME_EXIT
    if code != SUCCESS_EXIT:
        return code

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


# M002-S04: the BigQuery transport was removed. Its former commands and
# flags fail clearly (exit 3, no output written) instead of falling back to
# argparse's generic usage error or to any other storage path.
REMOVED_BIGQUERY_COMMANDS = ("run-bigquery", "run-bigquery-silver", "run-bigquery-gold")
REMOVED_BIGQUERY_FLAG_PREFIXES = ("--bq-", "--stage-payload-dry-run-dir")
MIGRATION_GUIDE = "docs/MIGRATING_FROM_BIGQUERY.md"


def _removed_bigquery_token(argv: list[str]) -> Optional[str]:
    """Return the first removed BigQuery command or flag in ``argv``.

    Removed flags match anywhere (option names start with ``--``); removed
    command names match only in the eddy subcommand position (the first
    non-option token after a leading ``eddy``), so an option value such as
    ``--run-dir run-bigquery`` is not mistaken for a removed command.
    """
    positionals: list[str] = []
    for token in argv:
        if token == "--":
            break
        if token.startswith(REMOVED_BIGQUERY_FLAG_PREFIXES):
            return token.split("=", 1)[0]
        if not token.startswith("-") and len(positionals) < 2:
            positionals.append(token)
    if len(positionals) == 2 and positionals[0] == "eddy" and positionals[1] in REMOVED_BIGQUERY_COMMANDS:
        return positionals[1]
    return None


def main(argv: Optional[list[str]] = None) -> int:
    _configure_logging()
    tokens = list(sys.argv[1:] if argv is None else argv)
    removed = _removed_bigquery_token(tokens)
    if removed is not None:
        print(
            f"miaproc: error: {removed!r} was removed in M002-S04 (BigQuery "
            "transport). Select explicit Parquet/CSV inputs and use "
            "'miaproc eddy run-silver' / 'miaproc eddy run-gold' with a fresh "
            "--run-dir, then 'miaproc eddy validate-run'. "
            f"See {MIGRATION_GUIDE}.",
            file=sys.stderr,
        )
        return VALIDATION_EXIT
    parser = _build_parser()
    args = parser.parse_args(tokens)
    if args.command == "run":
        return _run_command(args)
    if args.command == "eddy":
        if args.eddy_command == "run-silver":
            return _run_eddy_silver_command(args)
        if args.eddy_command == "run-gold":
            return _run_eddy_gold_command(args)
        if args.eddy_command == "validate-run":
            return _run_eddy_validate_command(args)
        parser.error(f"Unknown eddy command: {args.eddy_command!r}")
        return VALIDATION_EXIT  # pragma: no cover - argparse exits first
    parser.error(f"Unknown command: {args.command!r}")
    return VALIDATION_EXIT  # pragma: no cover - argparse exits first


if __name__ == "__main__":
    sys.exit(main())
