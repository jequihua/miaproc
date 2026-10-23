# Migrating from the BigQuery commands to file runs (M002-S04)

miaproc no longer talks to BigQuery. M002-S04 removed the BigQuery transport:

- the modules `miaproc.eddy.bigquery_runner` and `miaproc.eddy.bigquery_writeback`
  and their re-exports from `miaproc.eddy`;
- the `bigquery` install extra and its Google SDK dependencies
  (`google-cloud-bigquery`, `google-cloud-bigquery-storage`, `db-dtypes`);
- the CLI commands `miaproc eddy run-bigquery`, `miaproc eddy run-bigquery-silver`
  and `miaproc eddy run-bigquery-gold`, every `--bq-*` flag and
  `--stage-payload-dry-run-dir`.

Invoking a removed command or flag exits with code `3` (validation failure)
and a message on stderr that names the removed surface and points to this
guide and to the file workflow. There is no fallback: miaproc never reads or
writes a table, never looks up Google credentials and needs no network for
data processing.

Nothing scientific changed. The engines, defaults, stage-1 cleaning and
regularization, grouping, diagnostics and the source-facing column contract
(one `timestamp`, `air_temperature_c`, `VPD_hpa`, `co2_flux`, `u_star`,
`SWIN_1_1_1`, `P_RAIN_1_1_1`, `RH_1_1_1`, flux-side `RH`, every other unique
flux column, source `primary_key`/`site_id` carried unchanged) are the same;
they now apply to Parquet silver and gold files
([PARQUET_FILE_SCHEMA.md](PARQUET_FILE_SCHEMA.md)).

## The new shape

| BigQuery-era responsibility | File-workflow replacement | Owner |
|---|---|---|
| Read flux/biomet source tables | Export the selected rows to one immutable Parquet file per side and pass `--flux-parquet` / `--biomet-parquet` | Engineer/orchestrator (export); miaproc (read) |
| Read a silver stage table for gold | `--silver-table` pointing at the table of a **published** silver run | miaproc validates the silver run's manifest |
| Time window (`--bq-start-timestamp` / `--bq-end-timestamp`) | Choose the window when producing the input files; miaproc processes every row of the files it is given | Engineer/orchestrator |
| Per-site watermark table (`cf_s2_watermark`) | The orchestrator records which windows it has processed (for example from the `timestamp` range of a validated gold run) | Engineer/orchestrator |
| Stage-table writeback + validation SQL | A fresh run directory per invocation, staged and checked in local scratch, with a completion manifest written last | miaproc |
| Run-record table (`cf_s2_runs`) | The run JSON plus `_MIAPROC_RUN_COMPLETE.json` inside each run directory | miaproc writes; orchestrator indexes |
| `MERGE` into a final table | No miaproc equivalent. Consumers load only runs that pass `miaproc eddy validate-run`; any warehouse load, upsert or deduplication is outside miaproc | Engineer |
| Stage-payload dry run | The published Parquet silver/gold, its run JSON (output columns, `column_collision_actions`) and the manifest are the payload; run into a throwaway run directory to inspect | miaproc |

Engineers own mounts, IAM, service resources, scheduling and any loading of
results elsewhere. miaproc owns the processing and a conservative
publication of each run.

## Command mapping

| Removed | Use instead |
|---|---|
| `miaproc eddy run-bigquery-silver` | `miaproc eddy run-silver --flux-parquet FILE --biomet-parquet FILE ...` |
| `miaproc eddy run-bigquery-gold` | `miaproc eddy run-gold --silver-table <silver run>/silver.parquet ...` |
| `miaproc eddy run-bigquery` (one-shot) | `run-silver` then `run-gold` (two processes, two run directories). `miaproc run` remains the single-shot CSV-directory command. |
| — | `miaproc eddy validate-run --run-dir DIR [--stage silver\|gold]` before consuming any run |

## Flag mapping

| Removed flag | Replacement |
|---|---|
| `--bq-input-project`, `--bq-input-dataset`, `--bq-flux-table`, `--bq-biomet-table`, `--bq-billing-project`, `--bq-no-storage-api` | Export outside miaproc; pass `--flux-parquet` / `--biomet-parquet` (local or mounted paths; `gs://`/`s3://` URIs are rejected) |
| `--bq-silver-table` | `--silver-table` (a published silver run's table) |
| `--bq-start-timestamp`, `--bq-end-timestamp` | Select the window when writing the input files |
| `--bq-output-project`, `--bq-output-dataset`, `--bq-stage-table` | `--run-dir` (fresh, exclusive) with `--output-table`, `--output-run-json` and, for gold, `--output-diagnostics-json` inside it |
| `--bq-final-table`, `--bq-allow-final-merge` | None. Final-table loading is engineer-owned and must use validated runs |
| `--bq-control-dataset` | None. Run records live in each run directory |
| `--bq-run-id` | The run directory name (unique per attempt) |
| `--stage-payload-dry-run-dir` | None. Inspect the published run (or a run in a throwaway directory) |

Unchanged on `run-silver` / `run-gold`: `--group-column`,
`--output-groups-dir`, `--tz-in`, `--tz-out` (silver), `--engine`,
`--repo-root`, `--lt-min-night-samples` and the site metadata flags (gold).
New publication flags: `--run-dir`, `--scratch-dir`, `--scratch-max-bytes`
and, for a pre-M002-S03 CSV silver only, `--allow-legacy-silver-csv`.

Install commands drop the extra: use `pip install miaproc` (PyArrow is a core
dependency) plus `[hesseflux]` and/or `[reddyproc]` as needed. The product
Dockerfile now installs the extras `dev,hesseflux,reddyproc`; a product image
built before M002-S04 still contains the old Google packages until it is
rebuilt.

Python callers: the BigQuery configs, readers and writeback functions are
gone. The pure payload helpers remain in `miaproc.eddy.payload`
(`column_field_key` is the former `bigquery_field_key`; `GOLD_DERIVED_RENAME`
is the former `S2_FILT_1_RENAME_MAP`; `prepare_gold_stage_payload` is the
former `prepare_stage_dataframe` without its BigQuery-only join, column
filter and type cast). The watermark helper `max_timestamps_by_site` was
removed with the writeback module.

## Before and after

Before (removed):

```bash
miaproc eddy run-bigquery-silver --bq-input-project P --bq-input-dataset D \
    --bq-flux-table flux --bq-biomet-table biomet \
    --bq-start-timestamp 2026-01-01T00:00:00Z --bq-end-timestamp 2026-02-01T00:00:00Z \
    --group-column site_id --bq-output-project S --bq-output-dataset D \
    --bq-stage-table cf_s2_silver_stage --bq-control-dataset _orch \
    --output-table out/silver.parquet --output-run-json out/silver_run.json
```

After (one fresh run directory per invocation; inputs are read-only):

```bash
# 1. The orchestrator exports the January window to immutable files
#    /data/in/2026-01/flux.parquet and /data/in/2026-01/biomet.parquet.

# 2. Silver
miaproc eddy run-silver \
    --flux-parquet /data/in/2026-01/flux.parquet \
    --biomet-parquet /data/in/2026-01/biomet.parquet \
    --group-column site_id \
    --run-dir /data/out/silver-2026-01-a1 \
    --output-table /data/out/silver-2026-01-a1/silver.parquet \
    --output-run-json /data/out/silver-2026-01-a1/silver_run.json
miaproc eddy validate-run --run-dir /data/out/silver-2026-01-a1 --stage silver

# 3. Gold (validates the silver run's manifest before any engine work)
miaproc eddy run-gold \
    --silver-table /data/out/silver-2026-01-a1/silver.parquet \
    --engine reddyproc-reference --repo-root /app \
    --group-column site_id \
    --run-dir /data/out/gold-2026-01-a1 \
    --output-table /data/out/gold-2026-01-a1/gold.parquet \
    --output-diagnostics-json /data/out/gold-2026-01-a1/gold_diag.json \
    --output-run-json /data/out/gold-2026-01-a1/gold_run.json
miaproc eddy validate-run --run-dir /data/out/gold-2026-01-a1 --stage gold

# 4. Only now may a consumer load gold.parquet elsewhere (engineer-owned).
```

## Reruns, retries and recomputation

- **Retry after a failure:** start a new run with a new name
  (`silver-2026-01-a2`). Never reuse, delete or overwrite an earlier run
  directory; miaproc refuses an existing `--run-dir` (exit 3).
- **Exit 5 or an unknown exit status:** the manifest may already be valid.
  Decide with `validate-run`, not with the exit code; a valid run stays.
- **Recomputing a window** (late data, corrected sources): export new input
  files, run under new run names and let the consumer choose which validated
  run supersedes the old one (for example by `site_id` + `timestamp`). The
  old `MERGE` did this inside BigQuery; it is now an explicit downstream
  choice.
- **Window effects:** u* thresholds, gap filling and partitioning are fitted
  on the rows of each input (per group), exactly as they were on the rows of
  a BigQuery window. Select windows with the same care as before.
- **Concurrency:** an existence check is not a lock. The orchestrator must
  give each invocation an exclusive run name.

Mounted object storage (Cloud Storage FUSE, S3 drivers) has incomplete POSIX
semantics, delayed error reporting at close and cache visibility limits;
there is no atomic publication across several objects. Read
[RUN_PUBLICATION.md](RUN_PUBLICATION.md) for the publication contract, the
failure/retry table, FUSE caveats and resource budgeting. Each driver and
its settings need engineer qualification; none is performed or claimed by
this package.
