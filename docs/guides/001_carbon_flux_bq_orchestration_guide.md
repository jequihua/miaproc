# Milestone 001 - Carbon Flux BigQuery Orchestration Guide

> Status: implementation guide for a downstream repository. Revised after the milestone 001 and milestone 002 reviews.
> Source of truth for this milestone:
> - production primary input: `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance`
> - production side input: `manglaria.manglaria_lakehouse_ds.carbon_flux_biomet`
> - staging output: `manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1`
>
> The Docker image is treated as a pure file-in / file-out processor and must not be modified by this milestone.

---

## 0. What is confirmed, what is recommended, what is still a decision

This guide mixes three kinds of statements. Read each section with this map in mind.

| Class | Meaning |
| --- | --- |
| Confirmed fact | Backed by a cited live export in `90_legacy_review/exports/20260426_150929/`. Treat as ground truth. |
| Recommended default | A defensible default chosen for the downstream team to start from. Must be reviewable before production cutover. |
| Required pre-implementation decision | Open item. The downstream team or the architect must settle it. The guide does not invent a hard answer. |

Confirmed facts about the source and target tables, the legacy ingestion functions, the regions, and the bucket family suffixes are cited inline. Anything labeled "recommended" or "default" is a starting point, not a verdict. Anything in section 21 is unresolved.

---

## 1. Capability summary

Build an orchestration layer, in a separate downstream repository, that runs an existing Dockerized eddy covariance post-processor against a controlled slice of `carbon_flux_eddycovariance` (and a corresponding slice of `carbon_flux_biomet`) and lands the post-processed result into `carbon_flux_eddycovariance_s2_filt_1`.

The orchestration layer is responsible for everything that surrounds the container:

- selecting deterministic input slices from BigQuery for both flux and biomet
- exporting those slices to GCS as files at the paths the processor expects
- invoking the unchanged container with file inputs only
- loading the container's file outputs into a staging BigQuery table
- merging staging into the final processed BigQuery table on the canonical row identity
- recording run state and advancing a watermark only on success

The container itself is responsible only for the scientific transformation between input files and output files. Its contract is preserved end-to-end.

---

## 2. System context

### 2.1 What already exists (confirmed facts)

- Production BigQuery dataset `manglaria.manglaria_lakehouse_ds` contains:
  - `carbon_flux_eddycovariance` (12,141 rows, ~5 MB logical, location `us-central1`); REQUIRED columns include `primary_key`, `timestamp`, `site_id`. Source: `90_legacy_review/exports/20260426_150929/manglaria/bigquery/datasets/manglaria_lakehouse_ds/carbon_flux_eddycovariance.json`.
  - `carbon_flux_biomet` (12,142 rows, ~2 MB logical, location `us-central1`); REQUIRED columns include `primary_key`, `timestamp`, `site_id`. Source: `.../carbon_flux_biomet.json`.
  - Both tables are populated by the production Cloud Function `carbon-process-file-change`, which is triggered by GCS events on bucket `gcp-manglaria-lakehouse-manglaria_carbon_flux_bucket-3f9b3019` and references all three legacy tables (`carbon_flux_eddycovariance`, `carbon_flux_biomet`, `carbon_flux_site_metadata`). Source: `.../manglaria/functions/functions.json` lines 11-13.
- Staging BigQuery dataset `manglaria-staging.manglaria_lakehouse_ds` contains:
  - `carbon_flux_eddycovariance` with the same schema, currently 0 active rows.
  - `carbon_flux_biomet` with the same schema as production but only 8,048 rows; the staging biomet table is materially smaller than production.
  - The staging carbon function points at all three staging-side tables and at staging bucket family `a5bd2fa5`. Source: `.../manglaria-staging/functions/functions.json` lines 64-67.
- Staging BigQuery dataset contains the target output table `carbon_flux_eddycovariance_s2_filt_1` (currently 0 rows, location `us-central1`). Its schema extends the input schema by adding `dateAndTime` (STRING), `nee_f` (FLOAT), `nee_fqc` (INTEGER), `sw_in_f` (FLOAT), `ta_f` (FLOAT), `vpd_f` (FLOAT). `primary_key`, `timestamp`, and `site_id` are REQUIRED. Source: `.../carbon_flux_eddycovariance_s2_filt_1.json`.
- Both projects operate in `us-central1`. The active Dataplex/runtime bucket family is `3f9b3019` in production and `1aa088d6` in staging for camera trap workloads, but the live carbon flux ingestion bucket in staging is on family `a5bd2fa5`. Treat the carbon flux staging bucket suffix as live but a known drift signal; see `90_legacy_review/legacy_risks.md` risk #12.

### 2.2 Why production is the source of truth for both flux and biomet

- The production primary input has 12,141 rows and the production biomet has 12,142 rows. Row counts at near-parity strongly imply the two tables describe the same underlying half-hour observation set, in the same environment.
- Staging biomet has only 8,048 rows. Pulling biomet from staging while pulling flux from production would join two diverged datasets and silently drop or misalign rows.
- Production reads stay read-only for this milestone; using both flux and biomet from production preserves a single, consistent input environment.

Recommended default: read `carbon_flux_biomet` from `manglaria` (production), not from `manglaria-staging`. Required pre-implementation decision: confirm with the science owner that the production biomet table is the authoritative companion to the production eddy covariance table for the s2_filt_1 step.

### 2.3 What is being built

A downstream repository (not this one, not the legacy `manglaria-lakehouse` repo) that:

- reads from production input tables (flux and biomet)
- writes to staging output table
- shells out to the unchanged Docker image as a file-only processor
- carries its own SQL templates, control tables, runbooks, and IAM

### 2.4 Environment-contract risk

This milestone deliberately reads from production (`manglaria`) and writes to staging (`manglaria-staging`). That is unusual. Document it loudly in the downstream repo and in `05_governance/risks.md`:

- production is the input, so the orchestration layer needs read access to production tables from a staging-side service identity, or a staging-side reader proxy
- the output is in staging, so a regression that writes garbage will not corrupt production analytical tables, but it will pollute the staging copy used by anyone else relying on `_s2_filt_1`
- if the project ever flips to a same-environment posture (production input -> production output), the IAM and dataset references must change in lockstep. Make those references configurable, not hardcoded.

---

## 3. Authoritative source and target table set

| Role | Project | Dataset | Table | Class |
| --- | --- | --- | --- | --- |
| Primary input (read) | `manglaria` | `manglaria_lakehouse_ds` | `carbon_flux_eddycovariance` | Confirmed |
| Side input (read) | `manglaria` | `manglaria_lakehouse_ds` | `carbon_flux_biomet` | Recommended default -- see section 2.2 |
| Side input candidate (read, deferred) | `manglaria` | `manglaria_lakehouse_ds` | `carbon_flux_site_metadata` | Required pre-implementation decision -- only include if processor contract requires it (open question 21.3) |
| Do-not-use for this milestone | `manglaria-staging` | `manglaria_lakehouse_ds` | `carbon_flux_eddycovariance` | Same schema as production, currently 0 rows |
| Do-not-use for this milestone | `manglaria-staging` | `manglaria_lakehouse_ds` | `carbon_flux_biomet` | Same schema as production but materially smaller (8,048 rows vs 12,142) |
| Target (write) | `manglaria-staging` | `manglaria_lakehouse_ds` | `carbon_flux_eddycovariance_s2_filt_1` | Confirmed |

All four read/write tables are in `us-central1`. The orchestration layer must run in `us-central1` to avoid cross-region BigQuery + GCS egress and load constraints.

---

## 4. Processor input shape: dual-export by default, with a unified-export fallback

The exact input contract of the unchanged Docker image is a required pre-implementation decision (section 21.3). The orchestration must work with both possibilities. This guide describes both modes. The downstream repo implements one of them based on the confirmed processor contract.

### 4.1 Mode A -- separate flux and biomet inputs (recommended default)

The processor reads two distinct file groups from two distinct mount paths, for example `/in/flux/*.csv` and `/in/biomet/*.csv`. The wrapper exports flux and biomet slices to two GCS prefixes and presents both prefixes to the processor.

This mode is the recommended default because:

- the legacy ingestion writes flux and biomet to two distinct BigQuery tables with distinct schemas, which is consistent with two distinct file groups in the original EddyPro/biomet ecosystem
- it keeps the wrapper's responsibility narrower (export each slice as-is, no scientific join)
- it avoids the wrapper inventing a join that the processor may itself be doing differently inside

### 4.2 Mode B -- single unified prepared input

The processor reads one file group from a single mount path, and the wrapper produces it by joining flux and biomet on `(site_id, timestamp)` before export.

Mode B is implementable but riskier: the join key, gap policy, and column naming would be invented by the wrapper, not by the science owner. If Mode B is chosen, the join logic must be reviewed by the science owner before production cutover.

Mode A and Mode B are the only two input-contract modes used in this guide. The terms refer strictly to the processor's input file shape. They are not used for runtime fallbacks.

### 4.3 How the rest of this guide handles the two modes

Sections 6 (local path), 7 (cloud path), 8 (SQL), 9 (wrapper), and 18 (verification) are written for Mode A by default, and explicitly note where Mode B would consolidate the two slices into one. Picking the wrong mode is a contract failure, not a tunable parameter -- the downstream repo must confirm the processor's input shape before implementing.

---

## 5. Recommended orchestration architecture (Mode A)

```
+--------------------------------+   +--------------------------------+
| BigQuery (manglaria)           |   | BigQuery (manglaria)           |
|  carbon_flux_eddycovariance    |   |  carbon_flux_biomet            |
+--------------+-----------------+   +--------------+-----------------+
               | (1a) build flux slice              | (1b) build biomet slice
               v                                    v
+--------------------------------+   +--------------------------------+
| BigQuery (manglaria-staging)   |   | BigQuery (manglaria-staging)   |
|  _orch.cf_s2_flux_<run_id>     |   |  _orch.cf_s2_biomet_<run_id>   |
+--------------+-----------------+   +--------------+-----------------+
               | (2a) EXPORT DATA                   | (2b) EXPORT DATA
               v                                    v
+----------------------------------------------------------------+
| GCS (manglaria-staging) -- per-run staging area                |
|   gs://<orch-bucket>/cf_s2/runs/<run_id>/inputs/flux/          |
|   gs://<orch-bucket>/cf_s2/runs/<run_id>/inputs/biomet/        |
+--------------+-------------------------------------------------+
               | (3a) wrapper publishes this run into working area
               v
+----------------------------------------------------------------+
| GCS (manglaria-staging) -- fixed processor working area        |
|   gs://<orch-bucket>/cf_s2/working/inputs/flux/                |
|   gs://<orch-bucket>/cf_s2/working/inputs/biomet/              |
|   gs://<orch-bucket>/cf_s2/working/outputs/                    |
+--------------+-------------------------------------------------+
               | (3b) processor reads fixed mount paths via GCS FUSE
               v
+----------------------------------------------------------------+
| Processor Cloud Run Job (UNCHANGED IMAGE)                      |
|   /in/flux   -> mount of working/inputs/flux                   |
|   /in/biomet -> mount of working/inputs/biomet                 |
|   /out       -> mount of working/outputs                       |
+--------------+-------------------------------------------------+
               | (4) outputs to working area
               v
+----------------------------------------------------------------+
| GCS (manglaria-staging) -- wrapper archives back to per-run    |
|   gs://<orch-bucket>/cf_s2/runs/<run_id>/outputs/              |
+--------------+-------------------------------------------------+
               | (5) bq load
               v
+----------------------------------------------------------------+
| BigQuery (manglaria-staging)                                   |
|  _orch.cf_s2_stage_<run_id>                                    |
+--------------+-------------------------------------------------+
               | (6) validate + (7) MERGE on (site_id, timestamp)
               v
+----------------------------------------------------------------+
| BigQuery (manglaria-staging)                                   |
|  manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1   |
+----------------------------------------------------------------+
               | (8) advance watermark + record run row
               v
+----------------------------------------------------------------+
| _orch.cf_s2_runs                                               |
| _orch.cf_s2_watermark                                          |
+----------------------------------------------------------------+
```

Steps (1)-(2), (3a), (4)-(8) are owned by the wrapper artifact (section 9). Step (3b) is the unchanged processor Cloud Run Job (section 7). The processor only reads and writes the fixed `working/...` mount paths; the per-run isolation is handled by the wrapper before and after the processor execution (section 7.3).

In Mode B, steps (1a)/(1b) collapse into one slice with a documented join, (2a)/(2b) collapse into one export prefix, and the processor sees one input mount instead of two.

The key invariants:

1. The container only sees files. It never authenticates to BigQuery, never resolves a GCS URI directly, and never knows which environment it is in. GCS prefixes appear to it as plain filesystem paths via Cloud Storage FUSE volume mounts.
2. SQL transforms (slice build, dedup, merge) live in version-controlled `.sql` templates inside the downstream repo.
3. The wrapper artifact composes the SQL + GCS + processor-job-trigger steps. The wrapper artifact is not the processor.
4. State is captured in a small set of control tables that the wrapper owns.

---

## 6. Local execution path from the operator machine

Goal: an operator with `gcloud`, `bq`, `gsutil`, and Docker installed can perform a full dry run end-to-end without rebuilding the image and without writing to the shared `_s2_filt_1` table.

Recommended local flow (Mode A):

1. `gcloud auth application-default login` and `gcloud config set project manglaria-staging` (orchestration runs against staging; reads from production are scoped via cross-project SELECTs).
2. Generate a `run_id` locally, for example `local-$(date -u +%Y%m%dT%H%M%SZ)-$USER`.
3. Build flux slice into a personal slice table:
   `bq query --use_legacy_sql=false --destination_table=manglaria-staging:_orch.cf_s2_flux_<run_id> --replace=true < sql/build_flux_slice.sql` parameterized with a small bounded window.
4. Build biomet slice into a personal slice table:
   `bq query --use_legacy_sql=false --destination_table=manglaria-staging:_orch.cf_s2_biomet_<run_id> --replace=true < sql/build_biomet_slice.sql` for the same window.
5. Extract both to GCS:
   `bq extract --destination_format=CSV manglaria-staging:_orch.cf_s2_flux_<run_id> "gs://<orch-bucket>/cf_s2/local/<run_id>/inputs/flux/part-*.csv"`
   `bq extract --destination_format=CSV manglaria-staging:_orch.cf_s2_biomet_<run_id> "gs://<orch-bucket>/cf_s2/local/<run_id>/inputs/biomet/part-*.csv"`
6. Copy locally for offline execution:
   `gsutil -m cp -r "gs://<orch-bucket>/cf_s2/local/<run_id>/inputs" ./.runs/<run_id>/inputs`
7. Run the unchanged processor with two read-only input mounts and one output mount:
   `docker run --rm \
      -v "$PWD/.runs/<run_id>/inputs/flux:/in/flux:ro" \
      -v "$PWD/.runs/<run_id>/inputs/biomet:/in/biomet:ro" \
      -v "$PWD/.runs/<run_id>/outputs:/out" \
      <image>:<pinned-digest>`
   The mount paths must match the processor's documented input contract. If the processor expects different paths, change the mount targets, not the image.
8. Inspect `./.runs/<run_id>/outputs/` manually, or copy back to GCS for review:
   `gsutil cp -r ./.runs/<run_id>/outputs gs://<orch-bucket>/cf_s2/local/<run_id>/outputs`
9. Optional: load to a personal stage table for SQL inspection:
   `bq load --replace --source_format=CSV --schema=schemas/s2_filt_1.json manglaria-staging:_orch.cf_s2_stage_local_<user>_<run_id> "gs://<orch-bucket>/cf_s2/local/<run_id>/outputs/*.csv"`
10. **Do not** MERGE into `carbon_flux_eddycovariance_s2_filt_1` from a local run unless explicitly authorized. Local runs end at the personal stage table.

Mode B local flow: replace steps 3-4 with a single `build_unified_slice.sql`, replace step 5 with a single extract, mount one input directory in step 7.

Safety rails baked into the local path:

- Local `run_id`s are namespaced with `local-` so cloud runs and operator runs never collide.
- The image is pinned by digest in a config file the wrapper reads, not pulled as `latest`.
- The wrapper must refuse to MERGE if `run_id` starts with `local-` unless a `--allow-local-merge` flag is passed.
- The personal stage table name (`_orch.cf_s2_stage_local_<user>_<run_id>`) is distinct from the cloud stage table name.

---

## 7. Cloud execution path

The cloud path uses **two distinct Cloud Run Job definitions**, owned by the downstream repo.

### 7.1 Two-job artifact boundary

| Artifact | Image | Built by | Responsibilities |
| --- | --- | --- | --- |
| Wrapper Cloud Run Job | A new, downstream-built image (Python or Go; ships `bq`, `gsutil`, BigQuery + Storage + Cloud Run Admin SDKs, and the wrapper code) | The downstream repo | Steps 1-2, 3a, and 4-8 of the architecture in section 5: SQL slice builds, GCS extracts, per-run staging, publishing this run into the working area, triggering the processor job execution, polling for completion, archiving outputs back to the per-run prefix, GCS-to-stage load, validation SQL, MERGE, watermark advance, run record |
| Processor Cloud Run Job | The unchanged existing Docker image, pinned by digest | Not built by this milestone | Step 3b only: read fixed mount paths backed by GCS FUSE volumes, write to a fixed output mount path |

The wrapper image is the only artifact this milestone introduces. The processor image is consumed unchanged.

### 7.2 How the processor Cloud Run Job is configured without modifying the image

Cloud Run Jobs supports Cloud Storage FUSE volume mounts at the job-definition level. The processor job is defined once with:

- the unchanged image, pinned by digest
- one or more `volume_mounts` of type `gcs` mapped to the fixed paths the image already reads and writes (for example `/in/flux`, `/in/biomet`, `/out`)
- a fixed bucket per mount, declared in the job definition; the bucket is the orchestration bucket from open question 21.4
- no command override, no entrypoint override, no rebuild, no per-execution mutation of the volume definition

Cloud Storage FUSE mounts are a Cloud Run job configuration concern, not an image change. The image continues to read and write plain filesystem paths.

### 7.3 Run-specific isolation without rewriting the volume per execution

Cloud Run Jobs execution overrides cover container args, environment variables, task count, parallelism, and timeout; they do not cover volume bucket or volume prefix changes. Run-specific input/output routing must therefore be handled by the wrapper, on the GCS object layout side, before and after each processor execution. The unchanged processor only ever reads and writes its fixed mount paths and never receives a run identifier, a project ID, or any cloud-routing variable.

Recommended default pattern (see open question 21.9 for concurrency):

- the orchestration bucket has a stable working-area shape used as the fixed processor mount target:
  - `gs://<orch-bucket>/cf_s2/working/inputs/flux/`
  - `gs://<orch-bucket>/cf_s2/working/inputs/biomet/`
  - `gs://<orch-bucket>/cf_s2/working/outputs/`
- per-run files are kept in a parallel per-run prefix:
  - `gs://<orch-bucket>/cf_s2/runs/<run_id>/inputs/flux/`
  - `gs://<orch-bucket>/cf_s2/runs/<run_id>/inputs/biomet/`
  - `gs://<orch-bucket>/cf_s2/runs/<run_id>/outputs/` (filled after the run)
- before triggering the processor, the wrapper:
  1. acquires a serial-execution lock object in the bucket so only one processor run uses the working area at a time
  2. clears `cf_s2/working/inputs/flux/`, `cf_s2/working/inputs/biomet/`, and `cf_s2/working/outputs/`
  3. copies this run's slices from `cf_s2/runs/<run_id>/inputs/...` into the corresponding `cf_s2/working/inputs/...` prefixes
- the wrapper triggers the processor with no execution-time volume changes
- after the processor reaches a terminal state, the wrapper:
  1. copies `cf_s2/working/outputs/` to `cf_s2/runs/<run_id>/outputs/` for traceability
  2. clears the working-area prefixes
  3. releases the lock
  4. proceeds to the BigQuery load + merge + watermark steps

This pattern keeps the processor image, its entrypoint, and its environment strictly unchanged. All run isolation lives in the wrapper's GCS object operations.

If the team needs concurrent processor executions, this default pattern must be replaced; see open question 21.9. Replacement options include defining one processor Cloud Run Job per concurrent slot (each with its own bucket or its own bucket-side prefix in the volume mount) or switching the processor runtime to Cloud Batch, where per-task volume parameters can be configured per execution. Both options keep the image unchanged.

### 7.4 How the wrapper triggers the processor

The wrapper, running inside the wrapper Cloud Run Job, calls the Cloud Run Admin API (`projects.locations.jobs.run`) on the processor job after the staging in section 7.3 is complete. The wrapper polls execution status until terminal (succeeded or failed). The wrapper does not pass cloud-routing environment variables, GCS prefixes, project IDs, or BigQuery references into the processor execution; the processor's read and write paths are fixed at job-definition time. The wrapper does not run `docker` in the cloud -- Cloud Run does not provide a Docker daemon, and the unchanged image does not need one because Cloud Run Jobs runs containers natively.

### 7.5 Cloud-path differences from the local path

- inputs are exported directly to GCS; no local copy
- the processor Cloud Run Job reads inputs from GCS via fixed FUSE volume mounts; no `docker run` in the cloud
- the wrapper enforces serial access to the working area for the recommended default pattern
- after the processor job execution succeeds, the wrapper invokes `bq load`, runs validation queries, runs the merge, advances the watermark, and writes a run record
- on failure, the wrapper writes a failed run record and does not advance the watermark
- the wrapper job is triggered by Cloud Scheduler on a cadence chosen per open question 21.6, or invoked manually for backfills with explicit window overrides

### 7.6 Cloud Run Job shape choices

- one wrapper Cloud Run Job execution per `run_id`
- one processor Cloud Run Job execution per `run_id`
- one task per execution by default; only fan out if input volume justifies it (current source is ~12k rows; one task is fine)
- timeouts sized for the pipeline plus a safety margin
- dedicated service accounts for both jobs (see section 16 and open question 21.5)
- wrapper-job environment variables: `RUN_ID`, `PROJECT_INPUT=manglaria`, `PROJECT_OUTPUT=manglaria-staging`, `INPUT_FLUX_TABLE`, `INPUT_BIOMET_TABLE`, `STAGE_TABLE`, `FINAL_TABLE`, `IMAGE_DIGEST`, `STAGING_BUCKET`, `WINDOW_START`, `WINDOW_END`, `ALLOW_MERGE`, `MODE` (`A` or `B`)
- processor-job environment: only what the unchanged image already requires; nothing cloud-routing related is added

---

## 8. Recommended SQL responsibilities

Keep all SQL in the downstream repository under `sql/`. Minimum templates (Mode A):

- `sql/build_flux_slice.sql` -- selects rows from `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance` between `@window_start` and `@window_end`, applies the dedup rule from section 11, and writes them to `manglaria-staging:_orch.cf_s2_flux_<run_id>` with explicit column projection that matches the file contract the processor expects. No `SELECT *`.
- `sql/build_biomet_slice.sql` -- selects rows from `manglaria.manglaria_lakehouse_ds.carbon_flux_biomet` between the same `@window_start` and `@window_end`, applies the same dedup rule, writes them to `manglaria-staging:_orch.cf_s2_biomet_<run_id>` with explicit column projection.
- `sql/validate_stage.sql` -- runs row counts; NULL checks on REQUIRED columns (`primary_key`, `timestamp`, `site_id`); uniqueness on `(site_id, timestamp)`; uniqueness on `primary_key` (which the output table also requires as REQUIRED); referential checks if a `carbon_flux_site_metadata` join is in scope. The wrapper aborts the merge if validation fails.
- `sql/merge_to_final.sql` -- `MERGE` from `_orch.cf_s2_stage_<run_id>` into `manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1` keyed on `(site_id, timestamp)`. `WHEN MATCHED THEN UPDATE SET ...` for every non-key column including `primary_key`. `WHEN NOT MATCHED THEN INSERT`. No deletes.
- `sql/advance_watermark.sql` -- updates `_orch.cf_s2_watermark` only on a successful merge.
- `sql/record_run.sql` -- inserts into `_orch.cf_s2_runs` with `run_id`, status, window, row counts (flux input, biomet input, stage output, merged), image digest, durations, mode (A or B), error text if any.

Mode B variant: replace the two `build_*_slice.sql` templates with one `build_unified_slice.sql`. Do not run both. The wrapper picks one path based on the `MODE` config.

All templates are parameterized; parameters are passed by the wrapper, not interpolated as strings.

---

## 9. Recommended wrapper-script or job responsibilities

The wrapper is a single binary or single Python entrypoint. It is the same code path locally and in the wrapper Cloud Run Job; only the I/O substrate (local FS vs GCS) and the processor invocation step differ. Minimum responsibilities:

1. resolve config (project IDs, table names, bucket, image digest, window, run_id, mode)
2. preflight checks: source datasets exist, target table exists, image digest pinned, watermark readable, target schema matches checked-in schema
3. execute slice SQL (`build_flux_slice.sql` + `build_biomet_slice.sql` for Mode A; `build_unified_slice.sql` for Mode B) and validate primary_key uniqueness inside each slice before export
4. `bq extract` slices to the per-run GCS input prefixes (`cf_s2/runs/<run_id>/inputs/...`)
5. invoke the processor:
   - locally: `docker run` with bind mounts (read-only inputs, writable output)
   - in the cloud: acquire the working-area lock, publish this run's inputs into the working prefixes, trigger the processor Cloud Run Job execution via the Cloud Run Admin API, poll until terminal, then archive outputs back to the per-run prefix and release the lock
6. on processor success: confirm output prefix is non-empty
7. `bq load` outputs into the stage table with explicit schema
8. run validation SQL; on failure, record a failed run and stop
9. run merge SQL
10. advance watermark
11. record run row
12. emit structured logs and a final summary line a human can grep

The wrapper must never:

- modify the processor image or its behavior
- bypass validation on success
- write to `carbon_flux_eddycovariance_s2_filt_1` directly without going through the stage + merge path
- assume the watermark advanced if the merge failed
- pass GCP credentials, project IDs, run identifiers, GCS prefixes, or BigQuery references to the processor through env vars

---

## 10. Recommended control tables

Place all control state in a dedicated dataset, for example `manglaria-staging:_orch` or `manglaria-staging:carbon_flux_orch`. Keeping it out of `manglaria_lakehouse_ds` avoids polluting the analytical dataset and clarifies ownership.

Minimum tables:

- `cf_s2_runs` -- `run_id STRING`, `started_at TIMESTAMP`, `finished_at TIMESTAMP`, `status STRING (running|succeeded|failed|aborted)`, `window_start TIMESTAMP`, `window_end TIMESTAMP`, `mode STRING (A|B)`, `image_digest STRING`, `flux_input_rows INT64`, `biomet_input_rows INT64`, `stage_output_rows INT64`, `merged_inserted_rows INT64`, `merged_updated_rows INT64`, `error_text STRING`, `triggered_by STRING (cloud|local|backfill)`.
- `cf_s2_watermark` -- single-row table or one row per `site_id`. `site_id STRING`, `last_processed_timestamp TIMESTAMP`, `updated_at TIMESTAMP`, `last_run_id STRING`.
- `cf_s2_flux_<run_id>` -- transient flux slice table per run, autoscoped TTL (24 h). Wrapper creates and drops. (Mode A only.)
- `cf_s2_biomet_<run_id>` -- transient biomet slice table per run, autoscoped TTL (24 h). Wrapper creates and drops. (Mode A only.)
- `cf_s2_unified_<run_id>` -- transient unified slice table per run, autoscoped TTL (24 h). (Mode B only; replaces the two above.)
- `cf_s2_stage_<run_id>` -- transient stage table per run, autoscoped TTL (24 h). Wrapper creates and drops.

If `carbon_flux_site_metadata` becomes part of the input contract, the wrapper joins it server-side from production; there is no orchestration-owned copy.

---

## 11. Deduplication and merge-key alignment

The source tables have REQUIRED `primary_key`, `timestamp`, and `site_id`, and a NULLABLE `filename` on the eddy covariance side. The legacy ingestion uses GCS event triggers, which can deliver the same EddyPro file twice. The downstream pipeline must not assume `primary_key` uniqueness in the source.

Decisions the corrected guide makes:

- **Canonical row identity**: `(site_id, timestamp)`. This is enforced at the slice step (Mode A: per slice; Mode B: on the unified slice).
- **Tie-break when duplicates exist in the source**: pick latest by `filename` lexicographically when `filename` is not NULL; otherwise hash-stable choice. Recommended SQL pattern: `ROW_NUMBER() OVER (PARTITION BY site_id, timestamp ORDER BY filename DESC NULLS LAST)` keeping `rn = 1`. This is a recommended default and is listed in open question 21.1.
- **`primary_key` after dedup**: post-slice, the wrapper validates that `primary_key` is unique per slice. It must be: each `(site_id, timestamp)` survivor has exactly one `primary_key` value. If a primary_key collision is observed across the deduped slice, the wrapper aborts the run (this is a slice-build failure, not a merge failure).
- **Merge key**: `(site_id, timestamp)`. The merge body sets every non-key column including `primary_key`. This aligns the slice identity and the merge identity.
- **Why merge on `(site_id, timestamp)` instead of `primary_key`**: the slice identity is `(site_id, timestamp)`. Merging on `primary_key` would let the same `(site_id, timestamp)` exist twice in the final table if the processor ever emits a different `primary_key` for an existing row. Merging on `(site_id, timestamp)` makes the final table the same shape as the slice identity by construction.
- **Whether to drop rows with NULL in any field the processor requires**: the slice SQL filters NULLs out in REQUIRED columns and logs the count to the run record so it is visible.

The output table's schema lists `primary_key` as REQUIRED. Treat it as a deterministic surrogate that travels through the slice and merge unchanged. The wrapper enforces uniqueness on it during slice validation; the merge body keeps it consistent.

---

## 12. Incremental processing and recomputation window

The source is small today but is expected to grow. Two parameters drive the design:

- **Watermark advancement window**: how far the watermark moves on success. Recommend `last_processed_timestamp = MAX(timestamp_in_window) - safety_margin`, where the safety margin handles late-arriving raw rows from the event-driven Cloud Function ingest.
- **Recomputation window**: how far back from the watermark each run reprocesses. Recommend a default of 30 days back from `now`, with the watermark moved only forward to `now - 7 days`. This guarantees that the next 4 runs each get a chance to repair late-landed source rows.

These numbers are recommended defaults, not facts. They are explicitly listed in open question 21.2.

---

## 13. Cloud Run Jobs vs Cloud Run service vs other patterns

Recommendation: **Cloud Run Jobs**, in the two-job shape described in section 7.

Reasoning:

- This is a batch pipeline with a clear start, finish, and exit code. Cloud Run Jobs is the native fit; a Cloud Run service would force a fake HTTP handler and an external invoker.
- Native task retries and timeout controls match wrapper semantics.
- Cloud Run Jobs supports dedicated service accounts, explicit per-execution arguments, GCS FUSE volume mounts at the job-definition level, and scheduled invocation through Cloud Scheduler.
- The image is already file-in / file-out; we do not need a long-lived HTTP server. GCS FUSE mounts let the unchanged image read inputs and write outputs without modification.

Use a Cloud Run service only if the wrapper itself needs to expose an HTTP API that other ManglarIA systems will call. That is not currently in scope.

Other patterns considered and rejected for this milestone:

- Cloud Functions: timeouts and packaging do not fit a Dockerized scientific processor.
- Workflows + Batch: fits long parallel jobs, overkill for current data volume.
- Dataflow: introduces a new runtime ownership boundary for a problem that is not stream-shaped.
- Cloud Batch: a viable runtime fallback for the processor job if GCS FUSE proves unsuitable for the processor's IO pattern, or if concurrent processor executions are required and per-task volume parameters become useful. This is a runtime fallback for the processor job, not an input-contract mode change. See open questions 21.8 and 21.9.

If volume grows past what one Cloud Run Job task can finish inside its timeout, switch to a sharded Cloud Run Job with one task per `site_id` or per month-window before considering a different runtime.

---

## 14. Staging-load and final-merge pattern

Land the processor output through `bq load` into `_orch.cf_s2_stage_<run_id>` with an explicit JSON schema file checked into the downstream repo (`schemas/s2_filt_1.json`). Do not autodetect the schema.

After the load:

1. Run row count, NULL, primary-key uniqueness, and `(site_id, timestamp)` uniqueness checks against the stage table.
2. If checks pass, MERGE into `manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1` keyed on `(site_id, timestamp)`. The merge body sets every non-key column (including `primary_key`) from the staged row.
3. Capture inserted versus updated counts in the run record (BigQuery returns these via `MERGE` job statistics).
4. Drop the stage and slice tables.

Idempotency: the same `run_id` rerunning the same window should produce the same final-table state. The wrapper enforces this by overwriting the slice and stage tables at the start of each attempt and using a deterministic merge keyed on `(site_id, timestamp)`.

---

## 15. Watermark and run-state handling

- Watermark is read at the start of each cloud run to compute the default window if the operator did not pass one.
- Watermark is written only after the merge succeeds.
- Run rows are written at start (status `running`) and updated at finish (`succeeded` or `failed`).
- Crash recovery: if a run row stays in `running` past a configurable threshold, treat it as `aborted` during the next operator inspection. Do not auto-promote; this is a human checkpoint.

---

## 16. IAM, service account, bucket, and region considerations

The downstream repo cannot land safely without confirming these. Each is either a recommended default or a pre-implementation question.

- **Region**: `us-central1` for inputs, outputs, BigQuery datasets, GCS bucket, and both Cloud Run Job executions.
- **Service accounts**: two dedicated SAs in `manglaria-staging` (recommended; see open question 21.5):
  - `cf-s2-wrapper@manglaria-staging.iam.gserviceaccount.com` -- the wrapper Cloud Run Job runs as this identity. It runs all BigQuery jobs and triggers the processor job.
  - `cf-s2-processor@manglaria-staging.iam.gserviceaccount.com` -- the processor Cloud Run Job runs as this identity. It only needs GCS access to the working-area input and output prefixes.
- **Roles, least-privilege, corrected**:
  - On production project `manglaria` (read-only):
    - `roles/bigquery.dataViewer` granted to the wrapper SA on the specific tables `carbon_flux_eddycovariance` and `carbon_flux_biomet` (and on `carbon_flux_site_metadata` only if open question 21.3 confirms it is needed). Table-level grants only; no dataset-level or project-level reader role.
    - **No** `roles/bigquery.jobUser` on `manglaria`. Query jobs are created in `manglaria-staging` and reference production tables via fully-qualified names; `bigquery.jobs.create` is required only in the project that runs the job.
  - On staging project `manglaria-staging` (write surface):
    - `roles/bigquery.jobUser` on the project, granted to the wrapper SA. This is where every query, extract, load, merge, and watermark update runs.
    - `roles/bigquery.dataEditor` granted to the wrapper SA on the orchestration dataset and on `manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1`. Table-level on the analytical table; dataset-level only on the orchestration dataset.
    - `roles/storage.objectAdmin` granted to the wrapper SA on the orchestration bucket only (not project-wide).
    - `roles/storage.objectViewer` on the working input prefixes and `roles/storage.objectCreator` on the working output prefix granted to the processor SA. The processor never touches BigQuery. The processor's grants are scoped to the working-area prefixes only; the wrapper owns moves to and from the per-run prefixes. Validate this scoping against the Cloud Run volume-mount documentation before cutover; if `roles/storage.objectUser` on the working area proves operationally simpler, document the change in `05_governance/decision_log.md`.
    - `roles/run.developer` (or narrower equivalent that allows `run.executions.create` on the processor job) granted to the wrapper SA, scoped to the processor Cloud Run Job.
    - `roles/run.invoker` on the wrapper Cloud Run Job, granted to the Cloud Scheduler identity that triggers it.
- **GCS bucket**: a new dedicated bucket for orchestration inputs and outputs. See open question 21.4.
- **Secrets**: no Secret Manager access is required for the current container contract. Reverify if the processor takes config files that contain credentials.
- **Dataset-level grants**: avoid granting on `manglaria_lakehouse_ds`; scope to specific tables to limit blast radius.

These IAM details belong in the downstream repo's deployment doc and should be re-verified against live IAM before production cutover.

---

## 17. Docker contract boundaries

The container is a black box for this milestone. The orchestration layer must respect the following boundaries:

- **Inputs**: directories mounted at the paths the container expects. In Mode A this is at least two distinct paths (for example `/in/flux` and `/in/biomet`); in Mode B this is one path. Files arrive in a format the container already accepts (CSV by default; Parquet only if the container supports it).
- **Output**: a directory mounted at the path the container writes to (commonly `/out`). The output file count, format, and column order are defined by the image, not by orchestration.
- **No environment leakage**: do not pass GCP credentials, project IDs, run identifiers, GCS prefixes, or BigQuery references into the container. The wrapper handles all cloud I/O.
- **No image rebuild**: the container is pinned by digest in the wrapper config. Upgrades to the image are an explicit cross-team change, not a side effect of this milestone.
- **No command override**: in the cloud, the processor Cloud Run Job uses the unchanged image's default entrypoint. GCS access happens at the Cloud Run Job's volume-mount layer (Cloud Storage FUSE), not inside the image.
- **No per-execution volume mutation**: the processor job's volume mounts are declared once at job-definition time. Run-specific isolation is handled by the wrapper's GCS object operations on the working area, not by per-execution volume changes.
- **Determinism**: assume the container is deterministic for a fixed input set. If it is not, the dedup and merge logic absorbs nondeterminism, but flag it as a science-side risk.
- **Failure mode**: nonzero exit means abort. Output directory contents on failure are considered untrusted and not loaded.

If any of these boundaries cannot be honored without changing the image, escalate; do not work around them silently.

---

## 18. Verification checklist

Before declaring the orchestration usable in staging:

- [ ] The downstream repo can render every SQL template with deterministic parameter values and run them via `bq query --dry_run` without error.
- [ ] A local Mode-A run with a 1-day window completes end-to-end into a personal stage table; outputs match the expected column set.
- [ ] A wrapper Cloud Run Job execution with a 1-day window completes end-to-end and writes a `succeeded` row to `cf_s2_runs`.
- [ ] The wrapper Cloud Run Job triggered exactly one processor Cloud Run Job execution per `run_id`.
- [ ] The processor Cloud Run Job execution did not receive any cloud-routing env var, run identifier, GCS prefix, or BigQuery reference.
- [ ] A second wrapper Cloud Run Job execution with the same window is idempotent: row counts in `_s2_filt_1` are unchanged.
- [ ] Forcing a validation failure (for example, by injecting a NULL into a REQUIRED column in stage, or by injecting a primary_key collision into the slice) leaves `_s2_filt_1` untouched and writes a `failed` row to `cf_s2_runs`.
- [ ] The watermark only advances on success.
- [ ] Container image digest is pinned and recorded in `cf_s2_runs`.
- [ ] The slice and the merge both operate on `(site_id, timestamp)` identity.
- [ ] Production tables read by the run, per BigQuery audit logs or `INFORMATION_SCHEMA.JOBS` reflection, are exactly the tables in the authoritative source set:
  - `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance`
  - `manglaria.manglaria_lakehouse_ds.carbon_flux_biomet`
  - `manglaria.manglaria_lakehouse_ds.carbon_flux_site_metadata` only if open question 21.3 has been resolved in favor of including it
- [ ] No table in `manglaria` was written.
- [ ] No production-project `bigquery.jobs.create` permission was used (jobs run in `manglaria-staging`).

---

## 19. Operational cautions and runbook notes

- The legacy production carbon flux ingestion runs as `manglaria@appspot.gserviceaccount.com` against bucket family `3f9b3019`. Do not assume this orchestration can reuse that service account; it is the wrong identity for cross-project reads from a staging job.
- Staging carbon flux ingestion currently points at bucket family `a5bd2fa5` while the active Dataplex family for staging is `1aa088d6`. Do not adopt or interact with that legacy carbon flux bucket. The orchestration layer creates and owns its own bucket.
- Terraform state ownership across both projects is not yet fully reconciled (see `90_legacy_review/legacy_risks.md` risks #1 and #12). Treat any new bucket, IAM, dataset, Cloud Run Job, or Scheduler as needing its own ownership boundary; do not wedge any of them into the legacy `manglaria-lakehouse` Terraform root without explicit coordination.
- BigQuery schema drift on `_s2_filt_1` would silently break the load step. The wrapper should fetch the live target schema at preflight and refuse to run if the JSON schema in the repo disagrees.
- Public exposure: nothing about this milestone justifies a public endpoint. Both Cloud Run Jobs must require authenticated invocation only.
- On failure, do not retry blindly. Write the failed run row, page the on-call (if defined), and require a human to rerun with a known window.
- Mode change (A vs B) is a science-owner decision, not an operator decision. Do not let a runbook silently flip the mode.
- Working-area lock: if the lock object is left behind after a crash, an operator must inspect and clear it manually. Do not auto-break the lock.

---

## 20. Recommendations summary

- **Authoritative source set**: production `carbon_flux_eddycovariance` + production `carbon_flux_biomet`. Site metadata deferred. (Sections 2.2, 3.)
- **Processor input shape**: Mode A (separate flux and biomet inputs) by default; Mode B (unified prepared input) supported as a fallback if the science owner confirms the processor expects one input. (Section 4.)
- **Cloud runtime**: two Cloud Run Jobs -- wrapper (downstream-built) and processor (unchanged image) -- connected by fixed GCS FUSE volume mounts; per-run isolation handled by the wrapper's working-area staging pattern. (Sections 7, 13.)
- **Wrapper minimum**: config resolution, preflight, slice build per mode, GCS extract to per-run prefix, working-area staging and locking, processor invocation (local docker run or cloud Cloud Run Job execution), output archival to per-run prefix, GCS-to-stage load, validation, merge on `(site_id, timestamp)`, watermark advance, run record. (Section 9.)
- **SQL minimum**: `build_flux_slice.sql`, `build_biomet_slice.sql`, `validate_stage.sql`, `merge_to_final.sql`, `advance_watermark.sql`, `record_run.sql`. Mode B substitutes one `build_unified_slice.sql` for the two slice builds. (Section 8.)
- **Control tables minimum**: `cf_s2_runs`, `cf_s2_watermark`, transient slice tables (`cf_s2_flux_<run_id>`, `cf_s2_biomet_<run_id>`, or `cf_s2_unified_<run_id>`), and `cf_s2_stage_<run_id>`. (Section 10.)
- **Local-test workflow**: `bq query` per slice -> `bq extract` -> `gsutil cp` -> `docker run` with bind mounts -> personal stage table only. Refuse merge for `local-` `run_id`s by default. (Section 6.)
- **Cross-mode alignment**: identical SQL templates and identical wrapper code path used in local and cloud; only the I/O substrate (local FS vs GCS) and the processor invocation step differ. Pin the image by digest. Pin the wrapper version per release. Record both in `cf_s2_runs`. (Sections 6, 7, 17.)
- **Dedup and merge alignment**: row identity, slice dedup, and final merge all key on `(site_id, timestamp)`. `primary_key` is treated as a deterministic surrogate validated for uniqueness in the slice. (Section 11.)
- **IAM**: jobs created only in `manglaria-staging`; `manglaria` grants are read-only and table-scoped. (Section 16.)

---

## 21. Open questions and deferred decisions

These are not facts. The downstream team or the architect must settle each before production cutover. Recommended defaults are given where the evidence supports one.

### 21.1 Exact dedup rule
Recommended default: `ROW_NUMBER() OVER (PARTITION BY site_id, timestamp ORDER BY filename DESC NULLS LAST)` keeping `rn = 1`. The merge key is fixed to `(site_id, timestamp)` to align with this identity. Required pre-implementation decision: confirm `(site_id, timestamp)` is the canonical scientific identity and that `filename` is a usable tie-break.

### 21.2 Recomputation window
Recommended default: reprocess last 30 days each run; advance watermark only to `now - 7 days`. Required pre-implementation decision: confirm with the science owner that 7 days is enough late-arrival headroom for raw EddyPro deliveries.

### 21.3 Processor input shape (Mode A vs Mode B), and whether `carbon_flux_site_metadata` is required
Evidence: the legacy ingestion writes three sibling tables; row-count parity between production flux (12,141) and production biomet (12,142) supports a per-half-hour join.
Recommended default: Mode A (separate flux and biomet inputs from production). Site metadata is deferred until the processor contract is confirmed. Required pre-implementation decision: confirm the processor's input file contract -- number of input directories, file naming, whether site metadata is part of the input, and whether biomet is required at all. Do not infer it from the source schema alone.

### 21.4 Exact orchestration GCS bucket name
Recommended default: provision a new bucket in `manglaria-staging` in `us-central1`, named explicitly for this orchestration (for example, `gcp-manglaria-staging-carbon-flux-s2-orch-<deterministic-suffix>`), uniform bucket-level access on, public access prevention enforced, dedicated lifecycle rule (delete after 30 days). Do not reuse the live carbon flux ingest bucket on family `a5bd2fa5`. Required pre-implementation decision: name, suffix, and Terraform ownership path for the new bucket.

### 21.5 Service accounts, naming, and execution region
Recommended default: dedicated wrapper SA `cf-s2-wrapper@manglaria-staging.iam.gserviceaccount.com`, dedicated processor SA `cf-s2-processor@manglaria-staging.iam.gserviceaccount.com`, region `us-central1`. Required pre-implementation decision: who provisions and rotates the SAs, whether the read-only IAM grants on production `manglaria` will be approved at table scope, and whether the two-SA split is acceptable to the security owner.

### 21.6 Scheduling cadence
Recommended default: hourly Cloud Scheduler trigger during initial rollout, downgraded to daily once stable. Required pre-implementation decision before enabling Scheduler in production.

### 21.7 Ownership boundary against legacy Terraform
Recommended default: keep the orchestration bucket, dataset, both Cloud Run Jobs, IAM bindings, and Scheduler in a new Terraform module owned by the downstream repo. Do not extend the legacy `manglaria-lakehouse` root. Required pre-implementation decision before any `terraform apply`.

### 21.8 GCS FUSE suitability for the processor IO pattern
Recommended default: GCS FUSE volume mounts on the processor Cloud Run Job. Required pre-implementation decision: validate that the processor's IO pattern (read patterns, write atomicity, file count) is compatible with FUSE; if not, adopt the Cloud Batch fallback for the processor job (still unchanged image) and keep the wrapper on Cloud Run Jobs.

### 21.9 Run-specific volume routing and concurrency
Cloud Run Jobs execution overrides cover container args, environment variables, task count, parallelism, and timeout, not volume bucket or volume prefix changes. Recommended default: a fixed working-area mount on the processor Cloud Run Job, with the wrapper publishing this run's files into the working area before triggering and archiving outputs out of the working area afterward, enforced by a serial-execution lock object in the orchestration bucket. Required pre-implementation decision: whether concurrent processor executions are required. If they are, replace the default with one of:

- one separate processor Cloud Run Job per concurrent slot, each defined with its own bucket-side prefix at job-definition time, and the wrapper picks a free slot per run; or
- a runtime swap to Cloud Batch, where per-task volume parameters can be set per execution.

Both options keep the processor image unchanged.

---

## 22. Non-goals (reaffirmed)

- Do not modify the Docker image.
- Do not change the legacy carbon flux Cloud Function or its bucket.
- Do not redesign the production carbon flux ingestion path.
- Do not assume Terraform changes inside the legacy `manglaria-lakehouse` root are owned by this milestone.
- Do not implement the downstream code in this repository; this guide is the contract for that work.
- Do not pull biomet from `manglaria-staging` while pulling flux from `manglaria` -- the staging biomet table is materially smaller and cross-environment input would silently misalign.
- Do not pass run identifiers, GCS prefixes, or any cloud-routing variable into the processor container, in the cloud or locally.

---

## 23. Evidence base

This guide is grounded in the following artifacts. Re-check them if any claim above looks stale.

- `CLAUDE.md`
- `README.md`
- `docs/PROJECT_INIT_MANUAL.md`
- `prompts/for_coding_agent/001_carbon_flux_bq_orchestration_guide.md`
- `prompts/for_coding_agent/002_carbon_flux_bq_orchestration_guide_corrections.md`
- `prompts/for_coding_agent/003_carbon_flux_bq_orchestration_guide_runtime_and_reference_corrections.md`
- `prompts/for_review_agent/001_review_carbon_flux_bq_orchestration_guide.md`
- `prompts/for_review_agent/002_review_carbon_flux_bq_orchestration_guide_corrections.md`
- `05_governance/reviews/001_review_carbon_flux_bq_orchestration_guide.md`
- `05_governance/reviews/002_review_carbon_flux_bq_orchestration_guide_corrections.md`
- `08_implementation_guides/CONTEXT.md`
- `90_legacy_review/CONTEXT.md`, `feature_scope.md`, `repo_map.md`, `legacy_risks.md`, `migration_decision_log.md`, `reuse_candidate_log.md`
- `90_legacy_review/exports/20260426_150929/manglaria/bigquery/datasets/manglaria_lakehouse_ds/carbon_flux_eddycovariance.json`
- `90_legacy_review/exports/20260426_150929/manglaria/bigquery/datasets/manglaria_lakehouse_ds/carbon_flux_biomet.json`
- `90_legacy_review/exports/20260426_150929/manglaria-staging/bigquery/datasets/manglaria_lakehouse_ds/carbon_flux_eddycovariance.json`
- `90_legacy_review/exports/20260426_150929/manglaria-staging/bigquery/datasets/manglaria_lakehouse_ds/carbon_flux_biomet.json`
- `90_legacy_review/exports/20260426_150929/manglaria-staging/bigquery/datasets/manglaria_lakehouse_ds/carbon_flux_eddycovariance_s2_filt_1.json`
- `90_legacy_review/exports/20260426_150929/manglaria/functions/functions.json`
- `90_legacy_review/exports/20260426_150929/manglaria-staging/functions/functions.json`
- `06_infra/CONTEXT.md`, `architecture.md`, `deployment.md`
- `09_ops/CONTEXT.md`, `runbooks.md`, `scheduled_jobs.md`
