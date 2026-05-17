# Milestone 002 - Carbon Flux BigQuery Orchestration Guide

> Status: implementation guide for a downstream repository.
> Purpose: preserve the useful operational facts from
> `docs/guides/001_carbon_flux_bq_orchestration_guide.md`, but update the
> recommended architecture to a BigQuery-native batch runtime that can run in
> Cloud Run Jobs without requiring CSV staging as the primary path.
>
> Source of truth for this milestone:
> - production primary input:
>   `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance`
> - production side input:
>   `manglaria.manglaria_lakehouse_ds.carbon_flux_biomet`
> - staging output:
>   `manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1`
>
> This guide assumes the Docker image and package are now allowed to evolve if
> needed. The preferred runtime contract is no longer "files only at all
> costs". The preferred runtime contract is now:
>
> - BigQuery-native read path for cloud and local orchestration tests
> - in-memory processing inside the container
> - local-disk output for the first live test
> - staged BigQuery load + MERGE for the productionized cloud path
>
> Guide `001` remains useful as a fallback/export-based design reference, but
> this guide is the preferred "on rails" direction if the image and package can
> be modified.

---

## 0. Reading map

This guide mixes three kinds of statements.

| Class | Meaning |
| --- | --- |
| Confirmed fact | Backed by repository evidence or the legacy exports cited in guide `001`. Treat as ground truth unless superseded by a fresh live export. |
| Recommended default | A defensible default for the downstream implementation. Reviewable before production cutover. |
| Required pre-implementation decision | Open item. The downstream team or architect must settle it explicitly. |

This guide is intentionally opinionated about architecture, because its job is
to keep the downstream orchestration development on rails. It should be used as
a steering document, not just a note dump.

---

## 1. Capability summary

Build a downstream orchestration layer that runs `miaproc` against controlled
BigQuery slices of:

- `carbon_flux_eddycovariance`
- `carbon_flux_biomet`

and lands the processed result into:

- `carbon_flux_eddycovariance_s2_filt_1`

The preferred design in this guide is:

1. The container reads the relevant BigQuery tables directly.
2. The container converts the query results into in-memory pandas DataFrames.
3. The container runs the existing scientific processing path in memory.
4. The first live test writes outputs to local disk only.
5. The productionized cloud path writes outputs to a BigQuery staging table and
   MERGEs them into the final table.

This avoids forcing the cloud runtime through a CSV export path that is not a
good fit for the desired long-term Cloud Run Job workflow.

---

## 2. What stays true from guide 001

The following facts from guide `001` still matter and are carried forward:

### 2.1 Authoritative source and target tables

| Role | Project | Dataset | Table | Class |
| --- | --- | --- | --- | --- |
| Primary input (read) | `manglaria` | `manglaria_lakehouse_ds` | `carbon_flux_eddycovariance` | Confirmed |
| Side input (read) | `manglaria` | `manglaria_lakehouse_ds` | `carbon_flux_biomet` | Confirmed / recommended default |
| Side input candidate (deferred) | `manglaria` | `manglaria_lakehouse_ds` | `carbon_flux_site_metadata` | Required pre-implementation decision |
| Do not use as source for this milestone | `manglaria-staging` | `manglaria_lakehouse_ds` | `carbon_flux_eddycovariance` | Confirmed: same schema, currently 0 rows |
| Do not use as source for this milestone | `manglaria-staging` | `manglaria_lakehouse_ds` | `carbon_flux_biomet` | Confirmed: materially smaller than production |
| Final target (write) | `manglaria-staging` | `manglaria_lakehouse_ds` | `carbon_flux_eddycovariance_s2_filt_1` | Confirmed |

### 2.2 Production is the source of truth for both flux and biomet

This remains the recommended default because:

- production flux and biomet row counts are near parity
- staging biomet is materially smaller
- mixing production flux with staging biomet would silently misalign the run

### 2.3 Region and environment

- both projects operate in `us-central1`
- orchestration should run in `us-central1`
- production remains read-only for this milestone
- staging remains the write surface for this milestone

### 2.4 Operational asymmetry risk

This is still an unusual pattern:

- read from production
- write to staging

That must remain loud in the downstream repo and IAM design.

---

## 3. Why this guide changes direction

Guide `001` assumed the Docker image must remain a strict file-in / file-out
processor, so the orchestration had to:

- query BigQuery
- export slices to GCS
- mount files into the processor
- re-load outputs into BigQuery

That design is defensible, but it is not the best fit if all of the following
are true:

- the package can be modified
- the image can be modified
- the desired long-term runtime is Cloud Run Jobs
- the first test should mimic that eventual cloud runtime as closely as
  possible

In that situation, a better design is:

- BigQuery-native read path
- in-memory processing path
- BigQuery-native write path

This reduces orchestration surface area and removes a class of failure modes
around transient CSV exports, GCS object layout, and file-mount assumptions.

Guide `001` should therefore be treated as:

- a useful fallback design
- a reference for source/target facts, IAM cautions, and MERGE/watermark logic
- not the preferred architecture unless the image must remain file-only

---

## 4. Preferred architecture for Milestone 002

### 4.1 High-level shape

```
+----------------------------------------------+
| Cloud Run Job / Local Docker run             |
| image: versioned miaproc runtime             |
| command: module-aware CLI                    |
+------------------------+---------------------+
                         |
                         | reads BigQuery directly
                         v
+----------------------------------------------+
| BigQuery (production)                        |
| carbon_flux_eddycovariance                   |
| carbon_flux_biomet                           |
+------------------------+---------------------+
                         |
                         | in-memory DataFrames
                         v
+----------------------------------------------+
| miaproc eddy processing path                 |
| - stage-1 assembly from DataFrames           |
| - stage-2 preparation                        |
| - postproc(engine=reddyproc-reference)       |
+------------------------+---------------------+
                         |
            +------------+-------------+
            |                          |
            v                          v
+-----------------------+   +------------------------------+
| Local first test      |   | Cloud productionized path    |
| write outputs to disk |   | stage/load/MERGE to BigQuery |
+-----------------------+   +------------------------------+
```

### 4.2 Key properties

- The container remains a batch artifact, not a service.
- Cloud Run Jobs remains the preferred cloud runtime.
- The processor is now allowed to know about BigQuery for this mode.
- The scientific core still stays inside `miaproc`; cloud orchestration should
  not duplicate scientific transforms.
- Local and cloud runs should share the same code path as much as possible.

---

## 5. CLI shape must be module-aware

This repo already contains more than eddy processing (`miaproc.biomass` exists),
and more modules are expected in the future. Therefore, the CLI shape for new
cloud-native features should not assume eddy covariance is the only workload.

Recommended default:

```text
miaproc <domain> <command> ...
```

For this milestone, that means:

```text
miaproc eddy run ...
miaproc eddy run-bigquery ...
```

Not recommended as the long-term primary shape:

```text
miaproc run-bigquery ...
```

Reason:

- it does not scale cleanly once biomass or other future modules need their own
  job-oriented commands
- it forces eddy-specific assumptions into the top-level namespace

Backward compatibility note:

- the existing `miaproc run` CLI can remain for continuity
- new cloud-native work should be designed so it can migrate cleanly toward
  `miaproc eddy ...` without repainting the whole interface later

---

## 6. Recommended new capability

Add a BigQuery-native eddy batch mode that:

1. reads flux and biomet slices directly from BigQuery
2. converts them to pandas DataFrames in the container
3. builds stage-1 input from those DataFrames
4. runs the selected eddy engine
5. writes outputs either:
   - to local disk, or
   - to BigQuery staging/final tables

Recommended command family:

```bash
miaproc eddy run-bigquery \
  --engine reddyproc-reference \
  --bq-input-project manglaria \
  --bq-input-dataset manglaria_lakehouse_ds \
  --bq-flux-table carbon_flux_eddycovariance \
  --bq-biomet-table carbon_flux_biomet \
  --site-id RBRL \
  --repo-root /app \
  --output-table /out/processed.parquet \
  --output-diagnostics-json /out/diagnostics.json \
  --output-run-json /out/run.json
```

Cloud-write mode can be layered on later with additional output flags rather
than forced into the first test.

---

## 7. First live test agreed in advance

The first live test should be:

- all rows for site `RBRL`
- focus on `reddyproc-reference`
- use the already built local image tag `miaproc:cli-r45`
- use CSV only if needed as a fallback, but not as the preferred primary path
- write outputs to local disk only

This first test is not a production cutover. It is a contract-validation run.

Definition of success for the first test:

1. container authenticates to BigQuery from the local operator machine
2. container reads both production input tables
3. container filters to `site_id = 'RBRL'`
4. container completes the `reddyproc-reference` run without changing source
   tables
5. container writes:
   - processed table
   - diagnostics JSON
   - run metadata JSON
6. run metadata clearly records:
   - image identity
   - source tables
   - site filter
   - row counts
   - preflight result

---

## 8. Local-first execution path

The preferred local-first test is:

1. Operator authenticates locally with ADC / `gcloud`.
2. Operator runs the container locally.
3. The container reads BigQuery directly.
4. The container writes outputs to a mounted local output directory.

Recommended local command shape:

```powershell
docker run --rm `
  -v "${PWD}\.runs\rbrl-bq-test\outputs:/out" `
  miaproc:cli-r45 `
  miaproc eddy run-bigquery `
    --engine reddyproc-reference `
    --bq-input-project manglaria `
    --bq-input-dataset manglaria_lakehouse_ds `
    --bq-flux-table carbon_flux_eddycovariance `
    --bq-biomet-table carbon_flux_biomet `
    --site-id RBRL `
    --repo-root /app `
    --output-table /out/processed.parquet `
    --output-diagnostics-json /out/diagnostics.json `
    --output-run-json /out/run.json
```

Notes:

- The exact auth mechanism for local Docker must be decided explicitly. The
  usual candidates are:
  - mount ADC credentials into the container
  - inherit host auth through a supported local mechanism
- The first live test should stop at local disk outputs.
- No MERGE to `_s2_filt_1` from the first local run.

---

## 9. Cloud execution path

Preferred runtime: **Cloud Run Jobs**, not Cloud Run services.

Reasons:

- this is one-shot batch work
- the container does not need to listen for requests
- exit code semantics matter
- scheduled or manual execution fits the job model directly

Cloud execution should use the same BigQuery-native code path as the local test:

1. Cloud Run Job starts
2. job service account reads production BigQuery input tables
3. container processes `RBRL` or another requested slice in memory
4. container writes results to a staging destination
5. orchestration logic loads/MERGEs into final target

Two acceptable patterns:

### Pattern A - single container owns read + process + stage write

The container:

- reads BigQuery
- processes in memory
- writes outputs directly to staging BigQuery tables

Pros:

- fewer moving parts
- closest symmetry between local and cloud

Cons:

- more cloud I/O responsibilities inside the image

### Pattern B - wrapper container + processing library path

A wrapper entrypoint in the same versioned image:

- reads BigQuery
- calls the scientific processing library internally
- writes BigQuery staging/final tables

Pros:

- clearer separation between CLI/orchestration and scientific core
- still no CSV export dependency

Cons:

- slightly more internal structure

Recommended default: Pattern B, because it keeps the cloud-specific I/O logic
more obviously separate from the scientific code, while still avoiding the
guide `001` CSV/GCS-first constraint.

---

## 10. Package changes implied by this guide

The current package reads stage-1 input from CSV directories. To support the
BigQuery-native path cleanly, the package will likely need:

### 10.1 DataFrame-native stage-1 assembly

Add a new stage-1 entrypoint such as:

```python
load_stage1_from_dataframes(
    *,
    flux_df: pd.DataFrame,
    biomet_df: pd.DataFrame,
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    drop_rain_rows: bool = True,
    site_id: str | None = None,
) -> pd.DataFrame
```

This should reuse the existing logic in `miaproc.eddy.core` rather than
re-implement it in a cloud wrapper.

### 10.2 BigQuery runner module

Add a module like:

```text
miaproc.eddy.bigquery_runner
```

Responsibilities:

- build deterministic BigQuery reads
- filter to the requested site / optional time window
- materialize pandas DataFrames
- call `load_stage1_from_dataframes(...)`
- call `postproc(...)`
- write outputs

### 10.3 Module-aware CLI extension

Add a new CLI path for BigQuery-native batch runs without painting the whole
project into an eddy-only corner.

---

## 11. BigQuery read/write contract

### 11.1 Read contract

Preferred read path:

- BigQuery Python client with the BigQuery Storage Read API enabled when
  available

Why:

- faster reads
- natural DataFrame integration
- closer to the actual cloud runtime than host-side CSV export

### 11.2 Write contract

For the first local test:

- write to local disk only

For the productionized cloud path:

- write to a BigQuery staging table
- validate
- MERGE into
  `manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1`

### 11.3 Merge identity

Carry forward the guide `001` default:

- canonical row identity is `(site_id, timestamp)`

This must stay aligned across:

- source dedup
- stage validation
- final MERGE

`primary_key` should still be validated and preserved, but `(site_id, timestamp)`
remains the operational merge identity unless science ownership explicitly says
otherwise.

---

## 12. Dedup and recomputation posture

Carry forward the guide `001` reasoning, but adapt it to the BigQuery-native
path.

### 12.1 Dedup

Recommended default:

- dedup on `(site_id, timestamp)`
- use a deterministic tie-breaker such as `filename DESC NULLS LAST` if that is
  available and confirmed

Required decision:

- confirm the exact tie-break rule for real source duplicates

### 12.2 Recomputation window

Recommended default for future scheduled runs:

- reprocess a rolling recent window, not only brand-new rows

Guide `001` recommended:

- recompute last 30 days
- advance watermark only to `now - 7 days`

That is still a reasonable default because these time-series transforms are not
purely row-local.

For the first test, ignore the watermark and process all `RBRL` rows.

---

## 13. Output contract

The output contract should remain aligned with the existing `miaproc` CLI:

1. processed table
2. diagnostics JSON
3. run metadata JSON

For the BigQuery-native mode, add the following to run metadata:

- input project
- input dataset
- input flux table
- input biomet table
- site filter
- optional time window
- image tag / digest if available
- BigQuery read row counts before and after dedup
- engine
- preflight result for `reddyproc-reference`

The first local test should keep the table on disk.

The cloud path can later add:

- staging table reference
- final table reference
- MERGE inserted/updated counts

---

## 14. IAM and identity considerations

Carry forward the high-value cautions from guide `001`:

- read access to `manglaria` should remain read-only and table-scoped
- write access belongs in `manglaria-staging`
- jobs should be created in `manglaria-staging`, not `manglaria`
- region should remain `us-central1`

New implication for this guide:

- because the container now talks to BigQuery directly, the runtime identity
  must be allowed to:
  - read from the production input tables
  - optionally write to staging BigQuery tables in the cloud path

This is a stronger in-image cloud dependency than guide `001`, so it must be
documented very clearly in the downstream repo's deployment docs.

---

## 15. Local authentication considerations

For the local first test, confirm:

- `bq` can read the source dataset
- the local Docker runtime can authenticate to BigQuery from inside the
  container

Do not assume that host-side `gcloud` auth automatically works inside Docker.
Treat local auth wiring as part of the first-test contract.

Recommended default:

- use ADC-compatible credentials for the local run
- record the chosen method in the downstream repo's runbook

---

## 16. Cloud Run Job remains the preferred runtime

Even though the file-only constraint is relaxed, the runtime recommendation does
not change:

- use **Cloud Run Jobs**
- do not use a request-serving Cloud Run service for this batch flow

Reasons:

- explicit execution model
- exit codes matter
- scheduled batch is natural
- resource sizing and retries are job-oriented

If concurrency or very large working sets later become a problem, reassess with
Cloud Batch. But Cloud Run Jobs is the right first target.

---

## 17. First implementation scope

Keep the first implementation narrow and honest.

### 17.1 In scope

- local Docker run
- direct BigQuery reads inside the container
- `site_id = 'RBRL'`
- all available rows for `RBRL`
- engine `reddyproc-reference`
- outputs written to local disk only

### 17.2 Out of scope for the first test

- writing final outputs to BigQuery
- watermark advancement
- scheduled cloud runs
- MERGE automation
- biomass orchestration
- generalized multi-module cloud framework

### 17.3 Why the scope is narrow

The first test is about proving the new runtime contract:

- direct BigQuery read
- in-memory processing
- containerized `reddyproc-reference`
- module-aware CLI direction

Do not overload it with full production orchestration on day one.

---

## 18. Verification checklist for the first live test

Before claiming the first local BigQuery-native test works:

- [ ] The local image `miaproc:cli-r45` exists and is recorded.
- [ ] The container can authenticate to BigQuery from the local machine.
- [ ] The container can read:
  - `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance`
  - `manglaria.manglaria_lakehouse_ds.carbon_flux_biomet`
- [ ] The container filters to `site_id = 'RBRL'`.
- [ ] The container runs `reddyproc-reference`.
- [ ] Decision 010 preflight still approves the in-image R runtime.
- [ ] Outputs are written to local disk:
  - processed table
  - diagnostics JSON
  - run metadata JSON
- [ ] No BigQuery table is written during this first test.
- [ ] Run metadata captures enough detail to repeat the run.

---

## 19. Recommended follow-up after the first local test

If the first local BigQuery-native run succeeds, the next work should be:

1. Add cloud-run staging write support.
2. Add validation SQL / checks for staged outputs.
3. Add MERGE into `_s2_filt_1`.
4. Add run record + watermark handling.
5. Add Cloud Run Job deployment and IAM docs.

Do not skip directly from "local disk output worked" to "scheduled production
job" without the staging/MERGE layer.

---

## 20. Open questions and deferred decisions

These remain open until explicitly resolved.

### 20.1 Exact module-aware CLI shape

Recommended default:

- `miaproc eddy run-bigquery ...`

Required decision:

- whether to formally introduce `miaproc <domain> <command>` now, or stage it
  through backward-compatible aliases

### 20.2 Exact local Docker auth method

Required decision:

- how local credentials are made available inside the container for the first
  live test

### 20.3 Dedup tie-break rule

Recommended default:

- dedup on `(site_id, timestamp)` with deterministic tie-break

Required decision:

- exact tie-break column and ordering

### 20.4 Site metadata usage

Required decision:

- whether `carbon_flux_site_metadata` is actually required by the processing
  contract for this BigQuery-native path

### 20.5 Cloud write strategy

Required decision:

- should the container itself write staging/final BigQuery tables, or should a
  wrapper entrypoint in the same image own all writes

Recommended default:

- wrapper entrypoint in the same image

### 20.6 Whether guide 001 fallback must remain supported

Required decision:

- whether to preserve the CSV/GCS export path as a supported fallback, or only
  keep it as a historical/reference design

Recommended default:

- keep it as a fallback/reference until the BigQuery-native path is proven

---

## 21. Non-goals for this milestone

- Do not redesign the scientific core.
- Do not silently change Decision 010 or Decision 011 posture.
- Do not write to production BigQuery tables.
- Do not treat staging as the source of truth for biomet while production is
  the source of truth for flux.
- Do not assume eddy covariance is the only long-term use of the image.
- Do not let the new BigQuery-native direction collapse into an eddy-only CLI
  namespace that blocks future biomass or other modules.

---

## 22. Recommendations summary

- Keep the production source tables and staging target table from guide `001`.
- Switch the preferred architecture from CSV/GCS-first to BigQuery-native,
  in-memory processing.
- Keep Cloud Run Jobs as the preferred runtime.
- Use a module-aware CLI direction:
  `miaproc <domain> <command>`.
- First live test:
  - local Docker
  - `miaproc:cli-r45`
  - all `RBRL` rows
  - `reddyproc-reference`
  - local disk outputs only
- After that, add staging write + MERGE + watermark behavior.
- Keep guide `001` as fallback/reference, not primary direction.

---

## 23. Evidence base

This guide is grounded in:

- `docs/guides/001_carbon_flux_bq_orchestration_guide.md`
- `08_pkg/src/miaproc/cli.py`
- `08_pkg/src/miaproc/eddy/core.py`
- `08_pkg/src/miaproc/eddy/stage2.py`
- `docker/README.md`
- `docker/Dockerfile.miaproc-r45-reddyproc`
- `08_pkg/backend_contract.md`
- `05_governance/decision_log.md`
- `90_legacy_review/exports/20260426_150929/...` artifacts cited by guide `001`

If a future live export contradicts any source-table fact carried forward from
guide `001`, update this guide explicitly rather than letting the old claim
drift silently.
