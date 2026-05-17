# Eddy covariance — cloud handoff

One Docker image processes BigQuery flux + biomet inputs through the
REddyProc-reference R backend and writes back to a staging analytical
table under explicit operator opt-in MERGE.

## Image

Pull by immutable digest from Artifact Registry:

```
us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
```

Region `us-central1`, repo `cloud-run-source-deploy`. The `:cli-r45`
mutable tag also points here, but pin the digest in production.

This M23 image was rebuilt from the M22-accepted source tree and
contains all five eddy CLI commands needed for the four operations
documented below: `miaproc eddy run-bigquery`,
`miaproc eddy run-silver`, `miaproc eddy run-gold`,
`miaproc eddy run-bigquery-silver`, and
`miaproc eddy run-bigquery-gold`. The pre-existing cloud-validated
one-shot manifests at `06_infra/cloudrun/miaproc-eddy-rbrl-stage.yaml`
and `miaproc-eddy-rbrl-merge.yaml` remain pinned to the older
`sha256:0db1b2d96fa…3408` M11/M12/M13 digest because that digest was
the one cloud-validated under that pass; new deployments should pin
the M23 digest above.

## Command

```
miaproc eddy run-bigquery
  --engine reddyproc-reference
  --bq-input-project   manglaria
  --bq-input-dataset   manglaria_lakehouse_ds
  --bq-flux-table      carbon_flux_eddycovariance
  --bq-biomet-table    carbon_flux_biomet
  --bq-billing-project manglaria-staging
  --group-column       site_id          # M24: process every site present
  --repo-root          /app            # required; project-scoped R preflight
  --bq-output-project  manglaria-staging
  --bq-output-dataset  manglaria_lakehouse_ds
  --bq-stage-table     cf_s2_stage     # shared all-site stage table
  --bq-control-dataset _orch
  [--bq-final-table    carbon_flux_eddycovariance_s2_filt_1]
  [--bq-allow-final-merge]
  --output-table             /tmp/processed.parquet
  --output-diagnostics-json  /tmp/diagnostics.json
  --output-run-json          /tmp/run.json
```

Key contract:

- **Stage-only is the safe default.** Final-table mutation requires
  both `--bq-final-table` and `--bq-allow-final-merge`.
- M24: `--site-id` is no longer a CLI flag. Single-value site
  selection is gone from every eddy command. Use
  `--group-column site_id` to partition the all-data input into
  per-category runs (every non-null site in the read window is
  processed); the stacked output stages once into the shared stage
  table and a final MERGE advances **per-site watermarks** for
  every stacked site.
- `--repo-root /app` triggers the in-image project-scoped R preflight
  (Decision 010 / R11). No bypass flag exists; do not add one.
- Audit row goes to `<output-project>._orch.cf_s2_runs` (`site_id`
  field carries the unique site for single-site runs or the
  `<grouped>` label when stacked); watermark per-site goes to
  `cf_s2_watermark` and advances only on a successful MERGE — one
  watermark row per stacked site under M24.
- Production project `manglaria` is read-only at the package layer
  (`forbidden_write_projects=("manglaria",)`).
- Exit codes: `0` success / `2` preflight unapproved / `3` validation
  / `4` runtime.

## Four eddy operations cloud engineers must distinguish

There are four useful operations to wire into Cloud Run with this
image. They map onto three runtime shapes (one-shot, file-based
split, BigQuery-native split) but the four operations matter to
cloud engineers because each one has a distinct safety posture:

1. **One-shot `miaproc eddy run-bigquery`.** Reads bronze flux +
   biomet from BigQuery, runs the engine in memory, writes local
   artifacts, and (with the writeback flag set) optionally stages +
   MERGEs in a single invocation. Easiest to deploy as one job;
   harder to gate the MERGE separately.
2. **Split `miaproc eddy run-bigquery-silver`.** Reads bronze + runs
   stage-1 only; produces silver. **Stage-only by design** — there
   is no `--bq-final-table` / `--bq-allow-final-merge` flag for
   silver. No engine, no R, no preflight. Safe to schedule on a
   regular cadence.
3. **Split `miaproc eddy run-bigquery-gold` (stage-only).** Reads a
   silver-stage table and runs the engine; gold output is written
   locally and to a gold stage table. With
   `--engine reddyproc-reference` (the default) this requires
   `--repo-root /app` so the project-scoped R preflight can approve
   the runtime (Decision 010 / R11).
4. **Explicit `miaproc eddy run-bigquery-gold` final MERGE.** Same
   as operation 3 but with `--bq-final-table` and
   `--bq-allow-final-merge`; **mutates the staging final gold
   table**. Operators must opt in explicitly per execution; deploy
   stage-only and merge as separate Cloud Run Jobs so accidentally
   re-executing the routine job cannot MERGE.

Semantic rail: bronze/source = raw flux + biomet BigQuery tables;
silver = cleaned, joined, regularized **pre-backend** eddy table;
gold = post-`postproc(...)` analytical output. Silver must not be
derived from gold.

### 1. One-shot BigQuery path

Use `miaproc eddy run-bigquery` when the job should read flux +
biomet rows directly from BigQuery, run the selected eddy engine, and
optionally write the processed output back to BigQuery.

Operational shape:

```
BigQuery bronze/source tables
  -> miaproc eddy run-bigquery
  -> /tmp/processed.parquet + diagnostics + run JSON
  -> BigQuery stage table
  -> optional final-table MERGE
```

This is the path used by the stage-only / merge Cloud Run sketch
below. It owns the BigQuery read and writeback orchestration inside
the package. Use this when the desired cloud job is a single
end-to-end processing unit for one site.

### 2. File-based split silver -> gold path

Use `miaproc eddy run-silver` and `miaproc eddy run-gold` when cloud
orchestration wants an explicit intermediate file artifact between
ingestion/regularization and backend flux processing.

Operational shape:

```
bronze flux + biomet files
  -> miaproc eddy run-silver
  -> persisted silver table + silver run JSON
  -> miaproc eddy run-gold
  -> persisted gold table + diagnostics + gold run JSON
```

Important differences:

- `run-silver` is stage-1 only: load, clean, join, regularize, write a
  silver table. It has no engine dispatch, no R, and no project-scoped
  R preflight.
- `run-gold` reads a silver table from disk, dispatches the selected
  engine, and writes the analytical gold output. With the production
  `reddyproc-reference` engine it requires `--repo-root /app` so the
  in-image project-scoped R preflight can approve the runtime.
- The silver artifact must live somewhere durable between Cloud Run
  jobs. Do not rely on `/tmp` across jobs. Use a bucket mount,
  explicit GCS download/upload, or another infra-owned storage
  boundary.
- This file-based split path does not itself perform the M8 BigQuery
  stage/merge writeback. For BigQuery-native silver/gold see runtime
  shape 3 below.

### 3. BigQuery-native split silver -> gold path (M22)

Use `miaproc eddy run-bigquery-silver` and
`miaproc eddy run-bigquery-gold` when cloud orchestration wants the
silver / gold split but does **not** want to round-trip the silver
artifact through a bucket mount. Each stage reads from BigQuery and
optionally stages back to BigQuery directly.

Operational shape:

```
BigQuery bronze/source flux + biomet
  -> miaproc eddy run-bigquery-silver
  -> BigQuery silver stage table + local silver artifacts
  -> miaproc eddy run-bigquery-gold
  -> BigQuery gold stage/final table + local gold artifacts
```

Important differences from runtime shape 1 (one-shot) and shape 2
(file-based split):

- Silver is produced **before** any backend flux processing and is
  **not** a subset of gold. Cloud engineers must not derive silver by
  filtering a processed gold table — that inverts the project
  semantics.
- `run-bigquery-silver` has no `--engine`, no `--repo-root`, and no R
  preflight. It is the same stage-1 pipeline as `run-silver`.
- `run-bigquery-silver` writeback is **stage-only by design**. There is
  no `--bq-final-table` / `--bq-allow-final-merge` flag for silver in
  M22. A later milestone may add a justified silver table-promotion
  path if operators need one.
- `run-bigquery-gold --engine reddyproc-reference` keeps the
  Decision 010 / R11 project-scoped R preflight gate and requires
  `--repo-root /app`.
- Gold writeback reuses the accepted M8/M10 safety posture: stage-only
  default, explicit `--bq-allow-final-merge` for any final-table
  mutation, package-level `forbidden_write_projects=("manglaria",)`
  invariant unchanged.
- The silver BigQuery table written by `run-bigquery-silver` carries
  the M8 identity columns (`primary_key`, `site_id`, `timestamp`) plus
  `DateTime` plus the stage-1 columns. `run-bigquery-gold` reads
  silver back from that table and feeds it through the same gold
  engine path as `run-gold`.

## Runtime IAM (least privilege)

| Role | Resource |
|---|---|
| `roles/bigquery.jobUser` | project `manglaria-staging` |
| `roles/bigquery.dataEditor` | dataset `manglaria-staging:manglaria_lakehouse_ds` |
| `roles/bigquery.dataEditor` (or dataset-ACL `WRITER`) | dataset `manglaria-staging:_orch` |
| `roles/bigquery.dataViewer` | **table-level** on `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance` and `carbon_flux_biomet` |
| `roles/artifactregistry.reader` | repo `cloud-run-source-deploy` in `manglaria-staging` |

Do **not** grant any write role on production project `manglaria`.

## Cloud Run Job sketch (rough)

Stage-only and merge belong in separate jobs so accidental re-execution
of the routine job cannot MERGE. Deploy with `gcloud run jobs replace
<file>.yaml`, execute with `gcloud run jobs execute <name> --wait`.

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-<site>-stage
  namespace: manglaria-staging
spec:
  template:
    spec:
      template:
        spec:
          serviceAccountName: <runtime-sa>@manglaria-staging.iam.gserviceaccount.com
          maxRetries: 0
          timeoutSeconds: 1800
          containers:
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
            command: [miaproc]
            args:
            - eddy
            - run-bigquery
            - --engine=reddyproc-reference
            - --bq-input-project=manglaria
            - --bq-input-dataset=manglaria_lakehouse_ds
            - --bq-flux-table=carbon_flux_eddycovariance
            - --bq-biomet-table=carbon_flux_biomet
            - --bq-billing-project=manglaria-staging
            - --group-column=site_id
            - --repo-root=/app
            - --bq-output-project=manglaria-staging
            - --bq-output-dataset=manglaria_lakehouse_ds
            - --bq-stage-table=cf_s2_stage
            - --bq-control-dataset=_orch
            - --output-table=/tmp/processed.parquet
            - --output-diagnostics-json=/tmp/diagnostics.json
            - --output-run-json=/tmp/run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

For the **merge** job, clone the manifest, rename to
`miaproc-eddy-<site>-merge`, and add to the `args` list:

```yaml
            - --bq-final-table=carbon_flux_eddycovariance_s2_filt_1
            - --bq-allow-final-merge
```

## Split silver/gold Cloud Run sketches (rough)

These sketches are intentionally incomplete around storage. They show
the command split and the runtime expectations; cloud engineers should
replace the placeholder bucket mounts, service account, and object
paths with the team's preferred GCS / BigQuery export-import pattern.

### Bronze -> silver job

This job consumes bronze EddyPro-shaped flux + biomet files from a
durable mounted/input location and writes a silver table to durable
intermediate storage. It does not use R and does not need
`--repo-root`.

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-<site>-silver
  namespace: manglaria-staging
spec:
  template:
    spec:
      template:
        spec:
          serviceAccountName: <runtime-sa>@manglaria-staging.iam.gserviceaccount.com
          maxRetries: 0
          timeoutSeconds: 1800
          containers:
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
            command: [miaproc]
            args:
            - eddy
            - run-silver
            - --flux-dir=/mnt/bronze/flux
            - --biomet-dir=/mnt/bronze/biomet
            - --group-column=site_id
            - --output-table=/mnt/silver/<site>/silver.parquet
            - --output-run-json=/mnt/silver/<site>/silver_run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

Expected outputs:

- `/mnt/silver/<site>/silver.parquet`
- `/mnt/silver/<site>/silver_run.json`

The `/mnt/bronze` and `/mnt/silver` paths are placeholders for a real
bucket mount, sidecar download/upload step, or equivalent
infra-owned storage integration.

### Silver -> gold job

This job consumes the persisted silver table and runs the production
`reddyproc-reference` backend by default. It needs `--repo-root /app`
for the project-scoped R preflight.

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-<site>-gold
  namespace: manglaria-staging
spec:
  template:
    spec:
      template:
        spec:
          serviceAccountName: <runtime-sa>@manglaria-staging.iam.gserviceaccount.com
          maxRetries: 0
          timeoutSeconds: 1800
          containers:
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
            command: [miaproc]
            args:
            - eddy
            - run-gold
            - --silver-table=/mnt/silver/<site>/silver.parquet
            - --engine=reddyproc-reference
            - --repo-root=/app
            - --output-table=/mnt/gold/<site>/gold.parquet
            - --output-diagnostics-json=/mnt/gold/<site>/gold_diagnostics.json
            - --output-run-json=/mnt/gold/<site>/gold_run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

Expected outputs:

- `/mnt/gold/<site>/gold.parquet`
- `/mnt/gold/<site>/gold_diagnostics.json`
- `/mnt/gold/<site>/gold_run.json`

If the final destination is BigQuery, load `gold.parquet` into the
chosen staging/final table as a separate infra step, or use
`miaproc eddy run-bigquery` (one-shot) or the BigQuery-native split
sketches below.

## BigQuery-native silver/gold Cloud Run sketches (M22)

These sketches mirror the file-based split above but read from and
write to BigQuery directly. Cloud engineers must replace the
placeholder source/stage/final table names with the team's chosen
identifiers; the package layer does not hard-code any of them.

### BigQuery bronze/source -> silver job

Reads the bronze flux + biomet source tables in `manglaria` and
stages a per-site silver table in `manglaria-staging`. No R preflight,
no `--repo-root`. Stage-only by design.

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-<site>-bigquery-silver
  namespace: manglaria-staging
spec:
  template:
    spec:
      template:
        spec:
          serviceAccountName: <runtime-sa>@manglaria-staging.iam.gserviceaccount.com
          maxRetries: 0
          timeoutSeconds: 1800
          containers:
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
            command: [miaproc]
            args:
            - eddy
            - run-bigquery-silver
            - --bq-input-project=manglaria
            - --bq-input-dataset=manglaria_lakehouse_ds
            - --bq-flux-table=carbon_flux_eddycovariance
            - --bq-biomet-table=carbon_flux_biomet
            - --bq-billing-project=manglaria-staging
            - --group-column=site_id
            - --bq-output-project=manglaria-staging
            - --bq-output-dataset=manglaria_lakehouse_ds
            - --bq-stage-table=cf_s2_silver_stage
            - --bq-control-dataset=_orch
            - --output-table=/tmp/silver.parquet
            - --output-run-json=/tmp/silver_run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

### BigQuery silver -> gold job

Reads the silver stage table written above and runs
`reddyproc-reference` against it. Final-table MERGE is gated by an
explicit `--bq-allow-final-merge`; the safe default is stage-only.

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-<site>-bigquery-gold-stage
  namespace: manglaria-staging
spec:
  template:
    spec:
      template:
        spec:
          serviceAccountName: <runtime-sa>@manglaria-staging.iam.gserviceaccount.com
          maxRetries: 0
          timeoutSeconds: 1800
          containers:
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
            command: [miaproc]
            args:
            - eddy
            - run-bigquery-gold
            - --engine=reddyproc-reference
            - --bq-input-project=manglaria-staging
            - --bq-input-dataset=manglaria_lakehouse_ds
            - --bq-silver-table=cf_s2_silver_stage
            - --bq-billing-project=manglaria-staging
            - --group-column=site_id
            - --repo-root=/app
            - --bq-output-project=manglaria-staging
            - --bq-output-dataset=manglaria_lakehouse_ds
            - --bq-stage-table=cf_s2_gold_stage
            - --bq-control-dataset=_orch
            - --output-table=/tmp/gold.parquet
            - --output-diagnostics-json=/tmp/gold_diag.json
            - --output-run-json=/tmp/gold_run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

### BigQuery silver -> gold MERGE job (explicit opt-in)

This is the same shape as the gold stage-only job above, but with
`--bq-final-table` and `--bq-allow-final-merge` added. It **mutates
the staging final gold table**. Deploy and execute as a separate job
from the stage-only one so accidentally re-running the routine
stage-only job cannot MERGE.

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-<site>-bigquery-gold-merge
  namespace: manglaria-staging
spec:
  template:
    spec:
      template:
        spec:
          serviceAccountName: <runtime-sa>@manglaria-staging.iam.gserviceaccount.com
          maxRetries: 0
          timeoutSeconds: 1800
          containers:
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
            command: [miaproc]
            args:
            - eddy
            - run-bigquery-gold
            - --engine=reddyproc-reference
            - --bq-input-project=manglaria-staging
            - --bq-input-dataset=manglaria_lakehouse_ds
            - --bq-silver-table=cf_s2_silver_stage
            - --bq-billing-project=manglaria-staging
            - --group-column=site_id
            - --repo-root=/app
            - --bq-output-project=manglaria-staging
            - --bq-output-dataset=manglaria_lakehouse_ds
            - --bq-stage-table=cf_s2_gold_stage
            - --bq-final-table=cf_s2_gold
            - --bq-control-dataset=_orch
            - --bq-allow-final-merge
            - --output-table=/tmp/gold.parquet
            - --output-diagnostics-json=/tmp/gold_diag.json
            - --output-run-json=/tmp/gold_run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

These BigQuery-native split sketches are intentionally rough. The
table names, IAM bindings, and deployment cadence are operator-owned;
this repo only owns the package and image surface. Concrete draft
manifests for these three jobs live at:

- `06_infra/cloudrun/miaproc-eddy-rbrl-bigquery-silver.yaml`
- `06_infra/cloudrun/miaproc-eddy-rbrl-bigquery-gold-stage.yaml`
- `06_infra/cloudrun/miaproc-eddy-rbrl-bigquery-gold-merge.yaml`

All three are pinned to the M23 digest above and labeled as draft
examples, not deployed infrastructure.
