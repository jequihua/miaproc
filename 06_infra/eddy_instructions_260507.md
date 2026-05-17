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

## CLI commands

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
  --bq-stage-table     cf_s2_stage
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
- M24: `--site-id` is no longer a CLI flag. Use
  `--group-column site_id` to process every site present (the
  stacked all-site output stages once into a shared stage table
  and final MERGE advances per-site watermarks). Drop
  `--group-column` to process the whole input as one dataset.
- `--repo-root /app` triggers the in-image project-scoped R preflight
  (Decision 010 / R11). No bypass flag exists; do not add one.
- Audit row goes to `<output-project>._orch.cf_s2_runs`; watermark
  per-site goes to `cf_s2_watermark` and advances only on a
  successful MERGE.
- Production project `manglaria` is read-only at the package layer (`forbidden_write_projects=("manglaria",)`).
- Exit codes: `0` success / `2` preflight unapproved / `3` validation / `4` runtime.

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

But important for us are:

### BigQuery-native split silver -> gold path (M22)

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

## Runtime IAM (least privilege)

| Role | Resource |
|---|---|
| `roles/bigquery.jobUser` | project `manglaria-staging` |
| `roles/bigquery.dataEditor` | dataset `manglaria-staging:manglaria_lakehouse_ds` |
| `roles/bigquery.dataEditor` (or dataset-ACL `WRITER`) | dataset `manglaria-staging:_orch` |
| `roles/bigquery.dataViewer` | **table-level** on `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance` and `carbon_flux_biomet` |
| `roles/artifactregistry.reader` | repo `cloud-run-source-deploy` in `manglaria-staging` |

Do **not** grant any write role on production project `manglaria`.

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
this repo only owns the package and image surface.
