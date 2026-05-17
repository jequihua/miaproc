# Eddy covariance — cloud handoff (post-M26 image, 2026-05-08)

Requires the existence of 3 BigQuery tables (can have different names):
- carbon_flux_eddycovariance_s2_filt_1
- carbon_flux_eddycovariance_g1_stage
- carbon_flux_eddycovariance_g2_merged

Also see the attached schemas.

## Image

Pull by immutable digest from Artifact Registry:

```
us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:9e8b10d0fbd6f7c1bd7725e095fefae8dffc025e1ef3b23ecee1cf639473220c
```

Region `us-central1`, repo `cloud-run-source-deploy`. The
`:cli-r45` mutable tag also points here, but pin the digest in
production. The image was rebuilt and pushed under M27 from
post-M26 source: it carries the accepted M24 / M24A grouped
CLI (`--group-column site_id`, no `--site-id`) and the M26
silver validator that accepts staging-to-staging input/output
shapes for `eddy run-bigquery-silver` while keeping production
project `manglaria` read-only at the package layer
(`forbidden_write_projects=("manglaria",)`). The previous M25
image was deleted from Artifact Registry as part of the M27
publish — it predated the relaxed silver validator and would
reject a staging-mirrored bronze read into a same-project
silver writeback.


## How the CLI processes data

- The CLI reads **all rows** in the requested BigQuery window. There
  is no first-class single-site selection on any eddy command.
- `--group-column site_id` is the operator default for this project:
  the CLI loops through every non-null `site_id` present, processes
  each category independently, writes a per-category local artefact
  under `<output-table-stem>__groups/`, and writes the final
  `--output-table` as the deterministic stack of all per-category
  outputs.
- BigQuery writeback (when engaged) runs **once** with the stacked
  all-category stage payload, so shared stage tables such as
  `carbon_flux_eddycovariance_s2_filt_1` / `carbon_flux_eddycovariance_g1_stage1` are valid and the
  prior per-site `WRITE_TRUNCATE` race against a shared stage
  table is gone.
- Final MERGE (when explicitly enabled) advances **per-site
  watermarks**: one `cf_s2_watermark` row per distinct `site_id`
  in the staged frame, each carrying that site's max timestamp.
  Stage-only and failed runs do not advance any watermark.
- Single-site experiments are no longer a CLI option. Pre-filter
  the input or call package functions programmatically with a
  pre-filtered DataFrame.

## Key contract

- **Stage-only is the safe default.** Final-table mutation requires
  both `--bq-final-table` and `--bq-allow-final-merge`.
- M24 / M24A: there is no CLI single-site selector on any eddy
  command. Use `--group-column site_id` to process every site
  present.
- `--repo-root /app` triggers the in-image project-scoped R
  preflight (Decision 010 / R11). No bypass flag exists; do not
  add one.
- Audit row goes to `<output-project>._orch.cf_s2_runs`. For
  multi-site stacked runs the row's `site_id` field carries the
  sentinel `<grouped>`; for single-site runs (one site present in
  the staged frame, or a legacy single-site programmatic caller)
  the actual `site_id` is recorded.
- Watermark per-site goes to `cf_s2_watermark` and advances only
  on a successful MERGE — one watermark row per distinct
  `site_id` present in the staged frame.
- Production project `manglaria` is read-only at the package
  layer (`forbidden_write_projects=("manglaria",)`).
- Exit codes: `0` success / `2` preflight unapproved /
  `3` validation / `4` runtime.

## Three operations cloud engineers must distinguish

This file documents the BigQuery-to-BigQuery flow only. The
operations have distinct safety postures and should be deployed
as separate Cloud Run Jobs:

1. **`miaproc eddy run-bigquery-silver`** (bronze BigQuery →
   silver BigQuery stage). Stage-1 only. No engine, no R, no
   preflight. Writeback is **stage-only by design** — there is
   no `--bq-final-table` / `--bq-allow-final-merge` flag on
   silver. Safe to schedule on a regular cadence.
2. **`miaproc eddy run-bigquery-gold` (stage-only)** (silver
   BigQuery stage → gold BigQuery stage). With
   `--engine reddyproc-reference` (default), requires
   `--repo-root /app` so the project-scoped R preflight can
   approve the runtime (Decision 010 / R11). Writeback is
   stage-only without `--bq-allow-final-merge`.
3. **`miaproc eddy run-bigquery-gold` (explicit MERGE)**. Same
   as operation 2 plus `--bq-final-table` and
   `--bq-allow-final-merge`. **Mutates the staging final gold
   table.** Operators must opt in explicitly per execution.
   Deploy stage-only and merge as separate Cloud Run Jobs so
   accidentally re-executing the routine job cannot MERGE.


## Operational shape

```
BigQuery bronze/source flux + biomet
  -> miaproc eddy run-bigquery-silver  (--group-column site_id)
  -> shared BigQuery silver stage table (carbon_flux_eddycovariance_s2_filt_1)
  -> miaproc eddy run-bigquery-gold    (--group-column site_id)
  -> shared BigQuery gold stage table (carbon_flux_eddycovariance_g1_stage)
  [-> optional explicit MERGE into final gold table carbon_flux_eddycovariance_g2_merged]
```

Per-site watermarks advance on the explicit-MERGE step only.

## Runtime IAM (least privilege)

| Role | Resource |
|---|---|
| `roles/bigquery.jobUser` | project `manglaria-staging` |
| `roles/bigquery.dataEditor` | dataset `manglaria-staging:manglaria_lakehouse_ds` |
| `roles/bigquery.dataEditor` (or dataset-ACL `WRITER`) | dataset `manglaria-staging:_orch` |
| `roles/bigquery.dataViewer` | **table-level** on `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance` and `carbon_flux_biomet` |
| `roles/artifactregistry.reader` | repo `cloud-run-source-deploy` in `manglaria-staging` |

Do **not** grant any write role on production project
`manglaria`. The package layer additionally enforces
`forbidden_write_projects=("manglaria",)` on the writeback config.

## CLI command shapes (BigQuery-to-BigQuery)

### 1. Bronze BigQuery → silver BigQuery stage

```
miaproc eddy run-bigquery-silver
  --bq-input-project   manglaria
  --bq-input-dataset   manglaria_lakehouse_ds
  --bq-flux-table      carbon_flux_eddycovariance
  --bq-biomet-table    carbon_flux_biomet
  --bq-billing-project manglaria-staging
  --group-column       site_id
  --bq-output-project  manglaria-staging
  --bq-output-dataset  manglaria_lakehouse_ds
  --bq-stage-table     carbon_flux_eddycovariance_s2_filt_1
  --bq-control-dataset _orch
  --output-table       /tmp/silver.parquet
  --output-run-json    /tmp/silver_run.json
```

Stage-only by design. No `--engine`, no `--repo-root`. The
silver writeback writes one stacked all-site stage payload
into `carbon_flux_eddycovariance_s2_filt_1`.

**Allowed project shapes (M26).** Bronze/source may be read
from production or from a staging mirror. Both of these silver
shapes are accepted:

```
--bq-input-project   manglaria
--bq-output-project  manglaria-staging
```

```
--bq-input-project   manglaria-staging
--bq-output-project  manglaria-staging
```

Production writes remain forbidden. Setting
`--bq-output-project manglaria` is rejected at the package
layer by `BigQueryWritebackConfig.forbidden_write_projects`
(surfacing as exit `4`); the CLI no longer enforces an
unconditional `output != input` rule on silver.

### 2. Silver BigQuery stage → gold BigQuery stage (stage-only)

```
miaproc eddy run-bigquery-gold
  --engine             reddyproc-reference
  --bq-input-project   manglaria-staging
  --bq-input-dataset   manglaria_lakehouse_ds
  --bq-silver-table    carbon_flux_eddycovariance_s2_filt_1
  --bq-billing-project manglaria-staging
  --group-column       site_id
  --repo-root          /app
  --bq-output-project  manglaria-staging
  --bq-output-dataset  manglaria_lakehouse_ds
  --bq-stage-table     carbon_flux_eddycovariance_g1_stage
  --bq-control-dataset _orch
  --output-table             /tmp/gold.parquet
  --output-diagnostics-json  /tmp/gold_diag.json
  --output-run-json          /tmp/gold_run.json
```

Stage-only. No `--bq-final-table` / `--bq-allow-final-merge`.
Gold writeback writes one stacked all-site stage payload into
`carbon_flux_eddycovariance_g1_stage`.

### 3. Silver BigQuery stage → gold final MERGE (explicit opt-in)

```
miaproc eddy run-bigquery-gold
  --engine             reddyproc-reference
  --bq-input-project   manglaria-staging
  --bq-input-dataset   manglaria_lakehouse_ds
  --bq-silver-table    carbon_flux_eddycovariance_s2_filt_1
  --bq-billing-project manglaria-staging
  --group-column       site_id
  --repo-root          /app
  --bq-output-project  manglaria-staging
  --bq-output-dataset  manglaria_lakehouse_ds
  --bq-stage-table     carbon_flux_eddycovariance_g1_stage
  --bq-final-table     carbon_flux_eddycovariance_g2_merged
  --bq-control-dataset _orch
  --bq-allow-final-merge
  --output-table             /tmp/gold.parquet
  --output-diagnostics-json  /tmp/gold_diag.json
  --output-run-json          /tmp/gold_run.json
```

**Mutates the staging final gold table** on
`(site_id, timestamp)`. Per-site watermarks advance after the
merge succeeds — one `cf_s2_watermark` row per distinct
`site_id` in the staged frame.

## Cloud Run Job sketches (rough)

These sketches are intentionally minimal. Operators replace the
service account, region, namespace, resource limits, and any
window flags with their own values. Cloud engineers should
deploy stage-only and merge as **separate** jobs so accidental
re-execution of the routine job cannot MERGE.

### Sketch: bronze → silver

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-bigquery-silver
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
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:9e8b10d0fbd6f7c1bd7725e095fefae8dffc025e1ef3b23ecee1cf639473220c
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
            - --bq-stage-table=carbon_flux_eddycovariance_s2_filt_1
            - --bq-control-dataset=_orch
            - --output-table=/tmp/silver.parquet
            - --output-run-json=/tmp/silver_run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

### Sketch: silver → gold (stage-only)

```yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: miaproc-eddy-bigquery-gold-stage
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
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:9e8b10d0fbd6f7c1bd7725e095fefae8dffc025e1ef3b23ecee1cf639473220c
            command: [miaproc]
            args:
            - eddy
            - run-bigquery-gold
            - --engine=reddyproc-reference
            - --bq-input-project=manglaria-staging
            - --bq-input-dataset=manglaria_lakehouse_ds
            - --bq-silver-table=carbon_flux_eddycovariance_s2_filt_1
            - --bq-billing-project=manglaria-staging
            - --group-column=site_id
            - --repo-root=/app
            - --bq-output-project=manglaria-staging
            - --bq-output-dataset=manglaria_lakehouse_ds
            - --bq-stage-table=carbon_flux_eddycovariance_g1_stage
            - --bq-control-dataset=_orch
            - --output-table=/tmp/gold.parquet
            - --output-diagnostics-json=/tmp/gold_diag.json
            - --output-run-json=/tmp/gold_run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

### Sketch: silver → gold final MERGE (explicit opt-in)

Same as the gold stage-only sketch above with two args appended:

```yaml
            - --bq-final-table=carbon_flux_eddycovariance_g2_merged
            - --bq-allow-final-merge
```

Deploy and execute as a separate Cloud Run Job from the
stage-only one. Production `manglaria` remains read-only at the
package layer; the merge mutates only the staging final gold
table.
