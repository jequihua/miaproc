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
- Production project `manglaria` is read-only at the package layer
  (`forbidden_write_projects=("manglaria",)`).
- Exit codes: `0` success / `2` preflight unapproved / `3` validation
  / `4` runtime.

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

## BigQuery-native silver/gold split (M22)

The image also exposes BigQuery-native silver/gold subcommands that
mirror the file-based split but read and stage directly to BigQuery.
They are an alternative to the one-shot `run-bigquery` shape above —
not a replacement.

```
BigQuery bronze/source flux + biomet
  -> miaproc eddy run-bigquery-silver
  -> BigQuery silver stage table
  -> miaproc eddy run-bigquery-gold
  -> BigQuery gold stage/final table
```

Key contract:

- Silver is produced **before** any backend processing. Cloud
  orchestration must not derive silver by filtering a processed gold
  table — that inverts the project semantics.
- `run-bigquery-silver` is stage-only by design; it has no
  `--bq-final-table` / `--bq-allow-final-merge` flag and no R
  preflight.
- `run-bigquery-gold --engine reddyproc-reference` keeps the
  Decision 010 / R11 project-scoped R preflight and requires
  `--repo-root /app`.
- Gold writeback uses the same M8/M10 safety posture as the one-shot
  path: stage-only default, explicit `--bq-allow-final-merge` for any
  final-table mutation.

Sample commands:

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
  --bq-stage-table     cf_s2_silver_stage
  --bq-control-dataset _orch
  --output-table       /tmp/silver.parquet
  --output-run-json    /tmp/silver_run.json

miaproc eddy run-bigquery-gold
  --engine             reddyproc-reference
  --bq-input-project   manglaria-staging
  --bq-input-dataset   manglaria_lakehouse_ds
  --bq-silver-table    cf_s2_silver_stage
  --bq-billing-project manglaria-staging
  --group-column       site_id
  --repo-root          /app
  --bq-output-project  manglaria-staging
  --bq-output-dataset  manglaria_lakehouse_ds
  --bq-stage-table     cf_s2_gold_stage
  --bq-control-dataset _orch
  [--bq-final-table    carbon_flux_eddycovariance_s2_filt_1]
  [--bq-allow-final-merge]
  --output-table              /tmp/gold.parquet
  --output-diagnostics-json   /tmp/gold_diag.json
  --output-run-json           /tmp/gold_run.json
```

Cloud Run draft manifests for the silver and gold jobs live next to
the one-shot manifests under
[`cloudrun/`](cloudrun/). Both are placeholders — operators must
establish the silver / gold stage / final table identifiers and IAM
bindings before deployment.
