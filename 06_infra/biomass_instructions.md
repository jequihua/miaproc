# Biomass — cloud handoff

The same Docker image as eddy enriches a BigQuery individual-tree
forest-structure table with two appended columns
(`biomass_estimate`, `equation_used`) and writes back to a staging
final table under explicit operator opt-in MERGE.

## Image

Pulled by immutable digest:

```
us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:a59f763246bcc08ccb2b82f40309f98850de53f898533c8c5ac0c69a53acd36f
```

Region `us-central1`, repo `cloud-run-source-deploy`.

Biomass needs **no R**, never invokes the project-scoped preflight,
and does not require `--repo-root`. It's pure Python.

## Command

```
miaproc biomass run-bigquery
  --bq-input-project   manglaria
  --bq-input-dataset   manglaria_lakehouse_ds
  --bq-input-table     <SOURCE>          # operator-chosen
  --bq-billing-project manglaria-staging
  --bq-output-project  manglaria-staging
  --bq-output-dataset  manglaria_lakehouse_ds
  --bq-stage-table     <STAGE>           # operator-chosen
  --bq-control-dataset _orch
  --bq-merge-key       primary_key       # or any stable per-tree id
  [--bq-final-table    <FINAL>]          # operator-chosen
  [--bq-allow-final-merge]
  --output-table       /tmp/enriched.parquet
  --output-run-json    /tmp/run.json
```

Key contract:

- **Stage-only is the safe default.** Final-table mutation requires
  both `--bq-final-table` and `--bq-allow-final-merge`.
- Source / stage / final / merge-key names are **operator-chosen at
  runtime** — no hard-codes in the package.
- Default biomass dataset is `dina` (mangrove direct-biomass
  equations). Override with `--dataset infys` for volume rows or
  `--dataset ""` to disable filtering.
- Output preserves the source table verbatim and appends exactly two
  columns. `equation_used` is null when the row was ineligible
  (missing `dbh_cm`, non-adult life stage) or unmatched.
- M17A deterministic alias map only (two known mangrove typos);
  no fuzzy matching.
- Audit row goes to `<output-project>._orch.cf_biomass_runs`. Biomass
  has **no watermark** — per-tree identity-keyed enrichment, not
  time-series; idempotent re-runs via `MERGE` on the configured key.
- Production project `manglaria` is read-only at the package layer
  (`forbidden_write_projects=("manglaria",)`).
- Exit codes: `0` success / `3` validation / `4` runtime.

## Runtime IAM (least privilege)

| Role | Resource |
|---|---|
| `roles/bigquery.jobUser` | project `manglaria-staging` |
| `roles/bigquery.dataEditor` | dataset `manglaria-staging:<output-dataset>` |
| `roles/bigquery.dataEditor` (or dataset-ACL `WRITER`) | dataset `manglaria-staging:_orch` |
| `roles/bigquery.dataViewer` | **table-level** on the operator-established source table in `manglaria` |
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
  name: miaproc-biomass-stage
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
            - biomass
            - run-bigquery
            - --bq-input-project=manglaria
            - --bq-input-dataset=manglaria_lakehouse_ds
            - --bq-input-table=<SOURCE>
            - --bq-billing-project=manglaria-staging
            - --bq-output-project=manglaria-staging
            - --bq-output-dataset=manglaria_lakehouse_ds
            - --bq-stage-table=<STAGE>
            - --bq-control-dataset=_orch
            - --bq-merge-key=primary_key
            - --output-table=/tmp/enriched.parquet
            - --output-run-json=/tmp/run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

For the **merge** job, clone the manifest, rename to
`miaproc-biomass-merge`, and add to the `args` list:

```yaml
            - --bq-final-table=<FINAL>
            - --bq-allow-final-merge
```
