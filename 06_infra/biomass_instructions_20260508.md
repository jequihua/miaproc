# Biomass — cloud handoff (post-M26 image, 2026-05-08)

## Image

Pull by immutable digest from Artifact Registry:

```
us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:9e8b10d0fbd6f7c1bd7725e095fefae8dffc025e1ef3b23ecee1cf639473220c
```

Region `us-central1`, repo `cloud-run-source-deploy`. The
`:cli-r45` mutable tag also points here, but pin the digest in
production. The biomass CLI inside this image is
**byte-functionally equivalent** to the M23 / M25 image (no
biomass science change under M24 / M24A / M25 / M25A / M26 /
M27); the digest refresh only reflects the M26 eddy silver
staging-to-staging validator change and a clean rebuild from
current source. The previous M25 image was deleted from
Artifact Registry as part of the M27 publish.

## How the biomass CLI processes data

- Reads one operator-owned forest-structure / individual-tree
  source table from BigQuery.
- Enriches each row in memory with two appended columns:
  - `biomass_estimate` — the kg estimate from the matched
    equation,
  - `equation_used` — the matched equation's
    `source_record_id`, or `null` for ineligible / unmatched
    rows.
- Preserves every source column verbatim. No row dropping; no
  reshaping.
- Default equation dataset is `dina`. Override with `--dataset infys` for
  volume rows or `--dataset ""` to disable the dataset filter.
- M17A deterministic species-alias normalization runs on the
  matching path (two known mangrove-species typos only; not
  fuzzy matching).
- BigQuery writeback is **stage-only by default**. Final-table
  mutation requires both `--bq-final-table` and the explicit
  `--bq-allow-final-merge` opt-in.
- Merge identity is configurable via `--bq-merge-key` (default
  `primary_key`). Operators with a different stable per-tree
  identifier override at runtime.
- **No watermark** for biomass. Biomass is per-tree
  identity-keyed enrichment (not a time-series append), so
  re-runs simply MERGE on the configured key. Audit rows go to
  `<output-project>._orch.cf_biomass_runs`; there is no
  `cf_biomass_watermark` table.
- Production project `manglaria` is read-only at the package
  layer (`forbidden_write_projects=("manglaria",)`).
- Exit codes: `0` success / `3` validation / `4` runtime.

## Runtime IAM (least privilege)

| Role | Resource |
|---|---|
| `roles/bigquery.jobUser` | project `manglaria-staging` |
| `roles/bigquery.dataEditor` | dataset `manglaria-staging:<output-dataset>` |
| `roles/bigquery.dataEditor` (or dataset-ACL `WRITER`) | dataset `manglaria-staging:_orch` |
| `roles/bigquery.dataViewer` | **table-level** on the operator-established source table in `manglaria` |
| `roles/artifactregistry.reader` | repo `cloud-run-source-deploy` in `manglaria-staging` |

Do **not** grant any write role on production project
`manglaria`.

## CLI command shape

Stage-only by default; final-table MERGE requires the explicit
opt-in.

```
miaproc biomass run-bigquery
  --bq-input-project   manglaria
  --bq-input-dataset   manglaria_lakehouse_ds
  --bq-input-table     <SOURCE_TABLE>
  --bq-billing-project manglaria-staging
  --bq-output-project  manglaria-staging
  --bq-output-dataset  manglaria_lakehouse_ds
  --bq-stage-table     <STAGE_TABLE>
  --bq-control-dataset _orch
  --bq-merge-key       primary_key
  [--bq-final-table    <FINAL_TABLE>]
  [--bq-allow-final-merge]
  --output-table       /tmp/enriched.parquet
  --output-run-json    /tmp/run.json
```

Source, stage, final, and merge-key names are **operator-chosen
at runtime** — no hard-codes in the package.

## Cloud Run Job sketches (rough)

Cloud engineers replace the service account, region, namespace,
and resource limits with their own values. Deploy stage-only
and merge as **separate** Cloud Run Jobs so accidental
re-execution of the routine job cannot MERGE.

### Sketch: stage-only

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
          - image: us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy/miaproc@sha256:9e8b10d0fbd6f7c1bd7725e095fefae8dffc025e1ef3b23ecee1cf639473220c
            command: [miaproc]
            args:
            - biomass
            - run-bigquery
            - --bq-input-project=manglaria
            - --bq-input-dataset=manglaria_lakehouse_ds
            - --bq-input-table=<SOURCE_TABLE>
            - --bq-billing-project=manglaria-staging
            - --bq-output-project=manglaria-staging
            - --bq-output-dataset=manglaria_lakehouse_ds
            - --bq-stage-table=<STAGE_TABLE>
            - --bq-control-dataset=_orch
            - --bq-merge-key=primary_key
            - --output-table=/tmp/enriched.parquet
            - --output-run-json=/tmp/run.json
            resources:
              limits: {cpu: "2", memory: 4Gi}
```

### Sketch: explicit MERGE (delta)

Same sketch as above with two args appended:

```yaml
            - --bq-final-table=<FINAL_TABLE>
            - --bq-allow-final-merge
```

This variant **mutates** the staging final biomass table on the
configured `--bq-merge-key`. Production `manglaria` remains
read-only at the package layer.
