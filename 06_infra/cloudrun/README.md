# Cloud Run Examples

This folder contains placeholder Cloud Run Job examples for engineers who will build, push, and deploy `miaproc` themselves.

These files are examples, not pinned production manifests:

- no image digest is pinned;
- no service account is hard-coded;
- no live Google Cloud resource is assumed;
- all values wrapped in `<...>` must be replaced by the deploying team.

The Google Cloud role grants your team must configure are usually called **IAM bindings**: bindings of a member, commonly a service account, to a role on a project, dataset, table, Artifact Registry repository, or Cloud Run resource.

## Files

- `biomass-job.example.yaml` - one biomass BigQuery job template. Use it as stage-only by omitting merge args, or as explicit merge by filling `--bq-final-table` and keeping `--bq-allow-final-merge`.
- `eddy-split-jobs.example.yaml` - multi-document template for eddy silver dry-run, silver writeback, gold dry-run, gold stage-only, and gold explicit merge.

## Required Placeholders

```text
<PROJECT_ID>
<REGION>
<IMAGE_URI>
<SERVICE_ACCOUNT_EMAIL>
<BILLING_PROJECT>
<INPUT_PROJECT>
<INPUT_DATASET>
<OUTPUT_PROJECT>
<OUTPUT_DATASET>
<CONTROL_DATASET>
```

Domain-specific table placeholders appear inside each YAML.

## Recommended Order

For eddy, run dry-run jobs first and inspect `stage_payload_metadata.json` before any mutating writeback:

```text
columns_unique: true
duplicate_columns: []
missing_input_columns: []
bigquery_write_attempted: false
validation_sql_attempted: false
merge_attempted: false
watermark_advanced: false
```

For the known M31 humidity shape, the silver payload should contain `rH` and, when values diverge, `rH_norm_s`; it should not contain a BigQuery-case-insensitive duplicate such as both `RH` and `rH`.

For mutating jobs, keep production source projects read-only and write only to your staging/output project unless governance explicitly authorizes otherwise. The package default still rejects writes to project `manglaria`.
