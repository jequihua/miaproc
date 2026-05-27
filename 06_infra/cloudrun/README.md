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

Under the accepted M32A source-truth contract, the silver and gold dry-run
`columns` should carry source-truth final names and a single `timestamp`
column (no internal `DateTime`, `NEE`, `Tair`, `USTAR`, `VPD`, `Rg`,
`P_RAIN`, or `rH` in the final payload). Flux-side `RH` and biomet-side
`RH_1_1_1` are case-insensitively distinct and may both appear. The M28
defensive `rH_norm_s` fallback still resolves a divergent derived humidity
column; non-humidity case-insensitive duplicates raise rather than being
silently fused.

For mutating jobs, keep production source projects read-only and write only to your staging/output project unless governance explicitly authorizes otherwise. The package default still rejects writes to project `manglaria`.

## M35 disposable smoke

Before treating any of these YAMLs as the basis of a recurring job, run
the disposable BigQuery stage-write smoke documented in
`06_infra/eddy_handoff.md` ("Disposable BigQuery smoke commands (M35)").
That smoke runs locally from the engineer's machine through the same
Docker image the Cloud Run Job would use, writes to
`cf_s2_*_stage_m35_smoke_<YYYYMMDDHHMMSS>` disposable tables in
`manglaria-staging`, verifies the silver/gold schemas are casefold-unique
and carry the widened M34 carbon-flux pass-through (`h2o_flux`,
`qc_h2o_flux`, `sonic_temperature`, `air_pressure`, `wind_speed`, `TKE`,
`v_var`, `RH`, …) under source-truth names, and is cleaned up with
`bq rm -f -t ...` immediately after. The Cloud Run YAML must not be
applied against a canonical final table until the disposable smoke has
been accepted.
