# miaproc Docker Runtime

This directory contains the Docker runtime profile for the standalone
`miaproc` package repository.

The image bundles:

- Python with `miaproc[dev,hesseflux,reddyproc,bigquery]`
- R 4.5.1
- REddyProc 1.3.4 installed into a project-scoped library under `/app/renv`

The project-scoped R layout is required by the eddy
`reddyproc-reference` backend. Do not bypass it with global-R overrides.

## Build

From the repository root:

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
  -t miaproc:cli-r45 .
```

The public package repo is laid out with `pyproject.toml`, `src/`, `tests/`,
`docs/`, and `examples/` at the root. This Dockerfile is built for that layout.

## Smoke Checks

```bash
docker run --rm miaproc:cli-r45 miaproc --help
docker run --rm miaproc:cli-r45 miaproc eddy run-bigquery-silver --help
docker run --rm miaproc:cli-r45 miaproc eddy run-bigquery-gold --help
docker run --rm miaproc:cli-r45 miaproc biomass run-bigquery --help
```

Both eddy BigQuery split commands should expose:

```text
--stage-payload-dry-run-dir
```

Run the project-scoped R preflight:

```bash
docker run --rm miaproc:cli-r45 \
  python -W error -m miaproc.eddy.r_preflight --repo-root /app
```

Expected result: `Status: ok`, `Approved (project-scoped)`, R 4.5.1,
REddyProc 1.3.4, rpy2 3.6.7, no warnings, no errors.

On Git Bash for Windows, use:

```bash
MSYS_NO_PATHCONV=1 docker run --rm miaproc:cli-r45 \
  python -W error -m miaproc.eddy.r_preflight --repo-root /app
```

## Eddy BigQuery Dry-Run Payload Mode

The safest cloud-facing validation path is a dry-run payload write to local
disk. This reads BigQuery, builds the exact stage payload, and writes:

- `stage_payload.csv`
- `stage_payload_metadata.json`

It does **not** write a BigQuery stage table, run validation SQL, MERGE, or
advance watermarks.

Example silver dry-run:

```bash
mkdir -p .runs/eddy_silver_dry_run

docker run --rm \
  -v "$PWD/.runs/eddy_silver_dry_run:/out" \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -e GOOGLE_CLOUD_PROJECT=manglaria-staging \
  miaproc:cli-r45 \
  miaproc eddy run-bigquery-silver \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-flux-table carbon_flux_eddycovariance \
    --bq-biomet-table carbon_flux_biomet \
    --bq-billing-project manglaria-staging \
    --group-column site_id \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_s2_silver_stage \
    --bq-control-dataset _orch \
    --stage-payload-dry-run-dir /out/dry_run \
    --output-table /out/silver.parquet \
    --output-run-json /out/silver_run.json
```

Example gold dry-run:

```bash
mkdir -p .runs/eddy_gold_dry_run

docker run --rm \
  -v "$PWD/.runs/eddy_gold_dry_run:/out" \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -e GOOGLE_CLOUD_PROJECT=manglaria-staging \
  miaproc:cli-r45 \
  miaproc eddy run-bigquery-gold \
    --engine reddyproc-reference \
    --repo-root /app \
    --bq-input-project manglaria-staging \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-silver-table cf_s2_silver_stage \
    --bq-billing-project manglaria-staging \
    --group-column site_id \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_s2_gold_stage \
    --bq-final-table carbon_flux_eddycovariance_s2_filt_1 \
    --bq-allow-final-merge \
    --bq-control-dataset _orch \
    --stage-payload-dry-run-dir /out/dry_run \
    --output-table /out/gold.parquet \
    --output-diagnostics-json /out/gold_diag.json \
    --output-run-json /out/gold_run.json
```

The dry-run metadata should show:

- `columns_unique: true`
- `duplicate_columns: []`
- `missing_input_columns: []`
- `bigquery_write_attempted: false`
- `validation_sql_attempted: false`
- `merge_attempted: false`
- `watermark_advanced: false`

For the M28 humidity collision policy, source `rH` is preserved as `rH`; a
divergent normalized humidity duplicate becomes `rH_norm_s`; non-humidity
duplicates raise rather than being silently fused.

## Publishing

Publishing means tagging and pushing the image to the project Artifact Registry.
Do that only after the local/container dry-run evidence is accepted and the
operator-owned BigQuery-read dry-run has been run with the intended cloud
credentials.
