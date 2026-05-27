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

Under the M32A source-truth + M34 widened contract, the dry-run `columns`
should carry source-truth final names (`timestamp`, `co2_flux`, `qc_co2_flux`,
`air_temperature_c`, `u_star`, `VPD_hpa`, `SWIN_1_1_1`, `P_RAIN_1_1_1`,
`RH_1_1_1`) plus every wide carbon-flux source pass-through (`h2o_flux`,
`qc_h2o_flux`, `sonic_temperature`, `air_pressure`, `wind_speed`, `TKE`,
`v_var`, flux-side `RH`, etc.). No internal-name passthroughs (`DateTime`,
`NEE`, `QC_NEE`, `Tair`, `USTAR`, internal `VPD`, `Rg`, `P_RAIN`, internal
`rH`) should appear in the silver or gold final payload. The M28 humidity
fallback (`rH_norm_s`) still applies for a divergent derived humidity case;
non-humidity case-insensitive duplicates raise.

## Local CSV smoke (no BigQuery)

For a credential-free quality check, bind-mount a host directory containing
the case-study flux + biomet CSVs and run a small driver inside the
container. The driver should exercise the public package helpers
(`load_stage1_from_dataframes`, `apply_silver_source_truth_rename`,
`prepare_silver_stage_payload`, `prepare_stage_dataframe`) and write
silver + gold CSV/parquet artifacts under a bind-mounted output directory.

```bash
docker run --rm \
  -v "$PWD/.runs/m35:/out" \
  -v "$PWD/01_data/case_study/flux:/data/flux:ro" \
  miaproc:cli-r45 \
  python /out/driver_csv_smoke.py \
    --flux-csv /data/flux/flux_staging.csv \
    --output-dir /out/container_csv_smoke
```

The smoke driver should write a `summary.json` listing row counts, column
counts, casefold-unique check, missing source columns, and appended
processing columns. The expected M34 baseline for a wide 54-column
case-study bronze is silver/gold payload with **57 source-truth columns**
on the silver side (identity triple + 50 source pass-throughs + 5
unit-aware source-truth rebindings + 3 biomet-derived source-truth
columns, with timestamp counted once) and an appended gold-only block of
`nee_f`, `nee_fqc`, `sw_in_f`, `ta_f`, `vpd_f`, `GPP`, `Reco`,
`dateAndTime` on the gold side.

## Disposable BigQuery stage-write smoke (M35)

When you do have BigQuery credentials and a staging project, the
load-bearing cloud smoke is a disposable stage-write. Always use a
timestamped suffix so the smoke target is unique and easy to delete.

```bash
SMOKE_SUFFIX="m35_smoke_$(date -u +%Y%m%d%H%M%S)"

docker run --rm \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -v "$PWD/.runs/m35/bigquery_stage_write:/out" \
  -e GOOGLE_CLOUD_PROJECT=manglaria-staging \
  miaproc:cli-r45 \
  miaproc eddy run-bigquery-silver \
    --bq-input-project manglaria-staging \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-flux-table carbon_flux_eddycovariance \
    --bq-biomet-table carbon_flux_biomet \
    --bq-billing-project manglaria-staging \
    --group-column site_id \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_s2_silver_stage_${SMOKE_SUFFIX} \
    --bq-control-dataset _orch \
    --output-table /out/silver.parquet \
    --output-run-json /out/silver_run.json
```

Then a stage-write-only gold smoke using the silver smoke output:

```bash
docker run --rm \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -v "$PWD/.runs/m35/bigquery_stage_write:/out" \
  -e GOOGLE_CLOUD_PROJECT=manglaria-staging \
  miaproc:cli-r45 \
  miaproc eddy run-bigquery-gold \
    --engine reddyproc-reference \
    --bq-input-project manglaria-staging \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-silver-table cf_s2_silver_stage_${SMOKE_SUFFIX} \
    --bq-billing-project manglaria-staging \
    --group-column site_id \
    --repo-root /app \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_s2_gold_stage_${SMOKE_SUFFIX} \
    --bq-final-table cf_s2_gold_final_${SMOKE_SUFFIX} \
    --bq-control-dataset _orch \
    --output-table /out/gold.parquet \
    --output-diagnostics-json /out/gold_diag.json \
    --output-run-json /out/gold_run.json
```

Do **not** pass `--bq-allow-final-merge` unless `MIAPROC_M35_ALLOW_DISPOSABLE_GOLD_MERGE=1`
is set explicitly **and** the merge target is the disposable
`cf_s2_gold_final_${SMOKE_SUFFIX}` table — never a canonical final
table.

After the smoke writes, inspect the BigQuery schema and row counts:

```bash
bq show --schema manglaria-staging:manglaria_lakehouse_ds.cf_s2_silver_stage_${SMOKE_SUFFIX} | python -c "import json,sys; cols=[f['name'] for f in json.load(sys.stdin)]; print(len(cols), len({c.casefold() for c in cols}))"
```

The expected output is `<n> <n>` (column count equals case-insensitive
unique count). Delete the disposable smoke tables once the schema and
row counts are recorded:

```bash
bq rm -f -t manglaria-staging:manglaria_lakehouse_ds.cf_s2_silver_stage_${SMOKE_SUFFIX}
bq rm -f -t manglaria-staging:manglaria_lakehouse_ds.cf_s2_gold_stage_${SMOKE_SUFFIX}
bq rm -f -t manglaria-staging:manglaria_lakehouse_ds.cf_s2_gold_final_${SMOKE_SUFFIX}
```

## Publishing

Publishing means tagging and pushing the image to the project Artifact Registry.
Do that only after the local/container dry-run evidence is accepted and the
operator-owned BigQuery-read dry-run has been run with the intended cloud
credentials. The miaproc development repository does **not** push images;
the cloud engineer who controls the Artifact Registry repository owns the
tag/push/digest pin cycle.
