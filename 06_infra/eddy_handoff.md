# Eddy Cloud Engineer Handoff

Current as of M31. This is the only canonical eddy handoff in `06_infra/`.

## Goal

Run the `miaproc eddy` BigQuery split pipeline from the forward-facing repository. The intended cloud flow is two-stage:

1. Bronze BigQuery inputs -> silver stage/final payload using `miaproc eddy run-bigquery-silver`.
2. Silver BigQuery input -> gold stage/final payload using `miaproc eddy run-bigquery-gold`.

M31 specifically fixes the cloud-reported silver payload failures:

- Dry-run no longer falsely reports aliased bronze columns such as `co2_flux`, `qc_co2_flux`, `air_temperature`, and `u_star` as missing when they are represented in the silver payload as stage-1 aliases.
- BigQuery writeback payloads are now unique under BigQuery's case-insensitive field-name rules. A divergent humidity pair resolves to source `rH` plus derived `rH_norm_s`; non-humidity duplicates still fail loudly.

## Repository And Docker

Build from the forward-facing repository root:

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
  -t miaproc:cli-r45 .
```

Local smoke checks:

```bash
docker run --rm miaproc:cli-r45 miaproc --help
docker run --rm miaproc:cli-r45 miaproc eddy run-bigquery-silver --help
docker run --rm miaproc:cli-r45 miaproc eddy run-bigquery-gold --help
```

Both split commands should expose:

```text
--stage-payload-dry-run-dir
```

R preflight check for the REddyProc-capable image:

```bash
docker run --rm miaproc:cli-r45 \
  python -W error -m miaproc.eddy.r_preflight --repo-root /app
```

Expected: `Status: ok`, `Approved (project-scoped)`, R 4.5.1, REddyProc 1.3.4, rpy2 3.6.7, no warnings, no errors.

On Git Bash for Windows, add `MSYS_NO_PATHCONV=1` before the `docker run` command.

## Google Cloud Placeholders

Fill these before deployment:

```text
PROJECT_ID=<google-cloud-project>
BILLING_PROJECT=<billing-project>
REGION=<cloud-run-region>
ARTIFACT_REPOSITORY=<artifact-registry-repository>
IMAGE_URI=<REGION>-docker.pkg.dev/<PROJECT_ID>/<ARTIFACT_REPOSITORY>/miaproc:cli-r45
EDDY_SERVICE_ACCOUNT=<service-account-email>
INPUT_PROJECT=<bigquery-source-project>
INPUT_DATASET=<bigquery-source-dataset>
OUTPUT_PROJECT=<bigquery-output-project>
OUTPUT_DATASET=<bigquery-output-dataset>
CONTROL_DATASET=<orchestration-control-dataset>
FLUX_TABLE=<bronze-flux-table>
BIOMET_TABLE=<bronze-biomet-table>
SILVER_STAGE_TABLE=<silver-stage-table>
SILVER_FINAL_TABLE=<silver-final-table>
GOLD_STAGE_TABLE=<gold-stage-table>
GOLD_FINAL_TABLE=<gold-final-table>
GROUP_COLUMN=site_id
```

## IAM Bindings To Confirm

These are the Google Cloud IAM bindings your team should confirm or create. Exact role scope is your cloud team's call; prefer the narrowest dataset/repository/job scope that works.

```text
<EDDY_SERVICE_ACCOUNT> -> Artifact Registry reader on <ARTIFACT_REPOSITORY>
<EDDY_SERVICE_ACCOUNT> -> BigQuery jobUser on <BILLING_PROJECT or PROJECT_ID>
<EDDY_SERVICE_ACCOUNT> -> BigQuery dataViewer on bronze input dataset(s)
<EDDY_SERVICE_ACCOUNT> -> BigQuery dataViewer on silver input dataset for gold step
<EDDY_SERVICE_ACCOUNT> -> BigQuery dataEditor or narrower table-writer role on stage/final/control dataset(s)
<DEPLOYER_PRINCIPAL> -> Cloud Run developer/admin role for the target jobs
<DEPLOYER_PRINCIPAL> -> iam.serviceAccountUser on <EDDY_SERVICE_ACCOUNT>
```

The phrase you were trying to remember is usually **IAM bindings**: role grants binding a member, often a service account, to a Google Cloud resource.

## Recommended Cloud Test Order

Start with silver dry-run. This reads BigQuery and writes local artifacts inside the container without writing BigQuery tables, running validation SQL, merging, or advancing watermarks.

```bash
docker run --rm \
  -v "$PWD/out:/out" \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -e GOOGLE_CLOUD_PROJECT=<BILLING_PROJECT> \
  <IMAGE_URI> \
  miaproc eddy run-bigquery-silver \
    --bq-input-project <INPUT_PROJECT> \
    --bq-input-dataset <INPUT_DATASET> \
    --bq-flux-table <FLUX_TABLE> \
    --bq-biomet-table <BIOMET_TABLE> \
    --bq-billing-project <BILLING_PROJECT> \
    --group-column <GROUP_COLUMN> \
    --bq-output-project <OUTPUT_PROJECT> \
    --bq-output-dataset <OUTPUT_DATASET> \
    --bq-stage-table <SILVER_STAGE_TABLE> \
    --bq-control-dataset <CONTROL_DATASET> \
    --stage-payload-dry-run-dir /out/silver_dry_run \
    --output-table /out/silver.parquet \
    --output-run-json /out/silver_run.json
```

Inspect `/out/silver_dry_run/stage_payload_metadata.json` before any mutating write. Expected load-bearing fields:

```text
columns_unique: true
duplicate_columns: []
missing_input_columns: []
bigquery_write_attempted: false
validation_sql_attempted: false
merge_attempted: false
watermark_advanced: false
```

For the known humidity collision shape, expect `rH` and `rH_norm_s` in `columns`, and no case-insensitive duplicate such as `RH` plus `rH`.

Only after dry-run passes should your team run the mutating silver writeback by removing `--stage-payload-dry-run-dir` and confirming the stage/control/final targets are correct.

Then run gold dry-run, followed by gold writeback only when the silver step is accepted.

## Write Safety

The package rejects writes to the production project name `manglaria` by default. Stage/final writes should target a non-production/write-authorized project such as staging unless your governance process explicitly changes that invariant.

Do not store service account JSON keys in the repository. Use Workload Identity, attached Cloud Run service accounts, or operator-local credentials outside git.

## Deployment Notes

- This handoff does not pin a pushed image digest because this pass does not publish an image.
- Your team should build, tag, push, and deploy the image from the forward-facing repo.
- Use the checked-in Dockerfile from the forward-facing repo, not the development workspace Dockerfile.
- Keep deployment-specific environment variables, service account names, and target table names in your cloud deployment system, not in source code.
- If Cloud Run Job YAML is used, treat checked-in YAML as a template and replace placeholders with your environment-specific values before applying.
