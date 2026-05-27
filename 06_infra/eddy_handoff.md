# Eddy Cloud Engineer Handoff

Current as of M35 (built on accepted M32A + M33 + M33A + M34). This is the
only canonical eddy handoff in `06_infra/`.

## Goal

Run the `miaproc eddy` BigQuery split pipeline from the forward-facing
repository. The intended cloud flow is two-stage:

1. Bronze BigQuery inputs -> silver stage/final payload using
   `miaproc eddy run-bigquery-silver`.
2. Silver BigQuery input -> gold stage/final payload using
   `miaproc eddy run-bigquery-gold`.

M33 does not push a Docker image, does not mutate Artifact Registry, does not
run Cloud Run, and does not perform live BigQuery writes or MERGEs. It updates
the deployment-facing artifacts (schemas, this handoff, Cloud Run examples)
and the forward-facing repository so cloud engineers can build, push, and
deploy the image themselves.

## Source-Truth Column Contract (M32A)

Source/bronze columns are the truth. The silver and gold BigQuery payloads
carry **source-truth final names** and a single `timestamp` column. Internal
backend processing still uses canonical R-style names (`DateTime`, `NEE`,
`Tair`, `USTAR`, `VPD`, `Rg`, `P_RAIN`, `rH`), but those internal passthrough
names do **not** appear in final silver or gold payloads.

Accepted internal -> final mapping (M32A):

```text
DateTime       -> timestamp
NEE            -> co2_flux
QC_NEE         -> qc_co2_flux
Tair           -> air_temperature_c
USTAR          -> u_star
VPD            -> VPD_hpa
Rg             -> SWIN_1_1_1
P_RAIN         -> P_RAIN_1_1_1
rH             -> RH_1_1_1
```

Additional invariants:

- Silver and gold payloads carry exactly one `timestamp` and no `DateTime`.
- The gold step reconstructs internal `DateTime` from `timestamp` to drive the
  selected backend; that reconstructed `DateTime` is dropped before the gold
  stage payload is written.
- Flux-side `RH` and biomet-side `RH_1_1_1` are case-insensitively distinct
  and survive separately when present.
- BigQuery field-name uniqueness is checked with `casefold()`-style
  case-insensitive logic. Equivalent humidity duplicates resolve to source
  `rH_norm_s` as a defensive fallback; non-humidity case-insensitive
  duplicates raise.
- **M34 widened passthrough:** every unique column from the carbon-flux
  bronze source table survives bronze → silver → gold under its source
  name unless miaproc changes its physical units. Today the only
  unit-aware rebindings are the nine source-truth pairs above. Every
  other source column (`h2o_flux`, `qc_h2o_flux`, `H_strg`, `LE_strg`,
  `co2_strg`, `h2o_strg`, `co2_molar_density`, `co2_mole_fraction`,
  `co2_mixing_ratio`, `h2o_molar_density`, `h2o_mole_fraction`,
  `h2o_mixing_ratio`, `sonic_temperature`, `air_pressure`,
  `air_density`, `air_heat_capacity`, `air_molar_volume`, `ET`,
  `water_vapor_density`, `e`, `es`, `specific_humidity`, `Tdew`,
  `wind_speed`, `max_wind_speed`, `wind_dir`, `TKE`, `L`,
  `z_minus_d_div_L`, `bowen_ratio`, `x_peak`, `x_offset`, `x_10_pct`,
  `x_30_pct`, `x_50_pct`, `x_70_pct`, `x_90_pct`, `v_var`, …)
  survives under its bronze name. From the biomet bronze, only the
  three processing-used variables (`SWIN_1_1_1`, `P_RAIN_1_1_1`,
  `RH_1_1_1`) carry forward. The dry-run preservation guard fails
  loudly if any unique source column would have been dropped.

The authoritative column mapping is
`06_infra/schemas/eddy_bronze_to_stage_column_lineage_contract.csv`. The
example JSON schemas in `06_infra/schemas/` mirror that CSV.

## Repository And Docker

Build from the forward-facing repository root:

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
  -t miaproc:cli-r45 .
```

The forward-facing repository layout has `pyproject.toml`, `src/`, `tests/`,
`docs/`, and `examples/` at the repo root; its Dockerfile installs from
`/app`, not `/app/08_pkg`. Build from the forward-facing repo only, not from
the artifact-first development repo.

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

Expected: `Status: ok`, `Approved (project-scoped)`, R 4.5.1, REddyProc 1.3.4,
rpy2 3.6.7, no warnings, no errors.

On Git Bash for Windows, add `MSYS_NO_PATHCONV=1` before the `docker run`
command.

This repository update does **not** push a Docker image. Your cloud team
should tag and push the image from the forward-facing repo into your own
Artifact Registry repository on the cadence that fits your release process.

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

These are the Google Cloud **IAM bindings** (role grants binding a member,
commonly a service account, to a resource) your team should confirm or create.
Exact role scope is your cloud team's call; prefer the narrowest
dataset/repository/job scope that works.

```text
<EDDY_SERVICE_ACCOUNT> -> Artifact Registry reader on <ARTIFACT_REPOSITORY>
<EDDY_SERVICE_ACCOUNT> -> BigQuery jobUser on <BILLING_PROJECT or PROJECT_ID>
<EDDY_SERVICE_ACCOUNT> -> BigQuery dataViewer on bronze input dataset(s)
<EDDY_SERVICE_ACCOUNT> -> BigQuery dataViewer on silver input dataset for gold step
<EDDY_SERVICE_ACCOUNT> -> BigQuery dataEditor or narrower table-writer role on stage/final/control dataset(s)
<DEPLOYER_PRINCIPAL> -> Cloud Run developer/admin role for the target jobs
<DEPLOYER_PRINCIPAL> -> iam.serviceAccountUser on <EDDY_SERVICE_ACCOUNT>
```

## Recommended Cloud Test Order

Start with silver dry-run. This reads BigQuery and writes local artifacts
inside the container without writing BigQuery tables, running validation SQL,
merging, or advancing watermarks. The dry-run is the safest operator-side
check before any mutating write.

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

Inspect `/out/silver_dry_run/stage_payload_metadata.json` before any mutating
write. Expected load-bearing fields:

```text
columns_unique: true
duplicate_columns: []
missing_input_columns: []
bigquery_write_attempted: false
validation_sql_attempted: false
merge_attempted: false
watermark_advanced: false
```

Expected source-truth columns under `columns` (a subset, depending on the
source-table shape):

```text
primary_key, site_id, timestamp,
co2_flux, qc_co2_flux, air_temperature_c, u_star, VPD_hpa,
SWIN_1_1_1, P_RAIN_1_1_1, RH_1_1_1,
RH                  (only when the flux table carries it),
rH_norm_s           (only when a divergent derived humidity is detected),
H, qc_H, LE, qc_LE,
filename, DOY, daytime   (only when present in the source)
```

No internal-name passthroughs (`DateTime`, `NEE`, `QC_NEE`, `Tair`, `USTAR`,
`VPD`, `Rg`, `P_RAIN`, `rH`) should appear in the silver payload `columns`.

Only after dry-run passes should your team run the mutating silver writeback
by removing `--stage-payload-dry-run-dir` and confirming the stage / control /
final targets are correct.

Then run gold dry-run, followed by gold writeback only when the silver step is
accepted. The gold dry-run reconstructs the internal calc frame from the
source-truth silver `timestamp` and dispatches the selected backend without
writing the gold stage/final/control tables.

## Write Safety

The package rejects writes to the production project name `manglaria` by
default (`BigQueryWritebackConfig.forbidden_write_projects`). Stage/final
writes should target a non-production / write-authorized project such as
staging unless your governance process explicitly changes that invariant.

Do not store service account JSON keys in the repository. Use Workload
Identity, attached Cloud Run service accounts, or operator-local credentials
outside git.

## Disposable BigQuery smoke commands (M35)

Before pinning a production schedule, run the disposable stage-write smoke
to confirm the current Docker build reads BigQuery and writes a
BigQuery-compatible silver/gold stage payload. Always use a timestamped
suffix so the smoke target is unique and easy to delete; never write to
canonical tables.

```bash
SMOKE_SUFFIX="m35_smoke_$(date -u +%Y%m%d%H%M%S)"

# Silver disposable stage-write smoke (no MERGE; no final table):
docker run --rm \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -v "$PWD/.runs/m35:/out" \
  -e GOOGLE_CLOUD_PROJECT=manglaria-staging \
  <IMAGE_URI> \
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

# Gold disposable stage-write smoke (no --bq-allow-final-merge):
docker run --rm \
  -v "$HOME/.config/gcloud:/root/.config/gcloud:ro" \
  -v "$PWD/.runs/m35:/out" \
  -e GOOGLE_CLOUD_PROJECT=manglaria-staging \
  <IMAGE_URI> \
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

# Schema sanity check + cleanup:
bq show --schema manglaria-staging:manglaria_lakehouse_ds.cf_s2_silver_stage_${SMOKE_SUFFIX} \
  | python -c "import json,sys; cols=[f['name'] for f in json.load(sys.stdin)]; print('cols', len(cols), 'casefold-unique', len({c.casefold() for c in cols}))"
bq rm -f -t manglaria-staging:manglaria_lakehouse_ds.cf_s2_silver_stage_${SMOKE_SUFFIX}
bq rm -f -t manglaria-staging:manglaria_lakehouse_ds.cf_s2_gold_stage_${SMOKE_SUFFIX}
bq rm -f -t manglaria-staging:manglaria_lakehouse_ds.cf_s2_gold_final_${SMOKE_SUFFIX}
```

`--bq-allow-final-merge` is intentionally absent. If a disposable
final-table MERGE is required to exercise the merge path, gate it
behind the explicit operator opt-in
`MIAPROC_M35_ALLOW_DISPOSABLE_GOLD_MERGE=1` and verify the merge
target is always the `cf_s2_gold_final_${SMOKE_SUFFIX}` disposable
table.

## Deployment Notes

- This handoff does not pin a pushed image digest because this pass does not
  publish an image. The deployment team is the build/publish owner.
- Use the checked-in Dockerfile from the forward-facing repo, not the
  development workspace Dockerfile.
- Keep deployment-specific environment variables, service account names, and
  target table names in your cloud deployment system, not in source code.
- Cloud Run Job YAML files under `06_infra/cloudrun/` are examples with
  placeholders. They are not pinned production manifests; replace placeholders
  with your environment-specific values before applying.
- Under M35 the eddy handoff and Cloud Run examples describe the widened
  M34 carry-forward shape: silver/gold stage payloads carry every
  unique carbon-flux bronze column. Before pinning a production
  schedule, run the disposable smoke above and capture the run JSON
  + schema sanity check.
