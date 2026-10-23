# Eddy Cloud Engineer Handoff

Current as of M002-S04. This is the only canonical eddy handoff in
`06_infra/`. It is guidance for the engineers who deploy and orchestrate
miaproc; nothing here is deployed, qualified or executed against cloud
resources by this package.

## Goal

Run the `miaproc eddy` file pipeline as two batch stages over ordinary local
or mounted paths:

1. Immutable Parquet flux + biomet inputs -> a published silver run using
   `miaproc eddy run-silver`.
2. A published silver run -> a published gold run using
   `miaproc eddy run-gold`.

Consumers accept a run only after `miaproc eddy validate-run` succeeds. The
former BigQuery split commands, writeback, MERGE and watermark tables were
removed in M002-S04; the operator migration guide is
[`../docs/MIGRATING_FROM_BIGQUERY.md`](../docs/MIGRATING_FROM_BIGQUERY.md).
miaproc uses no cloud SDK, credentials or network for data processing.

## Responsibility split

| miaproc (this package) | Engineers / orchestrator |
|---|---|
| Read the given Parquet (or legacy CSV) inputs without modifying them | Select the input window and write immutable input files |
| Stage 1, engines, diagnostics, source-facing payloads | Choose the engine/profile and site metadata per job |
| Publish one fresh run directory per invocation with a completion manifest written last | Hand out exclusive, never-reused run names; schedule reruns with new names |
| Refuse reuse/overlap/traversal; validate silver before gold | Mount inputs read-only and outputs writable; size memory and scratch |
| `validate-run` consumer check | Load validated runs elsewhere (warehouse, dashboards), deduplicate or supersede older runs |
| Exit codes 0/2/3/4/5 | Retry policy, alerting, IAM, image publication, driver qualification |

## Source-Truth Column Contract (M32A)

Source/bronze columns are the truth. The Parquet silver and gold outputs
carry **source-truth final names** and a single `timestamp` column. Internal
backend processing still uses canonical R-style names (`DateTime`, `NEE`,
`Tair`, `USTAR`, `VPD`, `Rg`, `P_RAIN`, `rH`), but those internal passthrough
names do **not** appear in Parquet silver or gold outputs.

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

- Parquet silver and gold carry exactly one `timestamp` and no `DateTime`.
- The gold step reconstructs internal `DateTime` from `timestamp` to drive the
  selected backend; that reconstructed `DateTime` is dropped before the gold
  table is written.
- Source `primary_key` / `site_id` are carried unchanged (type and values,
  never synthesized). Gap rows inserted by the 30-minute regularization get
  the grid `timestamp`, the uniform group `site_id`, a null `primary_key` and
  missing measurements.
- Flux-side `RH` and biomet-side `RH_1_1_1` are distinct columns and survive
  separately when present.
- A literal duplicate `rH` resolves deterministically (equivalent copy
  suppressed, divergent copy renamed `rH_norm_s`); any other duplicate name
  fails the run. Parquet names are case-sensitive, so gold may carry names
  that differ only by case (for example hesseflux `TA_f` next to `ta_f`);
  case-insensitive consumers must select them by exact name or rename on
  load (D014).
- **M34 widened passthrough:** every unique column from the carbon-flux
  bronze source survives bronze → silver → gold under its source name unless
  miaproc changes its physical units. Today the only unit-aware rebindings
  are `air_temperature -> air_temperature_c` and `VPD -> VPD_hpa`; every
  other flux source column (`h2o_flux`, `qc_h2o_flux`, `H_strg`, `LE_strg`,
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
  `RH_1_1_1`) carry forward.

The authoritative column mapping is
`06_infra/schemas/eddy_bronze_to_stage_column_lineage_contract.csv`. The file
contract (inputs, type policy, outputs) is
[`../docs/PARQUET_FILE_SCHEMA.md`](../docs/PARQUET_FILE_SCHEMA.md).

## Repository And Docker

Build from the package root (the directory holding `pyproject.toml`):

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
  -t miaproc-eddy-parquet:product-local .
```

The Dockerfile installs the package with the extras `dev,hesseflux,reddyproc`
(no `bigquery` extra since M002-S04). An image built before M002-S04 still
contains the former Google packages and BigQuery commands until rebuilt.

Local smoke checks:

```bash
IMAGE=miaproc-eddy-parquet:product-local
docker run --rm --network none "$IMAGE" miaproc --help
docker run --rm --network none "$IMAGE" miaproc eddy run-silver --help
docker run --rm --network none "$IMAGE" miaproc eddy run-gold --help
docker run --rm --network none "$IMAGE" miaproc eddy validate-run --help
```

R preflight check for the REddyProc-capable image:

```bash
docker run --rm --network none "$IMAGE" \
  python -W error -m miaproc.eddy.r_preflight --repo-root /app
```

Expected: `Status: ok`, `Approved (project-scoped)`, R 4.5.1, REddyProc 1.3.4,
rpy2 3.6.7, no warnings, no errors.

On Git Bash for Windows, add `MSYS_NO_PATHCONV=1` before the `docker run`
command.

This repository does **not** push a Docker image. Your cloud team tags and
pushes the image into your own registry and pins it by digest.

## Deployment placeholders

Fill these before deployment:

```text
PROJECT_ID=<cloud-project>
REGION=<batch-region>
IMAGE_URI=<registry>/<repository>/miaproc@sha256:<digest>
EDDY_SERVICE_ACCOUNT=<service-account-email>
INPUT_BUCKET=<bucket-or-volume-with-immutable-inputs>
OUTPUT_BUCKET=<bucket-or-volume-for-run-directories>
WINDOW=<input-window-id>
SILVER_RUN=<unique silver run name>
GOLD_RUN=<unique gold run name>
GROUP_COLUMN=site_id
```

## Permissions To Confirm

Exact role scope is your cloud team's call; prefer the narrowest scope that
works:

```text
<EDDY_SERVICE_ACCOUNT> -> image pull on the registry repository
<EDDY_SERVICE_ACCOUNT> -> read on <INPUT_BUCKET> (mounted read-only)
<EDDY_SERVICE_ACCOUNT> -> object create/read on <OUTPUT_BUCKET> (mounted writable)
<DEPLOYER_PRINCIPAL>   -> job developer/admin role for the target jobs
<DEPLOYER_PRINCIPAL>   -> permission to act as <EDDY_SERVICE_ACCOUNT>
```

miaproc itself performs no credential lookup. Do not store service account
keys in the repository and do not mount cloud SDK configuration into the
container; it is not needed.

## Recommended Test Order

1. Local Docker, no network: run the CSV user path or the Parquet path in
   [`../docker/README.md`](../docker/README.md) with read-only input mounts
   and separate writable run directories; run `validate-run` on each run.
2. Mounted storage, disposable prefix: run silver and gold into a
   disposable output prefix on the actual mount (for example Cloud Storage
   FUSE) with unique run names. Check `validate-run`, then read the
   published Parquet tables and run JSONs. Exercise a deliberate failure
   (for example a too-small `--scratch-max-bytes`) and confirm no manifest
   is published and a retry with a new run name succeeds.
3. Batch job: deploy the example in
   [`cloudrun/eddy-split-jobs.example.yaml`](cloudrun/eddy-split-jobs.example.yaml)
   against the disposable prefix, then wire the downstream load to consume
   only validated runs.

Expected source-truth columns in a Parquet silver (a subset, depending on
the source shape):

```text
primary_key, site_id, timestamp,
co2_flux, qc_co2_flux, air_temperature_c, u_star, VPD_hpa,
SWIN_1_1_1, P_RAIN_1_1_1, RH_1_1_1,
RH                  (only when the flux input carries it),
rH_norm_s           (only when a divergent duplicate humidity is detected),
H, qc_H, LE, qc_LE,
filename, DOY, daytime   (only when present in the source)
```

No internal-name passthroughs (`DateTime`, `NEE`, `QC_NEE`, `Tair`, `USTAR`,
`VPD`, `Rg`, `P_RAIN`, `rH`) should appear in Parquet silver or gold.

## Publication and retry safety

- Each invocation owns one fresh `--run-dir`; an existing directory, outputs
  outside it and overlap with inputs are refused (exit 3).
- The completion manifest `_MIAPROC_RUN_COMPLETE.json` is written last and
  is the publication point. Exit 5 or an unknown status can coexist with a
  valid run: decide with `validate-run`.
- Retries always use a new run name; never delete or overwrite earlier runs.
- An existence check is not a lock; the orchestrator keeps run names
  exclusive. There is no atomic publication across several objects.
- FUSE mounts have incomplete POSIX semantics and caches; each driver and
  its flags need qualification. Budget memory for Python, R, scratch and the
  driver together. Details: [`../docs/RUN_PUBLICATION.md`](../docs/RUN_PUBLICATION.md).

## Deployment Notes

- This handoff does not pin an image digest because this repository does
  not publish images. The deployment team is the build/publish owner.
- Keep deployment-specific values (service accounts, buckets, run naming)
  in your deployment system, not in source code.
- Cloud Run Job YAML files under `06_infra/cloudrun/` are examples with
  placeholders, not production manifests.
- Live cloud qualification (mount behavior, permissions, resources,
  scheduling) is engineer-owned and has not been performed here.
