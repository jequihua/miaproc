# Architecture

## Package Architecture

`miaproc` should remain a Python package with domain modules:

- `miaproc.biomass`: existing biomass/allometry functionality.
- `miaproc.eddy`: eddy covariance ingestion, QC, preprocessing, and
  post-processing.

The eddy post-processing layer should dispatch through backend adapters:

- `hesseflux`: portable Python-native backend.
- `reddyproc-rpy2`: optional parity backend wrapping REddyProc.

## Backend Boundary

Backend adapters must accept normalized stage-1 data and return the common schema
defined in `08_pkg/backend_contract.md`.

Backend-specific details such as R object creation, hesseflux sentinels, u*
scenario column names, and quality flag names must stay inside the adapter.
Downstream code should consume normalized columns and diagnostics.

## Dependency Boundary

Python-only install:

- required for default package usage.
- includes hesseflux path.
- must be sufficient for default CI.

Optional R-backed install:

- requires R.
- requires REddyProc.
- requires `rpy2`.
- enables `engine="reddyproc-rpy2"` and optional parity tests.

## Data Boundary

Raw tower data are external inputs. Package source code must not assume one fixed
data folder or mutate raw files in place.

## Review Boundary

Any architectural change that affects backend selection, output schema, sign
conventions, or dependency requirements must update governance logs and pass a
review gate.

## Cloud Deployment Posture (M11)

The accepted M7→M10 BigQuery-native eddy path
(`miaproc eddy run-bigquery`) is deployable to Google Cloud as a
**Cloud Run Job**, not a Cloud Run service:

- the unit of work is one batch execution per RBRL slice, not a
  request handler;
- exit codes drive success / failure semantics for the orchestrator
  (`0` success, `2` preflight not project-scoped approved, `3`
  validation failure, `4` runtime processing failure);
- the image is the same `miaproc:cli-r45` runtime profile shipped
  for local execution; nothing in the cloud path requires a second
  container or a re-architecting of the runtime contract.

Job manifests live in [`cloudrun/`](cloudrun/); the deployment +
IAM contract + Terraform-deferral notes live in
[`deployment.md`](deployment.md); the operator runbook lives in
[`../09_ops/runbooks.md`](../09_ops/runbooks.md).

The cloud deployment **does not change** any of the following:

- Decision 010 / risk R11: project-scoped Python preflight runs in
  cloud the same way it runs locally; no env-var or flag bypass.
- Production-read-only posture: the Cloud Run Job is configured to
  bill its BigQuery query jobs to `manglaria-staging`
  (`--bq-billing-project=manglaria-staging`) but to read source
  tables from `manglaria` cross-project. Write IAM on
  `manglaria` is forbidden by both code
  (`forbidden_write_projects=("manglaria",)`) and IAM contract.
- Module-aware CLI shape (`miaproc <domain> <command>`): the same
  `eddy run-bigquery` subcommand is the only entrypoint the cloud
  job uses; future biomass / non-eddy modules drop in as siblings
  without a second deployment surface.
