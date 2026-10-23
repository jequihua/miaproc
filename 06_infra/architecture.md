# Architecture

## Package Architecture

`miaproc` is an eddy-covariance-only Python package (the former
biomass/allometry module was removed in M001-S02):

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

## Storage Boundary (M002)

Ordinary local or mounted file paths are the storage interface. The stage
commands `miaproc eddy run-silver` / `run-gold` read explicit immutable
Parquet files (or legacy CSV directories) and publish each invocation into
one fresh run directory with a completion manifest written last
([`../docs/RUN_PUBLICATION.md`](../docs/RUN_PUBLICATION.md)). The package
contains no cloud SDK, credential lookup or network access for data
processing; the BigQuery transport was removed in M002-S04
([`../docs/MIGRATING_FROM_BIGQUERY.md`](../docs/MIGRATING_FROM_BIGQUERY.md)).

## Batch Deployment Posture

When engineers deploy the image to a batch service (for example a Cloud Run
Job), the unit of work is one batch execution per stage, input window and
attempt, not a request handler:

- exit codes drive success / failure semantics for the orchestrator
  (`0` success, `2` preflight not project-scoped approved, `3`
  validation failure, `4` runtime processing failure, `5` published but
  finish unconfirmed — decide with `miaproc eddy validate-run`);
- inputs are mounted read-only, outputs go to a writable mount, and each
  execution receives an exclusive, never-reused run directory name;
- the image is the same Python + R 4.5.1 + REddyProc runtime profile used
  locally; Decision 010 / risk R11 project-scoped preflight runs in the
  job exactly as it runs locally, with no env-var or flag bypass.

The example job manifest lives in [`cloudrun/`](cloudrun/); deployment
notes live in [`deployment.md`](deployment.md); the operator runbook lives
in [`../09_ops/runbooks.md`](../09_ops/runbooks.md). These are examples for
engineers; no deployment is performed or qualified by this package.
