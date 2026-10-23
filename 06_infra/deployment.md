# Deployment

This page is guidance for the engineers who deploy miaproc. No deployment,
cloud resource, IAM binding or mounted-storage driver is created or
qualified by this package or its tests.

## Local / file-based deployment

The miaproc Python package is installable in two modes:

- Python-only mode (`pip install -e ".[dev,hesseflux]"`) for the
  `hesseflux-native` / `hesseflux-ltwrapper` engines, default CI and the
  silver stage. PyArrow is a core dependency, so Parquet inputs and outputs
  need no extra.
- R-backed mode (`".[dev,hesseflux,reddyproc]"`) for the
  `reddyproc-reference` engine via `rpy2`, gated by the project-scoped
  Decision-010 preflight.

The file contract is documented in
[`../docs/PARQUET_FILE_SCHEMA.md`](../docs/PARQUET_FILE_SCHEMA.md) and the
run publication contract in
[`../docs/RUN_PUBLICATION.md`](../docs/RUN_PUBLICATION.md).

The former `bigquery` extra, the BigQuery commands and their deployment
material (BigQuery IAM bindings, control tables, MERGE jobs) were removed
in M002-S04; see
[`../docs/MIGRATING_FROM_BIGQUERY.md`](../docs/MIGRATING_FROM_BIGQUERY.md).
Data processing needs no cloud SDK, credentials or network access.

## Containerized runtime profile

The `docker/Dockerfile.miaproc-r45-reddyproc` image bundles Python 3.12 +
R 4.5.1 + REddyProc 1.3.4 in a project-scoped layout under `/app` (M6
Task 3) and installs the package with the extras `dev,hesseflux,reddyproc`.
The Decision-010 preflight is the same in a container as it is locally.
A product image built before M002-S04 still contains the former BigQuery
packages until it is rebuilt.

Build (from the package root):

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
    -t miaproc-eddy-parquet:product-local .
```

[`../docker/README.md`](../docker/README.md) documents the local Docker
user path with read-only input mounts and separate writable run
directories.

## Batch job deployment (example)

A batch service such as a Cloud Run Job can run each stage as one
execution. [`cloudrun/eddy-split-jobs.example.yaml`](cloudrun/eddy-split-jobs.example.yaml)
is a placeholder template; engineers own:

- **Image publication.** Push the image to a registry and pin it by
  digest in the job definition. The helper scripts under
  `scripts/bash/` and `scripts/powershell/` are optional gcloud wrappers
  with placeholder defaults; review them before use.
- **Mounts.** Mount the input prefix read-only and an output prefix
  writable (for example Cloud Storage volume mounts). Pass mounted paths;
  miaproc rejects `gs://`/`s3://` URIs.
- **Run names.** Give every execution an exclusive, never-reused
  `--run-dir` (for example `<stage>-<site>-<window>-<attempt>`). miaproc
  refuses an existing run directory; an existence check is not a lock.
- **Scratch and resources.** On Cloud Run the local filesystem is in
  memory; set `--scratch-dir`/`--scratch-max-bytes` and the memory limit
  so that Python, R, scratch and the FUSE driver's buffers fit together
  ([`../docs/RUN_PUBLICATION.md`](../docs/RUN_PUBLICATION.md)).
- **Retries.** Prefer `maxRetries: 0` and let the orchestrator start a new
  execution with a new run name; never re-run into the same directory.
- **Consumers.** Load only runs that pass `miaproc eddy validate-run`;
  any warehouse load, upsert or deduplication is engineer-owned.
- **Identity.** The job identity needs read access to the input mount,
  write access to the output mount and image pull access; miaproc itself
  needs no further cloud permission.

## IAM and storage qualification

IAM bindings, bucket layout, FUSE/S3-driver flags, cache settings and
resource sizing must be qualified by the deploying team on real
infrastructure. Local tests simulate close/flush faults and mounted
read-only inputs only; they are not cloud qualification.

## Scheduling and IaC

Scheduling (for example Cloud Scheduler or a workflow engine), window
selection, reruns and any Terraform codification are engineer-owned. The
example YAML is stable enough to translate into an IaC job resource, but
none is provided or tested here.
