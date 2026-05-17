# Deployment

## Local / file-based deployment

The miaproc Python package is installable in two modes (unchanged
since M6):

- Python-only mode for the file-based `miaproc run` path, default CI,
  and the M7 file-based eddy contracts.
- Optional R-backed mode for the `reddyproc-reference` engine via
  `rpy2`, gated by the project-scoped Decision-010 preflight.

The file-based contract is documented in `08_pkg/README.md`.

## Containerized runtime profile

The `docker/Dockerfile.miaproc-r45-reddyproc` image bundles Python +
R 4.5 + REddyProc 1.3.4 in a project-scoped layout under `/app` (M6
Task 3) and ships the BigQuery extras (`google-cloud-bigquery`,
`bigquery-storage`, `db-dtypes`, `pyarrow`) so the M7/M8/M9/M10
BigQuery-native eddy path runs end-to-end. The Decision-010
preflight is the same in cloud as it is locally.

Build (from repo root):

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
    -t miaproc:cli-r45 .
```

## Cloud Run Jobs deployment (M11)

The accepted M7→M10 BigQuery-native eddy path is deployable as a
Cloud Run Job against `manglaria-staging`. The job is manual-deploy
and manual-execute for this milestone — Terraform/IaC codification
and Cloud Scheduler automation are explicitly **deferred** to a
later pass.

### Image publish

The Cloud Run Job pulls the image from the Artifact Registry repo
`us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy`.
The repo already exists.

Publish a new image revision:

```bash
# One-time per workstation:
gcloud auth configure-docker us-central1-docker.pkg.dev

# Per-revision:
scripts/bash/publish_image_to_artifact_registry.sh
# or, on Windows:
# powershell -ExecutionPolicy Bypass -File `
#   scripts/powershell/publish_image_to_artifact_registry.ps1
```

The script tags the locally-built `miaproc:cli-r45` image, pushes
it, and prints the immutable `sha256:...` digest. **Pin the digest
into the Cloud Run Job manifests** (under `06_infra/cloudrun/`)
rather than the mutable `:cli-r45` tag, so a job execution always
sees the exact image revision the operator tested.

### Job manifests

Two YAML manifests live in [`cloudrun/`](cloudrun/):

| Manifest | Job name | Mutates `_s2_filt_1`? |
|---|---|---|
| [`miaproc-eddy-rbrl-stage.yaml`](cloudrun/miaproc-eddy-rbrl-stage.yaml) | `miaproc-eddy-rbrl-stage` | no — stage + validate only |
| [`miaproc-eddy-rbrl-merge.yaml`](cloudrun/miaproc-eddy-rbrl-merge.yaml)  | `miaproc-eddy-rbrl-merge`  | yes — `--bq-allow-final-merge` baked in |

Both manifests:

- pin the image by `sha256:...` digest;
- set `--bq-billing-project=manglaria-staging` so all BigQuery
  query jobs are billed to the staging project (the source
  tables are still read-only via cross-project SELECT from
  `manglaria.*`);
- pass `--repo-root=/app` so the in-image project-scoped
  preflight resolves the baked `renv.lock` + R library
  (Decision 010);
- use `maxRetries: 0`, `parallelism: 1`, `taskCount: 1`; the
  `cf_s2_runs` audit row + watermark advance contract is
  per-execution, not per-task.

Deploy + execute:

```bash
# Deploy or update.
scripts/bash/deploy_cloud_run_job.sh \
    06_infra/cloudrun/miaproc-eddy-rbrl-stage.yaml

# Execute (blocking on completion).
gcloud run jobs execute miaproc-eddy-rbrl-stage \
    --project manglaria-staging --region us-central1 --wait
```

Or in one shot:

```bash
scripts/bash/deploy_cloud_run_job.sh \
    06_infra/cloudrun/miaproc-eddy-rbrl-stage.yaml --execute
```

The `--execute` shortcut is fine for the **stage-only** manifest.
For the **merge** manifest, deploy and execute in two operator
steps so an accidental re-execution doesn't silently re-MERGE.
The merge manifest mutates `_s2_filt_1`.

## IAM contract

The Cloud Run Job runs as an explicit service account. Two
identities are involved in the M11 deployment:

### Dedicated SA `cf-s2-eddy-runner@manglaria-staging` (M12 target; rollout blocked)

A dedicated job identity exists in the staging project:

```
cf-s2-eddy-runner@manglaria-staging.iam.gserviceaccount.com
```

The Cloud Run Job manifests in [`cloudrun/`](cloudrun/) are
configured to run as this identity (via
`serviceAccountName: cf-s2-eddy-runner@manglaria-staging.iam.gserviceaccount.com`,
M12). The operator who created the SA in M11 can `actAs` it, so
`gcloud run jobs replace` against either manifest succeeds today.
**Execution under this SA currently fails** because four of the
five required IAM bindings need Project-IAM-Admin and the M12
operator does not hold that role.

Today the dedicated SA carries:

- `WRITER` on `manglaria-staging:_orch` (granted in M11 via
  dataset ACL — works without Project-IAM-Admin because the
  operator owns `_orch`).

The remaining grants required for an end-to-end successful run
are documented in
[`cloudrun/iam-grants-required.md`](cloudrun/iam-grants-required.md)
with copy-pasteable `gcloud` / `bq` commands for the privileged
operator. The four bindings, in summary:

| Role | Resource | Why |
|---|---|---|
| `roles/bigquery.jobUser` | project `manglaria-staging` | run BigQuery query jobs (read + write side both bill to staging) |
| `roles/bigquery.dataEditor` | dataset `manglaria-staging:manglaria_lakehouse_ds` | stage + final table writes |
| `roles/bigquery.dataViewer` | tables `manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance` and `carbon_flux_biomet` | read production source tables (table-level grants only — **no** dataset- or project-wide reader role on `manglaria`) |
| `roles/artifactregistry.reader` | repo `cloud-run-source-deploy` in `manglaria-staging` | pull the image (often a no-op if the project's default service-agent bindings already cover it) |

Once those are in place no manifest, package, or image change is
required — the next `gcloud run jobs execute` of either deployed
job will produce the success path documented in
[`../09_ops/runbooks.md`](../09_ops/runbooks.md).

### Why the M11 default Compute Engine SA is no longer the live identity

The M11 manual cloud execution ran as the project's default
Compute Engine SA (`41539496581-compute@developer.gserviceaccount.com`)
because no `serviceAccountName` was set in the manifest. That
identity has broad project Editor and was sufficient for
`bigquery.jobUser` on `manglaria-staging` plus `dataEditor` on the
staging datasets, but it did **not** carry the production-side
`dataViewer` grant either. The M11 cloud execution therefore
exited with the production-read 403 documented in the
`run_summary.md` M11 block.

Switching to the dedicated SA in M12 trades one IAM blocker
(production read) for an earlier one (`bigquery.jobs.create` on
`manglaria-staging`) and is the right least-privilege posture
even before the rollout completes. To temporarily revert to the
default Compute SA for any reason (operator triage, bisecting
a different issue), remove the `serviceAccountName:` line from
the manifest and re-`replace`. **Do not** broaden the dedicated
SA's grants to anything wider than the four bindings above; the
production-read-only invariant is mirrored at the IAM layer by
table-grain `dataViewer` only.

### Production-read-only invariant

The Cloud Run Job **must never** be granted write access to any
table or dataset under `manglaria`. The
`forbidden_write_projects=("manglaria",)` invariant in
`miaproc.eddy.bigquery_writeback.BigQueryWritebackConfig.validate()`
already refuses such a configuration in code, but the IAM rollout
must mirror it: the dedicated SA gets `dataViewer` (read only) on
the two named source tables and nothing else on the production
project.

## Cloud Scheduler automation

Out of scope for M11. Cloud Scheduler invocation of the merge job
on a fixed cadence (e.g. nightly RBRL refresh) is a follow-up pass
and depends on the IAM rollout above. The runbook in
`09_ops/runbooks.md` documents how to invoke a one-off execution
manually until then.

## Terraform / IaC

Out of scope for M11. The deployment material in this directory is
intentionally `gcloud`-driven from the operator machine so the team
can decide whether to codify the deployment (Cloud Run Job, IAM,
Artifact Registry repo, optional Scheduler) in Terraform as a
separate decision. The YAML manifests under
[`cloudrun/`](cloudrun/) are stable enough to convert to a
`google_cloud_run_v2_job` Terraform resource later without
re-architecting the runtime.
