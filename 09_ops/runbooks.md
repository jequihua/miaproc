# Runbooks

## Cloud Run Job: `miaproc eddy run-bigquery` against `manglaria-staging` (M11)

The operator-driven Cloud Run Job runbook for the accepted M7→M10
BigQuery-native eddy path. Job manifests live in
[`../06_infra/cloudrun/`](../06_infra/cloudrun/); the deployment
contract (image publish, IAM, Terraform deferral) is documented in
[`../06_infra/deployment.md`](../06_infra/deployment.md).

### Preconditions

- Operator workstation has `gcloud` and `docker` installed and
  authenticated.
- ADC for `gcloud` set via
  `gcloud auth application-default login`.
- The active `gcloud` config points at the **staging** project
  (`gcloud config set project manglaria-staging`); this is just
  for default flag elision — every command in this runbook still
  passes `--project manglaria-staging` explicitly.
- The image was rebuilt locally if the package or Dockerfile
  changed (see `../docker/README.md`).
- `manglaria-staging:_orch` control dataset exists (created
  one-time during M9; idempotent recreation is safe).
- The dedicated job SA
  `cf-s2-eddy-runner@manglaria-staging.iam.gserviceaccount.com`
  exists (created in M11) and the four IAM bindings in
  [`../06_infra/cloudrun/iam-grants-required.md`](../06_infra/cloudrun/iam-grants-required.md)
  have been applied by a Project-IAM-Admin. **Until those four
  bindings land, executing either of the deployed Cloud Run
  Jobs will exit `4` with a BigQuery 403 — see "Failure modes"
  below for the exact error per missing binding.**

### 1. Publish the image

```bash
scripts/bash/publish_image_to_artifact_registry.sh
```

Capture the printed `sha256:...` digest. Update the `image:` field
in both manifests under `06_infra/cloudrun/` so the Cloud Run Job
pulls the exact revision the operator just tested locally. **Do
not** leave the manifest pointing at the previous digest — the
manifest is the source of truth for what runs in cloud.

### 2. Stage-only execution (safe default)

```bash
scripts/bash/deploy_cloud_run_job.sh \
    06_infra/cloudrun/miaproc-eddy-rbrl-stage.yaml --execute
```

Expected:

- exit 0 from the `gcloud run jobs execute --wait` invocation;
- a new row in
  `manglaria-staging._orch.cf_s2_runs` with
  `status = "stage_only_succeeded"`,
  `merge_attempted = false`,
  `watermark_advanced = false`,
  `stage_rows = 4 813` (RBRL slice as of 2026-04);
- `manglaria-staging.manglaria_lakehouse_ds.cf_s2_stage_rbrl`
  populated;
- `_s2_filt_1` for `RBRL` unchanged;
- `cf_s2_watermark` unchanged.

### 3. Explicit MERGE execution (mutates `_s2_filt_1`)

The merge manifest carries `--bq-allow-final-merge` baked into its
args; deploy and execute are kept as separate operator decisions
on purpose.

```bash
# Deploy or update the manifest.
scripts/bash/deploy_cloud_run_job.sh \
    06_infra/cloudrun/miaproc-eddy-rbrl-merge.yaml

# Pause. Re-read the args you just deployed:
gcloud run jobs describe miaproc-eddy-rbrl-merge \
    --project manglaria-staging --region us-central1 \
    --format='value(spec.template.spec.template.spec.containers[0].args)'

# Then execute (blocking on completion).
gcloud run jobs execute miaproc-eddy-rbrl-merge \
    --project manglaria-staging --region us-central1 --wait
```

Expected:

- exit 0;
- `cf_s2_runs` row with
  `status = "succeeded"`,
  `merge_attempted = true`,
  `merge_authorized = true`,
  `watermark_advanced = true`,
  `merge_inserted_rows + merge_updated_rows = 4 813`;
- `cf_s2_watermark` row for `RBRL` advanced to the max processed
  `timestamp`;
- `_s2_filt_1` for `RBRL` updated.

### 4. Inspect cloud execution after the fact

```bash
# Job state:
gcloud run jobs describe miaproc-eddy-rbrl-stage \
    --project manglaria-staging --region us-central1

# Recent executions:
gcloud run jobs executions list \
    --job miaproc-eddy-rbrl-stage \
    --project manglaria-staging --region us-central1

# Logs from the most recent execution:
gcloud logging read \
    "resource.type=cloud_run_job AND resource.labels.job_name=miaproc-eddy-rbrl-stage" \
    --project manglaria-staging --limit 80 \
    --format='value(textPayload)' --order=desc
```

The authoritative audit trail for what happened in BigQuery is the
`cf_s2_runs` row, not the Cloud Run logs. Always cross-check the
two when investigating a failed run.

### Failure modes

- **403 `bigquery.jobs.create denied` in `manglaria-staging`** at
  the very first BigQuery call — this is the M12-current failure
  mode under the dedicated `cf-s2-eddy-runner` SA when
  `roles/bigquery.jobUser` on `manglaria-staging` has not been
  granted. **No `cf_s2_runs` row is written** for this failure
  because the orchestrator never reaches `record_run_row(...)` —
  the BigQuery client refuses to submit any job, so the
  `ensure_control_tables_exist` / stage-write call path never
  starts. The authoritative audit trail is the Cloud Run
  execution log for the failed execution; the `cf_s2_runs`
  control table is silent. Apply the bindings in
  [`../06_infra/cloudrun/iam-grants-required.md`](../06_infra/cloudrun/iam-grants-required.md)
  to unblock.
- **403 `Access Denied: Table manglaria.manglaria_lakehouse_ds.*`**
  on the read step — `bigquery.jobUser` is in place but
  `roles/bigquery.dataViewer` on the two named source tables is
  not. **No `cf_s2_runs` row is written** for this failure
  either — the read fails before the writeback orchestrator
  runs. The audit trail is still the Cloud Run execution log.
  Production project was not written. (This is the M11
  default-Compute-SA failure mode; under the M12 dedicated SA it
  would surface only after the `jobUser` binding lands.)
- **`Unrecognized name: <column>`** on MERGE — the M10 schema
  mapping has drifted from the live `_s2_filt_1` schema. The
  stage write succeeded, so a `cf_s2_runs` row **does** exist
  for this failure with `status = "failed"`,
  `merge_attempted = true`, and the BigQuery error in
  `error_text`. Inspect the live target schema via the
  in-container BigQuery client (see `03_experiments/run_summary.md`
  M9 inspection block) and confirm `prepare_stage_dataframe` and
  the M6 backend output cover the columns the target requires.
- **`status = "validation_failed"`** in `cf_s2_runs` — the M8
  validation SQL caught a NULL in a REQUIRED column or a
  `(site_id, timestamp)` / `primary_key` duplicate. The MERGE
  was aborted; final table is untouched. Check the
  `validation_metrics` payload in the local `run.json` (when
  the operator captured one) and the `error_text` in the
  `cf_s2_runs` row.

In short: the writeback control tables are populated **only when
the orchestrator actually starts**, which requires the SA to
hold `bigquery.jobUser` on the project that bills the query.
Pre-orchestrator IAM failures live in the Cloud Run execution
log, not in `cf_s2_runs`.

### Decision-010 / R11 invariant

`reddyproc-reference` always runs the project-scoped Python
preflight before any BigQuery I/O — same gate in cloud as
locally. If a Cloud Run Job execution exits with code `2`, the
in-image `/app/renv.lock` + `/app/renv/library/R-4.5/...`
project-scoped layout was disturbed (most likely the operator
rebuilt the image without the `bigquery` extras or against a
`renv.lock` that disagrees with the bundled REddyProc 1.3.4).
Rebuild from a clean checkout. **No bypass flag exists** on the
writeback or schema-mapping path — the gate is enforced by the
Python preflight, not by the BigQuery code.
