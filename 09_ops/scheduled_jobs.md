# Scheduled Jobs

No scheduled jobs are in scope as of M11.

## Status

- The accepted M7→M10 BigQuery-native eddy path is deployable as a
  Cloud Run Job against `manglaria-staging` (M11 — see
  [`../06_infra/deployment.md`](../06_infra/deployment.md) and
  [`runbooks.md`](runbooks.md)).
- Job execution is **manual** today: an operator runs
  `gcloud run jobs execute` (or the helper scripts under
  [`../scripts/`](../scripts/)) on demand.
- No Cloud Scheduler / Workflows / Cloud Composer / cron trigger
  is wired to the job in this repository.

## Why scheduling is deferred

Two preconditions need to be met before scheduled production
execution can be turned on without weakening the safety contract
established by Decisions 010 / 011 and the M8 / M10 reviews:

1. **IAM rollout to the dedicated `cf-s2-eddy-runner@manglaria-staging`
   service account.** The Cloud Run Job manifests under
   `../06_infra/cloudrun/` are now configured to run as that
   dedicated SA (M12), but execution is **blocked** until a
   Project-IAM-Admin applies the four bindings documented in
   `../06_infra/cloudrun/iam-grants-required.md` (table-level
   `dataViewer` on the named production source tables,
   `bigquery.jobUser` on `manglaria-staging`, `dataEditor` on the
   staging dataset, and `artifactregistry.reader` on the image
   repo). A scheduled job under any broader identity would
   amplify the permission surface every cadence cycle, so
   scheduling has to wait for the least-privilege rollout.
2. **An explicit science-owner cadence decision.** The merge
   manifest mutates
   `manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1`
   on every execution. A daily / hourly cadence is an operational
   commitment, not a defaults choice. The recomputation-window
   policy from guide `001` §12 (recompute last 30 days; advance
   watermark to `now - 7 days`) is a recommended default but is
   not encoded in the job today.

When both preconditions are settled, a Cloud Scheduler trigger
calling `gcloud run jobs execute miaproc-eddy-rbrl-merge` (via the
Scheduler `cloud-run-jobs` invoker) is the smallest honest next
step. That deployment piece is a follow-up pass and should be
scoped narrowly: it is **not** an excuse to rewrite the runtime
contract.
