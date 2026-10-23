# Scheduled Jobs

No scheduled jobs are defined or run by this repository.

## Status

- The eddy stages run as one-shot file jobs (`eddy run-silver`,
  `eddy run-gold`, `eddy validate-run`); see [`runbooks.md`](runbooks.md)
  and the example template in
  [`../06_infra/cloudrun/`](../06_infra/cloudrun/).
- No Cloud Scheduler / Workflows / Composer / cron trigger is wired in this
  repository. Scheduling is engineer-owned.
- The former BigQuery jobs, their watermark table and MERGE cadence were
  removed in M002-S04; see
  [`../docs/MIGRATING_FROM_BIGQUERY.md`](../docs/MIGRATING_FROM_BIGQUERY.md).

## Scheduling guidance for engineers

A scheduler (or workflow engine) replaces the former watermark logic by
deciding explicitly what to process:

1. **Select the window.** Choose the input period per site or for all sites
   (for example the last complete month, or a recompute of the last N days)
   and write it as immutable Parquet inputs under a window-specific prefix.
   miaproc processes every row it is given; u* thresholds, gap filling and
   partitioning are fitted on that window, so choose window lengths with the
   same care as before.
2. **Name the attempt.** Derive an exclusive run name per stage, window and
   attempt (`silver-<site>-<window>-a<n>`). Never reuse a name, also not
   after a failure.
3. **Run and validate.** Run silver, `validate-run` it, run gold on the
   validated silver table, `validate-run` the gold run.
4. **Record progress outside miaproc.** Keep the orchestrator's own record of
   which windows have validated gold runs (the run JSON and manifest give
   the processed `timestamp` range and row counts).
5. **Recompute deliberately.** For late or corrected data, write new inputs,
   run under new names and let the downstream load decide which validated
   run supersedes an older one (for example keyed on `site_id` +
   `timestamp`).

Concurrency: an existence check is not a lock; the scheduler must not start
two executions with the same run name. Budget memory for Python, R, scratch
and the mount driver per execution
([`../docs/RUN_PUBLICATION.md`](../docs/RUN_PUBLICATION.md)).
