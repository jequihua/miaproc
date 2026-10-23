# Cloud Run Examples

This folder contains a placeholder Cloud Run Job template for engineers who
build, push and deploy `miaproc` themselves. It is an example, not a
deployed or qualified manifest:

- no image digest is pinned;
- no service account, bucket or project is hard-coded;
- no live cloud resource is assumed, created or tested;
- all values wrapped in `<...>` must be replaced by the deploying team;
- field names should be checked against the current Cloud Run Jobs
  documentation.

## Files

- `eddy-split-jobs.example.yaml` - multi-document template for the
  file-based eddy silver job, gold job (`reddyproc-reference`) and a
  `validate-run` consumer check. Inputs come from a read-only mount and
  each execution publishes one fresh run directory on a writable mount.

The former BigQuery jobs (dry-run, stage writeback, MERGE) were removed with
the BigQuery commands in M002-S04; see
[`../../docs/MIGRATING_FROM_BIGQUERY.md`](../../docs/MIGRATING_FROM_BIGQUERY.md).

## Required Placeholders

```text
<PROJECT_ID>
<SERVICE_ACCOUNT_EMAIL>
<IMAGE_URI_PINNED_BY_DIGEST>
<INPUT_BUCKET>
<OUTPUT_BUCKET>
<WINDOW>
<SILVER_RUN>
<GOLD_RUN>
<RUN_TO_CHECK>
<STAGE>
<SCRATCH_BYTES>
```

## Recommended Order

1. The orchestrator writes immutable Parquet inputs for the selected window
   (`<INPUT_BUCKET>/<WINDOW>/flux.parquet`, `biomet.parquet`).
2. Run the silver job with a new `<SILVER_RUN>` name.
3. Run the validate-run job on that silver run (`--stage=silver`).
4. Run the gold job with a new `<GOLD_RUN>` name and the validated silver
   table.
5. Run the validate-run job on the gold run (`--stage=gold`); only then may
   a consumer load the gold table elsewhere (engineer-owned).

Retries always use new run names; never delete or reuse a run directory.
An existence check is not a lock: the orchestrator must keep run names
exclusive.

## Identity and permissions

The job identity needs read access to the input bucket, write access to the
output bucket and image pull access. miaproc needs no other cloud permission
and performs no credential lookup.

## Mounted storage and resources

Cloud Storage FUSE has incomplete POSIX semantics (non-atomic rename, no
locks or appends, errors reported at close, metadata caches). The Cloud Run
local filesystem is in memory, so `--scratch-dir` consumes RAM: size the
memory limit for Python, R, scratch and the FUSE driver together. See
[`../../docs/RUN_PUBLICATION.md`](../../docs/RUN_PUBLICATION.md). Each
driver and its flags need engineer qualification.
