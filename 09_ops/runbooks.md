# Runbooks

## File-run batch job: `miaproc eddy run-silver` / `run-gold`

Operator runbook for running the eddy stages as batch jobs over mounted
storage (for example Cloud Run Jobs with Cloud Storage volume mounts). The
example job template lives in
[`../06_infra/cloudrun/`](../06_infra/cloudrun/); deployment notes are in
[`../06_infra/deployment.md`](../06_infra/deployment.md). Nothing here has
been deployed or qualified by this package; engineers own the cloud setup.
The former BigQuery runbook (stage writeback, MERGE, watermark and run
tables) was removed with the BigQuery commands in M002-S04; see
[`../docs/MIGRATING_FROM_BIGQUERY.md`](../docs/MIGRATING_FROM_BIGQUERY.md).

### Preconditions

- An image built from the current package, published by the deploying team
  and pinned by digest in the job definition (see `../docker/README.md`).
- The input window has been written as immutable Parquet files
  (`flux.parquet`, `biomet.parquet`) on a prefix the job mounts read-only.
- The output prefix is mounted writable; the job identity can create and
  read objects there.
- The orchestrator has an exclusive, never-used run name for this attempt
  (for example `silver-<site>-<window>-a1`).

### 1. Silver

Run the silver job (args as in the example template):

```bash
miaproc eddy run-silver \
    --flux-parquet /mnt/in/<WINDOW>/flux.parquet \
    --biomet-parquet /mnt/in/<WINDOW>/biomet.parquet \
    --group-column site_id \
    --run-dir /mnt/out/<SILVER_RUN> \
    --output-table /mnt/out/<SILVER_RUN>/silver.parquet \
    --output-run-json /mnt/out/<SILVER_RUN>/silver_run.json \
    --scratch-dir /tmp/miaproc-scratch
```

Expected: exit `0`; `/mnt/out/<SILVER_RUN>/` holds `silver.parquet`, the
per-group files, `silver_run.json` and `_MIAPROC_RUN_COMPLETE.json`.

### 2. Validate silver

```bash
miaproc eddy validate-run --run-dir /mnt/out/<SILVER_RUN> --stage silver
```

Exit `0` prints a JSON summary; exit `3` means the run is incomplete or
mismatched and must not be consumed.

### 3. Gold

```bash
miaproc eddy run-gold \
    --silver-table /mnt/out/<SILVER_RUN>/silver.parquet \
    --engine reddyproc-reference --repo-root /app \
    --group-column site_id \
    --run-dir /mnt/out/<GOLD_RUN> \
    --output-table /mnt/out/<GOLD_RUN>/gold.parquet \
    --output-diagnostics-json /mnt/out/<GOLD_RUN>/gold_diag.json \
    --output-run-json /mnt/out/<GOLD_RUN>/gold_run.json \
    --scratch-dir /tmp/miaproc-scratch
```

Gold validates the silver run's manifest before the R preflight or any
engine work. Then run `validate-run --stage gold` on the gold run; only a
validated gold run may be loaded downstream (engineer-owned).

### 4. Inspect after the fact

- The run directory is the audit record: `*_run.json` (engine, config,
  versions, row counts, input paths, collision actions, R preflight) and the
  completion manifest (artifact sizes, SHA-256, row counts, schema identity).
- Job logs carry the process output (including the R console for
  `reddyproc-reference`); cross-check them with the run JSON.

### Exit codes and actions

| Exit | Meaning | Operator action |
|---|---|---|
| `0` | Success, run published | Validate, then consume |
| `2` | `reddyproc-reference` preflight not project-scoped approved (or an argparse usage error) | Check `--repo-root /app` and that the image's project-scoped R layout is intact; rebuild from a clean checkout if needed |
| `3` | Input/config validation: missing/corrupt/empty input, URI path, existing or overlapping `--run-dir`, invalid silver run, removed BigQuery command/flag | Fix the input or arguments; retry with a **new** run name |
| `4` | Runtime failure before publication (engine error, scratch budget, ENOSPC, denied write, copy/close fault) | No manifest was published; inspect logs; retry with a **new** run name |
| `5` | Manifest written but finish unconfirmed | Run `validate-run`; a valid run is published and is never deleted |
| signal / timeout | Process killed | Run `validate-run`: without a manifest the run is not consumable; retry with a new name |

Never delete, overwrite or reuse a run directory; miaproc never does either.

### Decision-010 / R11 invariant

`reddyproc-reference` always runs the project-scoped Python preflight before
any engine work or output — the same gate in a job as locally. If an execution exits `2`, the in-image `/app/renv.lock` +
`/app/renv/library/R-4.5/...` layout was disturbed or `--repo-root` is
wrong. **No bypass flag exists.**

## Mounted storage: configuration, identity and limits

Nothing in this section has been deployed or qualified by the package
maintainers; it states what the deploying engineers must configure and
check. The local equivalent (read-only input, separate writable output and
scratch bind mounts, fresh `docker run --rm` per step, incomplete-run
rejection) is the "Parquet user path with mounted volumes" in
[`../docker/README.md`](../docker/README.md).

### Mount layout

| Path | Mode | Content |
|---|---|---|
| `/mnt/in` | read-only | immutable Parquet inputs of one window |
| `/mnt/out` | read-write | one fresh run directory per invocation |
| scratch (`--scratch-dir`) | read-write, local or in-memory | the invocation's staging subdirectory only |

Example (Google Cloud Storage through Cloud Run job volume mounts; check
flags against the current Cloud Run documentation): a `cloud-storage`
volume for the input bucket with `readonly=true`, a `cloud-storage` volume
for the output bucket, an `in-memory` volume with a `size-limit` for
scratch, `--max-retries 0` (a platform retry would reuse the run name and
exit `3`), and the job identity pinned explicitly. The job template is
[`../06_infra/cloudrun/eddy-split-jobs.example.yaml`](../06_infra/cloudrun/eddy-split-jobs.example.yaml).
An S3 mount (for example Mountpoint for Amazon S3 or s3fs) is a different
driver with different semantics and needs its own qualification; nothing
here covers it.

### Identity

miaproc performs no credential lookup and needs no network for processing.
The job identity needs object read on the input bucket, object create and
read on the output bucket (gold and `validate-run` re-read published
files; object delete lets best-effort cleanup of a failed invocation's
files succeed) and image pull access. It needs no access beyond read to
earlier runs: miaproc never modifies or deletes them.

### Limits to respect

- Object-store FUSE is not POSIX: no atomic rename, locks or appends,
  whole-object rewrites, errors reported at close, metadata/list caches
  that delay visibility to other readers. miaproc checks every close and
  relies on none of the rest; consumers must use `validate-run`, retrying
  with backoff while caches settle.
- No atomic publication across objects: readers can see artifacts before
  the manifest; only a validated run is consumable.
- Existence checks are not locks: the orchestrator issues exclusive run
  names (`<stage>-<site>-<window>-<attempt>`), never reused after failure.
- Memory: on Cloud Run the local filesystem and in-memory volumes use RAM.
  Budget Python frames, R (`reddyproc-reference`), the staged artifacts
  (cap with `--scratch-max-bytes`, default 4 GiB: lower it) and the FUSE
  driver's per-writer buffers and caches together; miaproc copies one
  artifact at a time.

### Failure checklist

1. Run `validate-run`; exit 0 means consumable, whatever the producer's
   exit status was.
2. Exit `3` at start: reused run name, a `gs://`/`s3://` URI instead of a
   mounted path, or missing/corrupt input. Fix it; use a new run name.
3. Exit `4` with a close/flush error: storage write failed (permissions,
   quota, driver). Check the identity and driver logs; new run name.
4. Exit `4` for the scratch budget or ENOSPC: raise memory together with
   `--scratch-max-bytes`.
5. Exit `2`: the image's project-scoped R layout is missing or
   `--repo-root` is not `/app`.
6. A consumer on another instance does not see a reported manifest: cache
   delay; retry `validate-run` before declaring failure.
