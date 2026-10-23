# Run publication on local and mounted storage (M002-S03)

`miaproc eddy run-silver` and `miaproc eddy run-gold` publish every result as
one **run directory** that the invocation owns. Ordinary local paths are the
storage interface: a local disk, a Docker bind mount or a FUSE mount
(for example Cloud Storage FUSE) all look the same to miaproc. Mounting,
permissions, caches and service resources are engineer-owned; actual cloud
qualification is not performed or claimed here.

## Contract

1. **Fresh run directory.** `--run-dir` (default: the parent directory of
   `--output-table`) must not exist. Every `--output-*` path, including
   `--output-groups-dir`, must lie inside it. Reuse of an existing directory,
   outputs outside the run (`..` traversal), a run inside or around an input,
   a scratch directory inside the run and case-insensitively colliding
   artifact names are refused with exit 3 before any computation. Group
   labels whose sanitized file names collide are refused with exit 4 (D014).
2. **Staging in bounded local scratch.** All artifacts are written and read
   back (row counts, Parquet schema, JSON parse) in an invocation-owned
   subdirectory of `--scratch-dir` (default `$MIAPROC_SCRATCH_DIR`, else the
   system temp directory). `--scratch-max-bytes` (default
   `$MIAPROC_SCRATCH_MAX_BYTES`, else 4 GiB; a positive integer) bounds the
   aggregate logical bytes written into that subdirectory: every per-group
   and combined table (Parquet or CSV), diagnostics, run metadata and the
   staged completion manifest. Bytes are charged as they are written, so a
   write that would exceed the limit is refused before it reaches scratch;
   the command exits 4 with nothing published. Only that subdirectory is
   removed, on success and on failure.
3. **Sequential, closed copies.** After the whole computation succeeded the
   run directory is created and each artifact is copied, one at a time, to a
   fresh file opened with exclusive create, flushed and closed; write, flush
   and close errors propagate (FUSE drivers commonly report upload failures
   at close). Each copy is re-hashed against the staged bytes.
4. **Completion manifest last.** `_MIAPROC_RUN_COMPLETE.json` is written
   last. It records `manifest_kind`, `manifest_version` (1), `stage`,
   producer versions, the relative `run_metadata`/`diagnostics` references and
   one record per artifact: relative POSIX `path`, `role`, `size`, `sha256`,
   and for tables `rows`, `columns` and `schema_sha256`. **The closed, valid
   manifest is the publication point.**
5. **Consumers validate.** `run-gold` locates the nearest ancestor of
   `--silver-table` holding a manifest, validates it (stage, canonical
   relative names, sizes, SHA-256) and requires the silver table to be the
   run's `table` artifact, before preflight or any engine work.
   `miaproc eddy validate-run --run-dir DIR [--stage silver|gold]` is the
   readback entry point (exit 0 with a JSON summary, exit 3 otherwise). The
   Python API is `miaproc.eddy.publish.validate_run`.

Nothing relies on rename/replace, file locks, append, in-place overwrite,
fsync as a commit or any cloud SDK.

## Failure and retry semantics

| Situation | Exit | What is left |
|---|---|---|
| Validation / reuse / overlap / traversal / incomplete silver | 3 | nothing created |
| Computation, serialization (scratch), scratch byte limit, ENOSPC, denied write, artifact copy/flush/close fault | 4 | no manifest; invocation-created files are removed best-effort |
| The completion manifest could not be created (its exclusive create failed) | 4 | no manifest from this invocation; invocation-created files are removed best-effort |
| Process killed before the manifest | (signal) | possibly partial artifacts and **no manifest**: not consumable |
| Any manifest write, flush or close fault after the manifest was created, or a failed post-publication readback | 5 | a run that may be published: it validates once read access works, or consumers reject it; **never deleted** |
| Process killed right after the manifest | (signal) | a valid, published run |

Producer exit status and storage commit cannot be atomic (D005): exit 5 or an
unknown status can coexist with a valid published run. Once the manifest file
exists, a write or flush error can follow a complete write that closing still
finalizes, and a failed, missing, stale or mismatching readback proves
nothing; so miaproc then deletes nothing. It removes its files only when the
manifest could not be created at all.
Always decide with `validate-run`, never with the exit code alone. Retries
always use a **new** run name; miaproc never deletes prior runs or shared
prefixes, and cleanup is limited to the scratch directory and files the
invocation created itself.

## Caller-owned exclusivity

An existence check is not a distributed lock. Two concurrent writers given
the same run name on object storage can race. The orchestrator must hand out
exclusive run names (for example `<stage>-<site>-<window>-<attempt>`), never
reuse a name after any failure, and treat a run as consumable only after
`validate-run` succeeds. There is no atomic publication across multiple
objects: readers may see some artifacts before the manifest exists, which is
why consumers require the manifest.

## FUSE and cache caveats

FUSE mounts of object storage implement an incomplete POSIX subset: rename is
not atomic (or unsupported), locks and appends are unsupported, in-place
writes may rewrite the whole object, and errors may only surface at close.
Metadata and listing caches can show stale existence or sizes to other
readers for the cache TTL; a consumer on another host can briefly miss a new
manifest. Cloud Storage FUSE and S3 drivers (for example Mountpoint for S3 or
s3fs) differ; each driver, its flags and cache settings need separate
engineer qualification. The tests simulate these limits by patching; they do
not mount anything.

## Resource budgeting

Size the task for Python (pandas/pyarrow frames, roughly several times the
table size during payload preparation), R for `reddyproc-reference`, the
staged artifacts in scratch and the FUSE driver's own buffers/cache.

`--scratch-max-bytes` is enforced only for the staged file bytes described
above (the file extent miaproc writes, counted once per byte). It is **not** a
cap on Python, R or Arrow memory, process RSS, filesystem allocation overhead
or FUSE/mount caches, and it does not bound the published copies in the run
directory. On Cloud Run the writable local filesystem is in memory, so staged
scratch also consumes RAM: put `--scratch-dir` on that in-memory filesystem or
a local volume and set `--scratch-max-bytes` within the memory left after
Python, R and FUSE. A run whose staged output exceeds the limit fails with
exit 4 before publication; raise the limit or split the input window.

## Migration from earlier output flags

Familiar flags are unchanged; only their directory must now be fresh and
separate per invocation.

```bash
# Before (one shared output directory)
miaproc eddy run-silver --flux-parquet in/flux.parquet --biomet-parquet in/biomet.parquet \
    --output-table out/silver.parquet --output-run-json out/silver_run.json
miaproc eddy run-gold --silver-table out/silver.parquet --engine hesseflux-native \
    --output-table out/gold.parquet --output-diagnostics-json out/gold_diag.json \
    --output-run-json out/gold_run.json

# After (one fresh run directory per invocation)
miaproc eddy run-silver --flux-parquet in/flux.parquet --biomet-parquet in/biomet.parquet \
    --output-table out/silver-0001/silver.parquet --output-run-json out/silver-0001/silver_run.json
miaproc eddy run-gold --silver-table out/silver-0001/silver.parquet --engine hesseflux-native \
    --output-table out/gold-0001/gold.parquet --output-diagnostics-json out/gold-0001/gold_diag.json \
    --output-run-json out/gold-0001/gold_run.json
miaproc eddy validate-run --run-dir out/gold-0001 --stage gold

# Explicit run directory with nested output names
miaproc eddy run-silver --flux-parquet in/flux.parquet --biomet-parquet in/biomet.parquet \
    --run-dir out/silver-0002 --output-table out/silver-0002/tables/silver.parquet \
    --output-run-json out/silver-0002/meta/silver_run.json --scratch-dir /tmp/miaproc-scratch

# Legacy compatibility: a pre-M002-S03 CSV silver without a manifest
miaproc eddy run-gold --silver-table old/silver.csv --allow-legacy-silver-csv \
    --engine hesseflux-native --output-table out/gold-0003/gold.csv \
    --output-diagnostics-json out/gold-0003/gold_diag.json --output-run-json out/gold-0003/gold_run.json
```

CSV-directory inputs (`--flux-dir`/`--biomet-dir`) and CSV outputs remain the
documented legacy compatibility shape; Parquet silver must always be a
published run. `miaproc run` (single-shot) is unchanged.
