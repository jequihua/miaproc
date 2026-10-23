# miaproc Docker Runtime

This directory contains the Docker runtime profile for the standalone
`miaproc` package (eddy covariance only: no biomass module since M001-S02,
no BigQuery commands or Google SDK since M002-S04/S05). Everything this
runbook needs is inside the package directory; the build context is the
package root.

The image bundles:

- Python 3.12 (Ubuntu `python3`) with `miaproc[dev,hesseflux,reddyproc]`
  installed in editable mode from the baked `/app/src` into
  `/opt/miaproc-venv`. No Google, gRPC, protobuf or db-dtypes package is
  installed: the constraint pins that served the removed `bigquery` extra
  were deleted in M002-S05 (D016). Images built before M002-S05 (for
  example tag `product-m001-s03`) still contain those packages;
- R 4.5.1 (base image `rocker/r-ver:4.5.1`, pinned by digest);
- REddyProc 1.3.4 and its dependencies installed into the project-scoped
  library `/app/renv/library/R-4.5/x86_64-pc-linux-gnu` from `renv.lock`;
- the package tests (including the synthetic input generator
  `/app/tests/synthetic_eddy.py`), docs, examples and lineage schemas
  (`/app/06_infra/schemas`, read by the tests only).

The project-scoped R layout is required by the eddy
`reddyproc-reference` backend. Do not bypass it with global-R overrides.

Commands below use POSIX shells. On Git Bash for Windows prefix `docker run`
with `MSYS_NO_PATHCONV=1` so container paths such as `/app` are not rewritten.

## Build

From the package root (the directory holding `pyproject.toml`, `src/`,
`tests/`, `docs/`, `examples/`, `06_infra/schemas/`, `renv.lock`, `docker/`):

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
  -t miaproc-eddy-parquet:product-local .
```

Pins: the base image digest in the Dockerfile; every R package of
`renv.lock` restored at its exact version into the project-scoped library
(Rcpp `1.1.1-1`, which CRAN no longer serves, is substituted by its CRAN
Archive successor `1.1.1-1.1`; the effective lockfile is kept at
`/app/renv/renv.lock.effective`); every installed Python package bounded by
`docker/constraints-py312.txt` (only `pip==25.0.1` is installed outside it).
The image records `/app/docker/pip-freeze.txt`, `/app/docker/pip-check.txt`
and `/app/docker/dpkg-packages.txt` (apt packages are **not** pinned; their
list is recorded instead). The Dockerfile-specific
`Dockerfile.miaproc-r45-reddyproc.dockerignore` keeps host caches and
egg-info out of the context. A rebuild is not bit-reproducible: apt and the
CRAN Archive download are fetched at build time, so every build has a new
image ID. Record the ID you test and deploy by digest.

Check that an image is Google-free:

```bash
IMAGE=miaproc-eddy-parquet:product-local
docker run --rm --network none "$IMAGE" sh -c \
  'grep -Ei "^(google|grpc|proto|db-dtypes)" /app/docker/pip-freeze.txt; python -c "import google" ; echo rc=$?'
```

Expected: no freeze line and `ModuleNotFoundError: No module named 'google'`
followed by `rc=1`.

## Smoke checks

```bash
docker run --rm "$IMAGE" miaproc --help
docker run --rm "$IMAGE" miaproc eddy run-silver --help
docker run --rm "$IMAGE" miaproc eddy run-gold --help
docker run --rm "$IMAGE" miaproc eddy validate-run --help
```

No help text mentions biomass or BigQuery;
`docker run --rm "$IMAGE" python -c "import miaproc.biomass"` fails with
`ModuleNotFoundError`, and a removed command such as
`miaproc eddy run-bigquery-silver` exits `3` with a pointer to
[`../docs/MIGRATING_FROM_BIGQUERY.md`](../docs/MIGRATING_FROM_BIGQUERY.md).

Run the project-scoped R preflight:

```bash
docker run --rm --network none "$IMAGE" python -W error -m miaproc.eddy.r_preflight --repo-root /app
```

Expected result: `Status: ok`, `Approved (project-scoped)`, R 4.5.1,
REddyProc 1.3.4, rpy2 3.6.7, no warnings, no errors. A gold run with
`--engine reddyproc-reference` and a `--repo-root` that does not hold the
project-scoped R layout exits `2` before reading or writing any table.

## Parquet user path with mounted volumes (recommended)

Inputs are bind-mounted **read-only**; outputs and scratch are **separate
writable** bind mounts. Every step is a fresh `docker run --rm` process;
no credentials, cloud configuration or network are needed. The input is a
deterministic synthetic two-site Parquet pair (no real observations):
`SITE_A` 120 days and `SITE_B` 105 days (>= 100 days per site for the
REddyProc u* estimation), typed as the case-study files are (`site_id`,
UTC `timestamp`, an `Int64` flux `primary_key` above 2**53).

```bash
mkdir -p .runs/in .runs/out .runs/scratch
D="docker run --rm --network none"

# 0. Input (the only step that writes the input directory).
$D -v "$PWD/.runs/in:/in" "$IMAGE" python /app/tests/synthetic_eddy.py /in --parquet \
    --site SITE_A:120:20261101 --site SITE_B:105:20261102

# 1. Silver: stage 1 only, no engine, no R. Input mounted :ro.
$D -v "$PWD/.runs/in:/in:ro" -v "$PWD/.runs/out:/out" -v "$PWD/.runs/scratch:/scratch" "$IMAGE" \
  miaproc eddy run-silver \
    --flux-parquet /in/flux.parquet --biomet-parquet /in/biomet.parquet \
    --group-column site_id --run-dir /out/silver-0001 \
    --output-table /out/silver-0001/silver.parquet \
    --output-run-json /out/silver-0001/silver_run.json \
    --scratch-dir /scratch

# 2. Gold (Python-only engine); the silver run is mounted :ro as its input.
$D -v "$PWD/.runs/out/silver-0001:/silver:ro" -v "$PWD/.runs/out:/out" \
   -v "$PWD/.runs/scratch:/scratch" "$IMAGE" \
  miaproc eddy run-gold \
    --silver-table /silver/silver.parquet --engine hesseflux-native \
    --group-column site_id --run-dir /out/gold-hesseflux-0001 \
    --output-table /out/gold-hesseflux-0001/gold.parquet \
    --output-diagnostics-json /out/gold-hesseflux-0001/gold_diag.json \
    --output-run-json /out/gold-hesseflux-0001/gold_run.json \
    --scratch-dir /scratch

# 3. R preflight, then gold with the REddyProc reference engine.
$D "$IMAGE" python -W error -m miaproc.eddy.r_preflight --repo-root /app
$D -v "$PWD/.runs/out/silver-0001:/silver:ro" -v "$PWD/.runs/out:/out" \
   -v "$PWD/.runs/scratch:/scratch" "$IMAGE" \
  miaproc eddy run-gold \
    --silver-table /silver/silver.parquet --engine reddyproc-reference --repo-root /app \
    --group-column site_id --run-dir /out/gold-reddyproc-0001 \
    --output-table /out/gold-reddyproc-0001/gold.parquet \
    --output-diagnostics-json /out/gold-reddyproc-0001/gold_diag.json \
    --output-run-json /out/gold-reddyproc-0001/gold_run.json \
    --scratch-dir /scratch

# 4. Consumers validate every run (output mounted read-only).
for r in silver-0001:silver gold-hesseflux-0001:gold gold-reddyproc-0001:gold; do
  $D -v "$PWD/.runs/out:/out:ro" "$IMAGE" \
    miaproc eddy validate-run --run-dir "/out/${r%%:*}" --stage "${r##*:}"
done

# 5. A separate reader process (reads files only).
$D -v "$PWD/.runs/out:/out:ro" "$IMAGE" python -c '
import pyarrow.parquet as pq
for run in ("silver-0001", "gold-hesseflux-0001", "gold-reddyproc-0001"):
    name = "silver.parquet" if run.startswith("silver") else "gold.parquet"
    t = pq.read_table(f"/out/{run}/{name}").to_pandas()
    print(run, t.groupby("site_id").size().to_dict(), list(t.columns).count("timestamp"))
'
```

Expected: every stage exits `0`; each `validate-run` prints a JSON summary
with `"ok": true`; the reader prints `{'SITE_A': 5760, 'SITE_B': 5040}`
(rows per site, gap rows included) and one `timestamp` column per table.
Each run directory holds the stacked table, `<stem>__groups/` with one
table (and for gold one `<site>__diag.json`) per site, the run JSON
(engine, configuration, per-group rows, versions and, for
`reddyproc-reference`, the preflight record), the top-level diagnostics
JSON for gold, and the completion manifest `_MIAPROC_RUN_COMPLETE.json`
(written last; the publication point). `.runs/scratch` is empty after each
step: only the invocation-owned scratch subdirectory is used and removed.
The input files are never modified (`sha256sum .runs/in/*` before and
after).

### Incomplete runs are rejected

A consumer must never use a run without a valid manifest and matching
artifacts. To see the rejection, damage copies of a published run:

```bash
mkdir -p .runs/out/broken
cp -r .runs/out/silver-0001 .runs/out/broken/no-manifest
rm .runs/out/broken/no-manifest/_MIAPROC_RUN_COMPLETE.json
cp -r .runs/out/silver-0001 .runs/out/broken/truncated
truncate -s 1000 .runs/out/broken/truncated/silver.parquet

$D -v "$PWD/.runs/out:/out:ro" "$IMAGE" \
  miaproc eddy validate-run --run-dir /out/broken/no-manifest --stage silver   # exit 3
$D -v "$PWD/.runs/out/broken/truncated:/broken:ro" -v "$PWD/.runs/out:/out" \
   -v "$PWD/.runs/scratch:/scratch" "$IMAGE" \
  miaproc eddy run-gold --silver-table /broken/silver.parquet --engine hesseflux-native \
    --run-dir /out/gold-from-broken --output-table /out/gold-from-broken/gold.parquet \
    --output-diagnostics-json /out/gold-from-broken/gold_diag.json \
    --output-run-json /out/gold-from-broken/gold_run.json --scratch-dir /scratch   # exit 3
```

Both commands exit `3`; `run-gold` refuses before any engine work and
creates no `gold-from-broken` directory.

## Local CSV user path (legacy compatibility)

CSV directories remain a documented compatibility input; CSV outputs keep
the legacy CSV shape (gold still carries the internal `DateTime`). New work
should use the Parquet path above. Each stage again publishes into its
**own fresh run directory**: reusing one, or writing silver and gold into
one shared directory, is refused with exit `3`.

```bash
mkdir -p .runs/user_path
RUN="docker run --rm --network none -v $PWD/.runs/user_path:/work -w /work $IMAGE"
$RUN python /app/tests/synthetic_eddy.py input \
    --site SITE_A:120:20261101 --site SITE_B:105:20261102
$RUN miaproc eddy run-silver \
    --flux-dir input/full_output --biomet-dir input/biomet \
    --group-column site_id \
    --output-table runs/silver/silver.csv \
    --output-run-json runs/silver/silver_run.json
$RUN miaproc eddy run-gold \
    --silver-table runs/silver/silver.csv --engine hesseflux-native \
    --group-column site_id \
    --output-table runs/gold_hesseflux/gold_hesseflux.parquet \
    --output-diagnostics-json runs/gold_hesseflux/gold_hesseflux_diag.json \
    --output-run-json runs/gold_hesseflux/gold_hesseflux_run.json
$RUN miaproc eddy run-gold \
    --silver-table runs/silver/silver.csv --engine reddyproc-reference \
    --repo-root /app --group-column site_id \
    --output-table runs/gold_reddyproc/gold_reddyproc.csv \
    --output-diagnostics-json runs/gold_reddyproc/gold_reddyproc_diag.json \
    --output-run-json runs/gold_reddyproc/gold_reddyproc_run.json
$RUN miaproc eddy validate-run --run-dir runs/silver --stage silver
$RUN miaproc eddy validate-run --run-dir runs/gold_hesseflux --stage gold
$RUN miaproc eddy validate-run --run-dir runs/gold_reddyproc --stage gold
```

Here the run directory defaults to the parent of `--output-table`. The
silver run holds 5760 `SITE_A` + 5040 `SITE_B` rows. hesseflux and
REddyProc results are not numerically equivalent by design.

## Failure, retry and resources

Exit codes: `0` success; `2` `reddyproc-reference` preflight not
project-scoped approved (nothing written); `3` input/configuration
validation, run-directory reuse/overlap/traversal, incomplete or
mismatched silver run, removed BigQuery command or flag (nothing created);
`4` processing or publication failure before the manifest (no manifest;
not consumable); `5` the manifest was written but the finish could not be
confirmed (decide with `validate-run`; never deleted). Retries always use a
new run directory name. Details, FUSE caveats and memory/scratch budgeting
are in [`../docs/RUN_PUBLICATION.md`](../docs/RUN_PUBLICATION.md); the
operator runbook for mounted cloud storage, identity/IAM ownership and the
failure checklist is [`../09_ops/runbooks.md`](../09_ops/runbooks.md);
moving from the former BigQuery commands is covered by
[`../docs/MIGRATING_FROM_BIGQUERY.md`](../docs/MIGRATING_FROM_BIGQUERY.md).

## Development-project records (not needed to use the image)

In the miaproc-dev-parquet development project the product image is built
only by its verification recorder (one recorded Docker build), and its
integration verifier runs the paths above by image ID with the current
source mounted. The M002-S05 record names tag
`miaproc-eddy-parquet:product-m002-s05`; the M001-S03 record (tag
`product-m001-s03`, built with the removed `bigquery` extra) is
superseded. A separate frozen comparison image (tags `r45-m001-s01`,
`baseline-frozen-m001-s01`) holds the same-engine baseline runtime; never
reuse those tags for new builds. These records live outside the package.

## Publishing

Publishing means tagging and pushing the image to a registry the deploying
team controls. The miaproc development repository does **not** push
images; the cloud engineer who controls the registry owns the
tag/push/digest pin cycle, the job definitions, mounts and their
qualification.
