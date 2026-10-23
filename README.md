# miaproc

**miaproc** is a Python library for preprocessing and post-processing multiple environmental data streams coming from sensors deployed in Mexican mangroves.

This package is being developed as part of the ManglarIA initiative — a project supported by WWF Mexico and Google, focused on monitoring, modeling, and protecting mangrove ecosystems in a rapidly changing climate.

More about the project:  
https://www.worldwildlife.org/descubre-wwf/historias/manglaria-utilizando-inteligencia-artificial-para-salvar-los-manglares-en-un-clima-en-constante-cambio

### Current modules

- Eddy covariance data preprocessing and post-processing (`miaproc.eddy`)

miaproc is eddy-covariance only. The former tree-level biomass module
(`miaproc.biomass`), its `miaproc biomass ...` CLI commands, the `biomass`
install extra, the packaged allometric-equation assets and the biomass
schemas, job examples and handoff docs were removed in M001-S02; they are
not a supported feature of this package.


---

## Current Status

At this stage, `miaproc` includes:

- Basic preprocessing utilities for environmental time series.
- A complete workflow for eddy covariance tower data (`miaproc.eddy`), including:
  - Data ingestion (multiple CSVs)
  - QC filtering
  - Rain filtering
  - Outlier detection (3σ)
  - Time standardization to 30-minute grid
  - USTAR filtering (fixed and dynamic modes)
  - MDS gap filling
  - Flux partitioning (NEE → GPP + RECO)
  - Optional REddyProc parity backend via `rpy2`
  - Optional Lloyd-Taylor alignment wrapper for Reco comparability
    studies (Milestone 5 closure; see below)

Future sensor modules will be added progressively.

Gate M5 (Case Study Validation) was closed on 2026-04-24 under
Decision 011 of the package's development history (the decision log is not
shipped with the package; the accepted limitations are summarized in
[`backend_contract.md`](backend_contract.md)) — see
[`docs/m5_reddyproc_hesseflux_magnitude_note.md`](docs/m5_reddyproc_hesseflux_magnitude_note.md)
for the magnitude-comparability note.

---

## Installation

The package supports two run modes. Choose the one that matches your
environment:

| Mode | Use when | Install |
|---|---|---|
| **Python-only** | No R available; standard EC post-processing with hesseflux. | `pip install -e ".[dev,hesseflux]"` |
| **R-backed** | REddyProc parity backend is needed (comparability studies, reference output). Requires R 4.5+ and `rpy2` 3.6+ bound via a project-scoped `renv`. | `pip install -e ".[dev,hesseflux,reddyproc]"` plus R/REddyProc setup per `docs/REDDYPROC_LOCAL_TIME_POLICY.md` |

PyArrow (the Parquet engine for `.parquet` silver/gold tables) is a core
dependency of `miaproc` and is installed in every mode; no extra is needed
for the eddy file workflow.

Install commands below run from the package root (the directory holding
`pyproject.toml`); from the miaproc-dev-parquet project root use
`pip install -e "08_pkg/miaproc[dev,hesseflux]"`. The tested runtime is
Python 3.12. The supported R-backed runtime is the Docker image
([`docker/README.md`](docker/README.md)), which carries R 4.5.1,
REddyProc 1.3.4 and rpy2 3.6.7 in the project-scoped layout the preflight
approves; a host R setup must reproduce that layout itself.

### Python-only mode

```bash
git clone https://github.com/jequihua/miaproc.git
cd miaproc
python -m venv .venv
# Linux/Mac:
source .venv/bin/activate
# Windows:
# .venv\Scripts\activate
pip install -e ".[dev,hesseflux]"
```

This is the supported default for routine development and CI.

### R-backed mode

Additional prerequisites: R 4.5+ on the host, `renv` initialized under
the repo root, and `REddyProc` installed into the project-scoped library
(the Dockerfile in `docker/` is the exact, tested recipe). Then:

```bash
pip install -e ".[dev,hesseflux,reddyproc]"
# R + REddyProc must be set up separately via renv.
```

Run the preflight before any live R call:

```bash
R_HOME="<approved-R-home>" \
  python -W error -m miaproc.eddy.r_preflight --repo-root .
```

The preflight must exit `0` with `approval_source` starting
`project-scoped` before `reddyproc-rpy2` is treated as authoritative
(Decision 010 / risk R11).

---

## Usage

### Stage 1: Load and clean eddy covariance data

```python
from miaproc.eddy import load_stage1

df_stage1 = load_stage1(
    path_full_output="path/to/full_output_folder",
    path_biomet="path/to/biomet_folder",
    tz_in="UTC",
    tz_out="UTC",
    skip_full_output=0,  # set to 1 if your export has a units row
    skip_biomet=0,
)
```

This performs:

- File aggregation
- Timestamp parsing
- QC filtering
- Rain filtering
- 3σ outlier removal
- Standardization to a continuous 30-minute grid

---

### Stage 2: USTAR filtering, gap filling and partitioning

`postproc(df_stage1, engine=...)` dispatches to one of two backends.
All backends return the same 13-column common contract (see
[`backend_contract.md`](backend_contract.md)):

```text
DateTime, NEE, NEE_f, NEE_fqc, GPP, Reco, Tair, Tair_f,
Rg, Rg_f, VPD, VPD_f, USTAR
```

Diagnostics are attached at `result.attrs["miaproc_diagnostics"]`.

#### Backend selection

| Engine | Purpose | Extras |
|---|---|---|
| `hesseflux` (default) | Portable Python-only post-processing. | `[hesseflux]` |
| `reddyproc-rpy2` | REddyProc reference backend; requires project-scoped R + rpy2. | `[reddyproc]` |

#### hesseflux — Python-only backend (default)

```python
from miaproc.eddy import HessefluxConfig, postproc

df_final = postproc(
    df_stage1,
    engine="hesseflux",
    hesseflux_config=HessefluxConfig(
        ustar_mode="dynamic",        # or "fixed" for the legacy path
        partition_method="lasslop",  # default; "reichstein" / "falge" also supported
        swthr=20.0,
        nogppnight=False,
        # reco_fit_mode="native" is the default; see below for opt-in
    ),
)
```

`ustar_mode="fixed"` preserves the legacy behavior (caller supplies
`ustar_fixed`). `ustar_mode="dynamic"` derives u* from the data via
the deterministic plateau estimator (Decision 009).

##### Optional Reco comparability wrapper

`reco_fit_mode="lt_reddyproc_aligned"` is an **opt-in** mode for
studies comparing hesseflux Reco to REddyProc output. It skips
`hf.nee2gpp` entirely and derives Reco from a Lloyd-Taylor fit on
nighttime `NEE_fqc == 0` rows using REddyProc's constants
(`Tref=15 °C`, `T0=-46.02 °C`); GPP is then `Reco - NEE_f`.

```python
df_cmp = postproc(
    df_stage1,
    engine="hesseflux",
    hesseflux_config=HessefluxConfig(
        ustar_mode="dynamic",
        partition_method="lasslop",
        swthr=20.0,
        nogppnight=False,
        reco_fit_mode="lt_reddyproc_aligned",  # opt-in
        lt_min_night_samples=100,
    ),
)
```

**Contract caveats** (recorded in Decision 011):

- Default `reco_fit_mode="native"` is unchanged. Wrapper is opt-in.
- Wrapper returns `NaN` for Reco where `Tair_f` is outside the LT
  temperature domain. Downstream consumers must handle NaN
  explicitly; the wrapper intentionally does not extrapolate.
- Wrapper failure (insufficient nighttime rows, invalid domain,
  optimizer failure, boundary-bound solution) raises
  `LTWrapperError` with **no silent fallback** to native.
- Fitted parameters (`Rref`, `E0`) are dataset/window-specific and
  must be refit per new site or window.

#### reddyproc-rpy2 — REddyProc parity backend

```python
from miaproc.eddy import ReddyProcConfig, postproc

df_r = postproc(
    df_stage1,
    engine="reddyproc-rpy2",
    reddyproc_config=ReddyProcConfig(
        site_name="Marismas_Nacionales",
        latitude=22.25,
        longitude=-105.50,
        timezone_hour=-7,
        local_tz="America/Mazatlan",
        ustar_n_sample=200,
        ustar_probs=(0.05, 0.5, 0.95),
        ustar_scenario="U50",
    ),
)
```

Requires the preflight described in the install matrix above.

#### Comparability vs parity

Per Decision 009 and Decision 011, hesseflux is **REddyProc-inspired,
not parity**. The Milestone 5 comparison produced strong `NEE_f`
agreement (r ≈ 0.97), moderate `GPP` agreement (r ≈ 0.87 with the
wrapper), and non-parity `Reco` magnitude (OLS slope ~2.15 against
REddyProc on the closure track). See
[`docs/m5_reddyproc_hesseflux_magnitude_note.md`](docs/m5_reddyproc_hesseflux_magnitude_note.md)
for the full comparison.

All backends still produce the 13-column contract:

- `NEE_f` (gap-filled NEE)
- `NEE_fqc` (fill quality class)
- `SW_IN_f` / `Rg_f`, `TA_f` / `Tair_f`, `VPD_f` (gap-filled drivers)
- `GPP`
- `Reco` / `RECO`

---

## Scientific Notes

### USTAR Filtering

The `hesseflux` backend exposes an explicit `HessefluxConfig.ustar_mode`
with two supported values:

- `ustar_mode="dynamic"` is the standard long-series mode and the
  closure-track default for the production CLI. The threshold is
  derived from the input data via the deterministic plateau
  estimator of development-history Decision 009 (`hesseflux-plateau-v1`;
  summarized in [`backend_contract.md`](backend_contract.md)). Sparse-data cases raise
  `DynamicUstarEstimationError` — there is no silent fallback to a
  fixed threshold.
- `ustar_mode="fixed"` preserves the legacy behavior (`ustar_fixed`,
  default `0.1 m/s`) for short-window or back-compatible callers.

In both modes, nighttime `NEE` rows below the threshold are masked
before MDS gap-filling. Lasslop-style partitioning is the
closure-track default for the standard long-series workflow
(Decision 011), with `reco_fit_mode="native"` as the default Reco
derivation. The opt-in `reco_fit_mode="lt_reddyproc_aligned"`
wrapper is available for REddyProc-comparability studies; it
propagates `LTWrapperError` on any fit failure with no silent
fallback. Per Decision 009 the hesseflux backend remains
REddyProc-**inspired**, not parity.

---

### Gap Filling

Gap filling uses the MDS method (Reichstein et al. 2005) via `hesseflux`.

Drivers used:

- SW_IN (radiation, W m⁻²)
- TA (air temperature, °C)
- VPD (hPa)

---

### Flux Partitioning

Partitioning uses:

```
method="lasslop"
```

Other supported methods (via hesseflux):

- `"reichstein"`
- `"falge"`

Units are automatically converted internally as required by the partitioning method.

---

## Required Columns for miaproc.eddy

The stage-1 dataframe must contain:

- `DateTime`
- `NEE`
- `USTAR`
- `Tair`
- `VPD`
- `Rg`
- `QC_NEE`

All numeric values must use consistent units:

- Tair in °C (internally converted as needed)
- VPD in hPa
- Radiation in W m⁻²
- USTAR in m s⁻¹

---

## Development

Create a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows
```

Install development tools:

```bash
pip install -e ".[dev,hesseflux]"
```

Run tests:

```bash
pytest -q
```

---

## Project Structure

```
miaproc/
├── eddy/
│   ├── core.py
│   ├── io.py
│   ├── time.py
│   ├── qc.py
│   ├── engine_hesseflux.py
│   ├── engines.py
│   └── constants.py
```

---

## Examples

Runnable entry points for new developers live in
[`examples/`](examples/README.md). One script per run mode; each takes
`--flux-dir` / `--biomet-dir` via CLI and prints a compact diagnostics
summary. **None of the examples write output files.**

| # | Script | Run mode | Backend |
|---|---|---|---|
| 01 | [`examples/01_python_only_hesseflux_dynamic.py`](examples/01_python_only_hesseflux_dynamic.py) | Python-only | `hesseflux` (dynamic u*, native Reco — Decision 011 closure-track default) |
| 02 | [`examples/02_hesseflux_ltwrapper_comparability.py`](examples/02_hesseflux_ltwrapper_comparability.py) | Python-only | `hesseflux` with the opt-in Lloyd-Taylor Reco wrapper for REddyProc-comparability studies |
| 03 | [`examples/03_r_backed_reddyproc_reference.py`](examples/03_r_backed_reddyproc_reference.py) | R-backed | `reddyproc-rpy2`; runs the project-scoped R preflight (Decision 010) first and aborts if not approved |

Framing aligned with Decisions 009 and 011:

- hesseflux is REddyProc-**inspired**, not parity.
- The Lloyd-Taylor wrapper (example 02) is **opt-in**. Default
  `reco_fit_mode="native"` is unchanged.
- Wrapper returns `NaN` for Reco outside the LT temperature domain
  (no extrapolation), and raises `LTWrapperError` on any fit failure
  with **no silent fallback** to native.
- Magnitude limitation on the RBMNN closure track (wrapper-vs-REddyProc
  OLS slope ~2.15) is documented in
  [`docs/m5_reddyproc_hesseflux_magnitude_note.md`](docs/m5_reddyproc_hesseflux_magnitude_note.md)
  and summarized in [`backend_contract.md`](backend_contract.md)
  (development-history Decision 011; the decision log is not shipped).

See [`examples/README.md`](examples/README.md) for per-script usage.

---

## CLI (production, non-interactive)

`miaproc` exposes a job-oriented CLI for routine long-series
processing. Install the package (any of the install matrix rows above
provides the `miaproc` console script via the `[project.scripts]`
entry), then:

```bash
miaproc --help
miaproc run --help
```

### Run-mode matrix

| `--engine` | Backend | Decision-011 framing | R required |
|---|---|---|---|
| `hesseflux-native` | `hesseflux` (`reco_fit_mode="native"`) | Closure-track default. | No |
| `hesseflux-ltwrapper` | `hesseflux` (`reco_fit_mode="lt_reddyproc_aligned"`) | Opt-in REddyProc-comparability mode; `LTWrapperError` propagates with no silent fallback. | No |
| `reddyproc-reference` | `reddyproc-rpy2` | R-backed REddyProc parity backend; refuses to run unless the project-scoped preflight approves the R runtime (Decision 010 / R11). | Yes |

### Required flags

`--flux-dir`, `--biomet-dir`, `--output-table`,
`--output-diagnostics-json`, `--output-run-json` are required for every
run. M24: ``--site-id`` is no longer a CLI flag on any eddy command;
the run processes every site present in the input and an optional
``--group-column site_id`` partitions it into per-category runs that
get stacked into the final ``--output-table``. Pre-filter the input
or call package functions programmatically when single-site
experimentation is needed.

`--repo-root` is required only for `--engine reddyproc-reference`
because the project-scoped preflight gate needs `renv.lock` and the
in-repo R library to be located under it.

### Output artifact contract

Every successful run writes three files:

1. **Processed table** at `--output-table`. Format inferred from the
   extension: `.csv` or `.parquet`. The 13-column common backend
   contract (`backend_contract.md`) is preserved.
2. **Backend diagnostics JSON** at `--output-diagnostics-json`. The
   `df.attrs["miaproc_diagnostics"]` payload, JSON-cleaned (tuples →
   lists; numpy scalars → native).
3. **Run metadata JSON** at `--output-run-json`. Includes the engine,
   resolved key configuration, ISO-8601 start/end timestamps, row
   counts, input/output paths, captured package versions, and (for
   `reddyproc-reference`) the preflight result.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `2` | `reddyproc-reference` preflight not project-scoped approved |
| `3` | Input/config validation failure (missing dirs, unsupported `--output-table` extension, missing `--repo-root` for the reddyproc engine) |
| `4` | Runtime processing failure (engine raised, including `LTWrapperError` on the `hesseflux-ltwrapper` engine — no silent fallback) |
| `5` | `eddy run-silver` / `eddy run-gold` only: the completion manifest was written but the finish could not be confirmed; the run is published and never deleted ([`docs/RUN_PUBLICATION.md`](docs/RUN_PUBLICATION.md)) |

`eddy run-silver` / `eddy run-gold` also use `3` for run-directory
reuse/overlap/traversal and an incomplete or mismatched silver run, and `4`
for any publication fault before the manifest. The removed BigQuery commands
and flags (`eddy run-bigquery*`, `--bq-*`, `--stage-payload-dry-run-dir`) exit
`3` with a pointer to
[`docs/MIGRATING_FROM_BIGQUERY.md`](docs/MIGRATING_FROM_BIGQUERY.md).

### Example

```bash
miaproc run \
    --engine hesseflux-native \
    --flux-dir /data/flux \
    --biomet-dir /data/biomet \
    --group-column site_id \
    --output-table out/processed.parquet \
    --output-diagnostics-json out/diagnostics.json \
    --output-run-json out/run.json
```

Grouping is **not** filtering: every non-null value of `site_id` in
the input is processed, the per-category outputs are written under
`<output-table-stem>__groups/`, and the final `--output-table` is the
deterministic stack of all per-category outputs. Drop
`--group-column` to process the whole input as a single dataset.

### Eddy namespace and removed BigQuery commands (M002-S04)

The module-aware `eddy` namespace (`miaproc <domain> <command> ...`)
holds the stage commands `eddy run-silver`, `eddy run-gold` and
`eddy validate-run`. The former BigQuery commands `eddy run-bigquery`,
`eddy run-bigquery-silver` and `eddy run-bigquery-gold`, every `--bq-*`
flag, `--stage-payload-dry-run-dir`, the `bigquery` install extra and the
`miaproc.eddy.bigquery_runner` / `miaproc.eddy.bigquery_writeback` modules
were removed in M002-S04. A removed command or flag exits `3` and points to
[`docs/MIGRATING_FROM_BIGQUERY.md`](docs/MIGRATING_FROM_BIGQUERY.md), which
maps table reads, time windows, watermarks, stage writeback, run tables and
`MERGE` onto explicit immutable Parquet inputs and fresh, validated run
directories. Data processing needs no cloud SDK, credentials or network.

For the in-memory ingestion API:

```python
from miaproc.eddy import load_stage1_from_dataframes

df_stage1 = load_stage1_from_dataframes(
    flux_df=flux_df,        # pandas DataFrame, case-study column shape
    biomet_df=biomet_df,
    site_id="RBRL",
    drop_rain_rows=False,
)
```

### Eddy silver/gold split (M14)

The same image exposes the two-stage contract for carbon-flux
processing. Inputs and outputs are ordinary local or mounted file paths;
the same commands run on a laptop, in Docker and in a batch job.

| Stage | Command | What it does | What it writes |
|---|---|---|---|
| **silver** | `miaproc eddy run-silver` | Stage-1 only: load + clean + regularize the joined flux + biomet slice from CSV directories (`--flux-dir`/`--biomet-dir`) or explicit single Parquet files (`--flux-parquet`/`--biomet-parquet`). **No engine, no R preflight.** | Silver table (`.csv` or `.parquet`) + run-metadata JSON. |
| **gold** | `miaproc eddy run-gold` | Stage-2 only: read a silver-stage table, dispatch the selected engine through `postproc(...)`, attach silver columns. Default engine is `reddyproc-reference` (Decision 010 / R11). | Gold table (`.csv` or `.parquet`) + diagnostics JSON + run-metadata JSON. |

Column-preservation contract:

- Silver output preserves the joined input columns coming from the
  flux + biomet slice (the existing accepted stage-1 contract from
  [`src/miaproc/eddy/core.py`](src/miaproc/eddy/core.py)) and
  appends the new stage-1 outputs (regularized 30-minute grid, QC- and
  rain- and 3-sigma-filtered fluxes).
- Gold output preserves all silver columns (LEFT-joined on `DateTime`)
  and appends the new analytical outputs from `postproc(...)`. The
  hesseflux backend already preserves input columns natively, so the
  silver-attach step is a no-op idempotent guard for it; for the
  reddyproc-rpy2 backend the helper provides the column-preservation
  guarantee on top of the strict 13-column backend contract.

Local CSV-first smoke against the realistic case-study source data
in the optional development-workspace case-study data under `01_data/case_study/` (same
EddyPro-shaped layout as the production source data; multi-site, so
`--group-column site_id` partitions every site present per
Decision 008):

```bash
# M002-S03: every invocation writes into its own FRESH run directory
# (here the parent of --output-table; it must not exist yet).

# Silver: stage-1 only (no R, no preflight)
miaproc eddy run-silver \
    --flux-dir 01_data/case_study/flux \
    --biomet-dir 01_data/case_study/biomet \
    --group-column site_id \
    --output-table .runs/m15_handoff/silver-0001/silver.parquet \
    --output-run-json .runs/m15_handoff/silver-0001/silver_run.json

# Gold: stage-2 from the published silver above (Python-only via hesseflux-native)
miaproc eddy run-gold \
    --silver-table .runs/m15_handoff/silver-0001/silver.parquet \
    --engine hesseflux-native \
    --output-table .runs/m15_handoff/gold-0001/gold.parquet \
    --output-diagnostics-json .runs/m15_handoff/gold-0001/gold_diag.json \
    --output-run-json .runs/m15_handoff/gold-0001/gold_run.json

# Consumers/readback: validate the completion manifest first
miaproc eddy validate-run --run-dir .runs/m15_handoff/gold-0001 --stage gold
```

For the R-backed default (Docker-runtime story; production priority
engine), drop `--engine` (defaults to `reddyproc-reference`) and pass
`--repo-root /app` so the project-scoped preflight (Decision 010) can
resolve the in-image `renv.lock` + R library:

```bash
miaproc eddy run-gold \
    --silver-table /out/silver-0001/silver.parquet \
    --repo-root /app \
    --output-table /out/gold-0001/gold.parquet \
    --output-diagnostics-json /out/gold-0001/gold_diag.json \
    --output-run-json /out/gold-0001/gold_run.json
```

Single-site experiments are no longer a CLI flag (M24): pre-filter
the flux/biomet CSVs to one site before invoking, or use the
package functions
([`miaproc.eddy.load_stage1`](src/miaproc/eddy/core.py)) with a
single-site DataFrame programmatically. Outputs accept `.csv` or
`.parquet` by extension on both stages. M002-S02: a `.parquet`/`.pq`
output is the source-facing contract (exactly one tz-aware
`timestamp`, no `DateTime`, source `primary_key`/`site_id` carried
unchanged, gold `nee_f`/`nee_fqc`/`sw_in_f`/`ta_f`/`vpd_f` +
`dateAndTime`); `.csv` keeps the documented legacy compatibility shape
(D014; gold CSV still carries `DateTime`), and CSV roundtrips
re-normalize time columns to tz-aware UTC on read. New workflows should
use Parquet silver/gold. Grouped `run-silver`/`run-gold` refuse group
labels whose per-group file names collide (exit 4); `miaproc run` keeps
its earlier overwrite behavior. Parquet gold can contain names that differ
only by case (hesseflux `TA_f` and `ta_f`), which case-insensitive
consumers cannot tell apart. Parquet inputs,
validation failures (URI schemes, corrupt/empty files, invalid
schemas, duplicate fields) and the type policy are documented in
[`docs/PARQUET_FILE_SCHEMA.md`](docs/PARQUET_FILE_SCHEMA.md).

**Run publication (M002-S03).** Each `run-silver`/`run-gold` invocation
owns one fresh run directory (`--run-dir`, default: the parent of
`--output-table`); an existing directory is refused (exit 3), as are
outputs outside it, overlap with inputs and colliding artifact names.
Artifacts are staged and validated in local scratch (`--scratch-dir`),
whose staged file bytes, including the manifest, are limited while they
are written by `--scratch-max-bytes` (not a cap on Python/R memory),
copied sequentially to fresh files with checked close, and
`_MIAPROC_RUN_COMPLETE.json` (relative names, sizes, SHA-256, row counts,
schema identity) is written last: only a run with that valid manifest is
published. `run-gold` and `eddy validate-run` reject incomplete or
mismatched runs; an unpublished legacy CSV silver needs
`--allow-legacy-silver-csv`. Exit 5 means publication could not be
confirmed (a manifest write, flush or close fault after the manifest was
created, or a failed readback): the run may be published, is never deleted, must be checked with
`eddy validate-run` before use, and a retry needs a new run name. Details,
migration examples, FUSE caveats and resource budgeting:
[`docs/RUN_PUBLICATION.md`](docs/RUN_PUBLICATION.md).

```bash
miaproc eddy run-silver \
    --flux-parquet /data/in/flux.parquet \
    --biomet-parquet /data/in/biomet.parquet \
    --output-table /data/out/silver-0001/silver.parquet \
    --output-run-json /data/out/silver-0001/silver_run.json
```

#### Colleague handoff (M15)

Orchestration wraps the same two commands without changing module logic:
an engineer-owned job selects the input window, writes immutable Parquet
inputs, runs silver and gold into fresh run directories on a mounted
volume, and lets consumers load only runs that pass `eddy validate-run`.
That wrapping is **not** owned by this repo. The engineer handoff
(responsibility split, Docker build, mount and resource checklist) lives
at [`06_infra/eddy_handoff.md`](06_infra/eddy_handoff.md); the migration
from the former BigQuery orchestration is
[`docs/MIGRATING_FROM_BIGQUERY.md`](docs/MIGRATING_FROM_BIGQUERY.md).

### Column preservation and uniqueness (M28-M31)

The silver/gold path preserves source columns forward:

- every unique flux input column is carried into silver;
- silver columns are carried into gold;
- newly derived silver/gold analytical columns are appended without
  dropping prior-stage columns.

Duplicate column names never fuse silently. The M28 humidity policy
resolves a literal duplicate `rH` deterministically:

- an equivalent duplicate humidity column is suppressed with an audit
  action;
- a divergent normalized humidity duplicate is renamed to `rH_norm_s`
  (or a deterministic suffixed variant if that name already exists);
- any other duplicate name raises `DuplicateStageColumnsError` (exit 4).

Resolved actions are written to the run JSON as
`column_collision_actions`. Parquet names are case-sensitive, so gold may
carry names that differ only by case (see
[`docs/PARQUET_FILE_SCHEMA.md`](docs/PARQUET_FILE_SCHEMA.md)).

### Source-truth column contract (M32A) and full-flux passthrough (M34)

Under the accepted M32A contract the Parquet silver and gold outputs
carry **source-truth final names** and a single `timestamp` column. Internal backend processing still uses canonical R-style
names (`DateTime`, `NEE`, `Tair`, `USTAR`, `VPD`, `Rg`, `P_RAIN`,
`rH`), but those internal passthroughs are dropped at the silver
output boundary and at the gold redundant-passthrough boundary
whenever the source-truth counterpart is present.

Accepted internal → final mapping (M32A):

```text
DateTime       -> timestamp
NEE            -> co2_flux
QC_NEE         -> qc_co2_flux
Tair           -> air_temperature_c
USTAR          -> u_star
VPD            -> VPD_hpa
Rg             -> SWIN_1_1_1
P_RAIN         -> P_RAIN_1_1_1
rH             -> RH_1_1_1
```

Under the M34 widened contract, **every unique column** from the
carbon-flux bronze source survives bronze → silver → gold
under its source name unless miaproc changes its physical units.
Today the only unit-aware rebindings are `air_temperature ->
air_temperature_c` and `VPD -> VPD_hpa`; every other source column
(`h2o_flux`, `qc_h2o_flux`, `H_strg`, `LE_strg`, `co2_strg`,
`h2o_strg`, `co2_molar_density`, `co2_mole_fraction`,
`co2_mixing_ratio`, `h2o_molar_density`, `h2o_mole_fraction`,
`h2o_mixing_ratio`, `sonic_temperature`, `air_pressure`,
`air_density`, `air_heat_capacity`, `air_molar_volume`, `ET`,
`water_vapor_density`, `e`, `es`, `specific_humidity`, `RH`,
`Tdew`, `wind_speed`, `max_wind_speed`, `wind_dir`, `TKE`, `L`,
`z_minus_d_div_L`, `bowen_ratio`, `x_peak`, `x_offset`, `x_10_pct`,
`x_30_pct`, `x_50_pct`, `x_70_pct`, `x_90_pct`, `v_var`, …)
survives under its bronze name. From the biomet bronze, only the
three processing-used variables carry forward: `SWIN_1_1_1`,
`P_RAIN_1_1_1`, `RH_1_1_1`.

Flux-side `RH` and biomet-side `RH_1_1_1` are case-insensitively
distinct and must both survive when both are present. The
authoritative column mapping is
`06_infra/schemas/eddy_bronze_to_stage_column_lineage_contract.csv`
(deployment-facing; the package does not read it at runtime).

To inspect a payload before it is consumed, run silver or gold into a
throwaway run directory and read the published Parquet table, its run JSON
(output columns, `column_collision_actions`) and the completion manifest;
the former `--stage-payload-dry-run-dir` was removed with the BigQuery
commands.

---

## Docker runtime profile

A bundled Python 3.12 + R 4.5.1 + REddyProc 1.3.4 image is defined by
[`docker/Dockerfile.miaproc-r45-reddyproc`](docker/Dockerfile.miaproc-r45-reddyproc);
the build context is the package root. It is intended for stateless
one-shot CLI jobs.

```bash
# Build (from the package root)
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
  -t miaproc-eddy-parquet:product-local .

# Help smoke check
docker run --rm miaproc-eddy-parquet:product-local miaproc --help
```

[`docker/README.md`](docker/README.md) documents the pins, the R
preflight smoke check, the local CSV user path and the Parquet mount user
path: a deterministic synthetic two-site input on a read-only input mount,
separate writable output and scratch mounts, `eddy run-silver`, `eddy
run-gold` with `hesseflux-native` and `reddyproc-reference`
(`--group-column site_id`), `eddy validate-run` and a separate reader
process, and the rejection of an incomplete run. Mount configuration,
identity/IAM ownership, FUSE limitations and memory/scratch budgeting for
operators are in [`09_ops/runbooks.md`](09_ops/runbooks.md). The
development project that maintains this package executes these paths in
its recorded product image with its own integration verifier; that
verifier is not part of the package.

The container honors the same Decision 010 posture: `reddyproc-reference`
runs only when the in-image `/app/renv.lock` + project-scoped
`/app/renv/library/R-4.5/...` library (baked at image build time, M6
Task 3) produces a project-scoped preflight approval; otherwise the
job exits `2`. Operators may shadow the baked layout with a host
mount when they intentionally want host repo state to win, but the
default image is preflight-approved as-is. See
[`docker/README.md`](docker/README.md) for the local Docker invocation
templates and mount guidance.

---

## Current release status

- Eddy covariance processing supports the silver -> gold split on explicit
  Parquet inputs (and legacy CSV directories) with source-facing Parquet
  outputs, fresh run directories and completion manifests.
- The BigQuery transport, commands, extra and Google SDK dependencies were
  removed (M002-S04); see
  [`docs/MIGRATING_FROM_BIGQUERY.md`](docs/MIGRATING_FROM_BIGQUERY.md).
- Docker packaging is validated locally for the package runtime. Cloud
  deployment, mounted-storage driver qualification and scheduling are
  engineer-owned follow-ups; none is performed or claimed here.

---

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

---

## License

MIT License
