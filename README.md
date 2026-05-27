# miaproc

**miaproc** is a Python library for preprocessing and post-processing multiple environmental data streams coming from sensors deployed in Mexican mangroves.

This package is being developed as part of the ManglarIA initiative — a project supported by WWF Mexico and Google, focused on monitoring, modeling, and protecting mangrove ecosystems in a rapidly changing climate.

More about the project:  
https://www.worldwildlife.org/descubre-wwf/historias/manglaria-utilizando-inteligencia-artificial-para-salvar-los-manglares-en-un-clima-en-constante-cambio

### Current modules

- Tree level biomass estimation
- Eddy covariance data preprocessing


---

## 🌳 Biomass Module (`miaproc.biomass`)

Tree-level estimation using species-specific allometric equations,
distributed with the package as a Parquet equation table and evaluated
through a small safe-eval layer.

The packaged equation source (M16 default) is the unified parquet
[`src/miaproc/biomass/data/equation_application_unified.zstd.parquet`](src/miaproc/biomass/data/equation_application_unified.zstd.parquet),
schema companion at
[`docs/equation_application_unified.zstd.json`](docs/equation_application_unified.zstd.json).
Two complementary equation families live in that single file:

| `source_dataset` | Family | Predictors | Output | Where evaluated |
|---|---|---|---|---|
| `infys` | Mexican volume workbook (national forest inventory) | `diam` (DBH cm), `alt` (height m), `wd` (wood density) | volume (e.g. `VRTAcc_m3`) | `equation_numpy` |
| `dina` | Mangrove direct-biomass equations (M16 — four mangrove species) | `diam` only (`wd` already substituted to species literals) | biomass (kg, `response_variable="B"`) | `equation_numpy_wd_fixed` |

---

### Required input columns

Defaults follow the field-contract reference at
[`docs/forest_data_schema.csv`](docs/forest_data_schema.csv):

- `species` (e.g. `"Avicennia germinans"`)
- `dbh_cm`
- `tree_height_m` (optional for direct-biomass `dina` rows; required for
  volume `infys` rows, since their expressions reference `alt`)
- `life_stage` (`"Adult"` is required to apply direct-biomass `dina`
  equations; juveniles and missing life-stage are not eligible)

Override the field names if your table uses different ones:

```python
from miaproc.biomass import BiomassColumns

cols = BiomassColumns(
    species="Species",
    dbh_cm="DBH (cm)",
    height_m="Total Height (m)",
    life_stage="LifeStage",
)
```

The mapping onto the parquet predictor names
(`species` → `scientific_name_apg_raw`, `dbh_cm` → `diam`,
optional `tree_height_m` → `alt`) is fixed by the parquet contract
and is not part of `BiomassColumns`.

---

### Basic usage — direct biomass (`dina`, M16)

```python
import pandas as pd
from miaproc.biomass import load_packaged_equations, estimate_trees

equations = load_packaged_equations()

df = pd.DataFrame([
    {"species": "Avicennia germinans",  "dbh_cm": 10.0, "tree_height_m": None, "life_stage": "Adult"},
    {"species": "Rhizophora mangle",    "dbh_cm": 12.5, "tree_height_m": None, "life_stage": "Adult"},
    {"species": "Laguncularia racemosa","dbh_cm":  9.0, "tree_height_m": None, "life_stage": "Adult"},
    {"species": "Conocarpus erectus",   "dbh_cm": 11.0, "tree_height_m": None, "life_stage": "Adult"},
])

out = estimate_trees(df, equations=equations, dataset="dina")
```

For each row, the output appends:

- `estimate_response_variable` (kg for direct biomass)
- `match_status`, `range_status`
- `response_variable`, `response_units` (`"B"` / `"kg"` for `dina`)
- `source_record_id`, `source_dataset` — load-bearing traceability fields
  (the M17 enrichment pass uses `source_record_id` as the equation-used
  identifier on the output table)
- `equation_code`, `assignment_level_used`, `state_used`,
  `equation_numpy_used` — canonical names
- `clave_ecuacion`, `nivel_asignacion`, `estado_ecuacion_usada`,
  `ecuacion_numpy` — legacy aliases preserved for back-compat

### Volume usage (`infys`)

```python
out = estimate_trees(
    df,
    equations=equations,
    state="Yucatán",
    dataset="infys",
    response_variable="VRTAcc_m3",
)
```

Volume rows still require non-null `tree_height_m` (the equations
reference `alt`); a missing height surfaces as
`match_status == "height_missing"` rather than a silent NaN.

### Custom equation override

```python
out = estimate_trees(
    df,
    equations=equations,
    custom_function="np.exp(-10 + 1.9*np.log(diam) + 1.0*np.log(alt))",
)
```

Mapping form (per-species):

```python
out = estimate_trees(
    df,
    equations=equations,
    custom_function={
        "Avicennia germinans": "np.exp(-10.1 + 1.97*np.log(diam) + 1.05*np.log(alt))",
        "Rhizophora mangle":   "0.00006*np.power(diam,2.05)*np.power(alt,0.80)",
    },
)
```

Custom functions bypass parquet matching and the life-stage gate. They
are an explicit escape hatch.

### Eligibility / fallback rules

- `dbh_cm` is always required. Missing → `match_status="dbh_missing"`,
  estimate NaN.
- For `dataset="dina"` (direct biomass), `life_stage` must normalize to
  `"adult"`. Missing or juvenile → `match_status="life_stage_not_adult"`,
  estimate NaN, with `source_record_id` of the rejected equation
  surfaced for audit.
- For `dataset="infys"` (volume), `tree_height_m` must be present.
- Species matching falls back to any-state when the input `state`
  doesn't match (this is also the natural matching path for `dina`
  rows, which carry no state).
- The `state` kwarg accepts `estado` as a deprecated alias.

### Species alias normalization (M17A)

After whitespace + case normalization, the matcher applies a small
**deterministic alias map** for known mangrove-species typos
(`miaproc.biomass.equations._SPECIES_ALIASES_NORMALIZED`):

```python
{
    "rizophora mangle":  "rhizophora mangle",
    "rizophora manlge":  "rhizophora mangle",
}
```

This is **not** fuzzy matching — only the two known bad spellings
resolve. Unknown species and null species still produce
`match_status="no_equation_found"`. Rows without `dbh_cm` still
classify as `dbh_missing` even when the species alias resolves.
Adding more aliases requires an explicit code edit and a recorded
review pass.

### Range policy

Equation-specific DBH / height bounds (`dbh_min_cm`, `dbh_max_cm`,
`height_min_m`, `height_max_m`) are honored when present. Policies:
`"warn"` (default), `"clip"`, `"error"`, `"ignore"`.

### Biomass table-enrichment CLI (M17)

`miaproc biomass enrich-table` is the canonical row-preserving CLI
wrapper around the library helper above. It reads a tree table from
CSV / parquet and writes the **same table back** with **exactly two
appended columns**:

- `biomass_estimate` — numeric, in the equation's response units
  (kg for `dina` direct-biomass; volume for `infys`);
- `equation_used` — the matched equation's `source_record_id`, or
  null for ineligible / unmatched rows.

Original rows + columns are preserved verbatim — no row dropping, no
reshaping. Default `--dataset dina`.

```bash
mkdir -p .runs/m17_biomass

miaproc biomass enrich-table \
    --input-table 01_data/case_study/biomass/forest_structure_biomass_test.csv \
    --output-table .runs/m17_biomass/enriched.csv \
    --output-run-json .runs/m17_biomass/run.json
```

The run-metadata JSON records the input/output paths, row counts,
estimated-vs-skipped breakdown, `match_status_counts` (skip-reason
audit: `dbh_missing`, `life_stage_not_adult`, `no_equation_found`,
`height_missing`), the dataset filter, and the equations source
(`packaged_default` or an explicit `--equations-path`).

For library-side calls (e.g. future cloud orchestration wrappers),
`miaproc.biomass.enrich_table(df, equations=..., dataset="dina")`
returns the same projection without going through argparse.

Override the column names if your table uses different ones:

```bash
miaproc biomass enrich-table \
    --input-table input.csv \
    --output-table out.csv \
    --output-run-json run.json \
    --species-col Species \
    --dbh-col "DBH (cm)" \
    --height-col "Total Height (m)" \
    --life-stage-col LifeStage \
    --biomass-estimate-col biomass_kg \
    --equation-used-col eq_id
```

Exit codes follow the project-wide CLI contract (`0` success,
`3` validation failure, `4` runtime processing failure). Cloud
orchestration of this command is **colleague-owned**; M17 keeps the
contract local-file-first and Docker-friendly. See
[`docker/README.md`](docker/README.md) for the container
mimic recipe.

#### Colleague handoff (M18)

The canonical biomass cloud-engineer handoff — responsibility
split, Docker build + Cloud Run command shapes, Google Cloud /
service-account binding placeholders, and an explicit
non-authoritative framing for infra colleagues — lives at
[`06_infra/biomass_handoff.md`](06_infra/biomass_handoff.md)
(parallel to the eddy-side handoff at
[`06_infra/eddy_handoff.md`](06_infra/eddy_handoff.md)).

#### BigQuery-native biomass enrichment (M19)

`miaproc biomass run-bigquery` reads an individual-tree
forest-structure source table directly from BigQuery and writes
the enriched local artifacts (table + run JSON) without an
external CSV / parquet export step. **Local artifacts are always
written**; BigQuery writeback is opt-in via the M20 flag set
documented in the next subsection (`--bq-stage-table` engages
stage-write + validation, `--bq-allow-final-merge` is required
for any final-table mutation).

Same row-preservation + exactly-two-appended-columns contract as
`enrich-table`. Default dataset `dina`. Required flags
`--bq-input-project` / `--bq-input-dataset` / `--bq-input-table` /
`--output-table` / `--output-run-json`; optional
`--bq-billing-project` / `--bq-row-limit` / `--bq-no-storage-api`,
plus the same enrichment flags as `enrich-table` (dataset,
equations-path, state, response-variable, BiomassColumns
overrides, output-column overrides).

```bash
mkdir -p .runs/m19_biomass

miaproc biomass run-bigquery \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-input-table forest_structure_biomass \
    --bq-row-limit 5000 \
    --output-table .runs/m19_biomass/enriched.parquet \
    --output-run-json .runs/m19_biomass/run.json
```

The run-metadata JSON records the BigQuery provenance under
`inputs` (project, dataset, table, billing_project, row_limit,
the rendered SELECT) so a downstream auditor can reproduce the
exact read state from the artifact alone. Library callers (e.g.
future cloud orchestrators) can use
`miaproc.biomass.read_bigquery_input(BigQueryBiomassConfig(...))`
directly without going through argparse.

Biomass needs no R, so this command never invokes the
project-scoped preflight (Decision 010 / R11 stays gated only
for `reddyproc-reference` eddy gold).

#### BigQuery writeback + merge control (M20)

`miaproc biomass run-bigquery` can also stage the enriched table
back into BigQuery, validate it, and (with explicit operator
opt-in) MERGE it into a final target table — all from the same
single CLI invocation. **Stage-only is the safe default**: final
table mutation never happens without `--bq-allow-final-merge`.

| Flag | Required when | Purpose |
| --- | --- | --- |
| `--bq-stage-table` | engaging writeback | engages stage-write + validation |
| `--bq-output-project` | writeback engaged | output project; must differ from `--bq-input-project` |
| `--bq-output-dataset` | writeback engaged | dataset for stage + final tables |
| `--bq-control-dataset` | writeback engaged | dataset for `cf_biomass_runs` (no watermark — see below) |
| `--bq-final-table` | merge requested | MERGE target table |
| `--bq-allow-final-merge` | merge requested | explicit operator opt-in |
| `--bq-merge-key` | optional | merge identity column (default `primary_key`) |
| `--bq-run-id` | optional | override the auto-generated run id |

**Watermark omitted by design.** The eddy parallel uses a
per-site `last_processed_timestamp` watermark because eddy is
time-series append-only. Biomass is per-tree identity-keyed
enrichment — every tree has a stable `primary_key`; re-running
biomass simply re-MERGEs on that key, so there is no "next batch
by time" to checkpoint. The biomass M20 design therefore keeps a
**runs control table only** (`cf_biomass_runs`), no watermark
table. See the M20 run-summary block for the full rationale.

```bash
# Stage + validate only (safe default; no final-table mutation).
miaproc biomass run-bigquery \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-input-table forest_structure_biomass \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_biomass_stage_smoke \
    --bq-control-dataset _orch \
    --output-table .runs/m20_biomass/enriched.parquet \
    --output-run-json .runs/m20_biomass/run.json

# Stage + validate + MERGE into a final table (explicit opt-in).
miaproc biomass run-bigquery \
    ...same as above... \
    --bq-final-table forest_structure_with_biomass \
    --bq-allow-final-merge
```

The CLI run-metadata JSON records, under a top-level `writeback`
key, the writeback status, stage row count, validation metrics,
merge attempt / authorization / inserted / updated counts, and
the FQNs of all three touched tables (stage, final, runs) so a
later auditor can reproduce the exact write state from the local
artifact alone.

Validation failure (zero rows, NULL `primary_key`, duplicate
`primary_key`) aborts the merge, propagates a non-zero CLI exit
code, and records `status="validation_failed"` in
`cf_biomass_runs` with the failing metrics in the error text.

The merge key is configurable via `--bq-merge-key` for
deployments whose forest-structure tables key on a different
stable column. Default: `primary_key`.
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
Decision 011 — see
[`05_governance/decision_log.md`](05_governance/decision_log.md) for the
accepted-limitations record, and
[`docs/m5_reddyproc_hesseflux_magnitude_note.md`](docs/m5_reddyproc_hesseflux_magnitude_note.md)
for the magnitude-comparability note.

---

## Installation

The package supports two run modes. Choose the one that matches your
environment:

| Mode | Use when | Install |
|---|---|---|
| **Python-only** | No R available; standard EC post-processing with hesseflux. | `pip install -e "08_pkg[dev,hesseflux]"` |
| **R-backed** | REddyProc parity backend is needed (comparability studies, reference output). Requires R 4.5+ and `rpy2` 3.6+ bound via a project-scoped `renv`. | `pip install -e "08_pkg[dev,hesseflux,reddyproc]"` plus R/REddyProc setup per `docs/REDDYPROC_LOCAL_TIME_POLICY.md` |

### Python-only mode

```bash
git clone https://github.com/jequihua/miaproc.git
cd miaproc
python -m venv .venv
# Linux/Mac:
source .venv/bin/activate
# Windows:
# .venv\Scripts\activate
pip install -e "08_pkg[dev,hesseflux]"
```

This is the supported default for routine development and CI.

### R-backed mode

Additional prerequisites: R 4.5+ on the host, `renv` initialized under
the repo root, and `REddyProc` installed into the project-scoped library
(see `03_experiments/m5_r_environment_setup.md` for the RBMNN
reproduction). Then:

```bash
pip install -e "08_pkg[dev,hesseflux,reddyproc]"
# R + REddyProc must be set up separately via renv.
```

Run the preflight before any live R call:

```bash
R_HOME="C:\Program Files\R\R-4.5.3" \
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
  estimator described in
  [`05_governance/decision_log.md`](05_governance/decision_log.md)
  Decision 009 (`hesseflux-plateau-v1`). Sparse-data cases raise
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
  and in the closure record
  [`05_governance/decision_log.md`](05_governance/decision_log.md)
  (Decision 011).

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

### BigQuery-native eddy run (M7)

The CLI also exposes a module-aware `eddy` namespace for cloud-native
workloads. `miaproc eddy run-bigquery` reads the flux and biomet inputs
**directly from BigQuery** (no CSV export round-trip), runs the
selected eddy engine in memory, and writes the same three artifacts
(processed table, diagnostics JSON, run metadata JSON) to local disk.

The first live test target is the production carbon-flux source
tables, processed for every site present in the read window with
`--group-column site_id` (M24); the `reddyproc-reference` engine
runs inside the bundled Python + R 4.5 + REddyProc image. This
first pass is **local-disk only**; no BigQuery write-back is
performed. See
[`docs/guides/002_carbon_flux_bq_orchestration_guide.md`](docs/guides/002_carbon_flux_bq_orchestration_guide.md)
for the full architectural rationale and the planned later
evolution to Cloud Run Jobs with staging-table writes + MERGE.

Install the BigQuery extras and use the `eddy run-bigquery`
subcommand:

```bash
pip install -e "08_pkg[dev,hesseflux,reddyproc,bigquery]"

miaproc eddy run-bigquery \
    --engine reddyproc-reference \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-flux-table carbon_flux_eddycovariance \
    --bq-biomet-table carbon_flux_biomet \
    --group-column site_id \
    --repo-root . \
    --output-table out/processed.parquet \
    --output-diagnostics-json out/diagnostics.json \
    --output-run-json out/run.json
```

**M24:** the BigQuery read no longer injects a
`WHERE site_id = @site_id` filter from the CLI; the read pulls every
site in the requested window and `--group-column site_id` partitions
it in Python. BigQuery writeback (when engaged) writes the stacked
all-site output **once**, so shared stage tables such as
`cf_s2_silver_stage` / `cf_s2_gold_stage` are valid. After a
successful explicit MERGE the watermark advances **per site** in
`cf_s2_watermark` (one row per stacked site).

The `eddy` namespace is the long-term shape:
`miaproc <domain> <command> ...`. Future modules
(e.g. `biomass`) will sit alongside `eddy` so the top-level CLI
stays free of single-domain assumptions.

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

Decision 010 / risk R11 are unaffected: when
`--engine reddyproc-reference` is selected the BigQuery path runs the
same project-scoped preflight before any BigQuery read, and exits `2`
on anything less than project-scoped approval.

### BigQuery writeback + merge control (M8)

`miaproc eddy run-bigquery` can also stage processed output back into
BigQuery, validate it, and (with explicit operator opt-in) MERGE it
into the staging final table. **All writes are confined to the
operator's `--bq-output-project`** — the production input project
remains read-only. The full sequence is:

1. ensure orchestration control tables exist (idempotent
   `CREATE TABLE IF NOT EXISTS` for `cf_s2_runs` and
   `cf_s2_watermark` under `--bq-control-dataset`);
2. `WRITE_TRUNCATE` the processed DataFrame into the stage table
   (`<output-project>.<output-dataset>.<bq-stage-table>`);
3. run validation SQL (row count, REQUIRED-column non-null,
   `(site_id, timestamp)` and `primary_key` uniqueness) against
   the stage table;
4. **only when `--bq-allow-final-merge` is passed**, MERGE the stage
   rows into `--bq-final-table` keyed on `(site_id, timestamp)`
   (no deletes; non-key columns including `primary_key` are
   updated from the stage row);
5. when the final-table MERGE succeeds, advance the per-site
   watermark in `cf_s2_watermark`;
6. record one row in `cf_s2_runs` with the run id, status,
   row counts, merge attempt/authorization, and watermark outcome.

**Stage-only is the safe default for local operator use.** The CLI
never mutates the final target table without `--bq-allow-final-merge`,
and the watermark never advances on a stage-only or failed run.
Validation failure aborts the merge and is recorded as
`status = "validation_failed"` in `cf_s2_runs`.

```bash
# Stage + validate only (default, safe).
miaproc eddy run-bigquery \
    --engine reddyproc-reference \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-flux-table carbon_flux_eddycovariance \
    --bq-biomet-table carbon_flux_biomet \
    --group-column site_id \
    --repo-root /app \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_s2_stage \
    --bq-control-dataset _orch \
    --output-table /out/processed.parquet \
    --output-diagnostics-json /out/diagnostics.json \
    --output-run-json /out/run.json

# Stage + validate + MERGE into the final staging table.
# Requires the explicit --bq-allow-final-merge opt-in.
miaproc eddy run-bigquery \
    ...same as above... \
    --bq-final-table carbon_flux_eddycovariance_s2_filt_1 \
    --bq-allow-final-merge
```

The shared `--bq-stage-table` (e.g. `cf_s2_stage`, no per-site
suffix) is now valid because the M24 grouped CLI invokes writeback
exactly once with the stacked all-category output. The final MERGE
keys on `(site_id, timestamp)` and advances watermarks per-site.

The CLI run-metadata JSON records, under a top-level `writeback`
key, the writeback status, stage row count, validation metrics,
merge attempt/authorization/inserted/updated counts, watermark
outcome, and the FQNs of all four touched tables (stage, final,
runs, watermark) so a later auditor can reproduce the exact write
state from the local artifact alone.

**Live status (M10, 2026-04-27).** Both the stage-only path and
the explicit MERGE path are verified live against
`manglaria-staging` for all RBRL rows. The M10 schema-mapping
helper (`miaproc.eddy.prepare_stage_dataframe`) reconciles the
M6 backend output (13-column scientific contract) with the live
`_s2_filt_1` extended source-flux schema (guide 001 §2.1: lowercase
`dateAndTime`/`nee_f`/`nee_fqc`/`sw_in_f`/`ta_f`/`vpd_f` plus the
EddyPro source-flux pass-through). Source `primary_key` is
preserved from the BigQuery flux read; regularized inserts fall
back to the M9 deterministic
`"<site_id>|<iso_utc_timestamp>"` synthesizer. The live MERGE
inserted 4 813 rows into `_s2_filt_1` for `RBRL`, advanced the
per-site watermark to `2026-02-01T00:00:00 UTC`, and recorded
`status = "succeeded"` in `cf_s2_runs`. Production `manglaria`
remained read-only.

What is **not** in scope for M8/M9 (deferred to follow-up passes):

- scheduled / cron-driven invocation (Cloud Scheduler trigger
  on the merge job is the smallest honest next step once the
  least-privilege IAM rollout below is settled);
- recomputation-window automation (the run uses whatever the
  operator's `--bq-start-timestamp` / `--bq-end-timestamp` window
  selects, with no rolling-window logic);
- least-privilege IAM rollout to the dedicated
  `cf-s2-eddy-runner@manglaria-staging` service account
  (Cloud Run Job manifests are now configured to run as the
  dedicated SA — M12 — but execution is **blocked** pending a
  Project-IAM-Admin handoff for `bigquery.jobUser` on
  `manglaria-staging`, `bigquery.dataEditor` on
  `manglaria-staging:manglaria_lakehouse_ds`, table-level
  `bigquery.dataViewer` on the two named `manglaria.*` source
  tables, and `artifactregistry.reader` on the AR repo;
  the canonical IAM-binding placeholders + recommended scopes
  for the eddy and biomass service accounts now live in the
  cloud-engineer handoffs at
  [`06_infra/eddy_handoff.md`](06_infra/eddy_handoff.md) and
  [`06_infra/biomass_handoff.md`](06_infra/biomass_handoff.md));
- Terraform / IaC codification (deferred decision; the YAML
  manifests under [`06_infra/cloudrun/`](06_infra/cloudrun/)
  convert cleanly to a `google_cloud_run_v2_job` resource later).

Cloud Run Job deployment (M11) is documented in
[`06_infra/deployment.md`](06_infra/deployment.md); the
operator runbook is [`09_ops/runbooks.md`](09_ops/runbooks.md).

### Eddy silver/gold split (M14)

The same image also exposes a clean two-stage local-first contract for
carbon-flux processing, intended for human teammates who want to mimic
cloud behavior with CSV files before any BigQuery / Cloud Run wiring
exists.

| Stage | Command | What it does | What it writes |
|---|---|---|---|
| **silver** | `miaproc eddy run-silver` | Stage-1 only: load + clean + regularize the joined flux + biomet slice. **No engine, no R preflight.** | Silver table (`.csv` or `.parquet`) + run-metadata JSON. |
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
EddyPro-shaped layout as the BigQuery source tables; multi-site, so
`--group-column site_id` partitions every site present per
Decision 008):

```bash
mkdir -p .runs/m15_handoff

# Silver: stage-1 only (no R, no preflight)
miaproc eddy run-silver \
    --flux-dir 01_data/case_study/flux \
    --biomet-dir 01_data/case_study/biomet \
    --group-column site_id \
    --output-table .runs/m15_handoff/silver.parquet \
    --output-run-json .runs/m15_handoff/silver_run.json

# Gold: stage-2 from the silver above (Python-only via hesseflux-native)
miaproc eddy run-gold \
    --silver-table .runs/m15_handoff/silver.parquet \
    --engine hesseflux-native \
    --output-table .runs/m15_handoff/gold.parquet \
    --output-diagnostics-json .runs/m15_handoff/gold_diag.json \
    --output-run-json .runs/m15_handoff/gold_run.json
```

For the R-backed default (Docker-runtime story; production priority
engine), drop `--engine` (defaults to `reddyproc-reference`) and pass
`--repo-root /app` so the project-scoped preflight (Decision 010) can
resolve the in-image `renv.lock` + R library:

```bash
miaproc eddy run-gold \
    --silver-table /out/silver.parquet \
    --repo-root /app \
    --output-table /out/gold.parquet \
    --output-diagnostics-json /out/gold_diag.json \
    --output-run-json /out/gold_run.json
```

Single-site experiments are no longer a CLI flag (M24): pre-filter
the flux/biomet CSVs to one site before invoking, or use the
package functions
([`miaproc.eddy.load_stage1`](src/miaproc/eddy/core.py)) with a
single-site DataFrame programmatically. Outputs accept `.csv` or
`.parquet` by extension on both stages; parquet preserves tz-aware
`DateTime` cleanly, CSV roundtrips re-normalize `DateTime` to
tz-aware UTC on read.

#### Colleague handoff (M15)

Future cloud orchestration can wrap the same two commands without
changing module logic — silver writes a stage-bucket file (or
BigQuery stage table); gold reads it and writes a final-bucket file
(or BigQuery final table). That wrapping is **not** owned by this
repo. The canonical eddy cloud-engineer handoff — responsibility
split, Docker build + disposable BigQuery smoke command shapes,
Google Cloud / service-account binding placeholders, and a clear
non-authoritative framing for infra colleagues — lives at
[`06_infra/eddy_handoff.md`](06_infra/eddy_handoff.md). For the
Terraform interaction with the disposable BigQuery smoke pattern
see [`docs/BIGQUERY_SMOKE_TABLES_AND_TERRAFORM.md`](../docs/BIGQUERY_SMOKE_TABLES_AND_TERRAFORM.md).

### BigQuery-native silver/gold split (M22)

The `eddy` namespace also exposes BigQuery-native silver and gold
commands that mirror the M14 file-based split but read from and
write to BigQuery directly. These complement the existing one-shot
`miaproc eddy run-bigquery` — they do not replace it.

| Command | Reads | Writes | R / preflight |
|---|---|---|---|
| `miaproc eddy run-bigquery-silver` | bronze flux + biomet from BigQuery | local silver artifacts; **optional** silver stage table in BigQuery (stage-only by design) | No |
| `miaproc eddy run-bigquery-gold` | a silver-stage table from BigQuery | local gold artifacts; optional gold stage table in BigQuery; optional MERGE into final gold table under explicit `--bq-allow-final-merge` | Yes for `--engine reddyproc-reference` (Decision 010 / R11) |

Why this split exists: cloud engineers should not have to derive
silver by filtering a processed gold/final table — silver is the
pre-backend, cleaned/regularized state; gold is the
post-`postproc(...)` analytical state. M22 makes the split
representable end-to-end in BigQuery without a CSV/parquet detour.

```bash
mkdir -p .runs/m22_handoff

# Silver: bronze BigQuery -> silver (no R, no preflight)
# M24: --group-column site_id partitions the all-data read; the
# stacked silver writes once into a shared cf_s2_silver_stage table.
miaproc eddy run-bigquery-silver \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-flux-table carbon_flux_eddycovariance \
    --bq-biomet-table carbon_flux_biomet \
    --bq-billing-project manglaria-staging \
    --group-column site_id \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_s2_silver_stage \
    --bq-control-dataset _orch \
    --output-table .runs/m22_handoff/silver.parquet \
    --output-run-json .runs/m22_handoff/silver_run.json

# Gold: silver BigQuery -> gold (project-scoped R preflight)
# M24: --group-column site_id processes every site present in the
# silver read; stacked gold writes once into a shared
# cf_s2_gold_stage table; final MERGE advances per-site watermarks.
miaproc eddy run-bigquery-gold \
    --engine reddyproc-reference \
    --bq-input-project manglaria-staging \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-silver-table cf_s2_silver_stage \
    --bq-billing-project manglaria-staging \
    --group-column site_id \
    --repo-root /app \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table cf_s2_gold_stage \
    --bq-control-dataset _orch \
    --output-table .runs/m22_handoff/gold.parquet \
    --output-diagnostics-json .runs/m22_handoff/gold_diag.json \
    --output-run-json .runs/m22_handoff/gold_run.json
```

Add `--bq-final-table` and `--bq-allow-final-merge` to the gold
invocation to MERGE into a final gold target table. Stage-only is
the safe default; the production-read-only invariant
(`forbidden_write_projects=("manglaria",)`) is unchanged. M22 silver
writeback is **stage-only by design** — there is no
`--bq-final-table` / `--bq-allow-final-merge` flag for silver.

### Stage-payload dry-run and column preservation (M28-M30)

The current BigQuery split path preserves source columns forward:

- bronze/source columns are carried into the silver stage payload;
- silver columns are carried into the gold stage payload;
- newly derived silver/gold analytical columns are appended without
  dropping prior-stage columns.

The silver writeback payload also enforces BigQuery-unique column names
before any stage write under case-insensitive `casefold()` comparison
(not pandas' case-sensitive `columns.is_unique`). The M28 defensive
humidity policy still applies as a fallback under the M32A
source-truth contract:

- equivalent duplicate humidity columns are suppressed with an audit
  action;
- a divergent normalized humidity duplicate is renamed to `rH_norm_s`
  (or a deterministic suffixed variant if that name already exists);
- non-humidity duplicate names raise instead of being silently fused.

### Source-truth column contract (M32A) and full-flux passthrough (M34)

Under the accepted M32A contract the silver and gold BigQuery stage
payloads carry **source-truth final names** and a single `timestamp`
column. Internal backend processing still uses canonical R-style
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
carbon-flux bronze source table survives bronze → silver → gold
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

Both BigQuery split commands expose a non-mutating dry-run mode:

```bash
miaproc eddy run-bigquery-silver ... \
    --stage-payload-dry-run-dir .runs/silver_payload_dry_run

miaproc eddy run-bigquery-gold ... \
    --stage-payload-dry-run-dir .runs/gold_payload_dry_run
```

The dry-run branch writes `stage_payload.csv` and
`stage_payload_metadata.json` locally and skips BigQuery stage writes,
validation SQL, MERGE, and watermark advancement. The metadata records
`columns_unique`, `duplicate_columns`, `missing_input_columns`,
`column_collision_actions`, the four write-safety booleans, and a
`would_write` block showing what targets would have been used by a real
writeback. M30 validated this behavior on the host and inside a locally
built Docker image; the remaining operator-owned cloud check is a
read-only BigQuery dry-run with real credentials before any real
writeback retry.

---

## Docker runtime profile

A bundled Python + R 4.5 + REddyProc 1.3.4 image is defined in the
repository under `docker/Dockerfile.miaproc-r45-reddyproc` in the
published package repo. In this artifact-first development workspace,
the same runtime profile lives one level above the package folder at
`../docker/Dockerfile.miaproc-r45-reddyproc`.
It is intended for stateless one-shot CLI jobs locally, on Cloud Run
Jobs, or on Cloud Batch.

```bash
# Build (from repo root)
docker build -f docker/Dockerfile.miaproc-r45-reddyproc -t miaproc:cli-r45 .

# Help smoke check
docker run --rm miaproc:cli-r45 miaproc --help
```

The container honors the same Decision 010 posture: `reddyproc-reference`
runs only when the in-image `/app/renv.lock` + project-scoped
`/app/renv/library/R-4.5/...` library (baked at image build time, M6
Task 3) produces a project-scoped preflight approval; otherwise the
job exits `2`. Operators may shadow the baked layout with a host
mount when they intentionally want host repo state to win, but the
default image is preflight-approved as-is. See
`docker/README.md` in the published repo (or
[`docker/README.md`](docker/README.md) in this development
workspace) for full local-docker
and Google Cloud invocation templates.

---

## Current release status

- Biomass local CSV/parquet enrichment and BigQuery enrichment are
  implemented with stage-only and explicit-MERGE writeback controls.
- Eddy covariance local file processing supports the silver -> gold split.
- Eddy BigQuery processing supports both the one-shot path and the
  two-step bronze -> silver -> gold path.
- BigQuery stage payloads enforce unique field names and preserve
  previous-stage columns through silver and gold.
- Docker packaging is validated locally for the package runtime and the
  dry-run payload path; publishing the next cloud image and running the
  real BigQuery-read dry-run are operator-owned follow-ups.

---

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

---

## License

MIT License
