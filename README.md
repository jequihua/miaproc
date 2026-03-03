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

`miaproc.biomass` provides tree-level volume estimation using species-specific allometric equations.

It is designed for mangrove forest structure datasets where each row represents a tree with:

- Species name
- DBH (cm)
- Total height (m)

Allometric equations are distributed with the package (Parquet format) and are evaluated dynamically using NumPy.

---

### Key Features

- Species-specific equation matching
- Filtering by Mexican state (`estado`)
- Automatic selection of minimum available assignment level (`nivel_asignacion`)
- Safe evaluation of stored NumPy-ready equations (`ecuacion_numpy`)
- Range checking for DBH and height (warn / clip / error / ignore)
- Full traceability of which equation was used
- Optional override with custom equations

Currently, the module estimates **volume (m³)** via the `response_variable` field (e.g., `VRTAcc_m3`).  
Future versions may include biomass and carbon conversion.

---

### Required Input Columns

Your dataframe must contain:

- Species (e.g., `"Avicennia germinans"`)
- DBH in cm
- Height in meters

Example column mapping:

```python
from miaproc.biomass import BiomassColumns

cols = BiomassColumns(
    species="Species",
    dbh_cm="DBH (cm)",
    height_m="Total Height (m)",
)
```

If height is missing, the estimate will return NaN.

Basic Usage
from miaproc.biomass import (
    load_packaged_equations,
    estimate_trees,
    BiomassColumns,
)

# Load packaged allometric equations
equations = load_packaged_equations()

# Estimate volume
out = estimate_trees(
    df,
    equations=equations,
    estado="Yucatán",
    columns=BiomassColumns(),
    response_variable="VRTAcc_m3",
    range_policy="warn",
)

out.head()

The output includes:

estimate_response_variable

match_status

range_status

clave_ecuacion

nivel_asignacion

estado_ecuacion_usada

fuente_referencia

Custom Equation Override

You can bypass parquet matching entirely:

out = estimate_trees(
    df,
    equations=equations,
    estado="Yucatán",
    custom_function="np.exp(-10 + 1.9*np.log(diam) + 1.0*np.log(alt))",
)

Or define species-specific custom equations:

custom = {
    "Avicennia germinans": "np.exp(-10.1 + 1.97*np.log(diam) + 1.05*np.log(alt))",
    "Rhizophora mangle": "0.00006*np.power(diam,2.05)*np.power(alt,0.80)",
}

out = estimate_trees(
    df,
    equations=equations,
    estado="Yucatán",
    custom_function=custom,
)

If a species is included in custom_function, parquet matching is skipped for that species.

Scientific Notes

DBH must be in cm

Height must be in meters

Range validation uses equation-specific DBH/height bounds when available

Fallback logic:

Match estado

If not found, fallback to any state for that species

Future versions may add:

Ecoregion-based fallback

Geographic proximity search

Biomass and carbon conversion

Species name normalization
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
  - USTAR filtering (fixed threshold)
  - MDS gap filling
  - Flux partitioning (NEE → GPP + RECO)

Future sensor modules will be added progressively.

---

## Installation

### Basic Installation

```bash
pip install git+https://github.com/jequihua/miaproc.git
```

Or clone locally and install in editable mode:

```bash
git clone https://github.com/jequihua/miaproc.git
cd miaproc
pip install -e .
```

---

### Optional: Install with hesseflux engine

For eddy covariance post-processing (USTAR filtering, MDS gap filling, partitioning):

```bash
pip install -e ".[hesseflux]"
```

This installs the optional dependency:

- `hesseflux` (pure Python implementation of standard EC post-processing methods)

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

```python
from miaproc.eddy import postproc, HessefluxConfig

df_final = postproc(
    df_stage1,
    engine="hesseflux",
    hesseflux_config=HessefluxConfig(
        ustar_fixed=0.1,
        partition_method="reichstein",
    ),
)
```

This produces:

- `NEE_f` (gap-filled NEE)
- `NEE_fqc` (fill quality class)
- `SW_IN_f`, `TA_f`, `VPD_f` (gap-filled drivers)
- `GPP`
- `RECO`

---

## Scientific Notes

### USTAR Filtering

Because short time series may not allow robust estimation of u* thresholds, the current implementation:

- Uses a fixed u* threshold (default = 0.1 m/s).
- Masks nighttime NEE when `USTAR < threshold`.

This mirrors the workaround commonly applied when less than 90 days of data are available.

Future versions may support dynamic u* estimation when long records are available.

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
method="reichstein"
```

Other supported methods (via hesseflux):

- `"lasslop"`
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

## Roadmap

- Add REddyProc engine via rpy2 (optional backend)
- Add additional sensor modules
- Improve uncertainty propagation
- Add CLI interface
- Automated metadata export
- Dockerized processing environment

---

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

---

## License

MIT License