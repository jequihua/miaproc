from __future__ import annotations

from pathlib import Path
import warnings

import pandas as pd

from .constants import (
    FULL_OUTPUT_RENAME_MAP,
    BIOMET_SW_IN,
    BIOMET_RAIN,
    BIOMET_RH,
    BIOMET_OUT_RENAME,
)
from .io import read_and_combine_csv, drop_unit_rows
from .time import create_datetime
from .qc import (
    safe_rename,
    convert_units,
    ensure_numeric,
    apply_qc_flags,
    apply_rain_filter,
    sigma_filter_many,
)

def hello_eddy() -> str:
    return "eddy module alive"

def load_stage1(
    *,
    path_full_output: str | Path,
    path_biomet: str | Path,
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    skip_full_output: int = 1,
    skip_biomet: int = 0,
    drop_rain_rows: bool = True,
) -> pd.DataFrame:
    """
    Replicates R script lines ~1–229:
      - read & combine multiple csv from two directories
      - remove unit rows (time == "[HH:MM]")
      - create DateTime robustly
      - rename key columns (safe)
      - select key biomet columns (safe)
      - left join by DateTime
      - unit conversions (Tair, VPD guarded)
      - QC filtering (QC_NEE/qc_H/qc_LE)
      - rain filtering
      - 3-sigma outlier masking for NEE/H/LE

    Returns the "full_output_limpio" equivalent after sigma filtering.
    """
    # --- Read ---
    full_output = read_and_combine_csv(Path(path_full_output), skip_rows=skip_full_output)
    biomet = read_and_combine_csv(Path(path_biomet), skip_rows=skip_biomet)

    # --- Drop unit rows ---
    full_output = drop_unit_rows(full_output)
    biomet = drop_unit_rows(biomet)

    # --- Create DateTime ---
    full_output_proc = create_datetime(full_output, tz_in=tz_in, tz_out=tz_out, warn_dups=True)
    biomet_proc = create_datetime(biomet, tz_in=tz_in, tz_out=tz_out, warn_dups=True)

    # --- Rename full output keys safely ---
    full_output_proc = safe_rename(full_output_proc, FULL_OUTPUT_RENAME_MAP)

    # Note: R has a no-op "VPD: only if you actually have it" block, but later divides VPD anyway.
    # We intentionally handle VPD conversion guarded in convert_units().

    # --- BIOMET key selection (R transmute with existence checks) ---
    # Create Rg, P_RAIN, rH, and keep DateTime
    biomet_key = pd.DataFrame({"DateTime": biomet_proc["DateTime"]})

    def _col_or_nan(src: pd.DataFrame, col: str) -> pd.Series:
        if col in src.columns:
            return pd.to_numeric(src[col], errors="coerce")
        warnings.warn(f"biomet_key: missing '{col}', filling with NaN.")
        return pd.Series([pd.NA] * len(src), dtype="float64")

    biomet_key["Rg"] = _col_or_nan(biomet_proc, BIOMET_SW_IN)
    biomet_key["P_RAIN"] = _col_or_nan(biomet_proc, BIOMET_RAIN)
    biomet_key["rH"] = _col_or_nan(biomet_proc, BIOMET_RH)

    # R: filter(!is.na(Rg) | !is.na(P_RAIN) | !is.na(rH))
    biomet_key = biomet_key.loc[
        biomet_key["Rg"].notna() | biomet_key["P_RAIN"].notna() | biomet_key["rH"].notna()
    ].copy()

    # --- Merge ---
    data_fusion = (
        full_output_proc.merge(biomet_key, on="DateTime", how="left")
        .sort_values("DateTime")
        .reset_index(drop=True)
    )

    if len(data_fusion) > 0:
        dt_min = data_fusion["DateTime"].min()
        dt_max = data_fusion["DateTime"].max()
        # match R message style via warning (keeps logs visible in notebooks/pytest)
        warnings.warn(f"Archivos combinados. Rango de fechas: {dt_min} a {dt_max}")

    # --- Unit conversion (guarded) ---
    data_fusion = convert_units(data_fusion)

    # --- QC numeric coercion (mirrors R mutate QC_NEE/qc_H/qc_LE to numeric) ---
    data_fusion = ensure_numeric(data_fusion, cols=["QC_NEE", "qc_H", "qc_LE"])

    # --- Apply QC filters ---
    full_output_limpio = apply_qc_flags(
        data_fusion,
        qc_to_var={"QC_NEE": "NEE", "qc_H": "H", "qc_LE": "LE"},
        bad_qc_value=2,
    )

    # --- Rain filter (mask + optionally drop) ---
    full_output_limpio = apply_rain_filter(
        full_output_limpio,
        rain_col="P_RAIN",
        flux_cols=("NEE", "H", "LE"),
        drop_rain_rows=drop_rain_rows,
    )

    # --- 3-sigma filter ---
    full_output_limpio = sigma_filter_many(full_output_limpio, cols=("NEE", "H", "LE"), nsigma=3.0)

    return full_output_limpio
