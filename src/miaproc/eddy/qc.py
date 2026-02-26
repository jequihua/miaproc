from __future__ import annotations

import warnings
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


def safe_rename(df: pd.DataFrame, rename_map: dict[str, str]) -> pd.DataFrame:
    """
    Rename only columns that exist; warn for missing expected columns.
    This avoids the R failure mode where rename() errors if a column is absent.
    """
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    missing = [k for k in rename_map.keys() if k not in df.columns]
    if missing:
        warnings.warn(f"Missing expected columns for rename (skipped): {missing}")
    return df.rename(columns=existing)


def ensure_numeric(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        else:
            warnings.warn(f"ensure_numeric: column '{c}' not found; skipping.")
    return out


def convert_units(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replicates R unit conversions:
      Tair = Tair - 273.15
      VPD  = VPD / 100

    But guarded: only apply if columns exist.
    """
    out = df.copy()

    if "Tair" in out.columns:
        out["Tair"] = pd.to_numeric(out["Tair"], errors="coerce") - 273.15
    else:
        warnings.warn("convert_units: 'Tair' not found; skipping K->C conversion.")

    if "VPD" in out.columns:
        out["VPD"] = pd.to_numeric(out["VPD"], errors="coerce") / 100.0
    else:
        # This is a known bug/risk in the R script: it tries VPD conversion even if missing.
        warnings.warn("convert_units: 'VPD' not found; skipping Pa->hPa conversion.")

    return out


def apply_qc_flags(
    df: pd.DataFrame,
    *,
    qc_to_var: dict[str, str],
    bad_qc_value: float | int = 2,
) -> pd.DataFrame:
    """
    Replicates:
      NEE = ifelse(QC_NEE == 2, NA, NEE)
      H   = ifelse(qc_H  == 2, NA, H)
      LE  = ifelse(qc_LE == 2, NA, LE)

    Guarded: if qc or var missing, warn and skip that pair.
    """
    out = df.copy()

    for qc_col, var_col in qc_to_var.items():
        if qc_col not in out.columns:
            warnings.warn(f"apply_qc_flags: QC column '{qc_col}' missing; skipping.")
            continue
        if var_col not in out.columns:
            warnings.warn(f"apply_qc_flags: variable column '{var_col}' missing; skipping.")
            continue

        qc = pd.to_numeric(out[qc_col], errors="coerce")
        out[var_col] = np.where(qc == bad_qc_value, np.nan, out[var_col])

    return out


def apply_rain_filter(
    df: pd.DataFrame,
    *,
    rain_col: str = "P_RAIN",
    flux_cols: Sequence[str] = ("NEE", "H", "LE"),
    drop_rain_rows: bool = True,
) -> pd.DataFrame:
    """
    Replicates R:
      is_raining <- P_RAIN > 0 & !is.na(P_RAIN)
      mutate(across(c(NEE,H,LE), ifelse(is_raining, NA, .)))
      full_output_limpio <- full_output_limpio[!is_raining,]
    """
    out = df.copy()

    if rain_col not in out.columns:
        warnings.warn(f"apply_rain_filter: '{rain_col}' missing; skipping rain filtering.")
        return out

    out[rain_col] = pd.to_numeric(out[rain_col], errors="coerce")
    is_raining = (out[rain_col] > 0) & out[rain_col].notna()

    # Mask fluxes during rain
    for c in flux_cols:
        if c in out.columns:
            out.loc[is_raining, c] = np.nan
        else:
            warnings.warn(f"apply_rain_filter: flux column '{c}' missing; skipping mask.")

    if drop_rain_rows:
        removed = int(is_raining.sum())
        out = out.loc[~is_raining].copy()
        # match R's print
        warnings.warn(f"Rain filter: removed {removed} rows where {rain_col} > 0.")

    return out


def sigma_filter(df: pd.DataFrame, col: str, nsigma: float = 3.0) -> pd.DataFrame:
    """
    Replicates R sigma_filter():
      m <- mean(var, na.rm=TRUE)
      s <- sd(var, na.rm=TRUE)
      set to NA if abs(x - m) > 3*s
    """
    out = df.copy()
    if col not in out.columns:
        warnings.warn(f"sigma_filter: column '{col}' missing; skipping.")
        return out

    x = pd.to_numeric(out[col], errors="coerce")
    m = float(np.nanmean(x))
    s = float(np.nanstd(x, ddof=1))  # R sd uses sample SD (n-1)
    if not np.isfinite(s) or s == 0:
        warnings.warn(f"sigma_filter: sd for '{col}' is not finite or zero; skipping.")
        out[col] = x
        return out

    out[col] = np.where(np.abs(x - m) > nsigma * s, np.nan, x)
    return out


def sigma_filter_many(df: pd.DataFrame, cols: Sequence[str] = ("NEE", "H", "LE"), nsigma: float = 3.0) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out = sigma_filter(out, c, nsigma=nsigma)
    return out
