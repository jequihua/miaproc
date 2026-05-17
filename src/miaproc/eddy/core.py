from __future__ import annotations

from pathlib import Path
import warnings

import pandas as pd

from .constants import (
    FULL_OUTPUT_RENAME_MAP,
    BIOMET_SW_IN,
    BIOMET_RAIN,
    BIOMET_RH,
)
from .io import read_and_combine_csv, drop_unit_rows
from .time import create_datetime, regularize_time_grid
from .qc import (
    safe_rename,
    convert_units,
    ensure_numeric,
    apply_qc_flags,
    apply_rain_filter,
    sigma_filter_many,
)


def _apply_site_filter(
    df: pd.DataFrame,
    *,
    side: str,
    site_id: str | None,
) -> pd.DataFrame:
    """Enforce the multi-site contract from Decision 008.

    - If ``df`` has no ``site_id`` column, return it unchanged.
    - If ``df`` has ``site_id`` with a single unique value, return it
      unchanged (``site_id`` kwarg, if given, must match).
    - If ``df`` has multiple sites and ``site_id`` is ``None``, raise
      ``ValueError`` listing the available sites and instructing the
      caller to pass ``site_id=...``.
    - If ``site_id`` is given and absent from ``df``, raise ``ValueError``
      naming the missing side and available sites.
    - Filters to ``site_id`` and resets the index so downstream
      parsing is index-safe.
    """
    if "site_id" not in df.columns:
        return df

    unique_sites = pd.Series(df["site_id"].dropna().unique()).sort_values().tolist()
    if site_id is None:
        if len(unique_sites) <= 1:
            return df
        raise ValueError(
            f"{side} input contains multiple site IDs: {unique_sites}. "
            "Pass site_id=... to load_stage1() to select one."
        )

    if site_id not in unique_sites:
        raise ValueError(
            f"{side} input does not contain site_id={site_id!r}. "
            f"Available sites: {unique_sites}."
        )

    return df.loc[df["site_id"] == site_id].reset_index(drop=True).copy()


def _resolve_ustar_alias(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the case-study ``u_star`` column into the legacy ``u.`` name.

    If both columns are present the legacy ``u.`` is kept (with a warning)
    to preserve the behavior that predates the case-study shape.
    """
    has_legacy = "u." in df.columns
    has_alias = "u_star" in df.columns
    if has_legacy and has_alias:
        warnings.warn(
            "Both 'u.' and 'u_star' columns present; keeping legacy 'u.' and "
            "dropping 'u_star'."
        )
        return df.drop(columns=["u_star"])
    if has_alias and not has_legacy:
        return df.rename(columns={"u_star": "u."})
    return df


def stage1_from_raw_frames(
    full_output: pd.DataFrame,
    biomet: pd.DataFrame,
    *,
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    drop_rain_rows: bool = True,
    site_id: str | None = None,
) -> pd.DataFrame:
    """Run the stage-1 cleaning pipeline on already-loaded raw frames.

    This is the shared post-ingestion path used by both ``load_stage1``
    (file/CSV-directory ingestion) and ``load_stage1_from_dataframes``
    (in-memory DataFrame ingestion, e.g. BigQuery-native mode). The
    behavior is the same for both ingestion modes: site filtering,
    legacy unit-row drop, ``u_star`` → ``u.`` alias resolution, DateTime
    construction, rename, biomet key selection + left join, unit
    conversion, QC filtering, rain filtering, 3-sigma masking, and
    regularization to a continuous 30-minute time grid.

    Exposed publicly (M24) so the grouped-CLI path can read raw
    flux + biomet frames once and run stage-1 per categorical group
    without re-reading the source files. ``site_id=None`` skips the
    Decision 008 multi-site filter so a pre-filtered group frame
    flows straight through.
    """
    # --- Site filtering (Decision 008) ---
    full_output = _apply_site_filter(full_output, side="flux", site_id=site_id)
    biomet = _apply_site_filter(biomet, side="biomet", site_id=site_id)

    # --- Drop unit rows (legacy CSV concern; no-op for BigQuery-shaped data) ---
    full_output = drop_unit_rows(full_output)
    biomet = drop_unit_rows(biomet)

    # --- Normalize u_star -> u. alias before the rename map runs ---
    full_output = _resolve_ustar_alias(full_output)

    # --- Create DateTime ---
    full_output_proc = create_datetime(full_output, tz_in=tz_in, tz_out=tz_out, warn_dups=True)
    biomet_proc = create_datetime(biomet, tz_in=tz_in, tz_out=tz_out, warn_dups=True)

    # --- Rename full output keys safely ---
    full_output_proc = safe_rename(full_output_proc, FULL_OUTPUT_RENAME_MAP)

    # --- BIOMET key selection ---
    biomet_key = pd.DataFrame({"DateTime": biomet_proc["DateTime"]})

    def _col_or_nan(src: pd.DataFrame, col: str) -> pd.Series:
        if col in src.columns:
            return pd.to_numeric(src[col], errors="coerce")
        warnings.warn(f"biomet_key: missing '{col}', filling with NaN.")
        return pd.Series([pd.NA] * len(src), dtype="float64")

    biomet_key["Rg"] = _col_or_nan(biomet_proc, BIOMET_SW_IN)
    biomet_key["P_RAIN"] = _col_or_nan(biomet_proc, BIOMET_RAIN)
    biomet_key["rH"] = _col_or_nan(biomet_proc, BIOMET_RH)

    # Keep only rows where at least one biomet variable is present
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
        warnings.warn(f"Archivos combinados. Range of dates: {dt_min} a {dt_max}")

    # --- Unit conversion ---
    data_fusion = convert_units(data_fusion)

    # --- QC numeric coercion ---
    data_fusion = ensure_numeric(data_fusion, cols=["QC_NEE", "qc_H", "qc_LE"])

    # --- Apply QC filters ---
    full_output_limpio = apply_qc_flags(
        data_fusion,
        qc_to_var={"QC_NEE": "NEE", "qc_H": "H", "qc_LE": "LE"},
        bad_qc_value=2,
    )

    # --- Rain filter ---
    full_output_limpio = apply_rain_filter(
        full_output_limpio,
        rain_col="P_RAIN",
        flux_cols=("NEE", "H", "LE"),
        drop_rain_rows=drop_rain_rows,
    )

    # --- 3-sigma filter ---
    full_output_limpio = sigma_filter_many(
        full_output_limpio,
        cols=("NEE", "H", "LE"),
        nsigma=3.0,
    )

    # --- Standardize to continuous 30-minute time grid ---
    full_output_limpio = regularize_time_grid(
        full_output_limpio,
        datetime_col="DateTime",
        freq="30min",
    )

    return full_output_limpio


def load_stage1(
    *,
    path_full_output: str | Path,
    path_biomet: str | Path,
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    skip_full_output: int = 1,
    skip_biomet: int = 0,
    drop_rain_rows: bool = True,
    site_id: str | None = None,
) -> pd.DataFrame:
    """
    Replicates R script lines ~1–251:
      - read & combine multiple csv from two directories
      - (optional) filter to a single ``site_id`` before parsing
      - remove unit rows (time == "[HH:MM]")
      - create DateTime robustly (legacy ``date + time`` or case-study
        ``timestamp``)
      - rename key columns (safe); ``u_star`` is normalized to the legacy
        ``u.`` alias before the rename map runs
      - select key biomet columns (safe)
      - left join by DateTime
      - unit conversions (Tair, VPD guarded)
      - QC filtering (QC_NEE/qc_H/qc_LE)
      - rain filtering
      - 3-sigma outlier masking for NEE/H/LE
      - standardize to continuous 30-minute time grid

    ``site_id`` follows Decision 008: real case-study data under
    ``01_data/case_study`` must be filtered to ``site_id="RBMNN"`` for
    default tests and validation. If the input files contain multiple
    sites and ``site_id`` is ``None``, the call fails loudly.

    Returns the "full_output_limpio" equivalent after time-grid regularization.
    """
    full_output = read_and_combine_csv(Path(path_full_output), skip_rows=skip_full_output)
    biomet = read_and_combine_csv(Path(path_biomet), skip_rows=skip_biomet)
    return stage1_from_raw_frames(
        full_output,
        biomet,
        tz_in=tz_in,
        tz_out=tz_out,
        drop_rain_rows=drop_rain_rows,
        site_id=site_id,
    )


def load_stage1_from_dataframes(
    *,
    flux_df: pd.DataFrame,
    biomet_df: pd.DataFrame,
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    drop_rain_rows: bool = True,
    site_id: str | None = None,
) -> pd.DataFrame:
    """In-memory stage-1 ingestion entrypoint.

    Same scientific contract as :func:`load_stage1`, but consumes
    pre-loaded pandas DataFrames instead of CSV directories. Intended
    for cloud/local orchestration paths that read the flux and biomet
    inputs directly (for example BigQuery-native mode, M7) without
    round-tripping through synthetic CSV files.

    The input frames are expected to carry the case-study column shape:
    a ``timestamp`` column (TIMESTAMP-typed or ISO string), an optional
    ``site_id`` column for Decision 008 multi-site filtering, the EddyPro
    flux columns on the flux side (``co2_flux``, ``air_temperature``,
    ``u_star`` / ``u.``, ``qc_co2_flux``, plus ``VPD``), and the
    Biomet keys on the biomet side (``SWIN_1_1_1``, ``P_RAIN_1_1_1``,
    ``RH_1_1_1``).

    The caller's frames are not mutated.
    """
    if not isinstance(flux_df, pd.DataFrame):
        raise TypeError("flux_df must be a pandas DataFrame")
    if not isinstance(biomet_df, pd.DataFrame):
        raise TypeError("biomet_df must be a pandas DataFrame")
    return stage1_from_raw_frames(
        flux_df.copy(),
        biomet_df.copy(),
        tz_in=tz_in,
        tz_out=tz_out,
        drop_rain_rows=drop_rain_rows,
        site_id=site_id,
    )
