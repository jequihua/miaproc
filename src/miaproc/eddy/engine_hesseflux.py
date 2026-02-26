from __future__ import annotations

from dataclasses import dataclass
import warnings
from typing import Literal

import numpy as np
import pandas as pd

# hesseflux is an optional dependency
try:
    import hesseflux as hf
except Exception:  # pragma: no cover
    hf = None  # type: ignore


@dataclass(frozen=True)
class HessefluxConfig:
    # fixed u* threshold (your chosen workaround)
    ustar_fixed: float = 0.1

    # day/night threshold in W m-2 (hesseflux docs default 10)
    swthr: float = 10.0

    # MDS gapfill controls (hesseflux defaults)
    sw_dev: float = 50.0
    ta_dev: float = 2.5
    vpd_dev: float = 5.0
    longgap: int = 60

    # missing value sentinel required by hesseflux (cannot be np.nan)
    undef: float = -9999.0

    # partitioning method
    partition_method: Literal["reichstein", "lasslop", "falge"] = "reichstein"

    # if True, set GPP=0 at night (hesseflux option)
    nogppnight: bool = False


def _require_hesseflux() -> None:
    if hf is None:
        raise ImportError(
            "hesseflux is not installed. Install with: pip install 'miaproc[hesseflux]' "
            "or add hesseflux to your environment."
        )


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _prepare_hesseflux_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map miaproc stage-1 columns into a minimal dataframe for hesseflux.

    Expected input columns (from your stage-1 pipeline):
      DateTime, NEE, USTAR, Tair, VPD, Rg, QC_NEE, rH (optional)

    Output columns:
      NEE, USTAR, SW_IN, TA, VPD

    Notes:
      - SW_IN in W m-2 (Rg in your data)
      - TA in deg C (for gapfill)
      - VPD in hPa (for gapfill)
    """
    needed = ["DateTime", "NEE", "USTAR", "Tair", "VPD", "Rg"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"hesseflux engine: missing required columns: {missing}")

    out = pd.DataFrame(index=df.index.copy())
    out["DateTime"] = df["DateTime"]

    out["NEE"] = _to_numeric(df["NEE"])
    out["USTAR"] = _to_numeric(df["USTAR"])
    out["SW_IN"] = _to_numeric(df["Rg"])
    out["TA"] = _to_numeric(df["Tair"])
    out["VPD"] = _to_numeric(df["VPD"])

    # hesseflux assumes index is time
    out = out.set_index("DateTime")

    return out


def _build_flag_frame(dfin: pd.DataFrame, *, undef: float) -> pd.DataFrame:
    """
    Follow hesseflux pattern: a flag df with same shape; non-zero => treated as missing. :contentReference[oaicite:4]{index=4}
    """
    df = dfin.copy()
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(undef)

    dff = df.copy(deep=True).astype(int)
    dff[:] = 0
    dff[df == undef] = 2
    return df, dff


def run_hesseflux_engine(
    df_stage1: pd.DataFrame,
    *,
    config: HessefluxConfig = HessefluxConfig(),
) -> pd.DataFrame:
    """
    Runs:
      1) fixed u* filtering (ustar_fixed) on nighttime NEE
      2) MDS gapfill for SW_IN, VPD, TA
      3) MDS gapfill for NEE (after u* masking)
      4) Partition NEE_f into GPP + RECO using nee2gpp(method=...)

    Returns the original df_stage1 plus:
      SW_IN_f, TA_f, VPD_f, NEE_f, NEE_fqc, GPP, RECO
    """
    _require_hesseflux()

    base = df_stage1.copy()

    # 1) Prepare and numeric-ize
    dfin0 = _prepare_hesseflux_frame(base)

    # clamp negative radiation to 0 (your R logic)
    dfin0["SW_IN"] = dfin0["SW_IN"].where(dfin0["SW_IN"].isna() | (dfin0["SW_IN"] >= 0), 0.0)

    # 2) Build undef-filled data + flags
    dfin, dff = _build_flag_frame(dfin0, undef=config.undef)

    # 3) Day/night (SW_IN > swthr) :contentReference[oaicite:5]{index=5}
    isday = dfin["SW_IN"] > config.swthr

    # 4) Fixed u* filtering:
    # flag NEE at night when USTAR < ustar_fixed
    # (any non-zero flag means "treat as missing" for gapfill) :contentReference[oaicite:6]{index=6}
    ustar_mask = (~isday) & (dfin["USTAR"] != config.undef) & (dfin["USTAR"] < config.ustar_fixed)
    if int(ustar_mask.sum()) > 0:
        dff.loc[ustar_mask, "NEE"] = 2

    # 5) Gap-fill meteorological drivers (SW_IN, TA, VPD) using MDS.
    # gapfill requires SW_IN/TA/VPD present in the dataframe. :contentReference[oaicite:7]{index=7}
    drv_cols = ["SW_IN", "TA", "VPD"]
    df_drv_f, dff_drv_f = hf.gapfill(
        dfin[drv_cols],
        flag=dff[drv_cols],
        sw_dev=config.sw_dev,
        ta_dev=config.ta_dev,
        vpd_dev=config.vpd_dev,
        longgap=config.longgap,
        undef=config.undef,
        err=False,
        verbose=0,
    )

    # 6) Gap-fill NEE using drivers + the NEE column
    fill_cols = ["NEE", "SW_IN", "TA", "VPD"]
    df_fill_in = pd.concat([dfin[["NEE"]], df_drv_f], axis=1)
    dff_fill_in = pd.concat([dff[["NEE"]], dff_drv_f], axis=1)

    df_nee_f, dff_nee_f = hf.gapfill(
        df_fill_in[fill_cols],
        flag=dff_fill_in[fill_cols],
        sw_dev=config.sw_dev,
        ta_dev=config.ta_dev,
        vpd_dev=config.vpd_dev,
        longgap=config.longgap,
        undef=config.undef,
        err=False,
        verbose=0,
    )

    # gapfill returns filled_data + quality_class (quality flags 1-3 for fill quality) :contentReference[oaicite:8]{index=8}
    # Here, dff_nee_f is the "quality class" dataframe
    # Keep just the filled target and its quality; drivers already stored from df_drv_f
    NEE_f = df_nee_f["NEE"].replace(config.undef, np.nan)
    NEE_fqc = dff_nee_f["NEE"].replace(config.undef, np.nan)

    SW_IN_f = df_drv_f["SW_IN"].replace(config.undef, np.nan)
    TA_f_C = df_drv_f["TA"].replace(config.undef, np.nan)
    VPD_f_hPa = df_drv_f["VPD"].replace(config.undef, np.nan)

    # 7) Partition (NEE_f -> GPP, RECO)
    # nee2gpp expects columns: (NEE/FC), SW_IN, TA, VPD. :contentReference[oaicite:9]{index=9}
    # Examples convert TA to Kelvin and VPD to Pa for partitioning. :contentReference[oaicite:10]{index=10}
    TA_for_part = TA_f_C + 273.15
    VPD_for_part = VPD_f_hPa * 100.0

    part_in = pd.DataFrame(
        {
            "NEE": NEE_f.fillna(config.undef),
            "SW_IN": SW_IN_f.fillna(config.undef),
            "TA": TA_for_part.fillna(config.undef),
            "VPD": VPD_for_part.fillna(config.undef),
        },
        index=dfin.index,
    )

    # Use NEE fill quality as flag for NEE; for drivers use their fill quality
    # (Any non-zero means ignore) :contentReference[oaicite:11]{index=11}
    part_flag = pd.DataFrame(
        {
            "NEE": NEE_fqc.fillna(0).astype(int),
            "SW_IN": dff_drv_f["SW_IN"].fillna(0).astype(int),
            "TA": dff_drv_f["TA"].fillna(0).astype(int),
            "VPD": dff_drv_f["VPD"].fillna(0).astype(int),
        },
        index=dfin.index,
    )

    try:
        df_part = hf.nee2gpp(
            part_in,
            flag=part_flag,
            isday=isday,
            undef=config.undef,
            method=config.partition_method,
            nogppnight=config.nogppnight,
            swthr=config.swthr,
        )
    except Exception as e:
        warnings.warn(
            f"hesseflux partitioning failed (method={config.partition_method}): {e}. "
            "Returning gap-filled NEE without GPP/RECO."
        )
        df_part = pd.DataFrame(index=dfin.index)

    # df_part typically contains GPP and RECO columns (and maybe more)
    # Convert undef -> NaN
    for c in list(df_part.columns):
        df_part[c] = df_part[c].replace(config.undef, np.nan)

    # 8) Attach outputs to the original dataframe
    # Note: keep naming aligned with your R intention
    base = base.copy()
    base["SW_IN_f"] = SW_IN_f.to_numpy()
    base["TA_f"] = TA_f_C.to_numpy()
    base["VPD_f"] = VPD_f_hPa.to_numpy()
    base["NEE_f"] = NEE_f.to_numpy()
    base["NEE_fqc"] = NEE_fqc.to_numpy()

    if "GPP" in df_part.columns:
        base["GPP"] = df_part["GPP"].to_numpy()
    if "RECO" in df_part.columns:
        base["RECO"] = df_part["RECO"].to_numpy()

    return base