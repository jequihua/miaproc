from __future__ import annotations

from dataclasses import dataclass
import warnings
from typing import Any, Literal, Optional, Sequence

import numpy as np
import pandas as pd

from .ustar import (
    DynamicUstarResult,
    estimate_dynamic_ustar_thresholds,
)

# Compatibility patch for older libraries expecting np.int and np.float
if not hasattr(np, "int"):
    np.int = int
if not hasattr(np, "float"):
    np.float = float

# hesseflux is an optional dependency
try:
    import hesseflux as hf
except Exception:  # pragma: no cover
    hf = None  # type: ignore


HESSEFLUX_BACKEND_NAME: str = "hesseflux"

# Common backend output schema, per 08_pkg/backend_contract.md and
# 01_data/schema.md. Shared with the reddyproc-rpy2 backend.
HESSEFLUX_COMMON_OUTPUT_COLUMNS: tuple[str, ...] = (
    "DateTime",
    "NEE",
    "NEE_f",
    "NEE_fqc",
    "GPP",
    "Reco",
    "Tair",
    "Tair_f",
    "Rg",
    "Rg_f",
    "VPD",
    "VPD_f",
    "USTAR",
)


@dataclass(frozen=True)
class HessefluxConfig:
    # ---- u* configuration --------------------------------------------------
    # Fixed mode keeps the legacy arbitrary-threshold behavior and remains
    # the default for backward compatibility. Dynamic mode estimates a
    # threshold from the input data (see miaproc.eddy.ustar). Legacy
    # callers that pass only ``ustar_fixed`` continue to work unchanged.
    ustar_mode: Literal["fixed", "dynamic"] = "fixed"

    # Fixed-mode threshold (legacy). Ignored when ustar_mode == "dynamic".
    ustar_fixed: float = 0.1

    # Dynamic-mode configuration. Matches the reddyproc-rpy2 backend's
    # vocabulary so M5 can compare diagnostics side by side.
    ustar_probs: tuple[float, float, float] = (0.05, 0.5, 0.95)
    ustar_scenario: str = "U50"
    # Conservative default chosen for the 90-day case study. REddyProc uses
    # a larger bootstrap budget; the plateau estimator here is deterministic
    # and needs enough nighttime samples to populate ~20 u* quantile classes
    # within each of ~4 temperature bins.
    ustar_min_night_samples: int = 500
    ustar_temp_bins: int = 4
    ustar_bins: int = 20
    ustar_plateau_fraction: float = 0.95
    # Reserved for a future bootstrap-based confidence estimate; M4 uses
    # a single deterministic pass.
    ustar_bootstrap_samples: int = 0
    random_seed: Optional[int] = 0

    # ---- Day/night + hesseflux gapfill controls ----------------------------
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
    partition_method: Literal["reichstein", "lasslop", "falge"] = "lasslop"

    # if True, set GPP=0 at night (hesseflux option)
    nogppnight: bool = False

    # ---- Reco fit mode (M5 Coding Prompt 022 / H2 corrective) ----------
    # ``native`` (default): native hesseflux ``nee2gpp`` partitioning.
    # Behavior and diagnostics are unchanged from prior milestones.
    #
    # ``lt_reddyproc_aligned``: opt-in Lloyd-Taylor alignment wrapper
    # (see ``miaproc.eddy.lt_reco_wrapper``). Skips ``nee2gpp``
    # entirely; fits Rref/E0 to nighttime ``NEE_f`` (quality flag 0)
    # using REddyProc's LT constants (Tref=15 °C, T0=-46.02 °C) and
    # derives ``Reco`` for all rows. ``GPP = Reco - NEE_f``. **No silent
    # fallback**: if the fit cannot be computed (sparse, invalid domain,
    # optimizer failure, boundary-bound solution), the engine raises
    # ``LTWrapperError`` and does not return partial partitioning.
    # Intended for Reco comparability studies (e.g. M5 H2 corrective);
    # not claimed as scientific parity per Decision 009.
    reco_fit_mode: Literal["native", "lt_reddyproc_aligned"] = "native"

    # Minimum paired-finite nighttime samples required by the LT
    # wrapper fit. Decoupled from ``ustar_min_night_samples`` because
    # post-u*-filter + fqc==0 gate typically yields far fewer rows
    # than the u* estimator needs; the LT fit needs only a handful of
    # dozens to identify Rref/E0 on a well-behaved nighttime signal.
    lt_min_night_samples: int = 100


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


def _resolve_ustar_threshold(
    df_stage1: pd.DataFrame,
    config: HessefluxConfig,
) -> tuple[float, Optional[DynamicUstarResult]]:
    """Return ``(threshold, dynamic_result)`` per ``config.ustar_mode``.

    - ``fixed``: returns ``(config.ustar_fixed, None)``.
    - ``dynamic``: runs :func:`estimate_dynamic_ustar_thresholds`. Failure
      raises :class:`DynamicUstarEstimationError` — the caller must **not**
      fall back to ``ustar_fixed`` silently (risk R4).

    Unknown modes raise ``ValueError``.
    """
    mode = config.ustar_mode
    if mode == "fixed":
        return float(config.ustar_fixed), None
    if mode == "dynamic":
        dyn = estimate_dynamic_ustar_thresholds(
            df_stage1,
            ustar_probs=config.ustar_probs,
            ustar_scenario=config.ustar_scenario,
            ustar_min_night_samples=config.ustar_min_night_samples,
            ustar_temp_bins=config.ustar_temp_bins,
            ustar_bins=config.ustar_bins,
            ustar_plateau_fraction=config.ustar_plateau_fraction,
            swthr=config.swthr,
        )
        return dyn.selected_threshold, dyn
    raise ValueError(
        f"HessefluxConfig.ustar_mode={mode!r} is not supported. "
        "Expected 'fixed' or 'dynamic'."
    )


def _build_hesseflux_diagnostics(
    config: HessefluxConfig,
    *,
    selected_threshold: float,
    dynamic_result: Optional[DynamicUstarResult],
    fraction_nee_filtered: float,
    extra_warnings: Sequence[str] = (),
    lt_wrapper_diag: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Assemble the diagnostics sidecar attached to ``df.attrs``.

    **Diagnostics shape change (Coding Prompt 022):** ``partitioning``
    is now a dict with keys ``method`` (legacy hesseflux partition
    method string), ``reco_fit_mode`` (``"native"`` or
    ``"lt_reddyproc_aligned"``), and ``lt_wrapper`` (``None`` in
    native mode; wrapper fit payload in LT-aligned mode). Callers that
    previously asserted ``diag["partitioning"] == "lasslop"`` should
    now read ``diag["partitioning"]["method"]``.
    """
    if dynamic_result is not None:
        ustar = {
            "mode": "dynamic",
            "scenario": dynamic_result.selected_scenario,
            "available_scenarios": dynamic_result.available_scenarios,
            "selected_threshold": dynamic_result.selected_threshold,
            "thresholds_by_scenario": dict(dynamic_result.thresholds_by_scenario),
            "thresholds_by_season": dynamic_result.thresholds_by_season,
            "night_sample_count": dynamic_result.night_sample_count,
            "method": dynamic_result.method,
            "fraction_nee_filtered": float(fraction_nee_filtered),
            "probs": tuple(config.ustar_probs),
        }
        warnings_ = tuple(dynamic_result.warnings) + tuple(extra_warnings)
    else:
        ustar = {
            "mode": "fixed",
            "scenario": None,
            "available_scenarios": (),
            "selected_threshold": float(selected_threshold),
            "thresholds_by_scenario": {},
            "thresholds_by_season": (),
            "night_sample_count": None,
            "method": "fixed-legacy",
            "fraction_nee_filtered": float(fraction_nee_filtered),
            "probs": tuple(config.ustar_probs),
        }
        warnings_ = tuple(extra_warnings)

    try:
        import hesseflux as _hf_mod

        hf_version = getattr(_hf_mod, "__version__", None)
    except Exception:
        hf_version = None

    return {
        "backend": HESSEFLUX_BACKEND_NAME,
        "ustar": ustar,
        "partitioning": {
            "method": str(config.partition_method),
            "reco_fit_mode": str(config.reco_fit_mode),
            "lt_wrapper": lt_wrapper_diag,
        },
        "versions": {"hesseflux": hf_version},
        "warnings": warnings_,
    }


def _attach_common_aliases(df: pd.DataFrame) -> None:
    """Add the 13-column common backend contract aliases in-place.

    Existing legacy column names (``SW_IN_f``, ``TA_f``, ``RECO``) are
    preserved alongside the contract aliases so legacy callers keep
    working. Called on the final output frame.

    Note on ``VPD_f``: ``run_hesseflux_engine`` gap-fills VPD as an MDS
    driver and writes the filled series to ``base["VPD_f"]`` before this
    helper runs, so ``VPD_f`` is typically already populated with real
    values. The ``NaN`` fallback below only fires for callers that
    invoke this helper on a frame that skipped the gap-fill step (e.g.
    direct unit tests of the aliases). It keeps the 13-column contract
    shape stable in those edge cases.
    """
    if "Rg_f" not in df.columns and "SW_IN_f" in df.columns:
        df["Rg_f"] = df["SW_IN_f"]
    if "Tair_f" not in df.columns and "TA_f" in df.columns:
        df["Tair_f"] = df["TA_f"]
    if "Reco" not in df.columns and "RECO" in df.columns:
        df["Reco"] = df["RECO"]
    if "VPD_f" not in df.columns:
        df["VPD_f"] = float("nan")


def run_hesseflux_engine(
    df_stage1: pd.DataFrame,
    *,
    config: HessefluxConfig = HessefluxConfig(),
) -> pd.DataFrame:
    """
    Runs:
      1) u* filtering on nighttime NEE
         - ``ustar_mode="fixed"``: uses ``config.ustar_fixed``.
         - ``ustar_mode="dynamic"``: estimates a threshold from the input
           data via :mod:`miaproc.eddy.ustar`. Failure raises
           :class:`DynamicUstarEstimationError` with no silent fallback.
      2) MDS gapfill for SW_IN, VPD, TA
      3) MDS gapfill for NEE (after u* masking)
      4) Partition NEE_f into GPP + RECO via ``nee2gpp(method=...)``

    Returns the original ``df_stage1`` plus legacy convenience columns
    (``SW_IN_f``, ``TA_f``, ``VPD_f``, ``NEE_f``, ``NEE_fqc``, and
    ``GPP``/``RECO`` when available) AND the common-contract aliases
    (``Rg_f``, ``Tair_f``, ``Reco``). Diagnostics are attached at
    ``out.attrs["miaproc_diagnostics"]``.
    """
    # --- Python-side validation ---------------------------------------
    # Resolve the u* threshold BEFORE touching the hesseflux optional
    # dependency guard:
    #
    #   - Dynamic-mode input errors (sparse nighttime data, missing
    #     columns, scenario-not-in-probs) surface as
    #     ``DynamicUstarEstimationError`` even on machines where
    #     hesseflux is not installed. This is the M4 review's P1 fix.
    #   - Unknown ``ustar_mode`` raises ``ValueError`` from
    #     ``_resolve_ustar_threshold`` before the dependency guard.
    #   - R4 invariant: the ``"dynamic"`` branch never reads
    #     ``config.ustar_fixed``. Failure is raised, not swallowed.
    selected_threshold, dynamic_result = _resolve_ustar_threshold(df_stage1, config)

    # --- Optional dependency guard ------------------------------------
    # Fixed mode can only execute with hesseflux installed; a dynamic
    # run that reached this line has already passed Python-side
    # validation but still needs the hesseflux engine to do gap-fill +
    # partitioning.
    _require_hesseflux()

    base = df_stage1.copy()

    # Work around pandas/hesseflux compatibility issue where hesseflux
    # receives read-only NumPy views from pandas objects. The toggle is a
    # **global** pandas option, so it MUST be restored even if any engine
    # step below raises (M4 review P1 fix).
    old_cow = pd.options.mode.copy_on_write
    pd.options.mode.copy_on_write = False
    try:
        # 1) Prepare and numeric-ize
        dfin0 = _prepare_hesseflux_frame(base)

        # clamp negative radiation to 0 (your R logic)
        dfin0["SW_IN"] = dfin0["SW_IN"].where(dfin0["SW_IN"].isna() | (dfin0["SW_IN"] >= 0), 0.0)

        # 2) Build undef-filled data + flags
        dfin, dff = _build_flag_frame(dfin0, undef=config.undef)

        # 3) Day/night (SW_IN > swthr)
        isday = dfin["SW_IN"] > config.swthr

        # 4) Nighttime low-turbulence filter. Same mask logic for both
        # modes; only the threshold value changes. (Any non-zero flag
        # means "treat as missing" for gapfill.)
        ustar_mask = (
            (~isday)
            & (dfin["USTAR"] != config.undef)
            & (dfin["USTAR"] < selected_threshold)
        )
        n_filtered = int(ustar_mask.sum())
        n_finite_nee = int(((dfin["NEE"] != config.undef) & dfin["NEE"].notna()).sum())
        fraction_nee_filtered = (
            float(n_filtered) / float(n_finite_nee) if n_finite_nee > 0 else 0.0
        )
        if n_filtered > 0:
            dff.loc[ustar_mask, "NEE"] = 2

        # 5) Gap-fill meteorological drivers (SW_IN, TA, VPD) using MDS.
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

        # gapfill returns filled_data + quality_class (quality flags 1-3)
        NEE_f = df_nee_f["NEE"].replace(config.undef, np.nan)
        NEE_fqc = dff_nee_f["NEE"].replace(config.undef, np.nan)

        SW_IN_f = df_drv_f["SW_IN"].replace(config.undef, np.nan)
        TA_f_C = df_drv_f["TA"].replace(config.undef, np.nan)
        VPD_f_hPa = df_drv_f["VPD"].replace(config.undef, np.nan)

        # 7) Partition (NEE_f -> GPP, RECO). nee2gpp expects columns
        # (NEE/FC), SW_IN, TA (Kelvin), VPD (Pa).
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

        # Use NEE fill quality as flag for NEE; for drivers use their fill
        # quality. Any non-zero means ignore.
        part_flag = pd.DataFrame(
            {
                "NEE": NEE_fqc.fillna(0).astype(int),
                "SW_IN": dff_drv_f["SW_IN"].fillna(0).astype(int),
                "TA": dff_drv_f["TA"].fillna(0).astype(int),
                "VPD": dff_drv_f["VPD"].fillna(0).astype(int),
            },
            index=dfin.index,
        )

        # Default (native) path: hesseflux nee2gpp. Wrapper path
        # (reco_fit_mode="lt_reddyproc_aligned"): Lloyd-Taylor-aligned
        # Reco derivation, skipping nee2gpp entirely. No silent
        # fallback in wrapper mode.
        lt_wrapper_diag: Optional[dict[str, Any]] = None
        if config.reco_fit_mode == "native":
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
        elif config.reco_fit_mode == "lt_reddyproc_aligned":
            from .lt_reco_wrapper import (
                LTWrapperError,
                fit_lloyd_taylor,
                predict_reco,
            )

            # Nighttime high-quality rows for the LT fit. ``isday`` was
            # computed from ``dfin["SW_IN"]`` (raw SW with undef where
            # NaN), so ``~isday`` treats missing-SW rows as night —
            # consistent with the native path.
            night_mask = ~isday
            fqc_mask = NEE_fqc.fillna(-1.0) == 0.0
            fit_mask = (
                night_mask
                & fqc_mask
                & NEE_f.notna()
                & TA_f_C.notna()
            )
            nee_night = NEE_f[fit_mask].to_numpy(dtype=float)
            ta_night = TA_f_C[fit_mask].to_numpy(dtype=float)

            try:
                lt_result = fit_lloyd_taylor(
                    nee_night,
                    ta_night,
                    min_night_samples=config.lt_min_night_samples,
                )
            except LTWrapperError:
                # Propagate: no silent fallback to native in wrapper
                # mode. The caller either gets a valid LT fit or a
                # raised error — never partial partitioning.
                raise

            reco_all = predict_reco(
                TA_f_C.to_numpy(dtype=float), lt_result.rref, lt_result.e0
            )
            gpp_all = reco_all - NEE_f.to_numpy(dtype=float)
            df_part = pd.DataFrame(
                {"GPP": gpp_all, "RECO": reco_all}, index=dfin.index
            )
            lt_wrapper_diag = lt_result.to_diag()
        else:
            raise ValueError(
                f"HessefluxConfig.reco_fit_mode={config.reco_fit_mode!r} "
                "is not supported. Expected 'native' or "
                "'lt_reddyproc_aligned'."
            )

        # df_part typically contains GPP and RECO columns (and maybe more)
        # Convert undef -> NaN
        for c in list(df_part.columns):
            df_part[c] = df_part[c].replace(config.undef, np.nan)

        # 8) Attach outputs to the original dataframe.
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

        # Common-contract aliases: Rg_f, Tair_f, Reco (legacy preserved).
        _attach_common_aliases(base)

        # GPP and Reco must exist in the common contract even if
        # partitioning failed upstream. Fill missing ones with NaN so the
        # schema is stable.
        for contract_col in ("GPP", "Reco"):
            if contract_col not in base.columns:
                base[contract_col] = float("nan")

        # Diagnostics sidecar.
        diagnostics = _build_hesseflux_diagnostics(
            config,
            selected_threshold=selected_threshold,
            dynamic_result=dynamic_result,
            fraction_nee_filtered=fraction_nee_filtered,
            lt_wrapper_diag=lt_wrapper_diag,
        )
        base.attrs["miaproc_diagnostics"] = diagnostics

        return base
    finally:
        # Always restore the caller's pandas copy_on_write setting,
        # including on exception paths. Re-raising happens automatically
        # because this finally block does not swallow exceptions.
        pd.options.mode.copy_on_write = old_cow
