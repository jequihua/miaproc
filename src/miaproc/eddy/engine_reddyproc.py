"""
Optional REddyProc-through-rpy2 parity backend.

This module is a thin Python adapter around the REddyProc R package, mirroring
the call sequence in ``90_legacy_review/R/R_manglaria.R``. It is optional:
importing ``miaproc`` must not require R, REddyProc, or rpy2. Only calls to
``run_reddyproc_engine`` (or ``postproc(..., engine="reddyproc-rpy2")``)
require those dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Optional, Sequence

import pandas as pd

from .stage2 import prepare_reddyproc_input


REDDYPROC_BACKEND_NAME: str = "reddyproc-rpy2"

# Columns of the normalized backend output, per 08_pkg/backend_contract.md
# and 01_data/schema.md "Common Backend Output Fields".
REDDYPROC_OUTPUT_COLUMNS: tuple[str, ...] = (
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

# REddyProc variables passed to sEddyProc$new, matching R_manglaria.R.
REDDYPROC_INPUT_VARS: tuple[str, ...] = (
    "NEE",
    "Ustar",
    "Tair",
    "VPD",
    "Rg",
    "rH",
    "QF",
)

_INSTALL_HINT = (
    "REddyProc rpy2 backend requires optional dependencies. Install Python "
    "extra miaproc[reddyproc], install R, and install the R package "
    "REddyProc."
)


@dataclass(frozen=True)
class ReddyProcConfig:
    """Configuration for the ``reddyproc-rpy2`` backend.

    Mirrors ``R_manglaria.R`` configuration: site name, location, timezone
    hour, half-hourly grid (``dts=48``), u* scenario estimation, and scenario
    selection. ``local_tz`` is forwarded to
    ``miaproc.eddy.stage2.prepare_reddyproc_input`` so that ``Year``, ``DoY``,
    and ``Hour`` follow REddyProc's local-time calendar convention
    (see ``docs/REDDYPROC_LOCAL_TIME_POLICY.md``).

    For the Marismas Nacionales parity workflow, set::

        ReddyProcConfig(
            site_name="Marismas_Nacionales",
            latitude=22.25,
            longitude=-105.50,
            timezone_hour=-7,
            local_tz="America/Mazatlan",
        )
    """

    site_name: str = "SiteName"
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    timezone_hour: Optional[float] = None
    local_tz: Optional[str] = None
    dts: int = 48
    ustar_n_sample: int = 200
    ustar_probs: tuple[float, float, float] = (0.05, 0.5, 0.95)
    ustar_scenario: str = "U50"


class MissingOptionalDependencyError(ImportError):
    """Raised when rpy2, R, or REddyProc are required but unavailable."""


class UnsupportedScenarioError(ValueError):
    """Raised when the configured u* scenario is not present in REddyProc's output."""


def _require_rpy2_and_reddyproc() -> tuple[Any, Any]:
    """Lazy-import rpy2 and REddyProc; raise a clear actionable error if absent.

    Returns a ``(rpy2, reddyproc_r_package)`` tuple when everything is
    available. Raises ``MissingOptionalDependencyError`` otherwise.
    """
    try:
        import rpy2  # noqa: F401
        import rpy2.robjects as ro
        from rpy2.robjects.packages import importr
    except ImportError as exc:
        raise MissingOptionalDependencyError(
            f"{_INSTALL_HINT} (rpy2 import failed: {exc})"
        ) from exc

    try:
        reddyproc = importr("REddyProc")
    except Exception as exc:  # rpy2 raises a package-specific error
        raise MissingOptionalDependencyError(
            f"{_INSTALL_HINT} (loading R package 'REddyProc' failed: {exc})"
        ) from exc

    return ro, reddyproc


def _scenario_label_from_prob(prob: float) -> str:
    """Return REddyProc's scenario label for a probability, e.g. 0.5 -> 'U50'.

    REddyProc names scenarios ``U<int(prob*100)>`` with zero-padding to two
    digits. 0.05 -> 'U05', 0.5 -> 'U50', 0.95 -> 'U95'.
    """
    return f"U{int(round(prob * 100)):02d}"


def _scenarios_from_config(config: ReddyProcConfig) -> tuple[str, ...]:
    return tuple(_scenario_label_from_prob(p) for p in config.ustar_probs)


def _validate_scenario(scenario: str, available: Sequence[str]) -> None:
    if scenario not in available:
        raise UnsupportedScenarioError(
            f"Requested u* scenario {scenario!r} is not available. "
            f"Available scenarios: {list(available)}. "
            "Configure ReddyProcConfig(ustar_scenario=...) to one of these."
        )


_SITE_METADATA_FIELDS: tuple[str, ...] = ("latitude", "longitude", "timezone_hour")


def _validate_site_metadata(config: ReddyProcConfig) -> bool:
    """Enforce all-or-nothing site-metadata rule.

    Returns ``True`` if all three of ``latitude``, ``longitude``, and
    ``timezone_hour`` are supplied (and ``sSetLocationInfo`` should be
    called live). Returns ``False`` if none are supplied.

    Raises ``ValueError`` if the caller supplied a subset. This runs before
    any rpy2 import so callers with partial metadata see a Python-side
    error, not a cryptic R-side one.
    """
    present = {f: getattr(config, f) is not None for f in _SITE_METADATA_FIELDS}
    n_present = sum(present.values())
    if n_present == 0:
        return False
    if n_present == len(_SITE_METADATA_FIELDS):
        return True
    missing = [f for f, ok in present.items() if not ok]
    supplied = [f for f, ok in present.items() if ok]
    raise ValueError(
        "ReddyProcConfig site metadata must be all-or-nothing: supply "
        "latitude, longitude, AND timezone_hour together, or none of them. "
        f"Supplied: {supplied}. Missing: {missing}."
    )


def _normalize_reddyproc_output(
    stage2: pd.DataFrame,
    reddyproc_export: pd.DataFrame,
    partition_export: pd.DataFrame,
    config: ReddyProcConfig,
) -> pd.DataFrame:
    """Assemble the normalized backend output from REddyProc export frames.

    Parameters
    ----------
    stage2
        Output of ``prepare_reddyproc_input``. Contributes ``DateTime``, raw
        ``NEE``, ``Tair``, ``Rg``, ``VPD``, and ``Ustar`` (renamed to
        ``USTAR``).
    reddyproc_export
        The ``sExportResults()`` frame from the gap-filling step. Must
        contain ``NEE_{scenario}_f``, ``NEE_{scenario}_fqc``, ``Tair_f``,
        ``Rg_f``, ``VPD_f``.
    partition_export
        The ``sExportResults()`` frame from the partitioning step. Must
        contain ``GPP_DT`` and ``Reco_DT`` from Lasslop day-time
        partitioning.
    config
        Backend configuration (used for scenario selection).

    Returns
    -------
    pd.DataFrame
        Frame with columns ``REDDYPROC_OUTPUT_COLUMNS``, in that order, with
        length equal to ``len(stage2)``.

    Raises
    ------
    UnsupportedScenarioError
        If ``NEE_{config.ustar_scenario}_f`` is absent from the gap-fill
        export.
    ValueError
        If row counts mismatch, or if any other required export column is
        missing. The message lists every missing column from both the
        gap-fill and partition exports in one shot so malformed inputs do
        not require multiple iterations to diagnose.
    """
    scenario = config.ustar_scenario
    nee_f_col = f"NEE_{scenario}_f"
    nee_fqc_col = f"NEE_{scenario}_fqc"

    available_scenarios = sorted(
        {
            c.split("_")[1]
            for c in reddyproc_export.columns
            if c.startswith("NEE_") and c.endswith("_f") and "_" in c[4:]
        }
    )
    # Scenario selection error comes first so callers who asked for the
    # wrong scenario get a purpose-built message rather than a generic
    # missing-columns list.
    if nee_f_col not in reddyproc_export.columns:
        _validate_scenario(scenario, available_scenarios)

    n = len(stage2)
    if len(reddyproc_export) != n or len(partition_export) != n:
        raise ValueError(
            "REddyProc export row counts do not match stage-2 input. "
            f"stage2={n}, reddyproc_export={len(reddyproc_export)}, "
            f"partition_export={len(partition_export)}."
        )

    required_gapfill: tuple[str, ...] = (
        nee_fqc_col,
        "Tair_f",
        "Rg_f",
        "VPD_f",
    )
    required_partition: tuple[str, ...] = ("GPP_DT", "Reco_DT")
    missing_gapfill = [
        c for c in required_gapfill if c not in reddyproc_export.columns
    ]
    missing_partition = [
        c for c in required_partition if c not in partition_export.columns
    ]
    if missing_gapfill or missing_partition:
        parts: list[str] = []
        if missing_gapfill:
            parts.append(f"gap-fill export missing: {missing_gapfill}")
        if missing_partition:
            parts.append(f"partition export missing: {missing_partition}")
        raise ValueError(
            "REddyProc export missing required columns for common backend "
            "output. " + "; ".join(parts) + "."
        )

    out = pd.DataFrame(
        {
            "DateTime": stage2["DateTime"].to_numpy(),
            "NEE": pd.to_numeric(stage2["NEE"], errors="coerce").to_numpy(),
            "NEE_f": pd.to_numeric(
                reddyproc_export[nee_f_col], errors="coerce"
            ).to_numpy(),
            "NEE_fqc": pd.to_numeric(
                reddyproc_export[nee_fqc_col], errors="coerce"
            ).to_numpy(),
            "GPP": pd.to_numeric(
                partition_export["GPP_DT"], errors="coerce"
            ).to_numpy(),
            "Reco": pd.to_numeric(
                partition_export["Reco_DT"], errors="coerce"
            ).to_numpy(),
            "Tair": pd.to_numeric(stage2["Tair"], errors="coerce").to_numpy(),
            "Tair_f": pd.to_numeric(
                reddyproc_export["Tair_f"], errors="coerce"
            ).to_numpy(),
            "Rg": pd.to_numeric(stage2["Rg"], errors="coerce").to_numpy(),
            "Rg_f": pd.to_numeric(
                reddyproc_export["Rg_f"], errors="coerce"
            ).to_numpy(),
            "VPD": pd.to_numeric(stage2["VPD"], errors="coerce").to_numpy(),
            "VPD_f": pd.to_numeric(
                reddyproc_export["VPD_f"], errors="coerce"
            ).to_numpy(),
            "USTAR": pd.to_numeric(stage2["Ustar"], errors="coerce").to_numpy(),
        }
    )
    return out.loc[:, list(REDDYPROC_OUTPUT_COLUMNS)]


def _ustar_diagnostics_from_scenarios(
    ustar_scenarios: Optional[pd.DataFrame],
    config: ReddyProcConfig,
) -> dict[str, Any]:
    """Summarize a REddyProc u* scenario table for the diagnostics sidecar.

    REddyProc's ``sGetUstarScenarios()`` returns a data frame whose first
    column is the season label and whose remaining columns are the
    scenario threshold columns (one per u* probability, named after the
    scenario suffixes e.g. ``U05``, ``U50``, ``U95``). See
    ``90_legacy_review/REddyProc-master/R/aEddy.R`` function
    ``sEddyProc_sGetUstarScenarios``.

    Parameters
    ----------
    ustar_scenarios
        The scenario data frame converted to pandas, or ``None`` /
        empty if unavailable.
    config
        Backend configuration. ``config.ustar_scenario`` is used to decide
        whether a single numeric threshold can be reported.

    Returns
    -------
    dict
        Keys:

        - ``available_scenarios``: tuple of scenario column names
          (everything except the first/season column). Empty tuple if the
          scenario table is ``None`` or empty.
        - ``selected_threshold``: ``float`` when the configured scenario
          column has exactly one finite unique value; otherwise ``None``.
        - ``thresholds_by_season``: JSON-serializable list of dicts
          ``{season, U05, U50, ...}`` preserving the season-indexed
          threshold view. Empty tuple if the scenario table is
          unavailable.
    """
    empty: dict[str, Any] = {
        "available_scenarios": (),
        "selected_threshold": None,
        "thresholds_by_season": (),
    }
    if ustar_scenarios is None:
        return empty
    if not isinstance(ustar_scenarios, pd.DataFrame):
        return empty
    if ustar_scenarios.empty or ustar_scenarios.shape[1] < 2:
        return empty

    season_col = ustar_scenarios.columns[0]
    available = tuple(str(c) for c in ustar_scenarios.columns[1:])

    scenario = config.ustar_scenario
    selected_threshold: Optional[float] = None
    if scenario in available:
        values = pd.to_numeric(ustar_scenarios[scenario], errors="coerce")
        finite = values.dropna()
        unique_vals = finite.unique()
        if len(unique_vals) == 1:
            selected_threshold = float(unique_vals[0])

    records: list[dict[str, Any]] = []
    for _, row in ustar_scenarios.iterrows():
        record: dict[str, Any] = {"season": _json_safe(row[season_col])}
        for s in available:
            record[s] = _json_safe(row[s])
        records.append(record)

    return {
        "available_scenarios": available,
        "selected_threshold": selected_threshold,
        "thresholds_by_season": tuple(records),
    }


def _json_safe(value: Any) -> Any:
    """Coerce a scenario-table cell into a JSON-serializable scalar.

    All missing-value sentinels (``None``, ``pd.NA``, ``pd.NaT``, ``np.nan``)
    return ``None``. Numpy/pandas scalars are unboxed via ``.item()``. Plain
    Python scalars pass through. Anything else is rendered with ``str()`` as
    a last resort.
    """
    if value is None:
        return None
    # pd.isna handles pd.NA, pd.NaT, np.nan, and numpy NaN-bearing scalars
    # uniformly. It can raise for array-like inputs; we only pass scalars
    # here, but guard the call for safety.
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    # pandas/numpy scalar -> native python. After unboxing, re-check for NaN
    # because a numpy float containing NaN becomes a Python float NaN.
    try:
        result = value.item()  # type: ignore[union-attr]
    except (AttributeError, ValueError):
        result = value
    if isinstance(result, float) and result != result:  # NaN self-inequality
        return None
    if isinstance(result, (int, float, str, bool)):
        return result
    return str(result)


def _build_diagnostics(
    config: ReddyProcConfig,
    *,
    available_scenarios: Sequence[str] = (),
    selected_threshold: Optional[float] = None,
    thresholds_by_season: Sequence[dict[str, Any]] = (),
    r_version: Optional[str] = None,
    reddyproc_version: Optional[str] = None,
    rpy2_version: Optional[str] = None,
    warnings_: Sequence[str] = (),
) -> dict[str, Any]:
    """Assemble the diagnostics sidecar attached to ``df.attrs``."""
    return {
        "backend": REDDYPROC_BACKEND_NAME,
        "site": {
            "site_name": config.site_name,
            "latitude": config.latitude,
            "longitude": config.longitude,
            "timezone_hour": config.timezone_hour,
            "local_tz": config.local_tz,
        },
        "dts": config.dts,
        "ustar": {
            "mode": "dynamic",
            "probs": tuple(config.ustar_probs),
            "n_sample": config.ustar_n_sample,
            "scenario": config.ustar_scenario,
            "available_scenarios": tuple(available_scenarios),
            "selected_threshold": selected_threshold,
            "thresholds_by_season": tuple(thresholds_by_season),
        },
        "partitioning": "lasslop",
        "versions": {
            "R": r_version,
            "REddyProc": reddyproc_version,
            "rpy2": rpy2_version,
        },
        "warnings": tuple(warnings_),
    }


def run_reddyproc_engine(
    df_stage1: pd.DataFrame,
    *,
    config: ReddyProcConfig = ReddyProcConfig(),
) -> pd.DataFrame:
    """Run the REddyProc-through-rpy2 backend.

    The flow mirrors ``R_manglaria.R``:

    1. Build REddyProc-style stage-2 input using
       ``prepare_reddyproc_input(..., local_tz=config.local_tz)``.
    2. Instantiate ``sEddyProc`` with ``REDDYPROC_INPUT_VARS`` and
       ``DTS=config.dts``.
    3. Set site location metadata.
    4. Estimate u* scenarios (``nSample=config.ustar_n_sample``,
       ``probs=config.ustar_probs``).
    5. MDS gap-fill ``Rg``, ``VPD``, ``Tair``.
    6. MDS gap-fill ``NEE`` across u* scenarios.
    7. Select ``config.ustar_scenario`` and build the Lasslop partitioning
       input (``NEE_f``, ``NEE_fsd``, ``NEE_fqc``, ``Tair_f``, ``Tair_fsd``,
       ``Tair_fqc``, ``Rg_f``, ``Rg_fsd``, ``Rg_fqc``, ``VPD_f``).
    8. Run ``sGLFluxPartition``.
    9. Normalize output to ``REDDYPROC_OUTPUT_COLUMNS``, attach diagnostics to
       ``df.attrs["miaproc_diagnostics"]``.

    Raises
    ------
    MissingOptionalDependencyError
        If rpy2, R, or REddyProc are unavailable.
    UnsupportedScenarioError
        If ``config.ustar_scenario`` is not among the estimated scenarios.
    """
    # Stage-2 preparation runs first and is validated here rather than inside
    # REddyProc: if the caller's stage-1 frame is malformed we want the error
    # to surface before touching R.
    stage2 = prepare_reddyproc_input(df_stage1, local_tz=config.local_tz)

    # Scenario must be consistent with configured probs before any R work.
    scenarios = _scenarios_from_config(config)
    _validate_scenario(config.ustar_scenario, scenarios)

    # Site metadata is all-or-nothing; partial configuration is a Python-side
    # error surfaced before any rpy2 import.
    set_location = _validate_site_metadata(config)

    # Optional deps. In a Python-only environment this is where execution
    # stops with a clear actionable error. Importing REddyProc also has the
    # side effect of attaching ``sEddyProc`` to R's global namespace, which
    # is what ``ro.r("sEddyProc$new")`` resolves below.
    ro, _reddyproc = _require_rpy2_and_reddyproc()

    # -------- REddyProc call sequence (R parity) --------
    # This section is only exercised when R, REddyProc, and rpy2 are
    # installed. Default Python-only tests cover everything up to the
    # _require_rpy2_and_reddyproc() call and the normalization helper in
    # isolation; they do NOT exercise the live R calls below. The code below
    # mirrors R_manglaria.R structurally, but exact rpy2 syntax for R6
    # method dispatch should be verified against a live environment during
    # the @pytest.mark.reddyproc smoke test and the Gate M3 review.
    from rpy2.robjects import pandas2ri  # lazy
    from rpy2.robjects.conversion import localconverter  # lazy

    with localconverter(ro.default_converter + pandas2ri.converter):
        r_stage2 = ro.conversion.py2rpy(stage2)

    # REddyProc's sEddyProc is an R6 class. Use the R-side ``$new`` accessor
    # for R6 instantiation to avoid attribute-name ambiguity in rpy2.
    s_eddy_proc_new = ro.r("sEddyProc$new")
    eddy_proc = s_eddy_proc_new(
        config.site_name,
        r_stage2,
        ro.StrVector(list(REDDYPROC_INPUT_VARS)),
        DTS=config.dts,
    )

    dollar = ro.r("`$`")

    def _call_method(obj: Any, method: str, *args: Any, **kwargs: Any) -> Any:
        """Dispatch an R6 method via R's ``$`` accessor for maximum portability.

        ``ro.r("$")`` is a parse error because ``$`` alone is not a valid R
        expression; the operator must be accessed via its backtick-quoted
        identifier form ``` `$` ``` so R's parser treats it as a name.
        """
        return dollar(obj, method)(*args, **kwargs)

    if set_location:
        _call_method(
            eddy_proc,
            "sSetLocationInfo",
            LatDeg=float(config.latitude),
            LongDeg=float(config.longitude),
            TimeZoneHour=float(config.timezone_hour),
        )

    _call_method(
        eddy_proc,
        "sEstimateUstarScenarios",
        nSample=config.ustar_n_sample,
        probs=ro.FloatVector(list(config.ustar_probs)),
    )

    # Capture the scenario threshold table before gap-filling. Extraction
    # failure must never abort the backend: fall back to the gap-fill
    # column-name heuristic for ``available_scenarios`` and record a
    # warning in the diagnostics sidecar.
    ustar_scenarios_df: Optional[pd.DataFrame] = None
    ustar_diag_warnings: list[str] = []
    try:
        r_scenarios = _call_method(eddy_proc, "sGetUstarScenarios")
        with localconverter(ro.default_converter + pandas2ri.converter):
            ustar_scenarios_df = ro.conversion.rpy2py(r_scenarios)
    except Exception as exc:  # noqa: BLE001
        ustar_diag_warnings.append(
            f"sGetUstarScenarios() extraction failed: {exc!r}"
        )

    _call_method(eddy_proc, "sMDSGapFill", "Rg", FillAll=True)
    _call_method(eddy_proc, "sMDSGapFill", "VPD", FillAll=True)
    _call_method(eddy_proc, "sMDSGapFill", "Tair", FillAll=True)
    _call_method(eddy_proc, "sMDSGapFillUStarScens", "NEE", FillAll=True)

    with localconverter(ro.default_converter + pandas2ri.converter):
        gapfill_export: pd.DataFrame = ro.conversion.rpy2py(
            _call_method(eddy_proc, "sExportResults")
        )

    # Build the Lasslop partitioning input as in R_manglaria.R, using the
    # configured scenario's filled columns.
    scenario = config.ustar_scenario
    required_gapfill_cols = [
        f"NEE_{scenario}_f",
        f"NEE_{scenario}_fsd",
        f"NEE_{scenario}_fqc",
        "Tair_f",
        "Tair_fsd",
        "Tair_fqc",
        "Rg_f",
        "Rg_fsd",
        "Rg_fqc",
        "VPD_f",
    ]
    missing_from_export = [
        c for c in required_gapfill_cols if c not in gapfill_export.columns
    ]
    if missing_from_export:
        raise ValueError(
            "REddyProc export missing expected columns for Lasslop "
            f"partitioning: {missing_from_export}"
        )

    part_input = pd.DataFrame(
        {
            "DateTime": stage2["DateTime"].to_numpy(),
            "Year": stage2["Year"].to_numpy(),
            "DoY": stage2["DoY"].to_numpy(),
            "Hour": stage2["Hour"].to_numpy(),
            "NEE_f": gapfill_export[f"NEE_{scenario}_f"].to_numpy(),
            "NEE_fsd": gapfill_export[f"NEE_{scenario}_fsd"].to_numpy(),
            "NEE_fqc": gapfill_export[f"NEE_{scenario}_fqc"].to_numpy(),
            "Tair_f": gapfill_export["Tair_f"].to_numpy(),
            "Tair_fsd": gapfill_export["Tair_fsd"].to_numpy(),
            "Tair_fqc": gapfill_export["Tair_fqc"].to_numpy(),
            "Rg_f": gapfill_export["Rg_f"].to_numpy(),
            "Rg_fsd": gapfill_export["Rg_fsd"].to_numpy(),
            "Rg_fqc": gapfill_export["Rg_fqc"].to_numpy(),
            "VPD_f": gapfill_export["VPD_f"].to_numpy(),
        }
    )

    with localconverter(ro.default_converter + pandas2ri.converter):
        r_part_input = ro.conversion.py2rpy(part_input)

    eddy_partition = s_eddy_proc_new(
        config.site_name,
        r_part_input,
        ro.StrVector(
            [
                "NEE_f",
                "NEE_fsd",
                "NEE_fqc",
                "Tair_f",
                "Tair_fsd",
                "Tair_fqc",
                "Rg_f",
                "Rg_fsd",
                "Rg_fqc",
                "VPD_f",
            ]
        ),
        DTS=config.dts,
    )
    if set_location:
        _call_method(
            eddy_partition,
            "sSetLocationInfo",
            LatDeg=float(config.latitude),
            LongDeg=float(config.longitude),
            TimeZoneHour=float(config.timezone_hour),
        )

    _call_method(
        eddy_partition,
        "sGLFluxPartition",
        NEEVar="NEE_f",
        QFNEEVar="NEE_fqc",
        TempVar="Tair_f",
        QFTempVar="Tair_fqc",
        RadVar="Rg_f",
        QFRadVar="Rg_fqc",
    )

    with localconverter(ro.default_converter + pandas2ri.converter):
        partition_export: pd.DataFrame = ro.conversion.rpy2py(
            _call_method(eddy_partition, "sExportResults")
        )

    normalized = _normalize_reddyproc_output(
        stage2, gapfill_export, partition_export, config
    )

    # Best-effort version strings; failures here must never break the run.
    def _safe_r_version() -> Optional[str]:
        try:
            return str(ro.r("paste(R.version$major, R.version$minor, sep='.')")[0])
        except Exception:
            return None

    def _safe_pkg_version(pkg: str) -> Optional[str]:
        try:
            return str(ro.r(f"as.character(packageVersion('{pkg}'))")[0])
        except Exception:
            return None

    rpy2_version: Optional[str]
    try:
        import rpy2 as _rpy2

        rpy2_version = getattr(_rpy2, "__version__", None)
    except Exception:
        rpy2_version = None

    ustar_diag = _ustar_diagnostics_from_scenarios(ustar_scenarios_df, config)
    # Fallback: if the scenario table was unavailable or empty, derive the
    # scenario set from the gap-fill export column names. Does not yield a
    # numeric threshold, only the scenario labels.
    if not ustar_diag["available_scenarios"]:
        if ustar_scenarios_df is not None:
            # Extraction succeeded but returned an empty/season-only frame;
            # make the degradation visible in diagnostics rather than
            # silently falling back.
            ustar_diag_warnings.append(
                "sGetUstarScenarios() returned empty or season-only frame; "
                "falling back to gap-fill column scenario labels."
            )
        fallback_scenarios = tuple(
            sorted(
                {
                    c.split("_")[1]
                    for c in gapfill_export.columns
                    if c.startswith("NEE_")
                    and c.endswith("_f")
                    and "_" in c[4:]
                }
            )
        )
        ustar_diag = {
            "available_scenarios": fallback_scenarios,
            "selected_threshold": None,
            "thresholds_by_season": (),
        }

    normalized.attrs["miaproc_diagnostics"] = _build_diagnostics(
        config,
        available_scenarios=ustar_diag["available_scenarios"],
        selected_threshold=ustar_diag["selected_threshold"],
        thresholds_by_season=ustar_diag["thresholds_by_season"],
        r_version=_safe_r_version(),
        reddyproc_version=_safe_pkg_version("REddyProc"),
        rpy2_version=rpy2_version,
        warnings_=tuple(ustar_diag_warnings),
    )
    return normalized


def config_to_dict(config: ReddyProcConfig) -> dict[str, Any]:
    """Return ``config`` as a plain dict (useful for diagnostics/serialization)."""
    return asdict(config)
