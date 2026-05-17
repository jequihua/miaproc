"""
Data-driven u* threshold estimation for the portable Python (hesseflux) backend.

This module implements a REddyProc-inspired seasonal-agnostic variant of the
nighttime plateau test described in
``90_legacy_review/REddyProc-master/R/EddyUStarFilterDP.R``:

1. Select nighttime records (``Rg <= swthr``).
2. Bin by temperature into ``ustar_temp_bins`` equal-count quantile bins.
3. Inside each temperature bin, bin by u* into ``ustar_bins`` equal-count
   quantile classes.
4. Estimate the plateau reference as the mean NEE over the top u* class of the
   temperature bin.
5. Find the lowest u* class whose mean NEE reaches
   ``plateau_fraction * plateau_reference``. That class's **upper u* edge** is
   the candidate threshold for this temperature bin.
6. Aggregate valid candidates across temperature bins with
   ``config.ustar_probs`` to produce scenario thresholds (``U05``, ``U50``,
   ``U95`` by default).

The implementation is deliberately small and testable. It is **not** a
byte-for-byte REddyProc port — scientific parity is Milestone 5's job — but it
is deterministic, honest about sparse-data failure, and rich in diagnostics so
M5 has something to compare against.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

import numpy as np
import pandas as pd


class DynamicUstarEstimationError(ValueError):
    """Raised when the dynamic u* estimator cannot produce a threshold.

    The message names the failing precondition (missing columns, too few
    nighttime samples, no candidates after binning, all-NaN plateau) so the
    caller can act without reading the estimator source.
    """


# Public column requirements for the estimator. Kept as a module-level
# constant so callers and tests can reference a single source of truth.
REQUIRED_INPUT_COLUMNS: tuple[str, ...] = (
    "DateTime",
    "NEE",
    "USTAR",
    "Tair",
    "Rg",
)


@dataclass(frozen=True)
class DynamicUstarResult:
    """Structured return from ``estimate_dynamic_ustar_thresholds``.

    ``thresholds_by_scenario`` maps each scenario label (``U05``, ``U50``,
    ``U95`` by default) to the quantile across temperature-bin candidates.
    ``thresholds_by_season`` is the per-temperature-bin candidate record,
    using the bin index as a pseudo-"season" label so the shape matches the
    REddyProc backend's diagnostics convention.
    """

    available_scenarios: tuple[str, ...]
    selected_scenario: str
    selected_threshold: float
    thresholds_by_scenario: dict[str, float]
    thresholds_by_season: tuple[dict[str, Any], ...]
    night_sample_count: int
    method: str
    warnings: tuple[str, ...] = field(default_factory=tuple)


def _scenario_label_from_prob(prob: float) -> str:
    """Match the REddyProc naming: 0.05 -> 'U05', 0.5 -> 'U50', 0.95 -> 'U95'."""
    return f"U{int(round(prob * 100)):02d}"


def _quantile_bin_edges(values: np.ndarray, n_bins: int) -> Optional[np.ndarray]:
    """Return ``n_bins + 1`` edges defined by equal-count quantiles.

    Returns ``None`` if there are too few distinct values to form ``n_bins``
    non-degenerate bins.
    """
    finite = values[np.isfinite(values)]
    if finite.size < n_bins:
        return None
    quantiles = np.linspace(0.0, 1.0, n_bins + 1)
    edges = np.quantile(finite, quantiles)
    # Ensure strictly increasing edges (ties are common at u*=0).
    edges = np.unique(edges)
    if edges.size < 2:
        return None
    return edges


def _assign_bin(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Assign each value to a half-open bin ``[edges[i], edges[i+1])``.

    Returns integer bin indices in ``[0, len(edges) - 2]`` for values inside
    the edge range; values outside become ``-1``. The last edge is treated
    as inclusive so the maximum is not silently dropped.
    """
    n_bins = len(edges) - 1
    # np.digitize gives 1..n_bins for values in-range; 0 for < first edge,
    # n_bins+1 for > last edge. Subtract 1 to get 0..n_bins-1.
    raw = np.digitize(values, edges, right=False) - 1
    # Clip the exact-last-value case: digitize(x=last_edge) returns n_bins,
    # which becomes n_bins-1 only after the max-inclusive fix.
    raw = np.where(values == edges[-1], n_bins - 1, raw)
    in_range = (raw >= 0) & (raw < n_bins) & np.isfinite(values)
    out = np.where(in_range, raw, -1)
    return out.astype(np.int64)


def _plateau_threshold_for_bin(
    ustar_edges: np.ndarray,
    nee_by_class: np.ndarray,
    plateau_fraction: float,
) -> Optional[float]:
    """Given per-class mean NEE in ascending u* order, find the threshold.

    The plateau reference is the class with the highest u* that has a finite
    mean NEE. Walking from the lowest u* class upward, the first class whose
    mean NEE reaches ``plateau_fraction * plateau_reference`` is the
    crossover; its **upper u* edge** is the candidate threshold.

    Returns ``None`` if no candidate exists (all-NaN NEE, or plateau never
    reached).
    """
    finite = np.isfinite(nee_by_class)
    if not finite.any():
        return None
    # Plateau reference: the highest-u* class with finite NEE.
    last_finite_idx = int(np.where(finite)[0][-1])
    plateau_ref = float(nee_by_class[last_finite_idx])
    if not np.isfinite(plateau_ref):
        return None
    # Target in the reference's sign: plateau_fraction * |ref| but with the
    # ref's sign so the comparison is orientation-agnostic.
    target = plateau_fraction * abs(plateau_ref)
    for i in range(last_finite_idx + 1):
        val = nee_by_class[i]
        if not np.isfinite(val):
            continue
        if abs(val) >= target:
            # upper edge of this u* class
            upper_edge = float(ustar_edges[i + 1])
            return upper_edge
    return None


def estimate_dynamic_ustar_thresholds(
    df: pd.DataFrame,
    *,
    ustar_probs: Sequence[float] = (0.05, 0.5, 0.95),
    ustar_scenario: str = "U50",
    ustar_min_night_samples: int = 100,
    ustar_temp_bins: int = 4,
    ustar_bins: int = 20,
    ustar_plateau_fraction: float = 0.95,
    swthr: float = 10.0,
) -> DynamicUstarResult:
    """Estimate u* threshold scenarios from a stage-1 eddy frame.

    Returns a :class:`DynamicUstarResult`. Raises
    :class:`DynamicUstarEstimationError` when estimation cannot produce a
    threshold for the requested scenario.

    The algorithm is deterministic given the same input and configuration
    — no RNG is used. ``DateTime`` is required only for the diagnostics
    frame and does not affect thresholds.
    """
    missing = [c for c in REQUIRED_INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise DynamicUstarEstimationError(
            "Dynamic u* estimator requires columns "
            f"{list(REQUIRED_INPUT_COLUMNS)}; missing: {missing}."
        )

    warnings_: list[str] = []

    # Coerce to numeric and select nighttime finite rows.
    work = df.loc[:, ["NEE", "USTAR", "Tair", "Rg"]].apply(
        pd.to_numeric, errors="coerce"
    )
    night_mask = (work["Rg"] <= swthr) & work[["NEE", "USTAR", "Tair"]].notna().all(axis=1)
    night = work.loc[night_mask]
    night_sample_count = int(len(night))

    if night_sample_count < int(ustar_min_night_samples):
        raise DynamicUstarEstimationError(
            f"Dynamic u* estimator: only {night_sample_count} nighttime "
            f"finite samples; need >= {ustar_min_night_samples}. Increase "
            "the observation window or relax ustar_min_night_samples."
        )

    tair_edges = _quantile_bin_edges(night["Tair"].to_numpy(), int(ustar_temp_bins))
    if tair_edges is None:
        raise DynamicUstarEstimationError(
            "Dynamic u* estimator: not enough distinct Tair values to form "
            f"{ustar_temp_bins} temperature bins."
        )

    tair_bin = _assign_bin(night["Tair"].to_numpy(), tair_edges)

    candidates: list[float] = []
    records: list[dict[str, Any]] = []
    for b in range(len(tair_edges) - 1):
        sel = tair_bin == b
        bin_night = night.loc[sel]
        record: dict[str, Any] = {
            "temp_bin": int(b),
            "temp_lower": float(tair_edges[b]),
            "temp_upper": float(tair_edges[b + 1]),
            "night_sample_count": int(len(bin_night)),
            "threshold": None,
        }
        if len(bin_night) < int(ustar_bins):
            warnings_.append(
                f"temp_bin={b} has {len(bin_night)} samples; need >= "
                f"{ustar_bins}. Skipped."
            )
            records.append(record)
            continue

        ustar_edges = _quantile_bin_edges(
            bin_night["USTAR"].to_numpy(), int(ustar_bins)
        )
        if ustar_edges is None or ustar_edges.size < 3:
            warnings_.append(
                f"temp_bin={b}: not enough distinct USTAR values. Skipped."
            )
            records.append(record)
            continue

        n_classes = len(ustar_edges) - 1
        ustar_class = _assign_bin(bin_night["USTAR"].to_numpy(), ustar_edges)

        nee_by_class = np.full(n_classes, np.nan)
        for c in range(n_classes):
            mask = ustar_class == c
            if mask.any():
                nee_by_class[c] = float(
                    np.nanmean(bin_night["NEE"].to_numpy()[mask])
                )

        candidate = _plateau_threshold_for_bin(
            ustar_edges, nee_by_class, float(ustar_plateau_fraction)
        )
        record["threshold"] = None if candidate is None else float(candidate)
        records.append(record)
        if candidate is not None and np.isfinite(candidate):
            candidates.append(float(candidate))

    if not candidates:
        raise DynamicUstarEstimationError(
            "Dynamic u* estimator: no valid threshold candidates across "
            "temperature bins. Input is likely too sparse or the nighttime "
            "NEE-u* relationship is too noisy for the plateau test."
        )

    candidates_arr = np.asarray(candidates, dtype=float)
    available_scenarios = tuple(_scenario_label_from_prob(p) for p in ustar_probs)
    thresholds_by_scenario: dict[str, float] = {
        label: float(np.quantile(candidates_arr, prob))
        for label, prob in zip(available_scenarios, ustar_probs)
    }

    if ustar_scenario not in thresholds_by_scenario:
        raise DynamicUstarEstimationError(
            f"Requested ustar_scenario={ustar_scenario!r} is not produced by "
            f"ustar_probs={tuple(ustar_probs)} "
            f"(available: {available_scenarios}). Adjust configuration."
        )

    selected_threshold = thresholds_by_scenario[ustar_scenario]

    return DynamicUstarResult(
        available_scenarios=available_scenarios,
        selected_scenario=ustar_scenario,
        selected_threshold=float(selected_threshold),
        thresholds_by_scenario=thresholds_by_scenario,
        thresholds_by_season=tuple(records),
        night_sample_count=night_sample_count,
        method="hesseflux-plateau-v1",
        warnings=tuple(warnings_),
    )
