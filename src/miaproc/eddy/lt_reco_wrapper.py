"""Optional Lloyd-Taylor alignment wrapper for hesseflux Reco (Coding Prompt 022).

Provides an opt-in Reco parameterization path that mirrors REddyProc's
standard Lloyd-Taylor convention (Tref = 15 °C, T0 = -46.02 °C), so that
the hesseflux backend can produce a Reco series aligned with the
REddyProc reference when callers explicitly request it.

Public surface:

- :func:`fit_lloyd_taylor` — deterministic Rref/E0 fit on nighttime NEE.
- :func:`predict_reco` — Reco prediction from fitted parameters over all rows.
- :class:`LTFitResult` — frozen dataclass carrying fit outputs + diagnostics.
- :class:`LTWrapperError` — raised on any unrecoverable fit condition.

No silent fallback. In wrapper mode the caller must either get a valid
:class:`LTFitResult` or a raised :class:`LTWrapperError` — never a
silent revert to native partitioning. This is the explicit contract per
Coding Prompt 022.

Isolation: this module contains only the LT math and guards. It does
**not** import from ``engine_hesseflux`` or orchestrate the engine
pipeline. The engine's wrapper branch composes calls into these
helpers.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np


# REddyProc Lloyd-Taylor constants. Tref = 15 °C; T0 = -46.02 °C
# (Reichstein et al. 2005, Lloyd & Taylor 1994). Using Celsius input
# keeps the functional form identical to the Kelvin form because both
# numerators and denominators cancel the 273.15 offset.
LT_TREF_C: float = 15.0
LT_T0_C: float = -46.02

# Fit parameter bounds. These are conservative literature ranges
# appropriate for a wide class of ecosystems; the fit is not expected
# to push up against them on well-behaved nighttime data.
LT_RREF_LOWER: float = 0.0
LT_RREF_UPPER: float = 50.0
LT_E0_LOWER: float = 0.0
LT_E0_UPPER: float = 500.0

# Deterministic initial guess — no random sampling in this module.
LT_RREF_INIT: float = 2.0
LT_E0_INIT: float = 200.0

# Temperature domain guard. Lloyd-Taylor's (T - T0) denominator must
# be strictly positive; we require a safety margin above T0 to avoid
# near-singular evaluations at the fit boundary.
LT_T_DOMAIN_MIN_C: float = LT_T0_C + 1.0

# Closeness-to-boundary tolerance. Fits that terminate at a parameter
# bound indicate a poorly-identified solution and are rejected.
LT_BOUNDARY_TOL: float = 1e-6


class LTWrapperError(ValueError):
    """Raised when the Lloyd-Taylor wrapper cannot produce a valid Reco series.

    The wrapper mode must not silently fall back to native partitioning
    on failure — callers see this exception and decide.
    """


@dataclass(frozen=True)
class LTFitResult:
    """Structured return from :func:`fit_lloyd_taylor`.

    ``to_diag()`` produces the JSON-safe dict the engine attaches under
    ``diagnostics.partitioning.lt_wrapper``.
    """

    rref: float
    e0: float
    n_night_samples: int
    night_fit_rmse: float
    fit_status: str
    tref_c: float = LT_TREF_C
    t0_c: float = LT_T0_C

    def to_diag(self) -> dict[str, Any]:
        return {
            "fit_status": self.fit_status,
            "n_night_samples": int(self.n_night_samples),
            "rref": float(self.rref),
            "e0": float(self.e0),
            "night_fit_rmse": float(self.night_fit_rmse),
            "tref_c": float(self.tref_c),
            "t0_c": float(self.t0_c),
            "bounds": {
                "rref_lower": LT_RREF_LOWER,
                "rref_upper": LT_RREF_UPPER,
                "e0_lower": LT_E0_LOWER,
                "e0_upper": LT_E0_UPPER,
            },
        }


def _lt_reco(t_c: np.ndarray, rref: float, e0: float) -> np.ndarray:
    """Lloyd-Taylor respiration function.

    ``Reco(T) = Rref * exp(E0 * (1/(Tref - T0) - 1/(T - T0)))``

    Input ``t_c`` is temperature in Celsius. Caller is responsible for
    ensuring ``t_c > LT_T_DOMAIN_MIN_C`` — this low-level function does
    not guard the domain.
    """
    denom = t_c - LT_T0_C
    return rref * np.exp(e0 * (1.0 / (LT_TREF_C - LT_T0_C) - 1.0 / denom))


def fit_lloyd_taylor(
    nee_night: np.ndarray,
    tair_night_c: np.ndarray,
    *,
    min_night_samples: int = 500,
) -> LTFitResult:
    """Fit ``(Rref, E0)`` to nighttime NEE vs Tair (°C) pairs.

    Deterministic: fixed initial guess, fixed bounds, no random
    sampling. Uses ``scipy.optimize.curve_fit`` with the Trust Region
    Reflective algorithm (``method="trf"``, selected automatically when
    bounds are provided).

    Parameters
    ----------
    nee_night
        Nighttime NEE values (one per row) in the standard eddy-flux
        sign convention (positive = ecosystem source). At night
        ``NEE ≈ Reco``, so this is the fit target.
    tair_night_c
        Air temperature in Celsius at the same rows.
    min_night_samples
        Minimum paired finite nighttime rows required to attempt the
        fit.

    Raises
    ------
    LTWrapperError
        If inputs are mis-shaped, too sparse, have invalid temperature
        domain, the optimizer raises, or the fit terminates at a
        parameter boundary.
    """
    nee = np.asarray(nee_night, dtype=float)
    tair = np.asarray(tair_night_c, dtype=float)
    if nee.shape != tair.shape:
        raise LTWrapperError(
            "shape mismatch between nee_night and tair_night_c: "
            f"{nee.shape} vs {tair.shape}"
        )
    if nee.ndim != 1:
        raise LTWrapperError(
            f"inputs must be 1-D; got nee.ndim={nee.ndim}"
        )

    finite_mask = np.isfinite(nee) & np.isfinite(tair)
    n = int(finite_mask.sum())
    if n < int(min_night_samples):
        raise LTWrapperError(
            f"insufficient paired-finite nighttime samples: got {n}, "
            f"need >= {int(min_night_samples)}"
        )

    nee_f = nee[finite_mask]
    tair_f = tair[finite_mask]

    if np.any(tair_f <= LT_T_DOMAIN_MIN_C):
        raise LTWrapperError(
            "nighttime Tair contains values at or below the Lloyd-Taylor "
            f"domain floor ({LT_T_DOMAIN_MIN_C} °C); refusing to fit"
        )

    # scipy is a hard dep of the scientific stack already in use.
    from scipy.optimize import curve_fit

    try:
        popt, _pcov = curve_fit(
            _lt_reco,
            tair_f,
            nee_f,
            p0=[LT_RREF_INIT, LT_E0_INIT],
            bounds=([LT_RREF_LOWER, LT_E0_LOWER], [LT_RREF_UPPER, LT_E0_UPPER]),
            maxfev=10_000,
        )
    except Exception as exc:
        raise LTWrapperError(
            f"scipy.optimize.curve_fit failed for Lloyd-Taylor fit: {exc!r}"
        ) from exc

    rref, e0 = float(popt[0]), float(popt[1])

    # Reject boundary-touching fits: an optimum at the constraint
    # boundary means the parameter is not actually identified from the
    # data.
    if (
        abs(rref - LT_RREF_LOWER) < LT_BOUNDARY_TOL
        or abs(rref - LT_RREF_UPPER) < LT_BOUNDARY_TOL
        or abs(e0 - LT_E0_LOWER) < LT_BOUNDARY_TOL
        or abs(e0 - LT_E0_UPPER) < LT_BOUNDARY_TOL
    ):
        raise LTWrapperError(
            "Lloyd-Taylor fit terminated at a parameter boundary "
            f"(rref={rref:.6g}, e0={e0:.6g}); fit is not well identified"
        )

    reco_pred = _lt_reco(tair_f, rref, e0)
    residuals = reco_pred - nee_f
    night_fit_rmse = float(np.sqrt(np.mean(residuals ** 2)))

    return LTFitResult(
        rref=rref,
        e0=e0,
        n_night_samples=n,
        night_fit_rmse=night_fit_rmse,
        fit_status="ok",
    )


def predict_reco(
    tair_c: np.ndarray, rref: float, e0: float
) -> np.ndarray:
    """Predict Reco over all rows using fitted Lloyd-Taylor parameters.

    Returns ``NaN`` where temperature is non-finite or below the
    domain floor (``LT_T_DOMAIN_MIN_C``). This preserves the 13-column
    output contract while honestly marking rows where the LT model
    cannot be evaluated.
    """
    t = np.asarray(tair_c, dtype=float)
    out = np.full(t.shape, np.nan, dtype=float)
    valid = np.isfinite(t) & (t > LT_T_DOMAIN_MIN_C)
    if valid.any():
        out[valid] = _lt_reco(t[valid], rref, e0)
    return out
