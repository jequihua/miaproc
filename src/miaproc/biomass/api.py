from __future__ import annotations

from dataclasses import asdict
from typing import Any, Callable, Mapping, Optional, Literal

import numpy as np
import pandas as pd

from .columns import BiomassColumns
from .equations import match_equation
from .safeeval import compile_numpy_expr


RangePolicy = Literal["warn", "clip", "error", "ignore"]


def _get_value(obs: Mapping[str, Any] | pd.Series, key: str) -> Any:
    return obs[key] if isinstance(obs, Mapping) else obs.loc[key]


def _range_check(
    diam_cm: float,
    alt_m: float,
    eq_row: pd.Series,
    policy: RangePolicy,
) -> tuple[float, float, str]:
    """
    Returns possibly adjusted (diam, alt) plus a status string.
    """
    if policy == "ignore":
        return diam_cm, alt_m, "range_ignored"

    # missing bounds are treated as "no bounds"
    dmin = eq_row.get("dbh_min_cm", np.nan)
    dmax = eq_row.get("dbh_max_cm", np.nan)
    hmin = eq_row.get("alt_min_m", np.nan)
    hmax = eq_row.get("alt_max_m", np.nan)

    in_dbh = True
    in_h = True

    if pd.notna(dmin) and diam_cm < float(dmin): in_dbh = False
    if pd.notna(dmax) and diam_cm > float(dmax): in_dbh = False
    if pd.notna(hmin) and alt_m < float(hmin): in_h = False
    if pd.notna(hmax) and alt_m > float(hmax): in_h = False

    if in_dbh and in_h:
        return diam_cm, alt_m, "in_range"

    if policy == "error":
        raise ValueError(f"Tree outside equation range (dbh_in={in_dbh}, height_in={in_h})")

    if policy == "clip":
        if pd.notna(dmin): diam_cm = max(diam_cm, float(dmin))
        if pd.notna(dmax): diam_cm = min(diam_cm, float(dmax))
        if pd.notna(hmin): alt_m = max(alt_m, float(hmin))
        if pd.notna(hmax): alt_m = min(alt_m, float(hmax))
        return diam_cm, alt_m, "clipped_to_range"

    # warn: compute anyway, but mark it
    return diam_cm, alt_m, "out_of_range"


def _resolve_custom_function(
    custom_function: str | Mapping[str, Any] | Callable[[Any, Any], Any] | None,
    species: str,
) -> Callable[[Any, Any], Any] | None:
    """
    Supports:
      - custom_function: str expression
      - custom_function: dict mapping species -> expr/callable
      - custom_function: callable(diam, alt)
    """
    if custom_function is None:
        return None

    if callable(custom_function):
        return custom_function

    if isinstance(custom_function, str):
        return compile_numpy_expr(custom_function)

    if isinstance(custom_function, Mapping):
        # exact key match as provided by user; they can normalize upstream if desired
        entry = custom_function.get(species)
        if entry is None:
            return None
        if callable(entry):
            return entry
        if isinstance(entry, str):
            return compile_numpy_expr(entry)
        raise TypeError("custom_function mapping values must be str or callable")

    raise TypeError("custom_function must be str, mapping, callable, or None")


def estimate_tree(
    obs: Mapping[str, Any] | pd.Series,
    *,
    equations: pd.DataFrame,
    estado: str,
    columns: BiomassColumns = BiomassColumns(),
    assignment_level: int | None = None,
    response_variable: str | None = None,
    range_policy: RangePolicy = "warn",
    custom_function: str | Mapping[str, Any] | Callable[[Any, Any], Any] | None = None,
) -> dict[str, Any]:
    """
    Estimate response_variable (currently volume m3) for a single tree observation.

    Rules:
      - If height missing -> NaN (your requirement)
      - If custom_function provided -> use it and do not match parquet
    """
    species = _get_value(obs, columns.species)
    diam = _get_value(obs, columns.dbh_cm)
    alt = _get_value(obs, columns.height_m)

    out: dict[str, Any] = {
        "species": species,
        "estado_requested": estado,
        "assignment_level_requested": assignment_level,
    }

    # Missing height => NaN
    if pd.isna(alt) or alt is None:
        out.update({
            "estimate_response_variable": np.nan,
            "match_status": "height_missing",
            "range_status": "not_evaluated",
            "response_variable": response_variable,
            "clave_ecuacion": None,
            "nivel_asignacion": None,
            "estado_ecuacion_usada": None,
            "ecuacion_numpy": None,
        })
        return out

    # Missing dbh is also non-evaluable for your equations as shown
    if pd.isna(diam) or diam is None:
        out.update({
            "estimate_response_variable": np.nan,
            "match_status": "dbh_missing",
            "range_status": "not_evaluated",
            "response_variable": response_variable,
            "clave_ecuacion": None,
            "nivel_asignacion": None,
            "estado_ecuacion_usada": None,
            "ecuacion_numpy": None,
        })
        return out

    diam = float(diam)
    alt = float(alt)

    # Custom function override (global or per species)
    f_custom = _resolve_custom_function(custom_function, str(species))
    if f_custom is not None:
        est = f_custom(diam, alt)
        out.update({
            "estimate_response_variable": float(est) if np.size(est) == 1 else est,
            "match_status": "custom_function",
            "range_status": "range_unchecked",
            "response_variable": response_variable,
            "clave_ecuacion": "custom",
            "nivel_asignacion": None,
            "estado_ecuacion_usada": None,
            "ecuacion_numpy": getattr(custom_function, "get", lambda _: None)(species) if isinstance(custom_function, Mapping) else (custom_function if isinstance(custom_function, str) else None),
        })
        return out

    # Match from parquet
    m = match_equation(
        equations=equations,
        estado=estado,
        species=str(species),
        assignment_level=assignment_level,
        response_variable=response_variable,
    )

    if m is None:
        out.update({
            "estimate_response_variable": np.nan,
            "match_status": "no_equation_found",
            "range_status": "not_evaluated",
            "response_variable": response_variable,
            "clave_ecuacion": None,
            "nivel_asignacion": None,
            "estado_ecuacion_usada": None,
            "ecuacion_numpy": None,
        })
        return out

    eq_row = m.row
    expr = eq_row["ecuacion_numpy"]

    # Range policy
    diam2, alt2, range_status = _range_check(diam, alt, eq_row, range_policy)

    f = compile_numpy_expr(expr)
    est = f(diam2, alt2)

    out.update({
        "estimate_response_variable": float(est) if np.size(est) == 1 else est,
        "match_status": m.match_status,
        "range_status": range_status,
        "response_variable": eq_row.get("response_variable"),
        "clave_ecuacion": eq_row.get("clave_ecuacion"),
        "nivel_asignacion": m.assignment_level_used,
        "estado_ecuacion_usada": eq_row.get("estado"),
        "ecuacion_numpy": expr,
        "fuente_referencia": eq_row.get("fuente_referencia"),
    })
    return out


def estimate_trees(
    df: pd.DataFrame,
    *,
    equations: pd.DataFrame,
    estado: str,
    columns: BiomassColumns = BiomassColumns(),
    assignment_level: int | None = None,
    response_variable: str | None = None,
    range_policy: RangePolicy = "warn",
    custom_function: str | Mapping[str, Any] | Callable[[Any, Any], Any] | None = None,
) -> pd.DataFrame:
    """
    Estimate for a dataframe. Returns a copy with appended result columns.
    """
    results = []
    for _, row in df.iterrows():
        r = estimate_tree(
            row,
            equations=equations,
            estado=estado,
            columns=columns,
            assignment_level=assignment_level,
            response_variable=response_variable,
            range_policy=range_policy,
            custom_function=custom_function,
        )
        results.append(r)

    res = pd.DataFrame(results)

    # Merge results back without duplicating input columns
    out = df.copy()
    # Keep output columns explicitly named and stable
    for c in [
        "estimate_response_variable",
        "match_status",
        "range_status",
        "response_variable",
        "clave_ecuacion",
        "nivel_asignacion",
        "estado_ecuacion_usada",
        "ecuacion_numpy",
        "fuente_referencia",
    ]:
        out[c] = res[c].values

    return out