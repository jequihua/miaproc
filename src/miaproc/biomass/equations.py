from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from importlib import resources


@dataclass(frozen=True)
class EquationMatch:
    row: pd.Series
    match_status: str  # e.g. "exact_estado", "fallback_any_estado"
    assignment_level_used: int


def load_packaged_equations(filename: str = "allometries_mx.parquet") -> pd.DataFrame:
    """
    Loads the packaged allometries parquet included inside miaproc.biomass/data/.
    Requires pyarrow or fastparquet at runtime.
    """
    pkg = "miaproc.biomass.data"
    with resources.as_file(resources.files(pkg).joinpath(filename)) as p:
        return pd.read_parquet(p)


def load_equations(path: str | Path | None = None) -> pd.DataFrame:
    """Load equations from a user path, or from packaged data if path is None."""
    if path is None:
        return load_packaged_equations()
    return pd.read_parquet(path)


def _normalize_estado(s: str) -> str:
    return str(s).strip().lower()


def _normalize_species(s: str) -> str:
    # minimal v1: exact-ish match (case/space normalized).
    # Later you can swap in a richer normalizer without changing the API.
    return " ".join(str(s).strip().lower().split())


def match_equation(
    *,
    equations: pd.DataFrame,
    estado: str,
    species: str,
    assignment_level: Optional[int] = None,
    response_variable: Optional[str] = None,
) -> EquationMatch | None:
    """
    Match species + estado to a single equation row.
    Policy:
      1) try estado==provided
      2) fallback to any estado if none found
      3) choose assignment_level: default min available
      4) if multiple remain, choose stable first by clave_ecuacion
    """
    if equations.empty:
        return None

    eq = equations.copy()

    # Normalize columns for matching, without mutating caller df
    eq["_estado_norm"] = eq["estado"].map(_normalize_estado)
    eq["_species_norm"] = eq["nombrecientifico_apg_raw"].map(_normalize_species)

    estado_norm = _normalize_estado(estado)
    species_norm = _normalize_species(species)

    # Optional response variable filter (volume etc)
    if response_variable is not None:
        eq = eq[eq["response_variable"] == response_variable]

    # Step 1: exact estado
    eq_estado = eq[(eq["_estado_norm"] == estado_norm) & (eq["_species_norm"] == species_norm)]
    if not eq_estado.empty:
        return _select_one(eq_estado, match_status="exact_estado", assignment_level=assignment_level)

    # Step 2: fallback any estado (hooks later: ecoregion, proximity)
    eq_any = eq[eq["_species_norm"] == species_norm]
    if not eq_any.empty:
        return _select_one(eq_any, match_status="fallback_any_estado", assignment_level=assignment_level)

    return None


def _select_one(df: pd.DataFrame, *, match_status: str, assignment_level: Optional[int]) -> EquationMatch | None:
    # Decide assignment level
    if "nivel_asignacion" not in df.columns or df["nivel_asignacion"].isna().all():
        # if missing, just choose first stable row
        chosen = df.sort_values(by=["clave_ecuacion"], kind="mergesort").iloc[0]
        return EquationMatch(row=chosen, match_status=match_status, assignment_level_used=-1)

    if assignment_level is None:
        level = int(df["nivel_asignacion"].min())
    else:
        level = int(assignment_level)

    df_level = df[df["nivel_asignacion"] == level]
    if df_level.empty:
        # if user asks a level not available, fall back to min available
        level = int(df["nivel_asignacion"].min())
        df_level = df[df["nivel_asignacion"] == level]

    chosen = df_level.sort_values(by=["clave_ecuacion"], kind="mergesort").iloc[0]
    return EquationMatch(row=chosen, match_status=match_status, assignment_level_used=level)