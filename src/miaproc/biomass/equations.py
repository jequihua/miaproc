from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from importlib import resources


# M16 default: unified equation parquet with both INFYS volume rows and the
# four new ``dina`` direct-biomass mangrove rows. Schema companion lives at
# ``08_pkg/docs/equation_application_unified.zstd.json``. The legacy
# ``allometries_mx.parquet`` remains in the package data dir for callers
# who pass an explicit ``filename=`` but it is no longer the default.
DEFAULT_EQUATIONS_FILENAME = "equation_application_unified.zstd.parquet"


@dataclass(frozen=True)
class EquationMatch:
    row: pd.Series
    match_status: str  # e.g. "exact_state", "fallback_any_state"
    assignment_level_used: int


def load_packaged_equations(
    filename: str = DEFAULT_EQUATIONS_FILENAME,
) -> pd.DataFrame:
    """Load the packaged equation parquet bundled inside ``miaproc.biomass.data/``.

    Default is the M16 unified parquet (``equation_application_unified.zstd.parquet``)
    which carries both volume (``source_dataset == "infys"``) and direct-biomass
    (``source_dataset == "dina"``) rows.

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


def _normalize_state(s: str | None) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    return str(s).strip().lower()


def _normalize_species(s: str) -> str:
    # minimal v1: exact-ish match (case/space normalized).
    # Later you can swap in a richer normalizer without changing the API.
    return " ".join(str(s).strip().lower().split())


# M17A — Conservative, deterministic alias map for known mangrove-species
# typos surfaced by the post-fixture-refresh M17 smoke
# (`01_data/case_study/biomass/forest_structure_biomass_test.csv`).
#
# Keys are POST-normalization (case- and whitespace-collapsed via
# ``_normalize_species``); values are also normalized so they match
# directly against the parquet's normalized
# ``scientific_name_apg_raw`` column. Both forms of the typo
# (`Rizophora mangle` / `Rizophora manlge`) are recorded explicitly.
#
# This is **not** a fuzzy-matching layer — no Levenshtein /
# Jaro-Winkler / phonetic logic. It is a small explicit lookup that
# only resolves the two known bad spellings. Unknown species still
# return ``no_equation_found``; null species still fail honestly;
# rows without ``dbh_cm`` still classify as ``dbh_missing`` even when
# the species alias resolves. Adding more aliases later requires an
# explicit code edit and a recorded review pass.
_SPECIES_ALIASES_NORMALIZED: dict[str, str] = {
    "rizophora mangle": "rhizophora mangle",
    "rizophora manlge": "rhizophora mangle",
}


def _resolve_species_alias(species_norm: str) -> str:
    """Apply the M17A alias map after normalization."""
    return _SPECIES_ALIASES_NORMALIZED.get(species_norm, species_norm)


def match_equation(
    *,
    equations: pd.DataFrame,
    state: str | None = None,
    species: str,
    assignment_level: Optional[int] = None,
    response_variable: Optional[str] = None,
    dataset: str | None = None,
    estado: str | None = None,  # legacy alias kept for back-compat
) -> EquationMatch | None:
    """Match species + (optional) state to a single equation row.

    Policy:
      1) optionally filter by ``dataset`` (``"infys"`` for volume,
         ``"dina"`` for direct biomass).
      2) optionally filter by ``response_variable``.
      3) try ``state == provided`` (volume rows have a state; biomass
         rows have ``state is None`` and therefore won't match step 1
         on a state-filter; they'll match step 2 below).
      4) fallback to any state if step 3 finds nothing.
      5) choose ``assignment_level``: default min available (volume only;
         biomass rows have null assignment_level and select stable-first).
      6) if multiple remain, choose stable first by ``equation_code``.

    ``estado`` is a deprecated alias for ``state`` retained so existing
    callers from the legacy ``allometries_mx.parquet`` era keep working
    until they move to the new field-contract vocabulary.
    """
    if equations.empty:
        return None

    # Resolve legacy ``estado`` alias.
    if state is None and estado is not None:
        state = estado

    eq = equations.copy()

    # Optional dataset filter (M16 contract: ``"dina"`` for direct biomass).
    if dataset is not None and "source_dataset" in eq.columns:
        eq = eq[eq["source_dataset"] == dataset]
        if eq.empty:
            return None

    # Optional response variable filter (e.g. ``"B"`` for biomass kg).
    if response_variable is not None and "response_variable" in eq.columns:
        eq = eq[eq["response_variable"] == response_variable]
        if eq.empty:
            return None

    # Normalize columns for matching, without mutating caller df.
    eq["_state_norm"] = eq.get("state", pd.Series([None] * len(eq), index=eq.index)).map(
        _normalize_state
    )
    eq["_species_norm"] = eq["scientific_name_apg_raw"].map(_normalize_species)

    # M17A: normalize first, alias second. The alias map is
    # deterministic and only resolves the two known typo spellings;
    # unknown / null species still fall through to a normal failure.
    species_norm = _resolve_species_alias(_normalize_species(species))

    # Step 1: exact state.
    if state is not None:
        state_norm = _normalize_state(state)
        eq_state = eq[
            (eq["_state_norm"] == state_norm) & (eq["_species_norm"] == species_norm)
        ]
        if not eq_state.empty:
            return _select_one(
                eq_state,
                match_status="exact_state",
                assignment_level=assignment_level,
            )

    # Step 2: fallback to any state (this is the path direct-biomass
    # ``dina`` rows take, since their ``state`` is null).
    eq_any = eq[eq["_species_norm"] == species_norm]
    if not eq_any.empty:
        return _select_one(
            eq_any,
            match_status="fallback_any_state",
            assignment_level=assignment_level,
        )

    return None


def _select_one(
    df: pd.DataFrame,
    *,
    match_status: str,
    assignment_level: Optional[int],
) -> EquationMatch | None:
    # Decide assignment level. Biomass (dina) rows have null
    # ``assignment_level``; in that case fall through to stable-first.
    if (
        "assignment_level" not in df.columns
        or df["assignment_level"].isna().all()
    ):
        sort_col = "equation_code" if "equation_code" in df.columns else None
        chosen = (
            df.sort_values(by=[sort_col], kind="mergesort").iloc[0]
            if sort_col is not None
            else df.iloc[0]
        )
        return EquationMatch(
            row=chosen, match_status=match_status, assignment_level_used=-1
        )

    if assignment_level is None:
        # Some rows in the filtered slice may have null ``assignment_level``;
        # use ``min()`` skipping NA.
        min_level = df["assignment_level"].min(skipna=True)
        if pd.isna(min_level):
            chosen = (
                df.sort_values(by=["equation_code"], kind="mergesort").iloc[0]
                if "equation_code" in df.columns
                else df.iloc[0]
            )
            return EquationMatch(
                row=chosen,
                match_status=match_status,
                assignment_level_used=-1,
            )
        level = int(min_level)
    else:
        level = int(assignment_level)

    df_level = df[df["assignment_level"] == level]
    if df_level.empty:
        # if user asks a level not available, fall back to min available
        level = int(df["assignment_level"].min(skipna=True))
        df_level = df[df["assignment_level"] == level]

    sort_col = "equation_code" if "equation_code" in df_level.columns else None
    chosen = (
        df_level.sort_values(by=[sort_col], kind="mergesort").iloc[0]
        if sort_col is not None
        else df_level.iloc[0]
    )
    return EquationMatch(
        row=chosen, match_status=match_status, assignment_level_used=level
    )
