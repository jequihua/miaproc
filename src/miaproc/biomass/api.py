from __future__ import annotations

from typing import Any, Callable, Mapping, Literal

import numpy as np
import pandas as pd

from .columns import BiomassColumns
from .equations import match_equation
from .safeeval import compile_numpy_expr


RangePolicy = Literal["warn", "clip", "error", "ignore"]


# Life-stage values from ``08_pkg/docs/forest_data_schema.csv`` (M16). The
# direct-biomass ``dina`` equations are only valid on adult trees; juveniles
# and trees with missing life-stage are not eligible. Comparison is
# case-insensitive and tolerant of leading/trailing whitespace.
_ADULT_LIFE_STAGE_TOKENS = {"adult"}


def _get_value(obs: Mapping[str, Any] | pd.Series, key: str) -> Any:
    if isinstance(obs, Mapping):
        return obs.get(key, np.nan)
    return obs.get(key, np.nan) if hasattr(obs, "get") else obs.loc[key]


def _is_adult(value: Any) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().lower() in _ADULT_LIFE_STAGE_TOKENS


def _is_direct_biomass_row(eq_row: pd.Series) -> bool:
    """Direct biomass = ``dina`` rows where the wd-substituted expression is
    populated. We key off ``equation_numpy_wd_fixed`` rather than
    ``source_dataset`` alone so future direct-biomass datasets that follow
    the same wd-fixed convention work without code edits."""
    fixed_expr = eq_row.get("equation_numpy_wd_fixed")
    return fixed_expr is not None and not (
        isinstance(fixed_expr, float) and pd.isna(fixed_expr)
    )


def _expression_for(eq_row: pd.Series) -> str:
    """Pick the right NumPy expression column for this equation row.

    M16 contract:
      - direct-biomass (``dina``) rows carry the wd-substituted expression
        in ``equation_numpy_wd_fixed`` and that is the canonical evaluable
        form. The unsubstituted ``equation_numpy`` for those same rows
        contains a free ``wd`` token that the safeeval would reject.
      - volume (``infys``) rows have null ``equation_numpy_wd_fixed`` and
        their evaluable form is ``equation_numpy``.
    """
    if _is_direct_biomass_row(eq_row):
        return str(eq_row["equation_numpy_wd_fixed"])
    return str(eq_row["equation_numpy"])


def _range_check(
    diam_cm: float,
    alt_m: float,
    eq_row: pd.Series,
    policy: RangePolicy,
) -> tuple[float, float, str]:
    """Returns possibly adjusted (diam, alt) plus a status string."""
    if policy == "ignore":
        return diam_cm, alt_m, "range_ignored"

    # Missing bounds are treated as "no bounds". Direct-biomass dina rows
    # do not carry range bounds today, so ``in_range`` is the natural
    # outcome unless the caller overrides via custom equations.
    dmin = eq_row.get("dbh_min_cm", np.nan)
    dmax = eq_row.get("dbh_max_cm", np.nan)
    hmin = eq_row.get("height_min_m", np.nan)
    hmax = eq_row.get("height_max_m", np.nan)

    in_dbh = True
    in_h = True

    if pd.notna(dmin) and diam_cm < float(dmin):
        in_dbh = False
    if pd.notna(dmax) and diam_cm > float(dmax):
        in_dbh = False
    if pd.notna(alt_m):
        if pd.notna(hmin) and alt_m < float(hmin):
            in_h = False
        if pd.notna(hmax) and alt_m > float(hmax):
            in_h = False

    if in_dbh and in_h:
        return diam_cm, alt_m, "in_range"

    if policy == "error":
        raise ValueError(
            f"Tree outside equation range (dbh_in={in_dbh}, height_in={in_h})"
        )

    if policy == "clip":
        if pd.notna(dmin):
            diam_cm = max(diam_cm, float(dmin))
        if pd.notna(dmax):
            diam_cm = min(diam_cm, float(dmax))
        if pd.notna(alt_m):
            if pd.notna(hmin):
                alt_m = max(alt_m, float(hmin))
            if pd.notna(hmax):
                alt_m = min(alt_m, float(hmax))
        return diam_cm, alt_m, "clipped_to_range"

    # warn: compute anyway, but mark it
    return diam_cm, alt_m, "out_of_range"


def _resolve_custom_function(
    custom_function: str | Mapping[str, Any] | Callable[[Any, Any], Any] | None,
    species: str,
) -> Callable[[Any, Any], Any] | None:
    """Supports str expression, dict mapping, or callable(diam, alt)."""
    if custom_function is None:
        return None

    if callable(custom_function):
        return custom_function

    if isinstance(custom_function, str):
        return compile_numpy_expr(custom_function)

    if isinstance(custom_function, Mapping):
        entry = custom_function.get(species)
        if entry is None:
            return None
        if callable(entry):
            return entry
        if isinstance(entry, str):
            return compile_numpy_expr(entry)
        raise TypeError("custom_function mapping values must be str or callable")

    raise TypeError("custom_function must be str, mapping, callable, or None")


def _empty_match_output(
    *,
    species: Any,
    state: Any,
    assignment_level: int | None,
    response_variable: str | None,
    match_status: str,
) -> dict[str, Any]:
    return {
        "species": species,
        "state_requested": state,
        "estado_requested": state,  # legacy alias
        "assignment_level_requested": assignment_level,
        "estimate_response_variable": np.nan,
        "match_status": match_status,
        "range_status": "not_evaluated",
        "response_variable": response_variable,
        "response_units": None,
        "source_record_id": None,
        "source_dataset": None,
        "equation_code": None,
        "clave_ecuacion": None,  # legacy alias
        "assignment_level_used": None,
        "nivel_asignacion": None,  # legacy alias
        "state_used": None,
        "estado_ecuacion_usada": None,  # legacy alias
        "equation_numpy_used": None,
        "ecuacion_numpy": None,  # legacy alias
        "fuente_referencia": None,
    }


def estimate_tree(
    obs: Mapping[str, Any] | pd.Series,
    *,
    equations: pd.DataFrame,
    state: str | None = None,
    columns: BiomassColumns = BiomassColumns(),
    assignment_level: int | None = None,
    response_variable: str | None = None,
    dataset: str | None = None,
    range_policy: RangePolicy = "warn",
    custom_function: str | Mapping[str, Any] | Callable[[Any, Any], Any] | None = None,
    estado: str | None = None,  # legacy alias
) -> dict[str, Any]:
    """Estimate ``response_variable`` (volume m3 or biomass kg) for a single
    tree observation.

    M16 contract:
      - default packaged equation source is the unified parquet (volume +
        direct-biomass);
      - direct-biomass (``dina``) rows are matched via
        ``scientific_name_apg_raw`` and applied via
        ``equation_numpy_wd_fixed``;
      - direct-biomass requires ``life_stage == "Adult"`` and a non-null
        ``dbh_cm``; height is optional;
      - volume (``infys``) rows still require height (the equations
        reference ``alt``);
      - output carries ``source_record_id`` + ``source_dataset`` so the M17
        biomass enrichment pass can append the equation-used identifier
        without re-deriving it.

    ``estado`` is a deprecated alias for ``state``; if both are given,
    ``state`` wins.
    """
    species = _get_value(obs, columns.species)
    diam = _get_value(obs, columns.dbh_cm)
    alt = _get_value(obs, columns.height_m)
    life_stage = _get_value(obs, columns.life_stage)

    if state is None and estado is not None:
        state = estado

    out_base = {
        "species": species,
        "state_requested": state,
        "estado_requested": state,  # legacy alias
        "assignment_level_requested": assignment_level,
    }

    # Custom function override (global or per species). Bypasses parquet
    # match, evaluation rules, and life-stage gate. Documented escape hatch.
    f_custom = _resolve_custom_function(custom_function, str(species))
    if f_custom is not None:
        if pd.isna(diam) or diam is None:
            return {
                **_empty_match_output(
                    species=species,
                    state=state,
                    assignment_level=assignment_level,
                    response_variable=response_variable,
                    match_status="dbh_missing",
                ),
                **out_base,
            }
        diam_f = float(diam)
        alt_f = float(alt) if (alt is not None and not pd.isna(alt)) else float("nan")
        est = f_custom(diam_f, alt_f)
        custom_expr = (
            custom_function
            if isinstance(custom_function, str)
            else (
                custom_function.get(species)
                if isinstance(custom_function, Mapping)
                else None
            )
        )
        return {
            **out_base,
            "estimate_response_variable": float(est)
            if np.size(est) == 1
            else est,
            "match_status": "custom_function",
            "range_status": "range_unchecked",
            "response_variable": response_variable,
            "response_units": None,
            "source_record_id": None,
            "source_dataset": None,
            "equation_code": "custom",
            "clave_ecuacion": "custom",  # legacy alias
            "assignment_level_used": None,
            "nivel_asignacion": None,  # legacy alias
            "state_used": None,
            "estado_ecuacion_usada": None,  # legacy alias
            "equation_numpy_used": custom_expr if isinstance(custom_expr, str) else None,
            "ecuacion_numpy": custom_expr if isinstance(custom_expr, str) else None,
            "fuente_referencia": None,
        }

    # Missing dbh always disqualifies (both volume and direct biomass).
    if pd.isna(diam) or diam is None:
        return {
            **_empty_match_output(
                species=species,
                state=state,
                assignment_level=assignment_level,
                response_variable=response_variable,
                match_status="dbh_missing",
            ),
            **out_base,
        }

    diam = float(diam)

    # Match equation from parquet.
    m = match_equation(
        equations=equations,
        state=state,
        species=str(species),
        assignment_level=assignment_level,
        response_variable=response_variable,
        dataset=dataset,
    )

    if m is None:
        return {
            **_empty_match_output(
                species=species,
                state=state,
                assignment_level=assignment_level,
                response_variable=response_variable,
                match_status="no_equation_found",
            ),
            **out_base,
        }

    eq_row = m.row
    is_direct = _is_direct_biomass_row(eq_row)

    # Direct-biomass equations require an Adult life stage.
    if is_direct and not _is_adult(life_stage):
        return {
            **_empty_match_output(
                species=species,
                state=state,
                assignment_level=assignment_level,
                response_variable=response_variable,
                match_status="life_stage_not_adult",
            ),
            **out_base,
            # Surface which equation was rejected so the caller can audit.
            "source_record_id": eq_row.get("source_record_id"),
            "source_dataset": eq_row.get("source_dataset"),
        }

    # Volume equations still require height (their expressions reference
    # ``alt``); direct-biomass equations do not.
    if not is_direct and (alt is None or pd.isna(alt)):
        return {
            **_empty_match_output(
                species=species,
                state=state,
                assignment_level=assignment_level,
                response_variable=response_variable,
                match_status="height_missing",
            ),
            **out_base,
            "source_record_id": eq_row.get("source_record_id"),
            "source_dataset": eq_row.get("source_dataset"),
        }

    alt_f = float(alt) if (alt is not None and not pd.isna(alt)) else float("nan")

    # Range policy check (uses M16 column names ``height_min_m`` / ``height_max_m``).
    diam2, alt2, range_status = _range_check(diam, alt_f, eq_row, range_policy)

    expr = _expression_for(eq_row)
    f = compile_numpy_expr(expr)
    est = f(diam2, alt2)

    return {
        **out_base,
        "estimate_response_variable": float(est) if np.size(est) == 1 else est,
        "match_status": m.match_status,
        "range_status": range_status,
        "response_variable": eq_row.get("response_variable"),
        "response_units": eq_row.get("response_units"),
        "source_record_id": eq_row.get("source_record_id"),
        "source_dataset": eq_row.get("source_dataset"),
        "equation_code": eq_row.get("equation_code"),
        "clave_ecuacion": eq_row.get("equation_code"),  # legacy alias
        "assignment_level_used": m.assignment_level_used,
        "nivel_asignacion": m.assignment_level_used,  # legacy alias
        "state_used": eq_row.get("state"),
        "estado_ecuacion_usada": eq_row.get("state"),  # legacy alias
        "equation_numpy_used": expr,
        "ecuacion_numpy": expr,  # legacy alias
        "fuente_referencia": eq_row.get("source_reference"),
    }


DEFAULT_BIOMASS_ESTIMATE_COL = "biomass_estimate"
DEFAULT_EQUATION_USED_COL = "equation_used"


def estimate_trees(
    df: pd.DataFrame,
    *,
    equations: pd.DataFrame,
    state: str | None = None,
    columns: BiomassColumns = BiomassColumns(),
    assignment_level: int | None = None,
    response_variable: str | None = None,
    dataset: str | None = None,
    range_policy: RangePolicy = "warn",
    custom_function: str | Mapping[str, Any] | Callable[[Any, Any], Any] | None = None,
    estado: str | None = None,  # legacy alias
) -> pd.DataFrame:
    """Estimate for a dataframe. Returns a copy with appended result columns."""
    if state is None and estado is not None:
        state = estado

    results = []
    for _, row in df.iterrows():
        r = estimate_tree(
            row,
            equations=equations,
            state=state,
            columns=columns,
            assignment_level=assignment_level,
            response_variable=response_variable,
            dataset=dataset,
            range_policy=range_policy,
            custom_function=custom_function,
        )
        results.append(r)

    res = pd.DataFrame(results)

    out = df.copy()
    appended = [
        "estimate_response_variable",
        "match_status",
        "range_status",
        "response_variable",
        "response_units",
        "source_record_id",
        "source_dataset",
        "equation_code",
        "clave_ecuacion",
        "assignment_level_used",
        "nivel_asignacion",
        "state_used",
        "estado_ecuacion_usada",
        "equation_numpy_used",
        "ecuacion_numpy",
        "fuente_referencia",
    ]
    for c in appended:
        if c in res.columns:
            out[c] = res[c].values

    return out


def enrich_table(
    df: pd.DataFrame,
    *,
    equations: pd.DataFrame,
    state: str | None = None,
    columns: BiomassColumns = BiomassColumns(),
    assignment_level: int | None = None,
    response_variable: str | None = None,
    dataset: str | None = "dina",
    range_policy: RangePolicy = "warn",
    custom_function: str | Mapping[str, Any] | Callable[[Any, Any], Any] | None = None,
    biomass_estimate_col: str = DEFAULT_BIOMASS_ESTIMATE_COL,
    equation_used_col: str = DEFAULT_EQUATION_USED_COL,
    estado: str | None = None,  # legacy alias
) -> pd.DataFrame:
    """Enrich a tree table with exactly two appended biomass columns.

    M17 product contract:

      - returns the original ``df`` with all original columns preserved
        in their original order, plus exactly two new columns appended
        at the end:

          * ``biomass_estimate_col`` (default ``"biomass_estimate"``):
            numeric biomass estimate in the equation's response units
            (kg for the M16 ``dina`` direct-biomass equations; volume
            for the ``infys`` rows when ``dataset="infys"``).
          * ``equation_used_col`` (default ``"equation_used"``): the
            ``source_record_id`` of the equation that produced the
            estimate, or ``None`` for ineligible / unmatched rows.

      - row count and row order are preserved exactly. Ineligible rows
        (missing ``dbh_cm``, non-adult ``life_stage`` for direct biomass,
        species not matched, height missing for volume rows) keep their
        full original payload, with the two appended columns set to
        NaN / None. **No row dropping, no destructive filtering.**

      - the default ``dataset="dina"`` selects the four mangrove
        direct-biomass equations from the M16 packaged parquet. Set
        ``dataset="infys"`` for the volume path or ``None`` for the
        full unfiltered match space.

    The CLI ``miaproc biomass enrich-table`` is the canonical wrapper
    around this helper; cloud orchestrators (M18+) can also call it
    directly as a library function without going through argparse.
    """
    if biomass_estimate_col == equation_used_col:
        raise ValueError(
            "biomass_estimate_col and equation_used_col must differ "
            f"(got {biomass_estimate_col!r} for both)."
        )
    for col in (biomass_estimate_col, equation_used_col):
        if col in df.columns:
            raise ValueError(
                f"Output column name {col!r} collides with an existing "
                "input column. Pick a different output column name or "
                "rename the input column before calling enrich_table."
            )

    full = estimate_trees(
        df,
        equations=equations,
        state=state,
        columns=columns,
        assignment_level=assignment_level,
        response_variable=response_variable,
        dataset=dataset,
        range_policy=range_policy,
        custom_function=custom_function,
        estado=estado,
    )

    out = df.copy()
    # Project to exactly the two M17 output columns. Row alignment is
    # guaranteed by ``estimate_trees`` returning a copy of ``df`` with
    # the same index + row order.
    estimate = full["estimate_response_variable"].values
    out[biomass_estimate_col] = estimate
    # M17 product semantics: ``equation_used`` is the equation that was
    # *actually applied* to produce this row's estimate, not just the
    # equation that was matched. When ``estimate_response_variable`` is
    # NaN (ineligible row, rejected match, missing predictor), no
    # equation was actually used, so ``equation_used`` is None — even
    # though the M16 ``source_record_id`` audit field may carry the
    # identifier of the *rejected* equation. This keeps the M17 output
    # contract clean for downstream BigQuery / cloud consumers.
    used = full["source_record_id"].where(full["estimate_response_variable"].notna())
    out[equation_used_col] = used.where(used.notna(), None).values
    return out
