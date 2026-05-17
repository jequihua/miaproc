from __future__ import annotations

from typing import Optional, Sequence

import pandas as pd


STAGE2_OUTPUT_COLUMNS: tuple[str, ...] = (
    "DateTime",
    "Year",
    "DoY",
    "Hour",
    "NEE",
    "Ustar",
    "Tair",
    "VPD",
    "Rg",
    "rH",
    "QF",
)

_REQUIRED_INPUT_COLUMNS: tuple[str, ...] = (
    "DateTime",
    "NEE",
    "USTAR",
    "QC_NEE",
    "Tair",
    "VPD",
    "Rg",
    "rH",
)

_NUMERIC_OUTPUT_COLUMNS: tuple[str, ...] = (
    "NEE",
    "Ustar",
    "rH",
    "Tair",
    "VPD",
    "Rg",
    "QF",
)


class MissingColumnsError(ValueError):
    """Raised when a required stage-1 input column is missing."""

    def __init__(self, missing: Sequence[str]) -> None:
        self.missing = tuple(missing)
        super().__init__(
            "prepare_reddyproc_input: missing required column(s): "
            + ", ".join(self.missing)
        )


def prepare_reddyproc_input(
    df: pd.DataFrame,
    *,
    local_tz: Optional[str] = None,
) -> pd.DataFrame:
    """
    Convert a stage-1 eddy DataFrame into a REddyProc-style stage-2 input table.

    Mirrors the construction of ``data_REddy`` in
    ``90_legacy_review/R/R_manglaria.R`` (lines ~276-293), but is Python-native
    and calls neither REddyProc, hesseflux, nor rpy2.

    Output columns, in order:
        DateTime, Year, DoY, Hour, NEE, Ustar, Tair, VPD, Rg, rH, QF

    Renames applied:
        USTAR  -> Ustar
        QC_NEE -> QF

    Transformations applied:
        - Year, DoY, Hour derived from DateTime (Hour = hour + minute/60).
        - NEE, Ustar, rH, Tair, VPD, Rg, QF coerced to numeric.
        - Negative Rg clamped to 0.
        - Rows with unparseable / missing DateTime are dropped
          (matches ``filter(!is.na(Year))`` in the R workflow).

    The input DataFrame is not mutated. No backend side effects.

    Parameters
    ----------
    df
        DataFrame produced by ``miaproc.eddy.load_stage1`` (or an equivalent
        stage-1 pipeline). Must contain the columns listed in
        ``_REQUIRED_INPUT_COLUMNS``.
    local_tz
        Optional IANA timezone name (e.g. ``"America/Mazatlan"``). If given and
        the input ``DateTime`` column is timezone-aware, ``Year``, ``DoY``, and
        ``Hour`` are computed from ``DateTime`` converted to this timezone -
        matching ``with_tz(DateTime, tzone = "America/Mazatlan")`` in
        ``R_manglaria.R``. The returned ``DateTime`` column always keeps its
        original values; only the derived calendar fields reflect ``local_tz``.
        If ``local_tz`` is ``None`` (default), calendar fields are computed
        from ``DateTime`` as given.

    Raises
    ------
    MissingColumnsError
        If any required input column is absent.
    """
    missing = [c for c in _REQUIRED_INPUT_COLUMNS if c not in df.columns]
    if missing:
        raise MissingColumnsError(missing)

    # Work on a copy; never mutate caller data.
    out = df.copy()

    # Ensure DateTime is datetime-like. Rows that fail to parse become NaT.
    out["DateTime"] = pd.to_datetime(out["DateTime"], errors="coerce")

    # Drop rows with unparseable / missing DateTime before deriving calendar
    # fields. Mirrors filter(!is.na(Year)) in R once Year is computed from
    # DateTime_Local.
    out = out.loc[out["DateTime"].notna()].copy()

    # Optional local-time conversion for calendar-field derivation only.
    if local_tz is not None:
        dt_for_calendar = out["DateTime"]
        if dt_for_calendar.dt.tz is None:
            # Match R's with_tz semantics: the source is assumed UTC when no
            # tz is attached, then converted. This is explicit so a caller
            # passing local_tz gets predictable behavior either way.
            dt_for_calendar = dt_for_calendar.dt.tz_localize("UTC")
        dt_for_calendar = dt_for_calendar.dt.tz_convert(local_tz)
    else:
        dt_for_calendar = out["DateTime"]

    out["Year"] = dt_for_calendar.dt.year.astype("int64")
    out["DoY"] = dt_for_calendar.dt.dayofyear.astype("int64")
    out["Hour"] = dt_for_calendar.dt.hour + dt_for_calendar.dt.minute / 60.0

    # Renames: USTAR -> Ustar, QC_NEE -> QF.
    out = out.rename(columns={"USTAR": "Ustar", "QC_NEE": "QF"})

    # Numeric coercion for the stage-2 value columns.
    for col in _NUMERIC_OUTPUT_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Clamp negative Rg to zero after numeric coercion, matching R.
    rg = out["Rg"]
    out["Rg"] = rg.where(~(rg < 0), 0.0)

    # Select and order the stage-2 schema; reset index so row order is stable.
    return out.loc[:, list(STAGE2_OUTPUT_COLUMNS)].reset_index(drop=True)
