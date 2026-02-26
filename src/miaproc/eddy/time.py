from __future__ import annotations

import warnings
from typing import Iterable

import pandas as pd

_ALLOWED_FORMATS = (
    # Mirrors R orders:
    # "Ymd HMS", "Ymd HM", "Y-m-d HMS", "Y-m-d HM"
    "%Y%m%d %H:%M:%S",
    "%Y%m%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
)


def _parse_datetime_multi(s: pd.Series) -> pd.Series:
    """
    Try multiple formats in order, filling remaining NaT each time.
    """
    out = pd.to_datetime(pd.Series([pd.NaT] * len(s)), errors="coerce")
    remaining = pd.Series([True] * len(s))
    for fmt in _ALLOWED_FORMATS:
        parsed = pd.to_datetime(s.where(remaining), format=fmt, errors="coerce")
        # fill where we parsed successfully
        ok = parsed.notna()
        out.loc[ok] = parsed.loc[ok]
        remaining = out.isna()
        if not remaining.any():
            break
    return out


def create_datetime(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    time_col: str = "time",
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    warn_dups: bool = True,
    drop_unparsed: bool = True,
) -> pd.DataFrame:
    """
    Replicates R create_datetime():
      - requires date + time
      - trims strings
      - parses with multiple formats
      - drops rows with failed parse
      - applies tz conversion (with_tz in R)
      - drops duplicate DateTime (keep first) and warns
    """
    if date_col not in df.columns or time_col not in df.columns:
        raise ValueError("ERROR: Data does not contain 'date' and 'time'")

    out = df.copy()

    # Ensure string-like and trim
    out[date_col] = out[date_col].astype(str).str.strip()
    out[time_col] = out[time_col].astype(str).str.strip()

    dt_str = out[date_col] + " " + out[time_col]
    parsed = _parse_datetime_multi(dt_str)

    # Apply tz_in then convert to tz_out:
    # - If timestamps are "naive" but represent tz_in, localize
    # - If already tz-aware (rare in CSV), normalize
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(tz_in, ambiguous="NaT", nonexistent="NaT")
    else:
        parsed = parsed.dt.tz_convert(tz_in)

    parsed = parsed.dt.tz_convert(tz_out)

    out["DateTime"] = parsed

    if drop_unparsed:
        before = len(out)
        out = out.loc[out["DateTime"].notna()].copy()
        dropped = before - len(out)
        if dropped > 0:
            warnings.warn(
                f"create_datetime: dropped {dropped} rows due to unparsed datetime. "
                f"Use find_unparsed_datetime_rows(...) to inspect examples."
            )

    if warn_dups:
        n_dups = out["DateTime"].duplicated().sum()
        if n_dups > 0:
            warnings.warn(f"Found {n_dups} duplicated DateTime rows; keeping the first occurrence.")

    out = out.drop_duplicates(subset=["DateTime"], keep="first")
    return out

def find_unparsed_datetime_rows(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    time_col: str = "time",
    n: int = 20,
) -> pd.DataFrame:
    """
    Utility: returns the first n rows that fail our datetime parsing,
    with the raw date/time strings and the combined string.
    """
    if date_col not in df.columns or time_col not in df.columns:
        raise ValueError("Data does not contain date/time columns")

    tmp = df.copy()
    tmp[date_col] = tmp[date_col].astype(str).str.strip()
    tmp[time_col] = tmp[time_col].astype(str).str.strip()
    tmp["_dt_str"] = tmp[date_col] + " " + tmp[time_col]
    parsed = _parse_datetime_multi(tmp["_dt_str"])
    bad = tmp.loc[parsed.isna(), [date_col, time_col, "_dt_str"]].head(n)
    return bad

def regularize_time_grid(
    df: pd.DataFrame,
    *,
    datetime_col: str = "DateTime",
    freq: str = "30min",
) -> pd.DataFrame:
    """
    Replicates R 4.5 (lines ~230–251):
      - start_time = min(DateTime)
      - end_time   = max(DateTime)
      - time_grid  = seq(from=start_time, to=end_time, by="30 min")
      - left_join(time_frame, df, by="DateTime")

    Returns a new dataframe with a continuous time grid, inserting rows where timestamps are missing.
    """
    if datetime_col not in df.columns:
        raise ValueError(f"regularize_time_grid: missing '{datetime_col}' column")

    if len(df) == 0:
        return df.copy()

    out = df.copy()

    # Ensure datetime type
    dt = out[datetime_col]
    if not pd.api.types.is_datetime64_any_dtype(dt):
        raise TypeError(f"regularize_time_grid: '{datetime_col}' must be datetime dtype")

    start_time = dt.min()
    end_time = dt.max()

    if pd.isna(start_time) or pd.isna(end_time):
        warnings.warn("regularize_time_grid: start_time or end_time is NaT; returning original df.")
        return out

    # Preserve timezone awareness if present
    tz = getattr(start_time, "tz", None)
    grid = pd.date_range(start=start_time, end=end_time, freq=freq, tz=tz)

    time_frame = pd.DataFrame({datetime_col: grid})

    # Left join like R: grid as left table
    merged = time_frame.merge(out, on=datetime_col, how="left")

    # Optional: keep sorted, and reset index
    merged = merged.sort_values(datetime_col).reset_index(drop=True)

    # Helpful message like R (optional)
    missing = int(merged.drop(columns=[datetime_col]).isna().all(axis=1).sum())
    if missing > 0:
        warnings.warn(f"regularize_time_grid: inserted {missing} missing timestamps at freq='{freq}'.")

    return merged