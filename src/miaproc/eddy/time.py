from __future__ import annotations

import warnings

import pandas as pd

_ALLOWED_FORMATS = (
    # Mirrors R orders:
    # "Ymd HMS", "Ymd HM", "Y-m-d HMS", "Y-m-d HM"
    "%Y%m%d %H:%M:%S",
    "%Y%m%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    # M002-S02 r3: fractional seconds (lubridate's HMS accepts them too), so a
    # CSV directory mixing ``HH:MM`` and ``HH:MM:SS.ffffff`` files loses no rows.
    "%Y%m%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S.%f",
)


def _parse_datetime_multi(s: pd.Series) -> pd.Series:
    """
    Try multiple formats in order, filling remaining NaT each time.

    The returned series preserves the input's index. This matters when the
    caller has filtered rows (e.g. by ``site_id``) and the input index is no
    longer a simple ``RangeIndex``.
    """
    idx = s.index
    out = pd.Series([pd.NaT] * len(s), index=idx, dtype="datetime64[ns]")
    remaining = pd.Series([True] * len(s), index=idx)
    for fmt in _ALLOWED_FORMATS:
        parsed = pd.to_datetime(s.where(remaining), format=fmt, errors="coerce")
        ok = parsed.notna()
        out.loc[ok] = parsed.loc[ok]
        remaining = out.isna()
        if not remaining.any():
            break
    return out


def _parse_timestamp_col(s: pd.Series) -> pd.Series:
    """
    Parse a single combined timestamp column (e.g. ``"2025-10-25 08:00:00 UTC"``).

    Uses pandas' format inference so trailing timezone abbreviations like
    ``UTC`` are honored. Unparseable values become ``NaT``. The returned
    series preserves the input's index.

    M002-S02 r3 (mixed precision): pandas infers one format from the first
    element, so strings of another precision (``...:00`` next to
    ``...:00.000000``) would become ``NaT``. Only those failed, non-empty
    elements are parsed again element-wise (``format="mixed"``); values the
    first pass parsed are unchanged. A retry whose timezone awareness does not
    match the first pass is not applied (those rows stay unparsed and are
    reported by :func:`instant_drop_counts`).
    """
    parsed = pd.to_datetime(s, errors="coerce")
    if pd.api.types.is_datetime64_any_dtype(s) or not pd.api.types.is_datetime64_any_dtype(parsed):
        return parsed
    text = s.astype("string").str.strip()
    failed = parsed.isna() & text.notna() & (text != "")
    if not failed.any():
        return parsed
    try:
        retry = pd.to_datetime(s[failed], errors="coerce", format="mixed")
    except (ValueError, TypeError):  # e.g. mixed timezones within the retried values
        return parsed
    if not pd.api.types.is_datetime64_any_dtype(retry) or not retry.notna().any():
        return parsed
    if not parsed.notna().any():
        return retry.reindex(s.index)
    first_tz, retry_tz = parsed.dt.tz, retry.dt.tz
    if (first_tz is None) != (retry_tz is None):
        return parsed
    if retry_tz is not None:
        retry = retry.dt.tz_convert(first_tz)
    out = parsed.copy()
    out.loc[failed] = retry.dt.as_unit(out.dt.unit)
    return out


def _parse_instants(
    df: pd.DataFrame,
    *,
    has_legacy: bool,
    date_col: str,
    time_col: str,
    timestamp_col: str,
    tz_in: str,
    tz_out: str,
) -> pd.Series:
    """Per-row observation instants exactly as :func:`create_datetime` builds them."""
    if has_legacy:
        dt_str = df[date_col].astype(str).str.strip() + " " + df[time_col].astype(str).str.strip()
        parsed = _parse_datetime_multi(dt_str)
    else:
        parsed = _parse_timestamp_col(df[timestamp_col])

    # Apply tz_in then convert to tz_out:
    # - If timestamps are "naive" but represent tz_in, localize
    # - If already tz-aware (CSV with timezone suffix), normalize
    if parsed.dt.tz is None:
        parsed = parsed.dt.tz_localize(tz_in, ambiguous="NaT", nonexistent="NaT")
    else:
        parsed = parsed.dt.tz_convert(tz_in)

    return parsed.dt.tz_convert(tz_out)


def instant_drop_counts(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    time_col: str = "time",
    timestamp_col: str = "timestamp",
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    unit_marker: str = "[HH:MM]",
) -> dict[str, int]:
    """Rows :func:`create_datetime` drops, by reason (read-only, for reporting).

    ``unparsed``: rows whose time does not parse (stage 1 drops them with a
    warning); a legacy units row (``time == "[HH:MM]"``, removed earlier by
    ``drop_unit_rows``) is not counted. ``duplicate``: parsed instants that
    repeat an earlier row (stage 1 keeps the first). Both are ``0`` when the
    frame has no time column.
    """
    has_legacy = date_col in df.columns and time_col in df.columns
    if not has_legacy and timestamp_col not in df.columns:
        return {"unparsed": 0, "duplicate": 0}
    if time_col in df.columns:
        df = df.loc[df[time_col].astype(str).str.strip() != unit_marker]
    parsed = _parse_instants(
        df, has_legacy=has_legacy, date_col=date_col, time_col=time_col,
        timestamp_col=timestamp_col, tz_in=tz_in, tz_out=tz_out,
    )
    return {"unparsed": int(parsed.isna().sum()),
            "duplicate": int(parsed.dropna().duplicated().sum())}


def duplicate_instant_count(df: pd.DataFrame, **kwargs) -> int:
    """Rows :func:`create_datetime` drops as repeated instants (it keeps the first)."""
    return instant_drop_counts(df, **kwargs)["duplicate"]


def create_datetime(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    time_col: str = "time",
    timestamp_col: str = "timestamp",
    tz_in: str = "UTC",
    tz_out: str = "UTC",
    warn_dups: bool = True,
    drop_unparsed: bool = True,
) -> pd.DataFrame:
    """
    Build a ``DateTime`` column from either legacy ``date + time`` or the
    case-study ``timestamp`` shape.

    Parsing priority:

    1. If both ``date_col`` and ``time_col`` exist, use the legacy R
       ``create_datetime`` pathway: string-trim, try multiple formats,
       drop unparsed, deduplicate.
    2. Else if ``timestamp_col`` exists, parse that column directly
       (pandas infers common formats, including tz-suffixed strings like
       ``"2025-10-25 08:00:00 UTC"``).
    3. Else raise ``ValueError`` naming all three candidate columns.

    Timezone behavior is identical for both paths:

    - naive timestamps are localized to ``tz_in``, then converted to ``tz_out``;
    - tz-aware timestamps are converted to ``tz_in`` (a no-op when the
      source is already UTC), then to ``tz_out``.
    """
    has_legacy = date_col in df.columns and time_col in df.columns
    has_timestamp = timestamp_col in df.columns
    if not has_legacy and not has_timestamp:
        raise ValueError(
            "ERROR: Data contains neither legacy 'date'+'time' columns nor "
            "'timestamp' column; cannot build DateTime."
        )

    out = df.copy()

    if has_legacy:
        # Ensure string-like and trim
        out[date_col] = out[date_col].astype(str).str.strip()
        out[time_col] = out[time_col].astype(str).str.strip()

    out["DateTime"] = _parse_instants(
        out, has_legacy=has_legacy, date_col=date_col, time_col=time_col,
        timestamp_col=timestamp_col, tz_in=tz_in, tz_out=tz_out,
    )

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

    Returns a new dataframe with a continuous time grid, inserting rows
    where timestamps are missing.

    M35A: when the input frame carries a separate source-truth
    ``timestamp`` column distinct from ``datetime_col``, gap rows
    inserted by the grid merge receive ``timestamp`` filled from the
    regularized ``datetime_col`` value instead of NaT. This preserves
    the M32A source-truth time series across the regularization
    boundary so the silver writeback identity contract
    (`prepare_silver_stage_payload` synthesizes
    `primary_key = site_id|iso(timestamp)`) does not see NaT on
    inserted rows and `validate_stage_table` does not refuse with
    `null_timestamp` / `null_primary_key`. The fill respects the
    existing tz-awareness of the source `timestamp` column. When the
    input frame carries a single uniform non-null ``site_id`` value,
    gap rows inherit that ``site_id`` so the group identity survives
    the gap. Measurement columns remain NaN/null on inserted rows.
    Behavior is unchanged when neither a separate `timestamp` column
    nor a uniform `site_id` is present.
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

    # M35A: count inserted rows from the row delta so the fill logic
    # below is independent of which measurement columns happen to be
    # all-NaN on real input rows.
    inserted_row_count = len(merged) - len(out)

    # M35A: fill source-truth ``timestamp`` from the regularized grid
    # value for rows where the source column is NaT but the grid join
    # key is non-null. Only applies when the input frame carries a
    # ``timestamp`` column distinct from ``datetime_col`` (i.e. the
    # M32A source-truth time series rides alongside the internal
    # ``DateTime``). Skipped when the frame has no separate
    # ``timestamp`` column, preserving legacy behavior for callers
    # that pass in ``DateTime``-only frames.
    if "timestamp" in merged.columns and "timestamp" != datetime_col:
        ts_isna = merged["timestamp"].isna()
        dt_present = merged[datetime_col].notna()
        fill_mask = ts_isna & dt_present
        if fill_mask.any():
            grid_values = pd.to_datetime(
                merged.loc[fill_mask, datetime_col], utc=True, errors="coerce"
            )
            existing_dtype = merged["timestamp"].dtype
            if isinstance(existing_dtype, pd.DatetimeTZDtype):
                grid_values = grid_values.dt.tz_convert(existing_dtype.tz)
            merged.loc[fill_mask, "timestamp"] = grid_values

    # M35A: propagate a uniform ``site_id`` value onto inserted rows
    # so the group identity survives the gap. Only fires when the
    # post-merge frame has exactly one distinct non-null ``site_id``
    # value (single-site or per-group call); multi-site frames are
    # left alone so the regularization path stays neutral on
    # cross-group data.
    if "site_id" in merged.columns:
        unique_sites = merged["site_id"].dropna().unique()
        if len(unique_sites) == 1:
            merged["site_id"] = merged["site_id"].fillna(unique_sites[0])

    if inserted_row_count > 0:
        warnings.warn(
            f"regularize_time_grid: inserted {inserted_row_count} missing timestamps at freq='{freq}'."
        )

    return merged