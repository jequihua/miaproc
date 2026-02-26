from __future__ import annotations

from pathlib import Path
import warnings
from typing import Iterable, Sequence

import pandas as pd

from .constants import NA_VALUES, UNIT_ROW_TIME_MARKER


def list_csv_files(path_to_files: Path) -> list[Path]:
    path_to_files = Path(path_to_files)
    if not path_to_files.exists():
        raise FileNotFoundError(f"Path does not exist: {path_to_files}")
    if not path_to_files.is_dir():
        raise NotADirectoryError(f"Not a directory: {path_to_files}")

    files = sorted([p for p in path_to_files.iterdir() if p.is_file() and p.suffix.lower() == ".csv"])
    return files


def read_and_combine_csv(
    path_to_files: Path,
    *,
    skip_rows: int,
    na_values: Sequence[str] = NA_VALUES,
) -> pd.DataFrame:
    """
    Replicates R read_and_combine_data():
      - list all *.csv in directory (non-recursive)
      - read each with comma sep, header=True, skip=skip_rows
      - combine row-wise
      - treat na_values as NaN
    """
    files = list_csv_files(Path(path_to_files))
    if len(files) == 0:
        raise FileNotFoundError(f"ERROR: No .csv files found in {path_to_files}")

    frames: list[pd.DataFrame] = []
    for f in files:
        # R uses read.table(fill=TRUE, strip.white=TRUE). pandas read_csv is stricter;
        # on malformed rows we prefer not to crash.
        try:
            df = pd.read_csv(
                f,
                sep=",",
                header=0,
                skiprows=skip_rows,
                na_values=list(na_values),
                keep_default_na=True,
                engine="python",
            )
        except Exception as e:
            raise RuntimeError(f"Failed reading CSV: {f}") from e

        # Basic normalization similar to strip.white:
        # - strip whitespace in column names
        df.columns = [str(c).strip() for c in df.columns]
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    return combined


def drop_unit_rows(
    df: pd.DataFrame,
    *,
    time_col: str = "time",
    unit_marker: str = UNIT_ROW_TIME_MARKER,
) -> pd.DataFrame:
    """
    Replicates:
      df %>% filter(time != "[HH:MM]")
    """
    if time_col not in df.columns:
        warnings.warn(f"drop_unit_rows: '{time_col}' column not found; nothing dropped.")
        return df

    mask = df[time_col].astype(str).str.strip() != unit_marker
    return df.loc[mask].copy()
