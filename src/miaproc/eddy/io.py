from __future__ import annotations

from pathlib import Path
import re
import warnings
from typing import Any, Sequence

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
    Replicates the R workflow's unit-row filter:
      df %>% filter(time != "[HH:MM]")

    This is a legacy concern: EddyPro-style ``full_output`` CSVs contain a
    second header row whose ``time`` cell is the literal string
    ``"[HH:MM]"``. Case-study files shaped with a single ``timestamp``
    column have no unit row to drop, and should pass through silently.

    Behavior:

    - If ``time_col`` exists, filter rows where it equals ``unit_marker``
      (legacy path).
    - If ``time_col`` is absent and a ``timestamp`` column is present, return
      the frame unchanged without warning — there is no unit row to drop.
    - If neither column is present, warn (the caller likely has the wrong
      frame shape entirely).
    """
    if time_col in df.columns:
        mask = df[time_col].astype(str).str.strip() != unit_marker
        return df.loc[mask].copy()

    if "timestamp" in df.columns:
        return df

    warnings.warn(f"drop_unit_rows: '{time_col}' column not found; nothing dropped.")
    return df


# ---------------------------------------------------------------------------
# M002-S02: explicit single-file Parquet inputs (local or mounted paths)
# ---------------------------------------------------------------------------

PARQUET_SUFFIXES = (".parquet", ".pq")

# ``scheme:`` followed by a slash; two or more scheme characters so a
# Windows drive letter (``C:\``) is never mistaken for a URI. Matches the
# raw argument (``gs://b/k``) and its ``pathlib`` form (``gs:/b/k``,
# ``gs:\b\k``).
_URI_SCHEME = re.compile(r"^([A-Za-z][A-Za-z0-9+.\-]+):[/\\]")

# Measurement columns the stage-1 science coerces to float. Parquet
# integer columns are read as nullable ``Int64`` (lossless); these
# measurement columns are converted to ``float64`` as the CSV reader
# produces them, so stage-1 QC/rain/sigma arithmetic is the same for both
# formats. Identity and other passthrough integer columns keep their
# lossless integer type.
SCIENCE_FLOAT_COLUMNS = (
    "co2_flux", "qc_co2_flux", "H", "qc_H", "LE", "qc_LE",
    "air_temperature", "VPD", "u_star", "u.",
    "SWIN_1_1_1", "P_RAIN_1_1_1", "RH_1_1_1",
    # silver/source-facing names read back by gold
    "air_temperature_c", "VPD_hpa",
)

# Minimum Parquet schemas per input role (names only; see
# docs/PARQUET_FILE_SCHEMA.md). ``timestamp`` may be replaced by legacy
# ``date`` + ``time`` on flux/biomet; silver needs ``timestamp`` (or a
# legacy ``DateTime``).
PARQUET_REQUIRED_COLUMNS = {
    "flux": ("co2_flux", "qc_co2_flux", "air_temperature", "VPD"),
    "biomet": ("SWIN_1_1_1", "P_RAIN_1_1_1", "RH_1_1_1"),
    "silver": (),
}


class ParquetInputError(ValueError):
    """An explicit Parquet input is unusable (path, file, schema or rows)."""


def reject_uri(path: object, *, flag: str) -> None:
    """Raise ``ParquetInputError`` when ``path`` names a URI scheme.

    Only ordinary local or mounted filesystem paths are supported; there
    is no ``gs://`` / ``s3://`` / ``http(s)://`` client in this package.
    """
    match = _URI_SCHEME.match(str(path))
    if match:
        raise ParquetInputError(
            f"{flag}: unsupported URI scheme {match.group(1)!r} in {str(path)!r}. "
            "Only local or mounted filesystem paths are accepted; mount the "
            "bucket and pass its filesystem path instead."
        )


def _arrow_to_frame(table) -> pd.DataFrame:
    """Lossless Arrow -> pandas conversion with a fixed dtype policy.

    - integers: nullable ``Int64`` (``UInt64`` for uint64), so identifiers
      above 2**53 stay exact even when stage 1 inserts null gap rows;
    - timestamps: nanosecond unit (lossless for s/ms/us; tz kept);
    - strings: ``object`` with ``None`` for nulls; floats keep NaN.

    Pandas metadata (index/extension dtypes of the writer) is ignored so
    the result depends only on the Arrow schema.
    """
    import pyarrow as pa

    def _mapper(arrow_type):
        if pa.types.is_integer(arrow_type):
            return pd.UInt64Dtype() if arrow_type == pa.uint64() else pd.Int64Dtype()
        return None

    return table.to_pandas(
        types_mapper=_mapper,
        ignore_metadata=True,
        coerce_temporal_nanoseconds=True,
    )


def read_parquet_table(path: str | Path, *, flag: str, role: str) -> pd.DataFrame:
    """Read and validate one explicit Parquet file for ``role``.

    ``role`` is ``"flux"``, ``"biomet"`` or ``"silver"``. Fails with
    :class:`ParquetInputError` (a ``ValueError``) on a URI scheme, a
    missing path, a directory (partitioned datasets are not supported),
    a non-Parquet suffix, a corrupt/truncated file, duplicate field
    names, nested columns, a missing time column or required column, a
    non-temporal ``timestamp`` and zero rows.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    reject_uri(path, flag=flag)
    p = Path(path)
    if p.suffix.lower() not in PARQUET_SUFFIXES:
        raise ParquetInputError(f"{flag}: expected a .parquet or .pq file, got {str(p)!r}.")
    if not p.exists():
        raise ParquetInputError(f"{flag}: file does not exist: {p}")
    if not p.is_file():
        raise ParquetInputError(
            f"{flag}: {p} is not a single file (partitioned Parquet datasets are not supported)."
        )
    try:
        table = pq.ParquetFile(str(p)).read()
    except (pa.ArrowException, OSError, ValueError) as exc:
        raise ParquetInputError(f"{flag}: cannot read Parquet file {p} (corrupt or truncated?): {exc}") from exc

    names = list(table.schema.names)
    dups = sorted({n for n in names if names.count(n) > 1})
    if dups:
        raise ParquetInputError(f"{flag}: duplicate column names in {p}: {dups}.")
    nested = [f.name for f in table.schema if pa.types.is_nested(f.type)]
    if nested:
        raise ParquetInputError(f"{flag}: nested (list/struct/map) columns are not supported: {nested}.")

    time_cols = ("timestamp", "DateTime") if role == "silver" else ("timestamp",)
    present = [c for c in time_cols if c in names]
    if not present and not (role != "silver" and "date" in names and "time" in names):
        need = "'timestamp' (or legacy 'DateTime')" if role == "silver" else "'timestamp' or 'date' + 'time'"
        raise ParquetInputError(f"{flag}: {role} schema has no time column; expected {need}. Columns: {names}.")
    for col in present:
        typ = table.schema.field(col).type
        if not (pa.types.is_timestamp(typ) or pa.types.is_string(typ) or pa.types.is_large_string(typ)):
            raise ParquetInputError(f"{flag}: column {col!r} must be a timestamp or ISO-8601 string, got {typ}.")
    required = list(PARQUET_REQUIRED_COLUMNS[role])
    if role == "flux" and "u_star" not in names and "u." not in names:
        required.append("u_star")
    missing = [c for c in required if c not in names]
    if missing:
        raise ParquetInputError(f"{flag}: {role} schema is missing required columns {missing}.")
    if table.num_rows == 0:
        raise ParquetInputError(f"{flag}: Parquet file {p} has no rows.")

    df = _arrow_to_frame(table)
    for col in SCIENCE_FLOAT_COLUMNS:
        if col in df.columns and isinstance(df[col].dtype, (pd.Int64Dtype, pd.UInt64Dtype)):
            df[col] = df[col].astype("float64")
    return df


def write_parquet_table(path: str | Path, df: pd.DataFrame, *, sink: Any = None) -> None:
    """Write ``df`` to one Parquet file with PyArrow and no index column.

    ``sink`` is an optional open binary file for ``path`` (for example a
    byte-budgeted scratch file); it is written to but not closed.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, sink if sink is not None else str(path))
