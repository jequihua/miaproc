"""Deterministic synthetic eddy-covariance fixtures (M001-S01).

Replaces the excluded raw observation fixtures
``tests/data/full_output/full_output.csv`` and ``tests/data/biomet/biomet.csv``
and provides the >=100-day baseline input. No real observations are used.

The generator is physically structured so that both partitioning backends
have a meaningful signal to fit:

* global radiation follows solar geometry for the configured site
  (latitude/longitude/standard-time offset) with day-to-day cloudiness;
* air temperature and humidity have seasonal and diurnal cycles, and VPD is
  derived from them (Magnus saturation vapour pressure);
* ecosystem respiration follows Lloyd & Taylor (1994) and GPP follows the
  Lasslop et al. (2010) rectangular light response with VPD limitation;
* night-time measured NEE is attenuated below a u* threshold, so u*
  filtering has a plateau to detect;
* QC flags, rain events, random and multi-hour gaps and dropped timestamps
  exercise QC, rain, gap-fill and regularization paths.

Files are written in the legacy EddyPro shape read by ``load_stage1``
(``date`` + ``time`` columns, a ``[HH:MM]`` unit row, ``-9999`` missing
values, Kelvin air temperature and Pa VPD). Values are rounded and written
with ``\\n`` line endings so the bytes are reproducible.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

GENERATOR_VERSION = "miaproc-synthetic-eddy-v1"

#: Fixture under ``tests/data`` that replaces the excluded raw files.
TEST_FIXTURE_DAYS = 45
TEST_FIXTURE_SEED = 20261021
TEST_FIXTURE_START = "2025-03-01 00:30"

FLUX_COLUMNS = (
    "date", "time", "co2_flux", "qc_co2_flux", "H", "qc_H", "LE", "qc_LE",
    "air_temperature", "VPD", "u.", "RH", "wind_speed",
)
FLUX_UNITS = (
    "[yyyy-mm-dd]", "[HH:MM]", "[umol+1s-1m-2]", "[#]", "[W+1m-2]", "[#]",
    "[W+1m-2]", "[#]", "[K]", "[Pa]", "[m+1s-1]", "[%]", "[m+1s-1]",
)
BIOMET_COLUMNS = ("date", "time", "SWIN_1_1_1", "P_RAIN_1_1_1", "RH_1_1_1", "TA_1_1_1")
BIOMET_UNITS = ("[yyyy-mm-dd]", "[HH:MM]", "[W+1m-2]", "[m]", "[%]", "[K]")


@dataclass(frozen=True)
class SyntheticSite:
    latitude: float = 22.25
    longitude: float = -105.50
    timezone_hour: float = -7.0


def _solar_cos_zenith(dt_utc: pd.DatetimeIndex, site: SyntheticSite) -> np.ndarray:
    doy = dt_utc.dayofyear.to_numpy(dtype=float)
    hour_utc = dt_utc.hour.to_numpy(dtype=float) + dt_utc.minute.to_numpy(dtype=float) / 60.0
    # Labels mark the END of each half-hour averaging interval (EddyPro and
    # REddyProc convention); evaluate the sun at the interval centre.
    hour_utc = hour_utc - 0.25
    solar_hour = hour_utc + site.longitude / 15.0
    decl = np.deg2rad(23.45) * np.sin(2.0 * np.pi * (284.0 + doy) / 365.0)
    lat = np.deg2rad(site.latitude)
    hour_angle = np.deg2rad(15.0 * (solar_hour - 12.0))
    return np.sin(lat) * np.sin(decl) + np.cos(lat) * np.cos(decl) * np.cos(hour_angle)


def generate_raw_frames(
    *,
    days: int,
    seed: int,
    start: str = TEST_FIXTURE_START,
    site: SyntheticSite = SyntheticSite(),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(flux, biomet)`` raw frames (numeric, before CSV formatting).

    Timestamps are naive UTC half-hour interval ends (the default start
    ``00:30`` makes the series end at midnight). Missing values are NaN.
    """
    if days < 1:
        raise ValueError("days must be >= 1")
    rng = np.random.default_rng(seed)
    n = days * 48
    dt = pd.date_range(pd.Timestamp(start), periods=n, freq="30min")
    dt_utc = dt.tz_localize("UTC")
    day_index = np.arange(n) // 48
    doy = dt_utc.dayofyear.to_numpy(dtype=float)
    local_hour = (
        dt_utc.hour.to_numpy(dtype=float)
        + dt_utc.minute.to_numpy(dtype=float) / 60.0
        + site.timezone_hour
    ) % 24.0

    # --- Radiation ---------------------------------------------------------
    cosz = np.clip(_solar_cos_zenith(dt_utc, site), 0.0, None)
    daily_clearness = rng.uniform(0.45, 0.95, days)[day_index]
    rg_clear = 1100.0 * cosz * 0.78
    rg = rg_clear * daily_clearness * np.clip(1.0 + rng.normal(0.0, 0.08, n), 0.4, 1.3)
    rg = np.where(cosz > 0.0, rg, 0.0) + rng.normal(0.0, 1.0, n)  # sensor offset noise at night

    # --- Temperature, humidity and VPD -------------------------------------
    seasonal = 24.0 + 4.0 * np.sin(2.0 * np.pi * (doy - 110.0) / 365.0)
    daily_anom = np.cumsum(rng.normal(0.0, 0.35, days))
    daily_anom = (daily_anom - daily_anom.mean())[day_index]
    diurnal = 4.5 * np.sin(2.0 * np.pi * (local_hour - 9.0) / 24.0) * (0.6 + 0.4 * daily_clearness)
    tair_c = seasonal + daily_anom + diurnal + rng.normal(0.0, 0.3, n)
    rh = 82.0 - 30.0 * np.clip(np.sin(2.0 * np.pi * (local_hour - 8.0) / 24.0), 0.0, None)
    rh = np.clip(rh - 4.0 * daily_anom + rng.normal(0.0, 3.0, n), 25.0, 99.0)
    es_hpa = 6.112 * np.exp(17.67 * tair_c / (tair_c + 243.5))
    vpd_hpa = es_hpa * (1.0 - rh / 100.0)

    # --- Fluxes ------------------------------------------------------------
    t_ref_k, t0_k, e0 = 288.15, 227.13, 160.0
    rref = 2.6 + 0.6 * np.sin(2.0 * np.pi * (doy - 150.0) / 365.0)
    reco = rref * np.exp(e0 * (1.0 / (t_ref_k - t0_k) - 1.0 / (tair_c + 273.15 - t0_k)))
    alpha, beta0, k_vpd, vpd0 = 0.045, 28.0, 0.04, 10.0
    beta = np.where(vpd_hpa > vpd0, beta0 * np.exp(-k_vpd * (vpd_hpa - vpd0)), beta0)
    rg_pos = np.clip(rg, 0.0, None)
    gpp = alpha * beta * rg_pos / (alpha * rg_pos + beta)

    ustar = np.where(
        cosz > 0.0,
        rng.lognormal(np.log(0.32), 0.35, n),
        rng.lognormal(np.log(0.17), 0.55, n),
    )
    ustar = np.clip(ustar, 0.01, 1.2)
    ustar_threshold = 0.12
    attenuation = np.where(cosz > 0.0, 1.0, np.clip(ustar / ustar_threshold, 0.0, 1.0))
    nee = (reco - gpp) * attenuation + rng.normal(0.0, 0.35, n) * (1.0 + 0.04 * gpp)

    h = 0.45 * rg_pos - 15.0 + rng.normal(0.0, 12.0, n)
    le = 0.35 * rg_pos + 8.0 * vpd_hpa / 10.0 + rng.normal(0.0, 10.0, n)
    qc_co2 = rng.choice([0, 1, 2], size=n, p=[0.82, 0.14, 0.04])
    qc_h = rng.choice([0, 1, 2], size=n, p=[0.85, 0.12, 0.03])
    qc_le = rng.choice([0, 1, 2], size=n, p=[0.85, 0.12, 0.03])
    wind = ustar * 7.5 + rng.normal(0.0, 0.2, n)

    # Rain events (a few per week) mask fluxes in stage 1.
    rain = np.where(rng.uniform(size=n) < 0.02, rng.exponential(0.0015, n), 0.0)

    # Missing flux records: random singles plus multi-hour gaps.
    nee_missing = rng.uniform(size=n) < 0.06
    for start_idx in rng.choice(n - 16, size=max(1, days // 6), replace=False):
        nee_missing[start_idx:start_idx + int(rng.integers(4, 16))] = True
    met_missing = rng.uniform(size=n) < 0.01
    nee = np.where(nee_missing, np.nan, nee)
    h = np.where(nee_missing, np.nan, h)
    le = np.where(nee_missing, np.nan, le)

    flux = pd.DataFrame(
        {
            "DateTime": dt,
            "co2_flux": nee,
            "qc_co2_flux": qc_co2,
            "H": h,
            "qc_H": qc_h,
            "LE": le,
            "qc_LE": qc_le,
            "air_temperature": np.where(met_missing, np.nan, tair_c + 273.15),
            "VPD": np.where(met_missing, np.nan, vpd_hpa * 100.0),
            "u.": ustar,
            "RH": rh,
            "wind_speed": np.clip(wind, 0.0, None),
        }
    )
    biomet = pd.DataFrame(
        {
            "DateTime": dt,
            "SWIN_1_1_1": np.where(met_missing, np.nan, rg),
            "P_RAIN_1_1_1": rain,
            "RH_1_1_1": rh,
            "TA_1_1_1": tair_c + 273.15,
        }
    )

    # Drop a few whole flux timestamps so stage-1 regularization inserts rows.
    drop = rng.choice(np.arange(1, n - 1), size=max(1, n // 400), replace=False)
    flux = flux.drop(index=drop).reset_index(drop=True)
    return flux, biomet


_DECIMALS = {
    "co2_flux": 4, "H": 3, "LE": 3, "air_temperature": 3, "VPD": 2,
    "u.": 4, "RH": 2, "wind_speed": 3, "SWIN_1_1_1": 2,
    "P_RAIN_1_1_1": 5, "RH_1_1_1": 2, "TA_1_1_1": 3,
}


def _format_frame(df: pd.DataFrame, columns: tuple[str, ...], units: tuple[str, ...]) -> str:
    out = pd.DataFrame(
        {
            "date": df["DateTime"].dt.strftime("%Y-%m-%d"),
            "time": df["DateTime"].dt.strftime("%H:%M"),
        }
    )
    for col in columns[2:]:
        values = df[col]
        if col in _DECIMALS:
            fmt = f"{{:.{_DECIMALS[col]}f}}"
            out[col] = [fmt.format(v) if np.isfinite(v) else "-9999" for v in values]
        else:
            out[col] = [str(int(v)) for v in values]
    lines = [",".join(columns), ",".join(units)]
    lines.extend(",".join(row) for row in out.itertuples(index=False, name=None))
    return "\n".join(lines) + "\n"


def render_csv_texts(*, days: int, seed: int, start: str = TEST_FIXTURE_START) -> dict[str, str]:
    """Return ``{"full_output/full_output.csv": text, "biomet/biomet.csv": text}``."""
    flux, biomet = generate_raw_frames(days=days, seed=seed, start=start)
    return {
        "full_output/full_output.csv": _format_frame(flux, FLUX_COLUMNS, FLUX_UNITS),
        "biomet/biomet.csv": _format_frame(biomet, BIOMET_COLUMNS, BIOMET_UNITS),
    }


def write_fixture(root: Path, *, days: int, seed: int, start: str = TEST_FIXTURE_START) -> dict[str, Path]:
    """Write the legacy two-directory fixture under ``root``; return paths."""
    written: dict[str, Path] = {}
    for rel, text in render_csv_texts(days=days, seed=seed, start=start).items():
        path = Path(root) / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(text.encode("utf-8"))
        written[rel] = path
    return written


def render_site_csv_texts(
    *, site_id: str, days: int, seed: int, start: str = TEST_FIXTURE_START
) -> dict[str, str]:
    """CSV texts for one site of a multi-site (``--group-column site_id``) input.

    Same generator and formatting as :func:`render_csv_texts` plus a trailing
    ``site_id`` column on both files, and **no unit row**: on a grouped run a
    unit row would become its own ``site_id`` category. Returns
    ``{"full_output/<site_id>.csv": text, "biomet/<site_id>.csv": text}``.
    """
    if not site_id or not all(ch.isalnum() or ch in "_-" for ch in site_id):
        raise ValueError(f"site_id must be a non-empty [A-Za-z0-9_-] label: {site_id!r}")
    flux, biomet = generate_raw_frames(days=days, seed=seed, start=start)
    texts: dict[str, str] = {}
    for folder, frame, columns, units in (
        ("full_output", flux, FLUX_COLUMNS, FLUX_UNITS),
        ("biomet", biomet, BIOMET_COLUMNS, BIOMET_UNITS),
    ):
        lines = _format_frame(frame, columns, units).splitlines()
        body = [lines[0] + ",site_id"] + [line + f",{site_id}" for line in lines[2:]]
        texts[f"{folder}/{site_id}.csv"] = "\n".join(body) + "\n"
    return texts


def write_site_fixture(
    root: Path, sites: list[tuple[str, int, int]], *, start: str = TEST_FIXTURE_START
) -> dict[str, Path]:
    """Write one flux and one biomet file per ``(site_id, days, seed)`` under ``root``."""
    written: dict[str, Path] = {}
    for site_id, days, seed in sites:
        for rel, text in render_site_csv_texts(site_id=site_id, days=days, seed=seed, start=start).items():
            path = Path(root) / rel
            if rel in written:
                raise ValueError(f"duplicate site_id {site_id!r}")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(text.encode("utf-8"))
            written[rel] = path
    return written


#: First flux ``primary_key`` of the typed Parquet inputs: above 2**53, so a
#: float64 round trip anywhere in the path would change the identifiers.
PARQUET_PRIMARY_KEY_BASE = 2**53 + 1


def site_parquet_frames(
    sites: list[tuple[str, int, int]], *, start: str = TEST_FIXTURE_START
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Typed multi-site ``(flux, biomet)`` frames in the case-study Parquet shape (M002-S05).

    Same generator as :func:`render_site_csv_texts`, but typed instead of
    CSV-formatted: ``site_id`` (string), ``timestamp`` (UTC instants), the
    raw measurement columns (missing values are null, not ``-9999``) and, on
    the flux side, an ``Int64`` ``primary_key`` starting at
    :data:`PARQUET_PRIMARY_KEY_BASE` (unique across sites).
    """
    flux_parts, biomet_parts = [], []
    seen: set[str] = set()
    for k, (site_id, days, seed) in enumerate(sites):
        if site_id in seen:
            raise ValueError(f"duplicate site_id {site_id!r}")
        seen.add(site_id)
        flux, biomet = generate_raw_frames(days=days, seed=seed, start=start)
        for frame in (flux, biomet):
            frame.insert(0, "timestamp", frame.pop("DateTime").dt.tz_localize("UTC"))
            frame.insert(0, "site_id", site_id)
        flux.insert(0, "primary_key", pd.array(
            PARQUET_PRIMARY_KEY_BASE + 10**7 * k + np.arange(len(flux)), dtype="Int64"))
        flux_parts.append(flux)
        biomet_parts.append(biomet)
    return pd.concat(flux_parts, ignore_index=True), pd.concat(biomet_parts, ignore_index=True)


def write_site_parquet(
    root: Path, sites: list[tuple[str, int, int]], *, start: str = TEST_FIXTURE_START
) -> dict[str, Path]:
    """Write ``root/flux.parquet`` and ``root/biomet.parquet`` (PyArrow, no index column)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    flux, biomet = site_parquet_frames(sites, start=start)
    Path(root).mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}
    for name, frame in (("flux", flux), ("biomet", biomet)):
        path = Path(root) / f"{name}.parquet"
        pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)
        written[f"{name}.parquet"] = path
    return written


def _site_spec(text: str) -> tuple[str, int, int]:
    site_id, days, seed = text.split(":")
    return site_id, int(days), int(seed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", type=Path)
    parser.add_argument("--days", type=int, default=TEST_FIXTURE_DAYS)
    parser.add_argument("--seed", type=int, default=TEST_FIXTURE_SEED)
    parser.add_argument("--start", default=TEST_FIXTURE_START)
    parser.add_argument(
        "--site",
        dest="sites",
        action="append",
        type=_site_spec,
        metavar="SITE_ID:DAYS:SEED",
        help=(
            "Write a multi-site input instead (repeatable): one flux and one "
            "biomet CSV per site with a site_id column and no unit row; "
            "--days/--seed are then ignored."
        ),
    )
    parser.add_argument(
        "--parquet",
        action="store_true",
        help=(
            "With --site: write ROOT/flux.parquet and ROOT/biomet.parquet (typed "
            "case-study shape: site_id, UTC timestamp, Int64 primary_key above "
            "2**53 on flux) instead of CSV folders."
        ),
    )
    args = parser.parse_args(argv)
    if args.parquet and not args.sites:
        parser.error("--parquet needs at least one --site")
    if args.parquet:
        write_site_parquet(args.root, args.sites, start=args.start)
    elif args.sites:
        write_site_fixture(args.root, args.sites, start=args.start)
    else:
        write_fixture(args.root, days=args.days, seed=args.seed, start=args.start)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
