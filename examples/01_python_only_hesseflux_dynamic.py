"""Example 01 — Python-only hesseflux workflow with dynamic u*.

Runs the standard Python-native post-processing pipeline on a user-provided
RBMNN-shape case study:

1. ``load_stage1`` ingests flux + biomet CSVs, applies QC, rain, and 3σ
   filters, and standardizes to a 30-minute grid.
2. ``postproc(engine="hesseflux", ...)`` runs dynamic u* + MDS gap-fill +
   ``nee2gpp(method="lasslop")`` with the Decision 011 closure-track defaults.
3. Prints a compact diagnostics summary.

Does **not** write output files. Extend this script yourself if you want
derived CSVs or Parquet.

Usage::

    python examples/01_python_only_hesseflux_dynamic.py \\
        --flux-dir /path/to/flux \\
        --biomet-dir /path/to/biomet \\
        --site-id RBMNN

Required input column shape: see ``examples/README.md`` and
``08_pkg/backend_contract.md``.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from miaproc.eddy import HessefluxConfig, load_stage1, postproc


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--flux-dir", required=True, type=Path)
    p.add_argument("--biomet-dir", required=True, type=Path)
    p.add_argument(
        "--site-id",
        default="RBMNN",
        help="Site ID used to filter multi-site case-study CSVs (Decision 008).",
    )
    p.add_argument(
        "--skip-flux",
        type=int,
        default=0,
        help="Rows to skip at the top of each flux CSV (1 if a units row is present).",
    )
    p.add_argument(
        "--skip-biomet",
        type=int,
        default=0,
        help="Rows to skip at the top of each biomet CSV.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    print(f"[01] Stage-1 load: flux={args.flux_dir} biomet={args.biomet_dir} "
          f"site_id={args.site_id}")
    df_stage1 = load_stage1(
        path_full_output=str(args.flux_dir),
        path_biomet=str(args.biomet_dir),
        skip_full_output=args.skip_flux,
        skip_biomet=args.skip_biomet,
        site_id=args.site_id,
        drop_rain_rows=False,  # Decision 011 closure-track default
    )
    print(f"[01] Stage-1 rows: {len(df_stage1)}")

    cfg = HessefluxConfig(
        ustar_mode="dynamic",
        ustar_probs=(0.05, 0.5, 0.95),
        ustar_scenario="U50",
        ustar_min_night_samples=500,
        ustar_temp_bins=4,
        ustar_bins=20,
        ustar_plateau_fraction=0.95,
        partition_method="lasslop",
        swthr=20.0,
        nogppnight=False,
        # reco_fit_mode="native" is the default; see example 02 for the opt-in.
    )
    print(f"[01] Running hesseflux (native, lasslop, swthr={cfg.swthr}, "
          f"ustar_mode={cfg.ustar_mode}) ...")
    out = postproc(df_stage1, engine="hesseflux", hesseflux_config=cfg)

    diag = out.attrs.get("miaproc_diagnostics") or {}
    ustar = diag.get("ustar") or {}
    part = diag.get("partitioning") or {}
    print("[01] Diagnostics:")
    print(f"  backend               : {diag.get('backend')}")
    print(f"  ustar.mode            : {ustar.get('mode')}")
    print(f"  ustar.scenario        : {ustar.get('scenario')}")
    print(f"  ustar.selected_threshold: {ustar.get('selected_threshold')}")
    print(f"  ustar.night_samples   : {ustar.get('night_sample_count')}")
    print(f"  partitioning.method   : {part.get('method')}")
    print(f"  partitioning.reco_mode: {part.get('reco_fit_mode')}")
    print(f"  rows in output        : {len(out)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
