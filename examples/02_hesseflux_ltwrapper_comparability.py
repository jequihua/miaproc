"""Example 02 — hesseflux with the opt-in Lloyd-Taylor Reco wrapper.

Same ingestion as example 01, but runs the hesseflux backend with
``reco_fit_mode="lt_reddyproc_aligned"``. Intended for
REddyProc-comparability studies (see Decision 011 and
``docs/m5_reddyproc_hesseflux_magnitude_note.md``).

Wrapper semantics (Decision 011, do not soften):

- **Opt-in**: ``reco_fit_mode="native"`` remains the default in
  ``HessefluxConfig``; this example flips the flag explicitly.
- **NaN outside LT domain**: wrapper returns ``NaN`` for Reco where
  ``Tair_f`` is outside the Lloyd-Taylor temperature domain
  (``T > T0 + 1 °C``). No extrapolation.
- **No silent fallback**: on any fit failure (insufficient nighttime
  samples, invalid domain, optimizer failure, boundary-bound solution)
  the engine raises ``LTWrapperError``. This example does not catch it —
  failures surface.

Usage::

    python examples/02_hesseflux_ltwrapper_comparability.py \\
        --flux-dir /path/to/flux \\
        --biomet-dir /path/to/biomet

Does **not** write output files.
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
    p.add_argument("--site-id", default="RBMNN")
    p.add_argument("--skip-flux", type=int, default=0)
    p.add_argument("--skip-biomet", type=int, default=0)
    p.add_argument(
        "--lt-min-night-samples",
        type=int,
        default=100,
        help=(
            "Minimum nighttime rows (fqc==0) required for the Lloyd-Taylor "
            "fit. Decoupled from the u* estimator's threshold "
            "(see Decision 011)."
        ),
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    print(f"[02] Stage-1 load: flux={args.flux_dir} biomet={args.biomet_dir} "
          f"site_id={args.site_id}")
    df_stage1 = load_stage1(
        path_full_output=str(args.flux_dir),
        path_biomet=str(args.biomet_dir),
        skip_full_output=args.skip_flux,
        skip_biomet=args.skip_biomet,
        site_id=args.site_id,
        drop_rain_rows=False,
    )
    print(f"[02] Stage-1 rows: {len(df_stage1)}")

    cfg = HessefluxConfig(
        ustar_mode="dynamic",
        ustar_probs=(0.05, 0.5, 0.95),
        ustar_scenario="U50",
        ustar_min_night_samples=500,
        ustar_temp_bins=4,
        ustar_bins=20,
        ustar_plateau_fraction=0.95,
        partition_method="lasslop",  # unused in wrapper path; kept for contract
        swthr=20.0,
        nogppnight=False,
        reco_fit_mode="lt_reddyproc_aligned",  # opt-in
        lt_min_night_samples=args.lt_min_night_samples,
    )
    print("[02] Running hesseflux (reco_fit_mode=lt_reddyproc_aligned) ...")
    print("[02] LTWrapperError propagates on any fit failure (no silent fallback).")
    out = postproc(df_stage1, engine="hesseflux", hesseflux_config=cfg)

    diag = out.attrs.get("miaproc_diagnostics") or {}
    ustar = diag.get("ustar") or {}
    part = diag.get("partitioning") or {}
    lt = (part.get("lt_wrapper") or {}) if isinstance(part, dict) else {}
    reco = out["Reco"] if "Reco" in out.columns else None
    n_finite_reco = int(reco.notna().sum()) if reco is not None else 0

    print("[02] Diagnostics:")
    print(f"  backend               : {diag.get('backend')}")
    print(f"  ustar.selected_threshold: {ustar.get('selected_threshold')}")
    print(f"  partitioning.reco_mode: {part.get('reco_fit_mode')}")
    print(f"  lt_wrapper.fit_status : {lt.get('fit_status')}")
    print(f"  lt_wrapper.n_night   : {lt.get('n_night_samples')}")
    print(f"  lt_wrapper.Rref       : {lt.get('rref')}")
    print(f"  lt_wrapper.E0         : {lt.get('e0')}")
    print(f"  rows in output        : {len(out)}")
    print(f"  finite Reco rows      : {n_finite_reco} "
          f"(NaN outside LT domain is expected — see Decision 011)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
