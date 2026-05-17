"""Example 03 — R-backed REddyProc reference via rpy2.

Runs the project-scoped R preflight first (Decision 010 / risk R11) and
aborts unless the discovered R runtime is **project-scoped approved**.
Only if the preflight passes does the script run
``postproc(engine="reddyproc-rpy2", ...)``.

Usage::

    # Windows PowerShell
    $env:R_HOME='C:\\Program Files\\R\\R-4.5.3'
    python examples/03_r_backed_reddyproc_reference.py \\
        --flux-dir /path/to/flux \\
        --biomet-dir /path/to/biomet \\
        --repo-root C:\\path\\to\\miaproc-dev

Requires: ``miaproc[reddyproc]`` extra (installs ``rpy2``), R 4.5+ with
a project-scoped ``renv`` + ``REddyProc 1.3.4`` under ``--repo-root``.

Does **not** write output files.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--flux-dir", required=True, type=Path)
    p.add_argument("--biomet-dir", required=True, type=Path)
    p.add_argument("--site-id", default="RBMNN")
    p.add_argument("--skip-flux", type=int, default=0)
    p.add_argument("--skip-biomet", type=int, default=0)
    p.add_argument(
        "--repo-root",
        required=True,
        type=Path,
        help=(
            "Repository root containing renv.lock and the project-scoped "
            "R library under renv/library/... (Decision 010)."
        ),
    )
    p.add_argument(
        "--site-name", default="Marismas_Nacionales",
        help="Site label passed to sEddyProc$new.",
    )
    p.add_argument("--latitude", type=float, default=22.25)
    p.add_argument("--longitude", type=float, default=-105.50)
    p.add_argument(
        "--timezone-hour", type=float, default=-7.0,
        help="REddyProc TimeZoneHour (wall-clock offset used by sSetLocationInfo).",
    )
    p.add_argument(
        "--local-tz", default="America/Mazatlan",
        help="IANA timezone used to derive DoY/Hour in local wall-clock time.",
    )
    return p


def _run_preflight(repo_root: Path) -> None:
    """Run the project-scoped R preflight; abort on anything less than
    an approved project-scoped runtime.

    Per Decision 010 / R11: the ``reddyproc-rpy2`` backend must not
    silently bind to a global R installation. This example refuses to
    proceed if the discovered runtime is not project-scoped approved.
    """
    from miaproc.eddy import (
        RRuntimePreflightPolicy,
        preflight_reddyproc_r_environment,
        render_r_preflight_report,
    )

    print(f"[03] Preflight: repo_root={repo_root} R_HOME={os.environ.get('R_HOME')!r}")
    policy = RRuntimePreflightPolicy(repo_root=str(repo_root))
    result = preflight_reddyproc_r_environment(policy=policy)
    report = render_r_preflight_report(result)
    print(report)

    approval_source = str(result.approval_source or "")
    if (
        result.status != "ok"
        or not result.approved
        or not approval_source.startswith("project-scoped")
    ):
        print(
            "[03] Preflight is not project-scoped approved. "
            "Refusing to run the reddyproc-rpy2 backend.",
            file=sys.stderr,
        )
        sys.exit(2)
    print("[03] Preflight approved (project-scoped).")


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    _run_preflight(args.repo_root)

    # Imports kept after the preflight so the error path does not depend
    # on rpy2 loading successfully.
    from miaproc.eddy import ReddyProcConfig, load_stage1, postproc

    print(f"[03] Stage-1 load: flux={args.flux_dir} biomet={args.biomet_dir} "
          f"site_id={args.site_id}")
    df_stage1 = load_stage1(
        path_full_output=str(args.flux_dir),
        path_biomet=str(args.biomet_dir),
        skip_full_output=args.skip_flux,
        skip_biomet=args.skip_biomet,
        site_id=args.site_id,
        drop_rain_rows=False,
    )
    print(f"[03] Stage-1 rows: {len(df_stage1)}")

    cfg = ReddyProcConfig(
        site_name=args.site_name,
        latitude=args.latitude,
        longitude=args.longitude,
        timezone_hour=args.timezone_hour,
        local_tz=args.local_tz,
        ustar_n_sample=200,
        ustar_probs=(0.05, 0.5, 0.95),
        ustar_scenario="U50",
    )
    print(f"[03] Running reddyproc-rpy2 with site={cfg.site_name}, "
          f"local_tz={cfg.local_tz} ...")
    out = postproc(df_stage1, engine="reddyproc-rpy2", reddyproc_config=cfg)

    diag = out.attrs.get("miaproc_diagnostics") or {}
    ustar = diag.get("ustar") or {}
    versions = diag.get("versions") or {}
    print("[03] Diagnostics:")
    print(f"  backend              : {diag.get('backend')}")
    print(f"  partitioning         : {diag.get('partitioning')}")
    print(f"  ustar.scenario       : {ustar.get('scenario')}")
    print(f"  ustar.available      : {ustar.get('available_scenarios')}")
    print(f"  versions.R           : {versions.get('R')}")
    print(f"  versions.REddyProc   : {versions.get('REddyProc')}")
    print(f"  versions.rpy2        : {versions.get('rpy2')}")
    print(f"  rows in output       : {len(out)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
