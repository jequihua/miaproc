from __future__ import annotations

from typing import Literal
import pandas as pd

from .engine_hesseflux import HessefluxConfig, run_hesseflux_engine
from .engine_reddyproc import ReddyProcConfig, run_reddyproc_engine


# The contract backend name is "reddyproc-rpy2". "reddyproc" is kept as a
# deprecated alias for the migrated legacy dispatch; both route to the same
# implementation. Prefer "reddyproc-rpy2" in new code.
EngineName = Literal["hesseflux", "reddyproc-rpy2", "reddyproc"]

_REDDYPROC_BACKEND_ALIASES: tuple[str, ...] = ("reddyproc-rpy2", "reddyproc")


def postproc(
    df_stage1: pd.DataFrame,
    *,
    engine: EngineName = "hesseflux",
    hesseflux_config: HessefluxConfig | None = None,
    reddyproc_config: ReddyProcConfig | None = None,
) -> pd.DataFrame:
    """Stage-2 eddy post-processing: u* filtering, gap-filling, partitioning.

    Backends
    --------
    ``engine="hesseflux"``
        Portable Python-native backend (currently fixed-u* only; see
        Milestone 4 for dynamic u*).
    ``engine="reddyproc-rpy2"``
        Optional REddyProc-through-rpy2 parity backend. Requires R,
        REddyProc, and ``rpy2`` (install with ``pip install
        'miaproc[reddyproc]'`` plus R/REddyProc). The alias
        ``engine="reddyproc"`` routes to the same backend.
    """
    if engine == "hesseflux":
        cfg = hesseflux_config or HessefluxConfig()
        return run_hesseflux_engine(df_stage1, config=cfg)

    if engine in _REDDYPROC_BACKEND_ALIASES:
        cfg = reddyproc_config or ReddyProcConfig()
        return run_reddyproc_engine(df_stage1, config=cfg)

    raise ValueError(
        f"Unknown engine: {engine!r}. Supported: 'hesseflux', "
        "'reddyproc-rpy2' (alias 'reddyproc')."
    )
