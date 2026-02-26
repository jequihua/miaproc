from __future__ import annotations

from typing import Literal
import pandas as pd

from .engine_hesseflux import HessefluxConfig, run_hesseflux_engine


EngineName = Literal["hesseflux", "reddyproc"]


def postproc(
    df_stage1: pd.DataFrame,
    *,
    engine: EngineName = "hesseflux",
    hesseflux_config: HessefluxConfig | None = None,
    # Future: reddyproc_config: ...
) -> pd.DataFrame:
    """
    Stage-2 eddy post-processing: u* filtering, gap-filling, and partitioning.

    - engine="hesseflux": pure-Python implementation (current)
    - engine="reddyproc": TODO (rpy2 + REddyProc)
    """
    if engine == "hesseflux":
        cfg = hesseflux_config or HessefluxConfig()
        return run_hesseflux_engine(df_stage1, config=cfg)

    if engine == "reddyproc":
        raise NotImplementedError(
            "engine='reddyproc' is TODO. Planned: rpy2 + REddyProc backend."
        )

    raise ValueError(f"Unknown engine: {engine}")