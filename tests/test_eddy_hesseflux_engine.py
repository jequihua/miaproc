from pathlib import Path
import pandas as pd

from miaproc.eddy import load_stage1
from miaproc.eddy import postproc, HessefluxConfig


def test_hesseflux_engine_runs():
    base = Path(__file__).parent / "data"
    df = load_stage1(
        path_full_output=base / "full_output",
        path_biomet=base / "biomet",
        tz_in="UTC",
        tz_out="UTC",
        skip_full_output=0,
        skip_biomet=0,
        drop_rain_rows=True,
    )

    out = postproc(
        df,
        engine="hesseflux",
        hesseflux_config=HessefluxConfig(
            ustar_fixed=0.1,
            partition_method="reichstein",
        ),
    )

    assert "NEE_f" in out.columns
    assert "NEE_fqc" in out.columns
    assert "SW_IN_f" in out.columns
    assert "TA_f" in out.columns
    assert "VPD_f" in out.columns

    # At least some filled values should exist (unless dataset is already complete)
    assert out["NEE_f"].notna().sum() > 0