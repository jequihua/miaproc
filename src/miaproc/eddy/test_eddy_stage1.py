from pathlib import Path
import pandas as pd
from miaproc.eddy import load_stage1

def test_stage1_regularizes_to_30min_grid():
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

    # Should be strictly 30-min spaced
    diffs = df["DateTime"].sort_values().diff().dropna()
    assert (diffs == pd.Timedelta(minutes=30)).all()

