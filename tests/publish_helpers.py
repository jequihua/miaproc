"""Test helper (M002-S03): publish a hand-built table as a valid run.

Not a test module. Gold consumes only published silver runs, so tests that
build a silver frame by hand publish it through the production publisher.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def publish_table(run_dir: Path, df: pd.DataFrame, name: str = "silver.parquet", *,
                  stage: str = "silver") -> Path:
    """Publish ``df`` as ``run_dir/name`` with a run JSON and a completion manifest."""
    from miaproc.cli import _write_json, _write_table
    from miaproc.eddy import publish

    run_dir = Path(run_dir)
    pub = publish.RunPublisher(stage=stage, run_dir=run_dir, outputs={}, inputs=[],
                               scratch_dir=run_dir.parent / f"_{run_dir.name}_scratch")
    try:
        _write_table(pub.staged(run_dir / name, role="table", expected_rows=int(len(df))), df)
        _write_json(pub.staged(run_dir / f"{stage}_run.json", role="run_metadata"),
                    {"stage": stage, "exit_code": 0})
        pub.publish(run_metadata=run_dir / f"{stage}_run.json")
    finally:
        pub.cleanup_scratch()
    return run_dir / name
