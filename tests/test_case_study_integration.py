"""Opt-in integration ingestion smoke test for real case-study files.

This test loads the actual repository case-study data under
``01_data/case_study`` with ``site_id="RBMNN"`` (Decision 008) and asserts
only ingestion/schema properties. It does **not** run scientific
comparisons — those belong to Milestone 5.

Run manually (Windows PowerShell):

    $env:MIAPROC_RUN_INTEGRATION = "1"
    cd 08_pkg
    ..\.venv\Scripts\python.exe -m pytest -v -m integration

The default ``pytest -v`` invocation does not execute this test's body.
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest


_INTEGRATION_ENV = "MIAPROC_RUN_INTEGRATION"


def _case_study_root() -> Path:
    """Repo-root-relative path to the case-study data.

    Resolved from this file's location so the test works regardless of the
    working directory the reviewer uses.
    """
    # tests file -> 08_pkg/tests -> 08_pkg -> repo root
    return Path(__file__).resolve().parents[2] / "01_data" / "case_study"


def _integration_enabled() -> bool:
    return os.environ.get(_INTEGRATION_ENV) == "1"


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _integration_enabled(),
        reason=(
            f"Opt-in integration test. Set {_INTEGRATION_ENV}=1 to run. "
            "This test reads 01_data/case_study real files and takes longer "
            "than the default Python-only suite."
        ),
    ),
]


def test_rbmnn_ingestion_smoke():
    from miaproc.eddy import load_stage1

    root = _case_study_root()
    flux_dir = root / "flux"
    biomet_dir = root / "biomet"
    if not flux_dir.exists() or not biomet_dir.exists():
        pytest.skip(
            f"Case-study directories not found under {root}. The repository "
            "may not include the raw case-study files in this clone."
        )

    df = load_stage1(
        path_full_output=flux_dir,
        path_biomet=biomet_dir,
        tz_in="UTC",
        tz_out="UTC",
        skip_full_output=0,
        skip_biomet=0,
        drop_rain_rows=True,
        site_id="RBMNN",
    )

    # Schema-only assertions per Decision 008 policy; no scientific checks.
    assert len(df) > 0
    # Required stage-1 columns per 01_data/schema.md "Stage-1 Output Fields".
    required_columns = {"DateTime", "NEE", "Tair", "USTAR", "QC_NEE", "Rg"}
    missing = required_columns - set(df.columns)
    assert not missing, f"missing required stage-1 columns: {sorted(missing)}"

    # Monotonic, duplicate-free DateTime.
    dt = df["DateTime"]
    assert pd.api.types.is_datetime64_any_dtype(dt)
    assert dt.is_monotonic_increasing, "DateTime must be sorted ascending"
    # After `regularize_time_grid` on a single-site slice, duplicates cannot
    # occur; assert explicitly so a regression surfaces here.
    assert dt.duplicated().sum() == 0

    # Enough rows for M4 dynamic-u* tests to have material to work with.
    assert len(df) >= 500, (
        f"expected >= 500 rows for M4 dynamic tests, got {len(df)}"
    )
