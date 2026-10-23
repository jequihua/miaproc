"""Tests for the M24 all-data grouped CLI execution semantics.

The M24 contract removes `--site-id` from every eddy CLI command and
introduces `--group-column` for partitioning the all-data input into
per-category runs that are stacked deterministically.

Coverage:

- every eddy command rejects ``--site-id`` at the argparse layer;
- ``--group-column site_id`` partitions the file-based silver path.

M002-S04 removed the BigQuery commands, read configs, writeback and
per-site watermarks that the other M24 tests in this module exercised.
Their storage-neutral grouped assertions (independent per-category runs,
deterministic stacking, per-group artifacts, null groups, colliding
labels) are covered by the file CLI tests named as replacements in
``retired_test_ids.json``. The ``--site-id`` parametrization keeps the
original ids for the surviving commands (``command0`` run, ``command2``
run-silver, ``command3`` run-gold).
"""
from __future__ import annotations

import pandas as pd
import pytest

from miaproc import cli


# ---------------------------------------------------------------------------
# Stub frames carrying two distinct site_ids so grouped tests have
# something to partition.
# ---------------------------------------------------------------------------


def _stub_multisite_flux_df() -> pd.DataFrame:
    """A bronze/source flux frame with two sites."""
    return pd.DataFrame(
        {
            "timestamp": [
                "2025-08-01T00:00:00Z",
                "2025-08-01T00:30:00Z",
                "2025-08-01T00:00:00Z",
                "2025-08-01T00:30:00Z",
            ],
            "site_id": ["RBRL", "RBRL", "RBMNN", "RBMNN"],
        }
    )


def _stub_multisite_biomet_df() -> pd.DataFrame:
    return _stub_multisite_flux_df().copy()


def _stub_silver_df_per_group(category: str, n: int = 4) -> pd.DataFrame:
    """A silver-shaped frame for one group; site_id is preserved so
    the gold->stage path can attach the right per-row site."""
    base_ts = pd.date_range(
        "2025-08-01", periods=n, freq="30min", tz="UTC"
    )
    return pd.DataFrame(
        {
            "DateTime": base_ts,
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "QC_NEE": [0, 0, 0, 0],
            "site_id": [category] * n,
        }
    )


# ---------------------------------------------------------------------------
# 1. Parser rejection: every eddy command must reject --site-id.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        pytest.param(["run"], id="command0"),
        pytest.param(["eddy", "run-silver"], id="command2"),
        pytest.param(["eddy", "run-gold"], id="command3"),
    ],
)
def test_eddy_commands_reject_site_id_flag(command):
    """M24: --site-id is removed from every eddy command surface."""
    parser = cli._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(command + ["--site-id", "RBRL"])


# ---------------------------------------------------------------------------
# 7. File-based silver grouped run still works without --site-id.
# ---------------------------------------------------------------------------


class TestFileBasedGroupedSilver:
    def test_run_silver_with_group_column_processes_each_site(
        self, tmp_path, monkeypatch
    ):
        flux = tmp_path / "flux"
        biomet = tmp_path / "biomet"
        flux.mkdir()
        biomet.mkdir()

        seen: list[str] = []

        def _fake_read_combine(p, skip_rows=0):
            return _stub_multisite_flux_df()

        def _fake_stage1_from_raw(
            full_output, biomet_in, *,
            tz_in="UTC", tz_out="UTC",
            drop_rain_rows=True, site_id=None,
        ):
            cat = (
                str(full_output["site_id"].iloc[0])
                if "site_id" in full_output.columns
                and len(full_output) > 0
                else "<none>"
            )
            seen.append(cat)
            return _stub_silver_df_per_group(cat)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "read_and_combine_csv", _fake_read_combine
        )
        monkeypatch.setattr(
            eddy_pkg, "stage1_from_raw_frames", _fake_stage1_from_raw
        )

        argv = [
            "eddy",
            "run-silver",
            "--flux-dir", str(flux),
            "--biomet-dir", str(biomet),
            "--output-table", str(tmp_path / "srun" / "silver.parquet"),
            "--output-run-json", str(tmp_path / "srun" / "silver_run.json"),
            "--group-column", "site_id",
        ]
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert seen == ["RBMNN", "RBRL"]
        # Stacked output exists.
        df = pd.read_parquet(tmp_path / "srun" / "silver.parquet")
        assert set(df["site_id"].unique()) == {"RBMNN", "RBRL"}
