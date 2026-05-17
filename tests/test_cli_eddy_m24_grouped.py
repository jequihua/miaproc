"""Tests for the M24 all-data grouped CLI execution semantics.

The M24 contract removes `--site-id` from every eddy CLI command and
introduces `--group-column` for partitioning the all-data input into
per-category runs. The shared stage tables only become valid because
BigQuery writeback runs **once** against the stacked output.

Coverage:

- every eddy command rejects ``--site-id`` at the argparse layer;
- ``--group-column site_id`` partitions BigQuery silver/gold reads
  and the file-based silver/run path; the BigQuery read configs do
  not carry a site filter;
- per-category artefacts and a stacked output exist after a grouped
  CLI run;
- BigQuery writeback is called **once** with the stacked DataFrame
  (so shared stage tables are valid);
- ``run_writeback`` advances a watermark per stacked site after a
  successful explicit MERGE; stage-only / failed runs do not advance
  watermarks;
- ``BigQueryEddyConfig`` accepts ``site_id=None`` and the rendered
  SELECTs have no ``WHERE site_id = @site_id`` clause.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from miaproc import cli
from miaproc.eddy import (
    BigQueryEddyConfig,
    BigQueryReadResult,
    BigQuerySilverInputConfig,
    BigQuerySilverReadResult,
    BigQueryWritebackConfig,
    GROUPED_RUN_ROW_SITE_LABEL,
    WritebackResult,
    max_timestamps_by_site,
)
from miaproc.eddy.bigquery_runner import (
    build_biomet_query,
    build_flux_query,
    build_silver_query,
)


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


def _stub_gold_df() -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=4, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "NEE_f": [0.1, 0.2, -0.1, 0.3],
            "NEE_fqc": [0, 0, 0, 0],
            "GPP": [0.0, 1.5, 3.0, 0.5],
            "Reco": [1.0, 1.0, 1.0, 1.0],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "Tair_f": [20.0, 21.0, 22.0, 19.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "Rg_f": [0.0, 100.0, 200.0, 50.0],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "VPD_f": [5.0, 6.0, 7.0, 4.0],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
        }
    )
    df.attrs["miaproc_diagnostics"] = {
        "backend": "hesseflux",
        "ustar": {"mode": "dynamic", "selected_threshold": 0.18},
        "partitioning": {
            "method": "lasslop",
            "reco_fit_mode": "native",
            "lt_wrapper": None,
        },
        "warnings": (),
    }
    return df


# ---------------------------------------------------------------------------
# 1. Parser rejection: every eddy command must reject --site-id.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "command",
    [
        ["run"],
        ["eddy", "run-bigquery"],
        ["eddy", "run-silver"],
        ["eddy", "run-gold"],
        ["eddy", "run-bigquery-silver"],
        ["eddy", "run-bigquery-gold"],
    ],
)
def test_eddy_commands_reject_site_id_flag(command):
    """M24: --site-id is removed from every eddy command surface."""
    parser = cli._build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(command + ["--site-id", "RBRL"])


# ---------------------------------------------------------------------------
# 2. BigQueryEddyConfig accepts site_id=None and the rendered SELECT
#    has no WHERE site_id filter.
# ---------------------------------------------------------------------------


class TestBigQueryConfigAllCategories:
    def test_default_site_id_is_none(self):
        cfg = BigQueryEddyConfig(
            input_project="manglaria",
            input_dataset="ds",
            flux_table="flux",
            biomet_table="biomet",
        )
        assert cfg.site_id is None

    def test_flux_query_has_no_site_filter_when_site_id_none(self):
        cfg = BigQueryEddyConfig(
            input_project="manglaria",
            input_dataset="ds",
            flux_table="flux",
            biomet_table="biomet",
            site_id=None,
        )
        sql = build_flux_query(cfg)
        assert "site_id = @site_id" not in sql
        assert "WHERE" not in sql or "timestamp" in sql

    def test_biomet_query_has_no_site_filter_when_site_id_none(self):
        cfg = BigQueryEddyConfig(
            input_project="manglaria",
            input_dataset="ds",
            flux_table="flux",
            biomet_table="biomet",
            site_id=None,
        )
        sql = build_biomet_query(cfg)
        assert "site_id = @site_id" not in sql

    def test_flux_query_keeps_site_filter_when_site_id_set(self):
        cfg = BigQueryEddyConfig(
            input_project="manglaria",
            input_dataset="ds",
            flux_table="flux",
            biomet_table="biomet",
            site_id="RBRL",
        )
        sql = build_flux_query(cfg)
        assert "site_id = @site_id" in sql

    def test_silver_query_has_no_site_filter_when_none(self):
        cfg = BigQuerySilverInputConfig(
            input_project="manglaria",
            input_dataset="ds",
            silver_table="silver",
            site_id=None,
        )
        sql = build_silver_query(cfg)
        assert "site_id = @site_id" not in sql


# ---------------------------------------------------------------------------
# 3. Per-site watermark helper.
# ---------------------------------------------------------------------------


class TestMaxTimestampsBySite:
    def test_single_site_one_entry(self):
        df = pd.DataFrame(
            {
                "site_id": ["RBRL"] * 3,
                "timestamp": pd.to_datetime(
                    [
                        "2025-08-01T00:00:00Z",
                        "2025-08-01T00:30:00Z",
                        "2025-08-01T01:00:00Z",
                    ]
                ),
            }
        )
        out = max_timestamps_by_site(df)
        assert set(out) == {"RBRL"}

    def test_two_sites_two_entries(self):
        df = pd.DataFrame(
            {
                "site_id": ["RBRL", "RBRL", "RBMNN", "RBMNN"],
                "timestamp": pd.to_datetime(
                    [
                        "2025-08-01T00:00:00Z",
                        "2025-08-01T01:00:00Z",
                        "2025-08-01T00:30:00Z",
                        "2025-08-01T02:00:00Z",
                    ]
                ),
            }
        )
        out = max_timestamps_by_site(df)
        assert set(out) == {"RBRL", "RBMNN"}
        # Each entry is that site's max.
        assert out["RBRL"].startswith("2025-08-01T01:00")
        assert out["RBMNN"].startswith("2025-08-01T02:00")

    def test_no_site_id_column_returns_empty(self):
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2025-08-01T00:00:00Z"]
                )
            }
        )
        assert max_timestamps_by_site(df) == {}


# ---------------------------------------------------------------------------
# 4. Grouped BigQuery silver run: read once, partition in Python,
#    stack output, single writeback call covering all sites.
# ---------------------------------------------------------------------------


class TestGroupedBigQuerySilver:
    def _patch_pipeline(self, monkeypatch):
        """Stub BigQuery read + load_stage1_from_dataframes +
        stage1_from_raw_frames so the test never hits a real loader."""
        bq_calls: dict[str, Any] = {"cfg": None, "stage1_calls": []}

        flux_df = _stub_multisite_flux_df()
        biomet_df = _stub_multisite_biomet_df()
        fake_result = BigQueryReadResult(
            flux_df=flux_df,
            biomet_df=biomet_df,
            flux_rows=len(flux_df),
            biomet_rows=len(biomet_df),
            flux_query=build_flux_query(
                BigQueryEddyConfig(
                    input_project="manglaria",
                    input_dataset="ds",
                    flux_table="flux",
                    biomet_table="biomet",
                    site_id=None,
                )
            ),
            biomet_query="SELECT * FROM biomet",
            query_parameters={},
        )

        def _fake_read(cfg, *, client=None):
            bq_calls["cfg"] = cfg
            return fake_result

        def _fake_stage1_from_raw(
            full_output, biomet, *,
            tz_in="UTC", tz_out="UTC",
            drop_rain_rows=True, site_id=None,
        ):
            # Mirror back the caller's category so the output rows
            # carry it; use the unique site_id of the input as the
            # category label.
            cat = (
                str(full_output["site_id"].iloc[0])
                if "site_id" in full_output.columns
                and len(full_output) > 0
                else "<none>"
            )
            bq_calls["stage1_calls"].append(cat)
            return _stub_silver_df_per_group(cat)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "stage1_from_raw_frames", _fake_stage1_from_raw
        )

        def _fake_load(**kwargs):
            return _stub_silver_df_per_group("ALL")

        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )

        return SimpleNamespace(calls=bq_calls)

    def _argv(self, tmp_path: Path, **overrides) -> list[str]:
        base = {
            "--bq-input-project": "manglaria",
            "--bq-input-dataset": "ds",
            "--bq-flux-table": "flux",
            "--bq-biomet-table": "biomet",
            "--output-table": str(tmp_path / "silver.parquet"),
            "--output-run-json": str(tmp_path / "silver_run.json"),
            "--group-column": "site_id",
        }
        base.update(overrides)
        argv = ["eddy", "run-bigquery-silver"]
        for k, v in base.items():
            argv.extend([k, str(v)])
        return argv

    def test_grouped_silver_processes_every_category(
        self, tmp_path, monkeypatch
    ):
        patch = self._patch_pipeline(monkeypatch)
        rc = cli.main(self._argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        # Both sites in deterministic order.
        assert patch.calls["stage1_calls"] == ["RBMNN", "RBRL"]
        # The BigQuery read config carried site_id=None.
        assert patch.calls["cfg"].site_id is None

    def test_grouped_silver_writes_per_category_artifacts(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        rc = cli.main(self._argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        run = json.loads(
            (tmp_path / "silver_run.json").read_text("utf-8")
        )
        assert run["group_column"] == "site_id"
        groups = run["groups"]
        assert [g["category_value"] for g in groups] == ["RBMNN", "RBRL"]
        # Per-category table artefacts exist.
        for g in groups:
            assert Path(g["table_path"]).exists()
        # Groups dir is reported.
        assert run["outputs"]["groups_dir"]

    def test_grouped_silver_stacks_final_output(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        rc = cli.main(self._argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        df = pd.read_parquet(tmp_path / "silver.parquet")
        # Stacked output is the concatenation of two groups.
        assert len(df) == 8
        # Both site_ids present.
        assert set(df["site_id"].unique()) == {"RBMNN", "RBRL"}

    def test_grouped_silver_writeback_called_once_with_stacked(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        captured: dict[str, Any] = {"calls": []}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["calls"].append(
                {
                    "stage_table": cfg.stage_table,
                    "rows": int(len(df)),
                    "sites": sorted(
                        str(v) for v in df["site_id"].dropna().unique()
                    ),
                    "site_id_arg": site_id,
                }
            )
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=False,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)
        argv = self._argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        # Exactly one call to run_writeback for the stacked frame.
        assert len(captured["calls"]) == 1
        call = captured["calls"][0]
        # Shared stage table name accepted (no per-site suffix needed).
        assert call["stage_table"] == "cf_s2_silver_stage"
        # Stacked rows from both groups.
        assert call["rows"] == 8
        # Both sites in the stage payload.
        assert call["sites"] == ["RBMNN", "RBRL"]
        # CLI does not pass a site_id (M24 grouped default).
        assert call["site_id_arg"] is None


# ---------------------------------------------------------------------------
# 5. Grouped BigQuery gold run: stacked stage payload + final MERGE
#    advances per-site watermark.
# ---------------------------------------------------------------------------


class TestGroupedBigQueryGold:
    def _patch_pipeline(self, monkeypatch):
        flux_df = pd.concat(
            [
                _stub_silver_df_per_group("RBMNN"),
                _stub_silver_df_per_group("RBRL"),
            ],
            ignore_index=True,
        )
        fake_result = BigQuerySilverReadResult(
            silver_df=flux_df,
            silver_rows=len(flux_df),
            silver_query=build_silver_query(
                BigQuerySilverInputConfig(
                    input_project="manglaria-staging",
                    input_dataset="ds",
                    silver_table="silver",
                    site_id=None,
                )
            ),
            query_parameters={},
        )
        seen: dict[str, list[Any]] = {"engine_inputs": []}

        def _fake_silver_read(cfg, *, client=None):
            seen["silver_cfg"] = cfg
            return fake_result

        def _fake_postproc(
            df, *, engine, hesseflux_config=None, reddyproc_config=None
        ):
            seen["engine_inputs"].append(
                df["site_id"].iloc[0] if len(df) else None
            )
            return _stub_gold_df()

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "read_bigquery_silver_input", _fake_silver_read
        )
        monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
        return SimpleNamespace(seen=seen)

    def _argv(self, tmp_path: Path, **overrides) -> list[str]:
        base = {
            "--engine": "hesseflux-native",
            "--bq-input-project": "manglaria-staging",
            "--bq-input-dataset": "ds",
            "--bq-silver-table": "cf_s2_silver_stage",
            "--output-table": str(tmp_path / "gold.parquet"),
            "--output-diagnostics-json": str(
                tmp_path / "gold_diag.json"
            ),
            "--output-run-json": str(tmp_path / "gold_run.json"),
            "--group-column": "site_id",
        }
        base.update(overrides)
        argv = ["eddy", "run-bigquery-gold"]
        for k, v in base.items():
            argv.extend([k, str(v)])
        return argv

    def test_grouped_gold_runs_engine_per_category(
        self, tmp_path, monkeypatch
    ):
        patch = self._patch_pipeline(monkeypatch)
        rc = cli.main(self._argv(tmp_path))
        assert rc == cli.SUCCESS_EXIT
        # Both categories processed in deterministic order.
        assert patch.seen["engine_inputs"] == ["RBMNN", "RBRL"]
        # The silver BQ read config carried site_id=None.
        assert patch.seen["silver_cfg"].site_id is None

    def test_grouped_gold_writeback_one_call_with_both_sites(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        captured: dict[str, Any] = {"calls": []}

        def _fake_run_writeback(
            df, cfg, *, run_id, started_at, site_id=None,
            run_payload_extras=None, client=None,
        ):
            captured["calls"].append(
                {
                    "rows": int(len(df)),
                    "sites": sorted(
                        str(v) for v in df["site_id"].dropna().unique()
                    ),
                }
            )
            return WritebackResult(
                run_id=run_id,
                status="stage_only_succeeded",
                stage_rows=int(len(df)),
                merge_attempted=False,
                merge_authorized=False,
                merge_inserted_rows=None,
                merge_updated_rows=None,
                watermark_advanced=False,
                watermark_value=None,
                validation_metrics={"row_count": int(len(df))},
                stage_table_fqn=cfg.stage_table_fqn(),
                final_table_fqn=cfg.final_table_fqn(),
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_run_writeback)
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_columns", lambda cfg, **_: None
        )
        monkeypatch.setattr(
            eddy_pkg, "read_final_table_schema", lambda cfg, **_: None
        )
        argv = self._argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_gold_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert len(captured["calls"]) == 1
        assert captured["calls"][0]["sites"] == ["RBMNN", "RBRL"]


# ---------------------------------------------------------------------------
# 6. run_writeback advances per-site watermark on stacked merge runs.
# ---------------------------------------------------------------------------


class TestRunWritebackPerSiteWatermark:
    def _stage_df_two_sites(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "primary_key": [
                    "RBRL|2025-08-01T00:00:00+00:00",
                    "RBRL|2025-08-01T01:00:00+00:00",
                    "RBMNN|2025-08-01T00:30:00+00:00",
                    "RBMNN|2025-08-01T02:00:00+00:00",
                ],
                "site_id": ["RBRL", "RBRL", "RBMNN", "RBMNN"],
                "timestamp": pd.to_datetime(
                    [
                        "2025-08-01T00:00:00Z",
                        "2025-08-01T01:00:00Z",
                        "2025-08-01T00:30:00Z",
                        "2025-08-01T02:00:00Z",
                    ]
                ),
                "nee_f": [0.1, 0.2, 0.3, 0.4],
            }
        )

    def _cfg(self):
        return BigQueryWritebackConfig(
            output_project="manglaria-staging",
            output_dataset="manglaria_lakehouse_ds",
            stage_table="cf_s2_gold_stage",
            control_dataset="_orch",
            final_table="cf_s2_gold",
            allow_final_merge=True,
            run_id="local-test",
        )

    def test_grouped_merge_advances_one_watermark_per_site(
        self, monkeypatch
    ):
        from miaproc.eddy import bigquery_writeback as wb_mod

        cfg = self._cfg()
        df = self._stage_df_two_sites()
        # Stub every BQ-side helper so no real client is needed.
        watermark_calls: list[tuple[str, str]] = []
        recorded_runs: list[dict[str, Any]] = []
        monkeypatch.setattr(
            wb_mod, "_resolve_client", lambda c, client: object()
        )
        monkeypatch.setattr(
            wb_mod, "ensure_control_tables_exist",
            lambda c, *, client=None: None,
        )
        monkeypatch.setattr(
            wb_mod, "write_processed_to_stage",
            lambda d, c, *, client=None: int(len(d)),
        )
        monkeypatch.setattr(
            wb_mod, "validate_stage_table",
            lambda c, *, client=None: {"row_count": int(len(df))},
        )
        monkeypatch.setattr(
            wb_mod, "merge_stage_into_final",
            lambda c, *, client=None: {"inserted": 4, "updated": 0},
        )

        def _fake_advance(
            cfg_, *, site_id, last_processed_timestamp,
            last_run_id, client=None,
        ):
            watermark_calls.append(
                (str(site_id), str(last_processed_timestamp))
            )

        monkeypatch.setattr(wb_mod, "advance_watermark", _fake_advance)

        def _fake_record_run(c, payload, *, client=None):
            recorded_runs.append(dict(payload))

        monkeypatch.setattr(wb_mod, "record_run_row", _fake_record_run)

        result = wb_mod.run_writeback(
            df,
            cfg,
            run_id="local-test",
            started_at="2025-08-01T00:00:00+00:00",
        )
        # Two distinct sites -> two watermark advances.
        assert len(watermark_calls) == 2
        sites = sorted(s for s, _ in watermark_calls)
        assert sites == ["RBMNN", "RBRL"]
        # Result records the per-site mapping.
        assert set(result.watermark_values_by_site) == {"RBRL", "RBMNN"}
        assert (
            result.watermark_values_by_site["RBRL"].startswith("2025-08-01T01:00")
        )
        assert (
            result.watermark_values_by_site["RBMNN"].startswith(
                "2025-08-01T02:00"
            )
        )
        # Run row labels the row as <grouped> for multi-site runs.
        assert result.status == "succeeded"
        assert recorded_runs[-1]["site_id"] == GROUPED_RUN_ROW_SITE_LABEL
        assert recorded_runs[-1]["watermark_advanced"] is True

    def test_stage_only_grouped_run_does_not_advance_watermark(
        self, monkeypatch
    ):
        from miaproc.eddy import bigquery_writeback as wb_mod

        cfg = BigQueryWritebackConfig(
            output_project="manglaria-staging",
            output_dataset="manglaria_lakehouse_ds",
            stage_table="cf_s2_gold_stage",
            control_dataset="_orch",
            final_table=None,
            allow_final_merge=False,
        )
        df = self._stage_df_two_sites()
        watermark_calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            wb_mod, "_resolve_client", lambda c, client: object()
        )
        monkeypatch.setattr(
            wb_mod, "ensure_control_tables_exist",
            lambda c, *, client=None: None,
        )
        monkeypatch.setattr(
            wb_mod, "write_processed_to_stage",
            lambda d, c, *, client=None: int(len(d)),
        )
        monkeypatch.setattr(
            wb_mod, "validate_stage_table",
            lambda c, *, client=None: {"row_count": int(len(df))},
        )
        monkeypatch.setattr(
            wb_mod, "advance_watermark",
            lambda *a, **kw: watermark_calls.append(("called", "")),
        )
        monkeypatch.setattr(
            wb_mod, "record_run_row", lambda c, p, *, client=None: None
        )

        result = wb_mod.run_writeback(
            df, cfg,
            run_id="local-stage-only",
            started_at="2025-08-01T00:00:00+00:00",
        )
        assert result.status == "stage_only_succeeded"
        assert result.watermark_advanced is False
        assert result.watermark_values_by_site == {}
        assert watermark_calls == []


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
            "--output-table", str(tmp_path / "silver.parquet"),
            "--output-run-json", str(tmp_path / "silver_run.json"),
            "--group-column", "site_id",
        ]
        rc = cli.main(argv)
        assert rc == cli.SUCCESS_EXIT
        assert seen == ["RBMNN", "RBRL"]
        # Stacked output exists.
        df = pd.read_parquet(tmp_path / "silver.parquet")
        assert set(df["site_id"].unique()) == {"RBMNN", "RBRL"}
