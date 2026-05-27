"""M32 CLI-level tests for the eddy source-truth column contract.

These tests exercise the full silver and gold BigQuery split CLI
paths with cloud-shape bronze and silver fixtures. They confirm:

- the silver stage payload (dry-run + real writeback) carries
  source-truth final names (``co2_flux``, ``qc_co2_flux``,
  ``air_temperature_c``, ``u_star``, ``VPD_hpa``, ``SWIN_1_1_1``,
  ``P_RAIN_1_1_1``, ``RH_1_1_1``) and not the backend-only
  inherited names (``NEE``, ``QC_NEE``, ``Tair``, ``USTAR``, ``VPD``,
  ``Rg``, ``P_RAIN``, ``rH``);
- flux-side ``RH`` and biomet ``RH_1_1_1`` are preserved separately;
- the gold BigQuery split can consume a source-truth silver input,
  reconstruct the internal calc frame for the backend, and stage a
  payload that preserves the source-truth silver columns;
- the M29 dry-run alias map is the M32 contract (only
  ``air_temperature``, ``VPD``, ``u.``);
- no live BigQuery, Docker, or cloud action is invoked.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from miaproc import cli
from miaproc.eddy import (
    BigQueryReadResult,
    BigQuerySilverReadResult,
    WritebackResult,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cloud_bronze_flux_df(n: int = 4) -> pd.DataFrame:
    """Bronze flux frame in the real cloud column shape."""
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "site_id": ["RBRL"] * n,
            "co2_flux": [0.1, 0.2, -0.1, 0.3],
            "qc_co2_flux": [0, 0, 0, 0],
            "air_temperature": [293.0, 294.0, 295.0, 292.0],
            "u_star": [0.2, 0.3, 0.4, 0.1],
            "VPD": [500.0, 600.0, 700.0, 400.0],
            "RH": [60.0, 61.0, 62.0, 63.0],
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


def _internal_silver_cloud_shape(n: int = 4) -> pd.DataFrame:
    """Stage-1-shaped silver in the cloud column shape with internal
    aliases (``NEE``, ``QC_NEE``, ``Tair``, ``USTAR``, ``VPD``,
    ``Rg``, ``P_RAIN``, ``rH``) joined with a flux-side ``RH``
    pass-through and a bronze-only sentinel.
    """
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, -0.1, 0.3],
            "QC_NEE": [0, 0, 0, 0],
            "Tair": [20.0, 21.0, 22.0, 19.0],
            "USTAR": [0.2, 0.3, 0.4, 0.1],
            "VPD": [5.0, 6.0, 7.0, 4.0],
            "Rg": [0.0, 100.0, 200.0, 50.0],
            "H": [10.0, 20.0, 30.0, 40.0],
            "LE": [50.0, 60.0, 70.0, 80.0],
            "P_RAIN": [0.0, 0.0, 0.0, 0.0],
            "rH": [85.0, 86.0, 87.0, 88.0],
            "RH": [60.0, 61.0, 62.0, 63.0],
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


def _source_truth_silver_for_gold(n: int = 4) -> pd.DataFrame:
    """Silver in the strict M32A source-truth shape, as gold would
    read from a BigQuery silver table that already follows the
    lineage-CSV contract: the time column is ``timestamp`` and there
    is no internal ``DateTime``."""
    return pd.DataFrame(
        {
            "timestamp": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "co2_flux": [0.1, 0.2, -0.1, 0.3],
            "qc_co2_flux": [0, 0, 0, 0],
            "air_temperature_c": [20.0, 21.0, 22.0, 19.0],
            "u_star": [0.2, 0.3, 0.4, 0.1],
            "VPD_hpa": [5.0, 6.0, 7.0, 4.0],
            "SWIN_1_1_1": [0.0, 100.0, 200.0, 50.0],
            "P_RAIN_1_1_1": [0.0, 0.0, 0.0, 0.0],
            "RH_1_1_1": [85.0, 86.0, 87.0, 88.0],
            "RH": [60.0, 61.0, 62.0, 63.0],
            "H": [10.0, 20.0, 30.0, 40.0],
            "LE": [50.0, 60.0, 70.0, 80.0],
            "bronze_only_flag": [1, 0, 1, 0],
        }
    )


def _stub_gold_df(n: int = 4) -> pd.DataFrame:
    """Gold backend output frame in the standard 13-column shape."""
    df = pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
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


def _make_silver_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    base = {
        "--bq-input-project": "manglaria",
        "--bq-input-dataset": "manglaria_lakehouse_ds",
        "--bq-flux-table": "carbon_flux_eddycovariance",
        "--bq-biomet-table": "carbon_flux_biomet",
        "--output-table": str(tmp_path / "silver.csv"),
        "--output-run-json": str(tmp_path / "silver_run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["eddy", "run-bigquery-silver"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


def _make_gold_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    base = {
        "--bq-input-project": "manglaria-staging",
        "--bq-input-dataset": "manglaria_lakehouse_ds",
        "--bq-silver-table": "cf_s2_silver",
        "--engine": "hesseflux-native",
        "--output-table": str(tmp_path / "gold.csv"),
        "--output-diagnostics-json": str(tmp_path / "gold_diag.json"),
        "--output-run-json": str(tmp_path / "gold_run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["eddy", "run-bigquery-gold"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------


class TestSilverM32SourceTruth:
    @staticmethod
    def _patch_pipeline(monkeypatch):
        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=_cloud_bronze_flux_df(),
                biomet_df=pd.DataFrame({"timestamp": [], "site_id": []}),
                flux_rows=4,
                biomet_rows=0,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        def _fake_load(**kwargs):
            return _internal_silver_cloud_shape()

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )

    def test_silver_dry_run_payload_uses_source_truth_final_names(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "run_writeback",
            lambda *_a, **_kw: (_ for _ in ()).throw(
                AssertionError("run_writeback must not be called")
            ),
        )

        dry_dir = tmp_path / "silver_m32_dry"
        argv = _make_silver_argv(
            tmp_path, **{"--stage-payload-dry-run-dir": str(dry_dir)}
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        cols = meta["columns"]
        # Source-truth final names for the inherited carbon-flux /
        # biomet variables (M32 contract §2; M32A adds the
        # ``DateTime -> timestamp`` row for the time column).
        for final in (
            "timestamp",
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
        ):
            assert final in cols, final
        # Backend-only inherited names must not appear (including
        # the internal ``DateTime`` time column under M32A).
        for backend in (
            "DateTime",
            "NEE",
            "QC_NEE",
            "Tair",
            "USTAR",
            "VPD",
            "Rg",
            "P_RAIN",
            "rH",
        ):
            assert backend not in cols, backend
        # Flux RH preserved separately from biomet RH_1_1_1.
        assert "RH" in cols
        assert "RH_1_1_1" in cols
        # Identity triple materialized.
        assert meta["identity_columns_present"] == {
            "primary_key": True, "site_id": True, "timestamp": True,
        }
        # Stage-payload uniqueness is enforced case-insensitively.
        assert meta["columns_unique"] is True
        assert meta["duplicate_columns"] == []
        keys = [c.casefold() for c in cols]
        assert len(set(keys)) == len(keys), cols
        # The four BigQuery-write safety flags remain false.
        for flag in (
            "bigquery_write_attempted",
            "validation_sql_attempted",
            "merge_attempted",
            "watermark_advanced",
        ):
            assert meta[flag] is False, flag

    def test_silver_dry_run_alias_map_is_m32_contract(
        self, tmp_path, monkeypatch
    ):
        """Under M32 the dry-run preservation alias map records only
        the unit-transformed names ``air_temperature`` -> ``air_temperature_c``
        and ``VPD`` -> ``VPD_hpa`` (and the legacy ``u.`` -> ``u_star``).
        All other bronze names survive into silver under their bronze
        name (exact match), not via an alias."""
        self._patch_pipeline(monkeypatch)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "run_writeback",
            lambda *_a, **_kw: (_ for _ in ()).throw(
                AssertionError("run_writeback must not be called")
            ),
        )

        dry_dir = tmp_path / "silver_m32_alias"
        argv = _make_silver_argv(
            tmp_path, **{"--stage-payload-dry-run-dir": str(dry_dir)}
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        # `air_temperature` resolves via its unit-baked source-truth
        # alias; `VPD` likewise. No other aliases are recorded.
        aliases = meta["input_column_payload_aliases"]
        assert aliases.get("air_temperature") == "air_temperature_c"
        assert aliases.get("VPD") == "VPD_hpa"
        # Bronze names that match their source-truth final name
        # exactly are *not* recorded in the alias map.
        for exact_match in (
            "co2_flux",
            "qc_co2_flux",
            "u_star",
            "RH",
            "bronze_only_flag",
            "site_id",
            "timestamp",
        ):
            assert exact_match not in aliases, (exact_match, aliases)
        assert meta["missing_input_columns"] == []

    def test_silver_real_writeback_hands_source_truth_payload(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)

        captured: dict[str, Any] = {}

        def _fake_writeback(df, cfg, **kwargs):
            captured["df"] = df.copy(deep=False)
            captured["cfg"] = cfg
            return WritebackResult(
                run_id=kwargs.get("run_id", "rid"),
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
                final_table_fqn=None,
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
                error_text=None,
                watermark_values_by_site={},
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_writeback)

        argv = _make_silver_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_silver_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT

        staged = captured["df"]
        cols = list(staged.columns)
        # M32A: ``timestamp`` is the inherited source-truth time
        # column; the internal ``DateTime`` must not appear.
        for final in (
            "timestamp",
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
            "RH",
        ):
            assert final in cols, final
        for backend in (
            "DateTime",
            "NEE",
            "QC_NEE",
            "Tair",
            "USTAR",
            "VPD",
            "Rg",
            "P_RAIN",
            "rH",
        ):
            assert backend not in cols, backend
        assert list(cols).count("timestamp") == 1
        # Case-insensitive uniqueness holds.
        keys = [str(c).casefold() for c in cols]
        assert len(set(keys)) == len(keys), cols

    def test_silver_local_artifact_uses_source_truth_names(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        argv = _make_silver_argv(tmp_path)
        assert cli.main(argv) == cli.SUCCESS_EXIT
        df = pd.read_csv(tmp_path / "silver.csv")
        # M32A: the local silver CSV uses ``timestamp`` (source-truth)
        # so a subsequent gold CLI invocation can read it back
        # without expecting an internal ``DateTime`` column.
        for final in (
            "timestamp",
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
            "RH",
        ):
            assert final in df.columns, final
        for backend in (
            "DateTime",
            "NEE",
            "QC_NEE",
            "Tair",
            "USTAR",
            "VPD",
            "Rg",
            "P_RAIN",
            "rH",
        ):
            assert backend not in df.columns, backend


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------


class TestGoldM32SourceTruth:
    @staticmethod
    def _patch_pipeline(monkeypatch, *, source_truth_silver: bool = True):
        silver_df = (
            _source_truth_silver_for_gold()
            if source_truth_silver
            else _internal_silver_cloud_shape()
        )
        fake_silver_result = BigQuerySilverReadResult(
            silver_df=silver_df,
            silver_rows=len(silver_df),
            silver_query="SELECT * FROM silver",
            query_parameters={},
        )

        calls: dict[str, Any] = {
            "postproc_input": None,
            "postproc_engine": None,
        }

        def _fake_silver_read(cfg, *, client=None):
            return fake_silver_result

        def _fake_postproc(
            df, *, engine, hesseflux_config=None, reddyproc_config=None,
        ):
            calls["postproc_input"] = df.copy(deep=False)
            calls["postproc_engine"] = engine
            return _stub_gold_df()

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "read_bigquery_silver_input", _fake_silver_read
        )
        monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
        return SimpleNamespace(calls=calls)

    def test_gold_consumes_source_truth_silver_and_runs_backend(
        self, tmp_path, monkeypatch
    ):
        """M32A: gold reads a strict source-truth silver table
        (``timestamp`` only, no ``DateTime``), reconstructs the
        internal calc frame with ``DateTime``, and dispatches the
        backend with the expected internal names."""
        ns = self._patch_pipeline(monkeypatch, source_truth_silver=True)

        argv = _make_gold_argv(tmp_path)
        assert cli.main(argv) == cli.SUCCESS_EXIT

        # Backend received the reconstructed internal calc frame —
        # source-truth ``timestamp`` was mapped back to internal
        # ``DateTime`` per the lineage CSV.
        passed_in = ns.calls["postproc_input"]
        assert passed_in is not None
        for internal in (
            "DateTime",
            "NEE",
            "QC_NEE",
            "Tair",
            "USTAR",
            "VPD",
            "Rg",
            "P_RAIN",
            "rH",
        ):
            assert internal in passed_in.columns, internal
        # Source-truth names are gone from the backend input.
        for final in (
            "timestamp",
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
        ):
            assert final not in passed_in.columns, final
        # Flux-side RH (not in the rename map) survives the
        # reconstruction step.
        assert "RH" in passed_in.columns

    def test_gold_stage_payload_preserves_source_truth_silver_columns(
        self, tmp_path, monkeypatch
    ):
        """The gold stage payload preserves every source-truth silver
        column and appends gold's new processing outputs. Backend
        passthroughs that duplicate source-truth columns are dropped."""
        self._patch_pipeline(monkeypatch, source_truth_silver=True)

        captured: dict[str, Any] = {}

        def _fake_writeback(df, cfg, **kwargs):
            captured["df"] = df.copy(deep=False)
            captured["cfg"] = cfg
            return WritebackResult(
                run_id=kwargs.get("run_id", "rid"),
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
                final_table_fqn=None,
                runs_table_fqn=cfg.runs_table_fqn(),
                watermark_table_fqn=cfg.watermark_table_fqn(),
                error_text=None,
                watermark_values_by_site={},
            )

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "run_writeback", _fake_writeback)
        from miaproc.eddy import bigquery_writeback as wb_mod

        monkeypatch.setattr(
            wb_mod, "read_final_table_columns",
            lambda cfg, *, client=None: None,
        )
        monkeypatch.setattr(
            wb_mod, "read_final_table_schema",
            lambda cfg, *, client=None: None,
        )

        argv = _make_gold_argv(
            tmp_path,
            **{
                "--bq-stage-table": "cf_s2_gold_stage",
                "--bq-output-project": "manglaria-staging",
                "--bq-output-dataset": "manglaria_lakehouse_ds",
                "--bq-control-dataset": "_orch",
            },
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT
        staged_cols = list(captured["df"].columns)
        # Source-truth silver columns survive (M32A: including the
        # source-truth ``timestamp`` time column).
        for final in (
            "timestamp",
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
            "RH",
            "H",
            "LE",
            "bronze_only_flag",
        ):
            assert final in staged_cols, final
        # New gold processing outputs are renamed to lowercase.
        for new in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f"):
            assert new in staged_cols, new
        # Backend passthroughs (DateTime, NEE, Tair, USTAR, Rg, VPD)
        # that duplicate source-truth columns are dropped.
        for redundant in ("DateTime", "NEE", "Tair", "USTAR", "Rg", "VPD"):
            assert redundant not in staged_cols, redundant
        assert list(staged_cols).count("timestamp") == 1
        # Case-insensitive uniqueness holds.
        keys = [c.casefold() for c in staged_cols]
        assert len(set(keys)) == len(keys), staged_cols

    def test_gold_dry_run_metadata_records_source_truth_payload(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch, source_truth_silver=True)

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "run_writeback",
            lambda *_a, **_kw: (_ for _ in ()).throw(
                AssertionError("run_writeback must not be called")
            ),
        )

        dry_dir = tmp_path / "gold_m32_dry"
        argv = _make_gold_argv(
            tmp_path,
            **{"--stage-payload-dry-run-dir": str(dry_dir)},
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT

        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        # M32A: payload uses ``timestamp`` for the time column.
        for final in (
            "timestamp",
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
            "RH",
            "H",
            "LE",
            "bronze_only_flag",
        ):
            assert final in meta["columns"], final
        for backend in ("DateTime", "NEE", "Tair", "USTAR", "Rg", "VPD"):
            assert backend not in meta["columns"], backend
        assert meta["columns"].count("timestamp") == 1
        assert meta["columns_unique"] is True
        # Source-truth silver columns are preserved (not aliased).
        # Identity columns (timestamp/site_id) come from the silver
        # input frame; they are exact matches so do not appear in
        # the alias map.
        for col in (
            "co2_flux",
            "qc_co2_flux",
            "air_temperature_c",
            "u_star",
            "VPD_hpa",
            "SWIN_1_1_1",
            "P_RAIN_1_1_1",
            "RH_1_1_1",
            "RH",
            "H",
            "LE",
            "bronze_only_flag",
        ):
            assert col in meta["preserved_input_columns"], col
        assert meta["missing_input_columns"] == []
        # All four BigQuery-write safety flags remain false.
        for flag in (
            "bigquery_write_attempted",
            "validation_sql_attempted",
            "merge_attempted",
            "watermark_advanced",
        ):
            assert meta[flag] is False, flag
