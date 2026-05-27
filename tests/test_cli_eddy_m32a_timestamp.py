"""M32A CLI-level tests for the source-truth ``timestamp`` contract.

These tests lock in the lineage-CSV row that M32 omitted: the silver
and gold pipelines must expose ``timestamp`` (no internal
``DateTime``) at every output boundary, and the gold CLI must be
able to consume a strict source-truth silver BigQuery read with
``timestamp`` only and reconstruct ``DateTime`` internally before
backend dispatch.

Specifically:

- the silver dry-run metadata records one ``timestamp`` and no
  ``DateTime``;
- the silver local CSV / parquet artifact carries ``timestamp`` only;
- the silver real-writeback payload handed to ``run_writeback`` has
  exactly one ``timestamp`` and no ``DateTime``;
- the gold CLI accepts a timestamp-only source-truth silver fixture,
  reconstructs the internal ``DateTime`` for backend dispatch, and
  carries ``timestamp`` (no ``DateTime``) into the gold stage payload;
- the constants-vs-CSV check passes including the ``DateTime ->
  timestamp`` row;
- the constants-derived dry-run alias map for silver does not record
  ``timestamp`` (it is a bronze-to-final exact match).
"""
from __future__ import annotations

import csv
import json
import pathlib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

from miaproc import cli
from miaproc.eddy import (
    BigQueryReadResult,
    BigQuerySilverReadResult,
    FINAL_TO_INTERNAL_RENAME,
    SILVER_BRONZE_TO_FINAL_ALIASES,
    SILVER_INTERNAL_TO_FINAL_RENAME,
    WritebackResult,
)


# ---------------------------------------------------------------------------
# Constants vs lineage CSV (including DateTime row)
# ---------------------------------------------------------------------------


class TestConstantsAgainstLineageCSVM32A:
    @staticmethod
    def _csv_path() -> pathlib.Path:
        # The lineage CSV lives under ``06_infra/schemas/`` at the
        # workspace root. The exact distance from this test file
        # depends on the layout: in the artifact-first dev workspace
        # the file is at ``<root>/08_pkg/tests/...`` (root is two
        # parents up), in the standalone front-facing repo it is at
        # ``<root>/tests/...`` (root is one parent up), and inside
        # the runtime Docker image it is at ``/app/tests/...`` (root
        # is one parent up). Walk upward to find the first ancestor
        # that actually contains the file rather than hard-coding the
        # distance, so the test passes in every layout we ship.
        relative = (
            pathlib.Path("06_infra")
            / "schemas"
            / "eddy_bronze_to_stage_column_lineage_contract.csv"
        )
        here = pathlib.Path(__file__).resolve()
        for ancestor in here.parents:
            candidate = ancestor / relative
            if candidate.is_file():
                return candidate
        raise FileNotFoundError(
            f"Could not locate {relative} above {here}. The lineage "
            f"CSV must exist under a 06_infra/schemas/ directory in "
            f"one of this file's ancestor directories."
        )

    def test_silver_internal_to_final_includes_DateTime_to_timestamp(self):
        assert SILVER_INTERNAL_TO_FINAL_RENAME.get("DateTime") == "timestamp"

    def test_final_to_internal_includes_timestamp_to_DateTime(self):
        assert FINAL_TO_INTERNAL_RENAME.get("timestamp") == "DateTime"

    def test_constants_match_csv_row_by_row_including_time(self):
        with self._csv_path().open(encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        expected_i2f: dict[str, str] = {}
        expected_aliases: dict[str, str] = {}
        for r in rows:
            bronze = r["v_name_bronze"]
            internal = r["v_name_eddyproc"]
            final = r["v_name_final"]
            if internal and final and internal != final:
                expected_i2f[internal] = final
            # Bronze-to-final alias map: only the unit-transformed
            # variables (or any bronze whose name differs from its
            # final and that has an internal alias too) need an
            # explicit entry. The ``timestamp`` bronze row matches
            # its final name exactly so it does not produce an alias.
            if (
                internal
                and bronze
                and final
                and bronze != final
                and bronze != "timestamp"
            ):
                expected_aliases[bronze] = final
        expected_aliases["u."] = "u_star"
        assert SILVER_INTERNAL_TO_FINAL_RENAME == expected_i2f
        assert FINAL_TO_INTERNAL_RENAME == {
            v: k for k, v in expected_i2f.items()
        }
        assert SILVER_BRONZE_TO_FINAL_ALIASES == expected_aliases
        # And ``timestamp`` is intentionally absent from the bronze
        # alias map — its bronze name matches the final name exactly.
        assert "timestamp" not in SILVER_BRONZE_TO_FINAL_ALIASES


# ---------------------------------------------------------------------------
# Helpers (mirror M32 fixtures but stripped of any ``DateTime`` leak)
# ---------------------------------------------------------------------------


def _internal_silver_with_DateTime(n: int = 4) -> pd.DataFrame:
    """Stage-1 internal-named silver frame (pre-rename); the CLI
    silver path applies ``apply_silver_source_truth_rename`` so the
    on-disk artifact and BQ payload only see ``timestamp``."""
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


def _source_truth_silver_no_DateTime(n: int = 4) -> pd.DataFrame:
    """Strict M32A source-truth silver: ``timestamp`` only, no
    ``DateTime``."""
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
    df.attrs["miaproc_diagnostics"] = {"backend": "hesseflux"}
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
# Silver-side
# ---------------------------------------------------------------------------


class TestSilverM32ATimestampContract:
    @staticmethod
    def _patch_pipeline(monkeypatch):
        def _fake_read(cfg, *, client=None):
            return BigQueryReadResult(
                flux_df=pd.DataFrame(
                    {
                        "timestamp": pd.date_range(
                            "2025-08-01",
                            periods=4,
                            freq="30min",
                            tz="UTC",
                        ),
                        "site_id": ["RBRL"] * 4,
                    }
                ),
                biomet_df=pd.DataFrame(
                    {"timestamp": [], "site_id": []}
                ),
                flux_rows=4,
                biomet_rows=0,
                flux_query="SELECT * FROM flux",
                biomet_query="SELECT * FROM biomet",
                query_parameters={},
            )

        def _fake_load(**kwargs):
            return _internal_silver_with_DateTime()

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(eddy_pkg, "read_bigquery_inputs", _fake_read)
        monkeypatch.setattr(
            eddy_pkg, "load_stage1_from_dataframes", _fake_load
        )

    def test_silver_local_csv_has_timestamp_no_DateTime(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        assert cli.main(_make_silver_argv(tmp_path)) == cli.SUCCESS_EXIT
        df = pd.read_csv(tmp_path / "silver.csv")
        assert "timestamp" in df.columns
        assert "DateTime" not in df.columns
        # Exactly one timestamp column survives.
        assert list(df.columns).count("timestamp") == 1

    def test_silver_dry_run_metadata_has_timestamp_no_DateTime(
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
        dry_dir = tmp_path / "silver_dry"
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
        assert "timestamp" in cols
        assert "DateTime" not in cols
        assert cols.count("timestamp") == 1
        # ``timestamp`` is an exact bronze-to-final match, NOT an
        # alias.
        aliases = meta["input_column_payload_aliases"]
        assert "timestamp" not in aliases
        # The four BigQuery-write safety flags remain false.
        for flag in (
            "bigquery_write_attempted",
            "validation_sql_attempted",
            "merge_attempted",
            "watermark_advanced",
        ):
            assert meta[flag] is False, flag

    def test_silver_real_writeback_payload_has_one_timestamp_no_DateTime(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        captured: dict[str, Any] = {}

        def _fake_writeback(df, cfg, **kwargs):
            captured["df"] = df.copy(deep=False)
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
        assert "timestamp" in cols
        assert "DateTime" not in cols
        assert cols.count("timestamp") == 1
        # Case-insensitive uniqueness still holds.
        keys = [str(c).casefold() for c in cols]
        assert len(set(keys)) == len(keys), cols


# ---------------------------------------------------------------------------
# Gold-side: timestamp-only source-truth silver drives the backend
# ---------------------------------------------------------------------------


class TestGoldM32ATimestampOnlySilver:
    @staticmethod
    def _patch_pipeline(monkeypatch):
        fake_silver_result = BigQuerySilverReadResult(
            silver_df=_source_truth_silver_no_DateTime(),
            silver_rows=4,
            silver_query="SELECT * FROM silver",
            query_parameters={},
        )
        calls: dict[str, Any] = {"postproc_input": None}

        def _fake_silver_read(cfg, *, client=None):
            return fake_silver_result

        def _fake_postproc(
            df, *, engine, hesseflux_config=None, reddyproc_config=None,
        ):
            calls["postproc_input"] = df.copy(deep=False)
            return _stub_gold_df()

        import miaproc.eddy as eddy_pkg

        monkeypatch.setattr(
            eddy_pkg, "read_bigquery_silver_input", _fake_silver_read
        )
        monkeypatch.setattr(eddy_pkg, "postproc", _fake_postproc)
        return SimpleNamespace(calls=calls)

    def test_gold_runs_with_timestamp_only_silver_and_reconstructs_DateTime(
        self, tmp_path, monkeypatch
    ):
        ns = self._patch_pipeline(monkeypatch)
        assert cli.main(_make_gold_argv(tmp_path)) == cli.SUCCESS_EXIT
        passed_in = ns.calls["postproc_input"]
        assert passed_in is not None
        # The reconstructed calc frame carries internal ``DateTime``;
        # source-truth ``timestamp`` is gone.
        assert "DateTime" in passed_in.columns
        assert "timestamp" not in passed_in.columns

    def test_gold_stage_payload_has_one_timestamp_no_DateTime(
        self, tmp_path, monkeypatch
    ):
        self._patch_pipeline(monkeypatch)
        captured: dict[str, Any] = {}

        def _fake_writeback(df, cfg, **kwargs):
            captured["df"] = df.copy(deep=False)
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
        assert "timestamp" in staged_cols
        assert "DateTime" not in staged_cols
        assert staged_cols.count("timestamp") == 1
        # Source-truth silver columns survive into gold.
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
            assert col in staged_cols, col
        # Case-insensitive uniqueness holds across the gold payload.
        keys = [c.casefold() for c in staged_cols]
        assert len(set(keys)) == len(keys), staged_cols

    def test_gold_dry_run_metadata_has_timestamp_no_DateTime(
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
        dry_dir = tmp_path / "gold_dry"
        argv = _make_gold_argv(
            tmp_path, **{"--stage-payload-dry-run-dir": str(dry_dir)}
        )
        assert cli.main(argv) == cli.SUCCESS_EXIT
        meta = json.loads(
            (dry_dir / "stage_payload_metadata.json").read_text(
                encoding="utf-8"
            )
        )
        cols = meta["columns"]
        assert "timestamp" in cols
        assert "DateTime" not in cols
        assert cols.count("timestamp") == 1
        assert meta["missing_input_columns"] == []
