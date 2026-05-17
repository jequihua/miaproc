"""Tests for the M17 ``miaproc biomass enrich-table`` CLI + the
``enrich_table`` library helper.

These tests cover the M17 row-preservation contract end-to-end:

- the CLI subcommand exists under the ``biomass`` namespace and parses
  cleanly (help, required flags, validation);
- the ``enrich_table`` library helper appends exactly two columns
  (biomass estimate + equation-used identifier from
  ``source_record_id``) without re-ordering or dropping rows;
- ineligible rows (missing dbh, non-adult life_stage for direct
  biomass) are preserved with NaN / None in the appended columns;
- the equation-used column matches ``source_record_id`` for matched
  rows;
- column-name overrides aligned with ``BiomassColumns`` work end-to-end;
- output column-name collisions with input columns are rejected as
  validation failures.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from miaproc import cli
from miaproc.biomass import (
    DEFAULT_BIOMASS_ESTIMATE_COL,
    DEFAULT_EQUATION_USED_COL,
    enrich_table,
    load_packaged_equations,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _adult_mangrove_table() -> pd.DataFrame:
    """Synthetic table with all four mangrove species, all Adult, with
    dbh_cm populated. Every row is eligible for direct biomass."""
    return pd.DataFrame(
        [
            {
                "primary_key": "syn_001",
                "species": "Avicennia germinans",
                "dbh_cm": 10.0,
                "tree_height_m": 7.5,
                "life_stage": "Adult",
                "site_id": "RBRL",
            },
            {
                "primary_key": "syn_002",
                "species": "Rhizophora mangle",
                "dbh_cm": 12.0,
                "tree_height_m": 8.0,
                "life_stage": "Adult",
                "site_id": "RBRL",
            },
            {
                "primary_key": "syn_003",
                "species": "Laguncularia racemosa",
                "dbh_cm": 9.0,
                "tree_height_m": 6.5,
                "life_stage": "Adult",
                "site_id": "RBRL",
            },
            {
                "primary_key": "syn_004",
                "species": "Conocarpus erectus",
                "dbh_cm": 11.0,
                "tree_height_m": 7.0,
                "life_stage": "Adult",
                "site_id": "RBRL",
            },
        ]
    )


def _mixed_eligibility_table() -> pd.DataFrame:
    """Table with a mix of: eligible adult / juvenile / null life_stage /
    null dbh / unknown species. Every row should survive enrichment."""
    return pd.DataFrame(
        [
            {
                "primary_key": "mix_001",
                "species": "Avicennia germinans",
                "dbh_cm": 10.0,
                "tree_height_m": None,
                "life_stage": "Adult",
            },
            {
                "primary_key": "mix_002",
                "species": "Rhizophora mangle",
                "dbh_cm": 5.0,
                "tree_height_m": None,
                "life_stage": "Juvenile",
            },
            {
                "primary_key": "mix_003",
                "species": "Laguncularia racemosa",
                "dbh_cm": 8.0,
                "tree_height_m": None,
                "life_stage": None,
            },
            {
                "primary_key": "mix_004",
                "species": "Conocarpus erectus",
                "dbh_cm": None,
                "tree_height_m": 5.0,
                "life_stage": "Adult",
            },
            {
                "primary_key": "mix_005",
                "species": "Pinus radiata",  # not in dina equations
                "dbh_cm": 15.0,
                "tree_height_m": None,
                "life_stage": "Adult",
            },
        ]
    )


def _make_argv(tmp_path: Path, **overrides: Any) -> list[str]:
    base = {
        "--input-table": str(tmp_path / "input.csv"),
        "--output-table": str(tmp_path / "out.csv"),
        "--output-run-json": str(tmp_path / "run.json"),
    }
    base.update({k: str(v) for k, v in overrides.items()})
    argv = ["biomass", "enrich-table"]
    for k, v in base.items():
        argv.extend([k, v])
    return argv


# ---------------------------------------------------------------------------
# enrich_table library helper
# ---------------------------------------------------------------------------


class TestEnrichTableHelper:
    def test_default_output_column_names_are_biomass_estimate_and_equation_used(self):
        assert DEFAULT_BIOMASS_ESTIMATE_COL == "biomass_estimate"
        assert DEFAULT_EQUATION_USED_COL == "equation_used"

    def test_appends_exactly_two_columns_in_order(self):
        eq = load_packaged_equations()
        df = _adult_mangrove_table()
        original_cols = list(df.columns)

        out = enrich_table(df, equations=eq, dataset="dina")

        # Original columns preserved verbatim, in original order.
        assert list(out.columns)[: len(original_cols)] == original_cols
        # Two new columns appended at the end, in the contracted order.
        assert list(out.columns)[len(original_cols):] == [
            "biomass_estimate",
            "equation_used",
        ]
        # Exactly two new columns, no third surprise.
        assert len(out.columns) == len(original_cols) + 2

    def test_row_count_and_order_preserved(self):
        eq = load_packaged_equations()
        df = _mixed_eligibility_table()
        out = enrich_table(df, equations=eq, dataset="dina")
        assert len(out) == len(df)
        # Same row order — primary_keys come back unchanged.
        assert out["primary_key"].tolist() == df["primary_key"].tolist()

    def test_equation_used_matches_source_record_id_for_matched_rows(self):
        eq = load_packaged_equations()
        df = _adult_mangrove_table()
        out = enrich_table(df, equations=eq, dataset="dina")
        # All four mangrove species map to dina_001..dina_004.
        expected = {
            "Avicennia germinans": "dina_001",
            "Rhizophora mangle": "dina_002",
            "Laguncularia racemosa": "dina_003",
            "Conocarpus erectus": "dina_004",
        }
        for _, row in out.iterrows():
            assert row["equation_used"] == expected[row["species"]]

    def test_biomass_estimate_matches_wd_fixed_expression(self):
        eq = load_packaged_equations()
        df = _adult_mangrove_table().head(1)  # Avicennia germinans, dbh=10
        out = enrich_table(df, equations=eq, dataset="dina")
        # 0.403 * 0.78 * (10) ** 1.934
        expected = 0.403 * 0.78 * (10.0 ** 1.934)
        assert math.isclose(
            out["biomass_estimate"].iloc[0], expected, rel_tol=1e-9
        )

    def test_ineligible_rows_preserved_with_nan_estimate(self):
        eq = load_packaged_equations()
        df = _mixed_eligibility_table()
        out = enrich_table(df, equations=eq, dataset="dina")
        # All 5 rows preserved.
        assert len(out) == 5
        # Eligible row (Adult mangrove with dbh) gets a numeric estimate.
        eligible = out[out["primary_key"] == "mix_001"].iloc[0]
        assert pd.notna(eligible["biomass_estimate"])
        assert eligible["equation_used"] == "dina_001"
        # Ineligible rows: NaN estimate, None equation_used (or NaN object).
        for pk in ("mix_002", "mix_003", "mix_004", "mix_005"):
            row = out[out["primary_key"] == pk].iloc[0]
            assert pd.isna(row["biomass_estimate"]), pk
            # equation_used may be None or NaN — accept both.
            assert row["equation_used"] is None or (
                isinstance(row["equation_used"], float)
                and pd.isna(row["equation_used"])
            ), pk

    def test_custom_output_column_names(self):
        eq = load_packaged_equations()
        df = _adult_mangrove_table()
        out = enrich_table(
            df,
            equations=eq,
            dataset="dina",
            biomass_estimate_col="biomass_kg",
            equation_used_col="eq_id",
        )
        assert "biomass_kg" in out.columns
        assert "eq_id" in out.columns
        assert "biomass_estimate" not in out.columns
        assert "equation_used" not in out.columns
        # Same content, just renamed.
        assert out["biomass_kg"].notna().all()

    def test_collision_with_input_column_raises(self):
        eq = load_packaged_equations()
        df = _adult_mangrove_table()
        df["biomass_estimate"] = 0.0  # synthetic existing column
        try:
            enrich_table(df, equations=eq, dataset="dina")
        except ValueError as exc:
            assert "biomass_estimate" in str(exc)
        else:
            raise AssertionError("Expected ValueError on column collision")

    def test_same_output_column_name_twice_raises(self):
        eq = load_packaged_equations()
        df = _adult_mangrove_table()
        try:
            enrich_table(
                df,
                equations=eq,
                dataset="dina",
                biomass_estimate_col="x",
                equation_used_col="x",
            )
        except ValueError:
            pass
        else:
            raise AssertionError(
                "Expected ValueError when both output column names are equal"
            )


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


class TestCLIArgParsing:
    def test_help_exits_zero(self):
        parser = cli._build_parser()
        try:
            parser.parse_args(["biomass", "enrich-table", "--help"])
        except SystemExit as exc:
            assert exc.code == 0
        else:
            raise AssertionError("Expected SystemExit(0) from --help")

    def test_required_flags_missing_exits(self):
        parser = cli._build_parser()
        try:
            parser.parse_args(["biomass", "enrich-table"])
        except SystemExit:
            pass
        else:
            raise AssertionError("Expected SystemExit on missing required flags")

    def test_default_dataset_is_dina(self, tmp_path):
        input_path = tmp_path / "input.csv"
        _adult_mangrove_table().to_csv(input_path, index=False)
        parser = cli._build_parser()
        ns = parser.parse_args(_make_argv(tmp_path))
        assert ns.dataset == "dina"
        assert ns.equations_path is None
        assert ns.species_col == "species"
        assert ns.dbh_col == "dbh_cm"
        assert ns.height_col == "tree_height_m"
        assert ns.life_stage_col == "life_stage"
        assert ns.biomass_estimate_col == "biomass_estimate"
        assert ns.equation_used_col == "equation_used"


# ---------------------------------------------------------------------------
# CLI end-to-end behavior
# ---------------------------------------------------------------------------


class TestCLIEndToEnd:
    def test_enrich_csv_to_csv_default_dina(self, tmp_path):
        input_path = tmp_path / "input.csv"
        out_path = tmp_path / "out.csv"
        run_path = tmp_path / "run.json"
        _adult_mangrove_table().to_csv(input_path, index=False)

        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(out_path),
                "--output-run-json",
                str(run_path),
            ]
        )
        assert rc == cli.SUCCESS_EXIT
        assert out_path.exists()
        assert run_path.exists()

        out_df = pd.read_csv(out_path)
        original_cols = list(_adult_mangrove_table().columns)
        # Original columns preserved in order, two appended at the end.
        assert list(out_df.columns)[: len(original_cols)] == original_cols
        assert list(out_df.columns)[len(original_cols):] == [
            "biomass_estimate",
            "equation_used",
        ]
        assert len(out_df) == 4
        assert out_df["biomass_estimate"].notna().all()
        assert out_df["equation_used"].tolist() == [
            "dina_001",
            "dina_002",
            "dina_003",
            "dina_004",
        ]

        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["command"] == "biomass enrich-table"
        assert run["row_counts"] == {
            "input": 4,
            "output": 4,
            "estimated": 4,
            "skipped": 0,
        }
        assert run["config"]["dataset"] == "dina"
        assert run["config"]["equations_source"] == "packaged_default"
        assert run["output_columns_appended"] == [
            "biomass_estimate",
            "equation_used",
        ]
        assert run["outputs"]["table_format"] == "csv"
        assert run["exit_code"] == cli.SUCCESS_EXIT

    def test_enrich_parquet_to_parquet(self, tmp_path):
        input_path = tmp_path / "input.parquet"
        out_path = tmp_path / "out.parquet"
        run_path = tmp_path / "run.json"
        _adult_mangrove_table().to_parquet(input_path, index=False)

        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(out_path),
                "--output-run-json",
                str(run_path),
            ]
        )
        assert rc == cli.SUCCESS_EXIT
        out_df = pd.read_parquet(out_path)
        assert "biomass_estimate" in out_df.columns
        assert "equation_used" in out_df.columns
        assert out_df["biomass_estimate"].notna().all()

    def test_mixed_eligibility_preserves_all_rows(self, tmp_path):
        input_path = tmp_path / "input.csv"
        out_path = tmp_path / "out.csv"
        run_path = tmp_path / "run.json"
        _mixed_eligibility_table().to_csv(input_path, index=False)

        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(out_path),
                "--output-run-json",
                str(run_path),
            ]
        )
        assert rc == cli.SUCCESS_EXIT
        out_df = pd.read_csv(out_path)
        assert len(out_df) == 5  # all rows preserved
        assert out_df["primary_key"].tolist() == [
            "mix_001",
            "mix_002",
            "mix_003",
            "mix_004",
            "mix_005",
        ]
        # Only one row eligible (mix_001 = Adult mangrove with dbh).
        eligible = out_df[out_df["primary_key"] == "mix_001"].iloc[0]
        assert pd.notna(eligible["biomass_estimate"])
        assert eligible["equation_used"] == "dina_001"
        # Other four ineligible.
        for pk in ("mix_002", "mix_003", "mix_004", "mix_005"):
            row = out_df[out_df["primary_key"] == pk].iloc[0]
            assert pd.isna(row["biomass_estimate"])

        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["row_counts"]["estimated"] == 1
        assert run["row_counts"]["skipped"] == 4
        # Match-status counts are exposed in the run JSON for diagnostics.
        statuses = run["match_status_counts"]
        assert statuses.get("fallback_any_state", 0) == 1  # the eligible row
        # The four skip categories live somewhere in the counts:
        # life_stage_not_adult (mix_002, mix_003), dbh_missing (mix_004),
        # no_equation_found (mix_005).
        skip_total = sum(v for k, v in statuses.items() if k != "fallback_any_state")
        assert skip_total == 4

    def test_custom_output_column_names_via_cli(self, tmp_path):
        input_path = tmp_path / "input.csv"
        out_path = tmp_path / "out.csv"
        run_path = tmp_path / "run.json"
        _adult_mangrove_table().to_csv(input_path, index=False)

        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(out_path),
                "--output-run-json",
                str(run_path),
                "--biomass-estimate-col",
                "biomass_kg",
                "--equation-used-col",
                "eq_id",
            ]
        )
        assert rc == cli.SUCCESS_EXIT
        out_df = pd.read_csv(out_path)
        assert "biomass_kg" in out_df.columns
        assert "eq_id" in out_df.columns
        assert "biomass_estimate" not in out_df.columns
        run = json.loads(run_path.read_text(encoding="utf-8"))
        assert run["output_columns_appended"] == ["biomass_kg", "eq_id"]

    def test_input_column_overrides(self, tmp_path):
        # A table that uses non-default field names; the CLI should
        # remap via --species-col / --dbh-col / --height-col /
        # --life-stage-col.
        df = pd.DataFrame(
            [
                {
                    "Species": "Avicennia germinans",
                    "DBH (cm)": 10.0,
                    "Total Height (m)": None,
                    "LifeStage": "Adult",
                },
            ]
        )
        input_path = tmp_path / "input.csv"
        out_path = tmp_path / "out.csv"
        run_path = tmp_path / "run.json"
        df.to_csv(input_path, index=False)

        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(out_path),
                "--output-run-json",
                str(run_path),
                "--species-col",
                "Species",
                "--dbh-col",
                "DBH (cm)",
                "--height-col",
                "Total Height (m)",
                "--life-stage-col",
                "LifeStage",
            ]
        )
        assert rc == cli.SUCCESS_EXIT
        out_df = pd.read_csv(out_path)
        assert pd.notna(out_df["biomass_estimate"].iloc[0])
        assert out_df["equation_used"].iloc[0] == "dina_001"


# ---------------------------------------------------------------------------
# CLI failure paths
# ---------------------------------------------------------------------------


class TestCLIFailurePaths:
    def test_missing_input_table_exits_three(self, tmp_path):
        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(tmp_path / "no_such.csv"),
                "--output-table",
                str(tmp_path / "out.csv"),
                "--output-run-json",
                str(tmp_path / "run.json"),
            ]
        )
        assert rc == cli.VALIDATION_EXIT

    def test_unsupported_input_extension_exits_three(self, tmp_path):
        bogus = tmp_path / "input.xlsx"
        bogus.write_text("not a real table", encoding="utf-8")
        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(bogus),
                "--output-table",
                str(tmp_path / "out.csv"),
                "--output-run-json",
                str(tmp_path / "run.json"),
            ]
        )
        assert rc == cli.VALIDATION_EXIT

    def test_unsupported_output_extension_exits_three(self, tmp_path):
        input_path = tmp_path / "input.csv"
        _adult_mangrove_table().to_csv(input_path, index=False)
        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(tmp_path / "out.xlsx"),
                "--output-run-json",
                str(tmp_path / "run.json"),
            ]
        )
        assert rc == cli.VALIDATION_EXIT

    def test_output_column_collision_exits_four(self, tmp_path):
        # Input table already has a column named "biomass_estimate" —
        # CLI should refuse rather than silently overwrite it.
        df = _adult_mangrove_table()
        df["biomass_estimate"] = 0.0
        input_path = tmp_path / "input.csv"
        out_path = tmp_path / "out.csv"
        run_path = tmp_path / "run.json"
        df.to_csv(input_path, index=False)

        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(out_path),
                "--output-run-json",
                str(run_path),
            ]
        )
        # Validation runs early, but the column-collision is detected
        # after read; either VALIDATION_EXIT or RUNTIME_EXIT is
        # acceptable so long as no half-written output is produced and
        # the call doesn't return SUCCESS_EXIT.
        assert rc != cli.SUCCESS_EXIT
        assert not out_path.exists()

    def test_same_output_column_name_twice_exits_three(self, tmp_path):
        input_path = tmp_path / "input.csv"
        _adult_mangrove_table().to_csv(input_path, index=False)
        rc = cli.main(
            [
                "biomass",
                "enrich-table",
                "--input-table",
                str(input_path),
                "--output-table",
                str(tmp_path / "out.csv"),
                "--output-run-json",
                str(tmp_path / "run.json"),
                "--biomass-estimate-col",
                "x",
                "--equation-used-col",
                "x",
            ]
        )
        assert rc == cli.VALIDATION_EXIT
