"""Tests for the M16 biomass equation refresh (`dina` direct-biomass update).

These tests cover the M16 contract:

- the packaged default loader returns the new unified parquet
  (``equation_application_unified.zstd.parquet``) which carries both
  volume (``source_dataset == "infys"``) and the four new direct-biomass
  (``source_dataset == "dina"``) rows;
- direct-biomass equations are selectable through the normal biomass API
  using incoming ``species`` mapped onto parquet ``scientific_name_apg_raw``,
  applied via ``equation_numpy_wd_fixed``;
- the matching layer enforces the M16 eligibility rules: non-null
  ``dbh_cm`` and ``life_stage == "Adult"`` for direct-biomass;
- ``source_record_id`` is preserved in the output so the M17 enrichment
  pass can append the equation-used identifier;
- ``BiomassColumns`` defaults match ``08_pkg/docs/forest_data_schema.csv``;
- the optional ``tree_height_m`` -> ``alt`` path is reserved and remains
  configurable for future height-aware direct-biomass equations.
"""
from __future__ import annotations

import math

import pandas as pd

from miaproc.biomass import (
    BiomassColumns,
    estimate_tree,
    estimate_trees,
    load_equations,
    load_packaged_equations,
)
from miaproc.biomass.equations import (
    DEFAULT_EQUATIONS_FILENAME,
    _SPECIES_ALIASES_NORMALIZED,
    match_equation,
)


# ---------------------------------------------------------------------------
# Packaged-data refresh
# ---------------------------------------------------------------------------


class TestPackagedDefault:
    def test_default_filename_is_unified_parquet(self):
        assert DEFAULT_EQUATIONS_FILENAME == "equation_application_unified.zstd.parquet"

    def test_load_packaged_equations_returns_unified_parquet(self):
        eq = load_packaged_equations()
        for col in (
            "source_dataset",
            "source_record_id",
            "scientific_name_apg_raw",
            "equation_numpy",
            "equation_numpy_wd_fixed",
            "response_variable",
            "response_units",
        ):
            assert col in eq.columns, col

    def test_load_equations_default_uses_packaged(self):
        a = load_packaged_equations()
        b = load_equations()  # path=None should defer to packaged
        assert list(a.columns) == list(b.columns)
        assert len(a) == len(b)

    def test_legacy_parquet_still_loadable_by_explicit_filename(self):
        legacy = load_packaged_equations(filename="allometries_mx.parquet")
        # Sanity: non-empty, doesn't crash. M16 doesn't promise this is
        # supported by the matching API, just that the file is preserved
        # for archaeology / explicit-path callers.
        assert len(legacy) > 0


# ---------------------------------------------------------------------------
# The four new dina direct-biomass rows
# ---------------------------------------------------------------------------


class TestDinaRowsPresent:
    def test_exactly_four_dina_rows(self):
        eq = load_packaged_equations()
        dina = eq[eq["source_dataset"] == "dina"]
        assert len(dina) == 4

    def test_dina_record_ids_are_dina_001_to_dina_004(self):
        eq = load_packaged_equations()
        dina = eq[eq["source_dataset"] == "dina"]
        assert sorted(dina["source_record_id"].tolist()) == [
            "dina_001",
            "dina_002",
            "dina_003",
            "dina_004",
        ]

    def test_dina_species_are_the_four_mangrove_species(self):
        eq = load_packaged_equations()
        dina = eq[eq["source_dataset"] == "dina"]
        assert set(dina["scientific_name_apg_raw"]) == {
            "Avicennia germinans",
            "Rhizophora mangle",
            "Laguncularia racemosa",
            "Conocarpus erectus",
        }

    def test_dina_rows_carry_wd_fixed_expression(self):
        eq = load_packaged_equations()
        dina = eq[eq["source_dataset"] == "dina"]
        assert dina["equation_numpy_wd_fixed"].notna().all()

    def test_dina_rows_have_no_state_or_assignment_level(self):
        eq = load_packaged_equations()
        dina = eq[eq["source_dataset"] == "dina"]
        assert dina["state"].isna().all()
        assert dina["assignment_level"].isna().all()


# ---------------------------------------------------------------------------
# BiomassColumns defaults track the forest_data_schema
# ---------------------------------------------------------------------------


class TestBiomassColumnsDefaults:
    def test_default_field_names_match_forest_data_schema(self):
        cols = BiomassColumns()
        assert cols.species == "species"
        assert cols.dbh_cm == "dbh_cm"
        assert cols.height_m == "tree_height_m"
        assert cols.life_stage == "life_stage"

    def test_field_names_are_overridable(self):
        # M16 contract: today's defaults are not hard-coded. A caller with
        # a table that uses different column names must be able to remap
        # without touching the package.
        cols = BiomassColumns(
            species="Species",
            dbh_cm="DBH (cm)",
            height_m="Total Height (m)",
            life_stage="LifeStage",
        )
        assert cols.species == "Species"
        assert cols.dbh_cm == "DBH (cm)"
        assert cols.height_m == "Total Height (m)"
        assert cols.life_stage == "LifeStage"


# ---------------------------------------------------------------------------
# match_equation against the new column contract
# ---------------------------------------------------------------------------


class TestMatchEquation:
    def test_match_dina_row_via_species(self):
        eq = load_packaged_equations()
        m = match_equation(
            equations=eq,
            species="Avicennia germinans",
            dataset="dina",
        )
        assert m is not None
        assert m.row["source_dataset"] == "dina"
        assert m.row["source_record_id"] == "dina_001"
        assert m.row["scientific_name_apg_raw"] == "Avicennia germinans"

    def test_dataset_filter_excludes_volume_rows(self):
        eq = load_packaged_equations()
        # ``Acacia farnesiana`` exists in the volume (infys) slice but not
        # in dina. With dataset="dina" we should not match it.
        m = match_equation(
            equations=eq,
            species="Acacia farnesiana",
            dataset="dina",
        )
        assert m is None

    def test_match_falls_back_to_any_state_for_dina(self):
        # dina rows have null ``state``; passing a state kwarg should
        # not block them — they match via the fallback step.
        eq = load_packaged_equations()
        m = match_equation(
            equations=eq,
            species="Rhizophora mangle",
            state="Yucatán",
            dataset="dina",
        )
        assert m is not None
        assert m.match_status == "fallback_any_state"
        assert m.row["source_record_id"] == "dina_002"

    def test_estado_legacy_alias_routes_to_state(self):
        eq = load_packaged_equations()
        m = match_equation(
            equations=eq,
            species="Laguncularia racemosa",
            estado="Yucatán",  # legacy alias for state
            dataset="dina",
        )
        assert m is not None
        assert m.row["source_record_id"] == "dina_003"


# ---------------------------------------------------------------------------
# Adult life-stage gate for direct-biomass
# ---------------------------------------------------------------------------


def _adult_obs(species: str, dbh_cm: float, height_m: float | None = None) -> dict:
    obs = {
        "species": species,
        "dbh_cm": dbh_cm,
        "tree_height_m": height_m,
        "life_stage": "Adult",
    }
    return obs


class TestAdultLifeStageGate:
    def test_adult_dbh_present_height_missing_succeeds_for_dina(self):
        eq = load_packaged_equations()
        out = estimate_tree(
            _adult_obs("Avicennia germinans", dbh_cm=10.0, height_m=None),
            equations=eq,
            dataset="dina",
        )
        assert out["match_status"] in ("exact_state", "fallback_any_state")
        assert out["source_dataset"] == "dina"
        assert out["source_record_id"] == "dina_001"
        assert out["response_variable"] == "B"
        assert out["response_units"] == "kg"
        # 0.403 * 0.78 * 10 ** 1.934 = 0.403 * 0.78 * 85.93... = ~27.0
        expected = 0.403 * 0.78 * (10.0 ** 1.934)
        assert math.isclose(
            out["estimate_response_variable"], expected, rel_tol=1e-9
        )

    def test_juvenile_life_stage_blocks_direct_biomass(self):
        eq = load_packaged_equations()
        obs = {
            "species": "Avicennia germinans",
            "dbh_cm": 10.0,
            "tree_height_m": None,
            "life_stage": "Juvenile",
        }
        out = estimate_tree(obs, equations=eq, dataset="dina")
        assert out["match_status"] == "life_stage_not_adult"
        assert pd.isna(out["estimate_response_variable"])
        # Even on rejection we surface which equation was rejected so
        # the caller can audit.
        assert out["source_record_id"] == "dina_001"
        assert out["source_dataset"] == "dina"

    def test_missing_life_stage_blocks_direct_biomass(self):
        eq = load_packaged_equations()
        obs = {
            "species": "Avicennia germinans",
            "dbh_cm": 10.0,
            "tree_height_m": None,
            "life_stage": None,
        }
        out = estimate_tree(obs, equations=eq, dataset="dina")
        assert out["match_status"] == "life_stage_not_adult"
        assert pd.isna(out["estimate_response_variable"])

    def test_adult_token_is_case_insensitive_and_whitespace_tolerant(self):
        eq = load_packaged_equations()
        for token in ("adult", "ADULT", "  Adult  "):
            obs = {
                "species": "Rhizophora mangle",
                "dbh_cm": 5.0,
                "tree_height_m": None,
                "life_stage": token,
            }
            out = estimate_tree(obs, equations=eq, dataset="dina")
            assert out["match_status"] in ("exact_state", "fallback_any_state"), token

    def test_missing_dbh_blocks_direct_biomass(self):
        eq = load_packaged_equations()
        obs = {
            "species": "Conocarpus erectus",
            "dbh_cm": None,
            "tree_height_m": None,
            "life_stage": "Adult",
        }
        out = estimate_tree(obs, equations=eq, dataset="dina")
        assert out["match_status"] == "dbh_missing"
        assert pd.isna(out["estimate_response_variable"])


# ---------------------------------------------------------------------------
# Output traceability for the M17 enrichment pass
# ---------------------------------------------------------------------------


class TestM17TraceabilityFields:
    def test_estimate_tree_returns_source_record_id(self):
        eq = load_packaged_equations()
        out = estimate_tree(
            _adult_obs("Conocarpus erectus", dbh_cm=8.0),
            equations=eq,
            dataset="dina",
        )
        assert "source_record_id" in out
        assert out["source_record_id"] == "dina_004"
        assert out["source_dataset"] == "dina"

    def test_estimate_trees_appends_source_record_id_column(self):
        eq = load_packaged_equations()
        df = pd.DataFrame(
            [
                _adult_obs("Avicennia germinans", dbh_cm=10.0),
                _adult_obs("Rhizophora mangle", dbh_cm=12.0),
                _adult_obs("Laguncularia racemosa", dbh_cm=9.0),
                _adult_obs("Conocarpus erectus", dbh_cm=11.0),
            ]
        )
        out = estimate_trees(df, equations=eq, dataset="dina")
        assert "source_record_id" in out.columns
        assert out["source_record_id"].tolist() == [
            "dina_001",
            "dina_002",
            "dina_003",
            "dina_004",
        ]
        assert (out["source_dataset"] == "dina").all()


# ---------------------------------------------------------------------------
# Direct-biomass evaluates equation_numpy_wd_fixed (not equation_numpy)
# ---------------------------------------------------------------------------


class TestExpressionSelection:
    def test_dina_uses_wd_fixed_expression(self):
        eq = load_packaged_equations()
        out = estimate_tree(
            _adult_obs("Rhizophora mangle", dbh_cm=10.0),
            equations=eq,
            dataset="dina",
        )
        # The wd-fixed expression for Rhizophora mangle is
        # ``0.722*0.91*(diam)**1.731``; the un-substituted expression is
        # ``0.722*wd*(diam)**1.731`` and would fail safeeval (free ``wd``).
        assert "wd" not in out["equation_numpy_used"]
        assert "0.91" in out["equation_numpy_used"]
        assert out["ecuacion_numpy"] == out["equation_numpy_used"]  # legacy alias

    def test_all_four_dina_species_evaluate(self):
        eq = load_packaged_equations()
        for species in (
            "Avicennia germinans",
            "Rhizophora mangle",
            "Laguncularia racemosa",
            "Conocarpus erectus",
        ):
            out = estimate_tree(
                _adult_obs(species, dbh_cm=10.0),
                equations=eq,
                dataset="dina",
            )
            assert out["match_status"] in ("exact_state", "fallback_any_state"), species
            assert pd.notna(out["estimate_response_variable"]), species
            assert out["estimate_response_variable"] > 0, species


# ---------------------------------------------------------------------------
# Optional tree_height_m -> alt path stays open for future equations
# ---------------------------------------------------------------------------


class TestHeightPathReserved:
    def test_height_passed_through_when_present(self):
        # Direct-biomass dina equations don't actually use ``alt``, but
        # height should still be accepted on the input side without
        # error so future height-aware direct-biomass equations work
        # without a disruptive rewrite.
        eq = load_packaged_equations()
        out = estimate_tree(
            _adult_obs("Avicennia germinans", dbh_cm=10.0, height_m=8.0),
            equations=eq,
            dataset="dina",
        )
        assert out["match_status"] in ("exact_state", "fallback_any_state")
        assert pd.notna(out["estimate_response_variable"])

    def test_volume_path_still_requires_height(self):
        # The volume (infys) equations reference ``alt`` and so must still
        # require height. Pick a known infys species; with no height,
        # the API should refuse without claiming a NaN result is "valid".
        eq = load_packaged_equations()
        obs = {
            "species": "Acacia farnesiana",
            "dbh_cm": 10.0,
            "tree_height_m": None,
            "life_stage": "Adult",
        }
        out = estimate_tree(obs, equations=eq, dataset="infys")
        assert out["match_status"] == "height_missing"
        assert pd.isna(out["estimate_response_variable"])

    def test_custom_height_column_name(self):
        eq = load_packaged_equations()
        cols = BiomassColumns(
            species="Species",
            dbh_cm="DBH (cm)",
            height_m="Total Height (m)",
            life_stage="LifeStage",
        )
        obs = {
            "Species": "Avicennia germinans",
            "DBH (cm)": 10.0,
            "Total Height (m)": 8.0,
            "LifeStage": "Adult",
        }
        out = estimate_tree(obs, equations=eq, columns=cols, dataset="dina")
        assert out["match_status"] in ("exact_state", "fallback_any_state")
        assert pd.notna(out["estimate_response_variable"])


# ---------------------------------------------------------------------------
# Legacy output aliases stay populated for back-compat
# ---------------------------------------------------------------------------


class TestM17ASpeciesAliases:
    """M17A — conservative deterministic species-typo recovery.

    Recovers exactly the two known mangrove typos surfaced by the
    post-fixture-refresh M17 smoke (`Rizophora mangle` and
    `Rizophora manlge`). Not a fuzzy-matcher: unknown species and
    null species still fail honestly, and rows without `dbh_cm`
    still classify as `dbh_missing` even when the alias resolves.
    """

    def test_alias_map_contains_both_known_typos_only(self):
        # Lock the alias map content to keep code review easy. If a
        # future pass adds more aliases it must update this test
        # explicitly — that is the intended audit trigger.
        assert _SPECIES_ALIASES_NORMALIZED == {
            "rizophora mangle": "rhizophora mangle",
            "rizophora manlge": "rhizophora mangle",
        }

    def test_typo_rizophora_mangle_resolves_to_dina_002(self):
        eq = load_packaged_equations()
        m = match_equation(
            equations=eq,
            species="Rizophora mangle",
            dataset="dina",
        )
        assert m is not None
        assert m.row["source_record_id"] == "dina_002"
        assert m.row["scientific_name_apg_raw"] == "Rhizophora mangle"

    def test_typo_rizophora_manlge_resolves_to_dina_002(self):
        eq = load_packaged_equations()
        m = match_equation(
            equations=eq,
            species="Rizophora manlge",
            dataset="dina",
        )
        assert m is not None
        assert m.row["source_record_id"] == "dina_002"

    def test_typo_with_trailing_whitespace_resolves(self):
        # The fixture has rows like "Rizophora manlge " with trailing
        # whitespace; `_normalize_species` strips/lowercases first,
        # then the alias lookup applies — so both space-padded and
        # uppercase variants should resolve.
        eq = load_packaged_equations()
        for variant in ("Rizophora manlge ", "  rizophora MANGLE  "):
            m = match_equation(
                equations=eq, species=variant, dataset="dina"
            )
            assert m is not None, variant
            assert m.row["source_record_id"] == "dina_002", variant

    def test_typo_resolves_through_estimate_tree_with_adult_dbh(self):
        eq = load_packaged_equations()
        out = estimate_tree(
            {
                "species": "Rizophora mangle",  # typo
                "dbh_cm": 10.0,
                "tree_height_m": None,
                "life_stage": "Adult",
            },
            equations=eq,
            dataset="dina",
        )
        assert out["match_status"] in ("exact_state", "fallback_any_state")
        assert out["source_record_id"] == "dina_002"
        assert out["source_dataset"] == "dina"
        # Alias resolution does not produce a different estimate from
        # the canonical species: the wd-fixed expression for dina_002
        # is `0.722*0.91*(diam)**1.731`.
        expected = 0.722 * 0.91 * (10.0 ** 1.731)
        assert math.isclose(
            out["estimate_response_variable"], expected, rel_tol=1e-9
        )

    def test_unknown_species_still_returns_no_equation_found(self):
        # The alias map must NOT silently reinterpret unrelated
        # species names. Anything outside the explicit alias keys
        # falls through to the normal exact-match path.
        eq = load_packaged_equations()
        out = estimate_tree(
            {
                "species": "Pinus radiata",  # not a mangrove
                "dbh_cm": 10.0,
                "tree_height_m": None,
                "life_stage": "Adult",
            },
            equations=eq,
            dataset="dina",
        )
        assert out["match_status"] == "no_equation_found"
        assert pd.isna(out["estimate_response_variable"])

    def test_null_species_remains_honest_failure(self):
        # Null species must NOT be aliased to anything — that would
        # be a guess, not a deterministic correction.
        eq = load_packaged_equations()
        out = estimate_tree(
            {
                "species": None,
                "dbh_cm": 10.0,
                "tree_height_m": None,
                "life_stage": "Adult",
            },
            equations=eq,
            dataset="dina",
        )
        assert out["match_status"] == "no_equation_found"
        assert pd.isna(out["estimate_response_variable"])

    def test_typo_with_missing_dbh_still_classifies_dbh_missing(self):
        # Alias recovery must not change the dbh_missing semantics.
        # A typo row with no dbh_cm still fails at the dbh check —
        # the matcher never gets reached.
        eq = load_packaged_equations()
        out = estimate_tree(
            {
                "species": "Rizophora mangle",  # typo
                "dbh_cm": None,
                "tree_height_m": None,
                "life_stage": "Adult",
            },
            equations=eq,
            dataset="dina",
        )
        assert out["match_status"] == "dbh_missing"
        assert pd.isna(out["estimate_response_variable"])

    def test_typo_with_juvenile_life_stage_still_classifies_not_adult(self):
        # Alias recovery resolves the species, but the adult gate
        # still applies — a juvenile typo row stays rejected.
        eq = load_packaged_equations()
        out = estimate_tree(
            {
                "species": "Rizophora manlge",  # typo
                "dbh_cm": 10.0,
                "tree_height_m": None,
                "life_stage": "Juvenile",
            },
            equations=eq,
            dataset="dina",
        )
        assert out["match_status"] == "life_stage_not_adult"
        assert pd.isna(out["estimate_response_variable"])
        # The audit field still surfaces dina_002 (the equation that
        # would have matched if the row were adult) — preserves the
        # M16 rejection-path traceability.
        assert out["source_record_id"] == "dina_002"


class TestLegacyOutputAliases:
    def test_legacy_keys_populated_alongside_new_keys(self):
        eq = load_packaged_equations()
        out = estimate_tree(
            _adult_obs("Avicennia germinans", dbh_cm=10.0),
            equations=eq,
            dataset="dina",
        )
        # New canonical keys.
        assert "equation_code" in out
        assert "assignment_level_used" in out
        assert "state_used" in out
        assert "equation_numpy_used" in out
        # Legacy aliases that earlier callers consume.
        assert "clave_ecuacion" in out
        assert out["clave_ecuacion"] == out["equation_code"]
        assert "nivel_asignacion" in out
        assert out["nivel_asignacion"] == out["assignment_level_used"]
        assert "estado_ecuacion_usada" in out
        assert out["estado_ecuacion_usada"] == out["state_used"]
        assert "ecuacion_numpy" in out
        assert out["ecuacion_numpy"] == out["equation_numpy_used"]
