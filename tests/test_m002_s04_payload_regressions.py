"""M002-S04 storage-neutral replacements for retired BigQuery-module tests (D011).

The silver/gold payload, column-uniqueness and humidity-collision
assertions formerly in ``tests/test_eddy_bigquery_writeback.py`` (which
reached the same objects through the deleted
``miaproc.eddy.bigquery_writeback`` re-exports) run here unchanged
against :mod:`miaproc.eddy.payload`:

- ``column_field_key`` is the former ``bigquery_field_key`` (casefold
  key used for the case-insensitive uniqueness invariant);
- ``prepare_gold_stage_payload`` is the former ``prepare_stage_dataframe``
  without the BigQuery-only source-flux join, final-table target-column
  filter and target-type cast, which left with the transport;
- the source-identity assertion of the former source-flux join is kept
  on the file-workflow gold payload (``prepare_local_gold_payload``),
  which carries source ``primary_key`` / ``site_id`` from silver.

No Google/BigQuery module is imported here.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from miaproc.eddy.payload import (
    COLUMN_COLLISION_ATTRS_KEY,
    DuplicateStageColumnsError,
    HUMIDITY_DERIVED_RENAME,
    HUMIDITY_SOURCE_COLUMN,
    column_field_key,
    ensure_unique_stage_columns,
    prepare_gold_stage_payload,
    prepare_local_gold_payload,
    prepare_silver_stage_payload,
    validate_source_columns_unique,
)


# ---------------------------------------------------------------------------
# M28: stage-payload column-uniqueness guard, silver preservation,
# gold preservation, and rH / rH_norm_s humidity collision policy.
# ---------------------------------------------------------------------------


def _silver_with_bronze_sentinel(n: int = 3) -> pd.DataFrame:
    """A silver-shaped frame that carries an EddyPro source-only
    column ``bronze_only_flag`` so the M28 bronze->silver preservation
    contract can be exercised end-to-end."""
    return pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=n, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, 0.3],
            "USTAR": [0.2, 0.3, 0.4],
            "Tair": [20.0, 21.0, 22.0],
            "VPD": [5.0, 6.0, 7.0],
            "Rg": [0.0, 100.0, 200.0],
            "QC_NEE": [0, 0, 0],
            "H": [10.0, 20.0, 30.0],
            "LE": [50.0, 60.0, 70.0],
            "P_RAIN": [0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0],
            # Source-only sentinel: not produced by stage1, but
            # preserved through the bronze->silver pipeline.
            "bronze_only_flag": [1, 0, 1],
        }
    )


def _silver_with_duplicate_rh() -> pd.DataFrame:
    """Stage-1-shaped frame carrying two columns literally named
    ``rH``. Pandas allows this even though BigQuery rejects it; the
    M28 dedup helper must resolve the collision before any
    ``load_table_from_dataframe`` call."""
    df = pd.DataFrame(
        {
            "DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            ),
            "NEE": [0.1, 0.2, 0.3],
            "USTAR": [0.2, 0.3, 0.4],
            "Tair": [20.0, 21.0, 22.0],
            "VPD": [5.0, 6.0, 7.0],
            "Rg": [0.0, 100.0, 200.0],
            "QC_NEE": [0, 0, 0],
            "H": [10.0, 20.0, 30.0],
            "LE": [50.0, 60.0, 70.0],
            "P_RAIN": [0.0, 0.0, 0.0],
            "rH": [60.0, 70.0, 80.0],  # source
        }
    )
    # Append a second ``rH`` column with diverging values.
    extra = pd.DataFrame({"rH": [55.0, 75.0, 85.0]})
    df = pd.concat([df, extra], axis=1)
    # Sanity check: the fixture must produce a literal duplicate.
    assert list(df.columns).count("rH") == 2
    return df


class TestEnsureUniqueStageColumns:
    def test_unique_input_is_no_op(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        out, actions = ensure_unique_stage_columns(df)
        assert list(out.columns) == ["a", "b"]
        assert actions == []
        # Marker still attached for downstream introspection.
        assert out.attrs[COLUMN_COLLISION_ATTRS_KEY] == []

    def test_caller_frame_not_mutated(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        before = df.copy(deep=True)
        _ = ensure_unique_stage_columns(df)
        pd.testing.assert_frame_equal(df, before)

    def test_equivalent_rH_duplicate_is_suppressed(self):
        base = pd.DataFrame(
            {"DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            )}
        )
        rh = pd.Series([60.0, 70.0, 80.0])
        # Two columns literally named ``rH`` with identical values.
        df = pd.concat(
            [base, rh.rename("rH"), rh.copy().rename("rH")], axis=1
        )
        out, actions = ensure_unique_stage_columns(df)
        assert list(out.columns).count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME not in out.columns
        assert any(
            a["action"] == "suppressed_equivalent_duplicate" for a in actions
        )

    def test_divergent_rH_duplicate_renamed_to_rH_norm_s(self):
        df = _silver_with_duplicate_rh()
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        assert cols.count("rH") == 1, cols
        assert HUMIDITY_DERIVED_RENAME in cols
        # Source values stay under ``rH``; derived under ``rH_norm_s``.
        assert list(out["rH"]) == [60.0, 70.0, 80.0]
        assert list(out[HUMIDITY_DERIVED_RENAME]) == [55.0, 75.0, 85.0]
        assert any(
            a["action"] == "renamed_divergent_duplicate" for a in actions
        )

    def test_divergent_rH_with_existing_rH_norm_s_uses_suffix(self):
        df = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                HUMIDITY_DERIVED_RENAME: [10.0, 20.0, 30.0],
            }
        )
        df = pd.concat(
            [
                df,
                pd.Series([60.0, 70.0, 80.0]).rename("rH"),
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),
            ],
            axis=1,
        )
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        assert cols.count("rH") == 1
        # Existing rH_norm_s is preserved; new derived gets a suffix.
        assert HUMIDITY_DERIVED_RENAME in cols
        assert f"{HUMIDITY_DERIVED_RENAME}_2" in cols
        assert any(
            a["renamed_to"] == f"{HUMIDITY_DERIVED_RENAME}_2"
            for a in actions
        )

    def test_other_duplicate_raises_duplicate_stage_columns_error(self):
        base = pd.DataFrame(
            {"DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            )}
        )
        df = pd.concat(
            [
                base,
                pd.Series([1, 2, 3]).rename("NEE_f"),
                pd.Series([4, 5, 6]).rename("NEE_f"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            ensure_unique_stage_columns(df)
        assert "NEE_f" in str(exc_info.value)
        assert exc_info.value.duplicate_columns == ("NEE_f",)

    def test_nan_aware_equivalence_treats_paired_nans_as_equal(self):
        base = pd.DataFrame({"x": [1, 2, 3]})
        a = pd.Series([1.0, float("nan"), 3.0]).rename("rH")
        b = pd.Series([1.0, float("nan"), 3.0]).rename("rH")
        df = pd.concat([base, a, b], axis=1)
        out, actions = ensure_unique_stage_columns(df)
        assert list(out.columns).count("rH") == 1
        assert any(
            a_["action"] == "suppressed_equivalent_duplicate"
            for a_ in actions
        )


class TestEnsureUniqueStageColumnsCaseInsensitiveM31:
    """M31: BigQuery field keys are case-insensitive.

    Pandas considered ``RH`` and ``rH`` distinct, so the M28 guard
    let them through and ``load_table_from_dataframe`` failed with
    ``Field rH already exists in schema``. M31 closes that hole by
    operating on ``casefold``-d field keys for both the duplicate
    detection and the reserved-name search for the divergent-derived
    rename. The humidity family is normalized to the canonical
    source name ``rH``; non-humidity case collisions still raise.
    """

    @staticmethod
    def _two_humidity_variants_diverge() -> pd.DataFrame:
        base = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
                "USTAR": [0.2, 0.3, 0.4],
            }
        )
        return pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),
            ],
            axis=1,
        )

    @staticmethod
    def _two_humidity_variants_equivalent() -> pd.DataFrame:
        base = pd.DataFrame({"x": [1, 2, 3]})
        return pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([60.0, 70.0, 80.0]).rename("rH"),
            ],
            axis=1,
        )

    def test_column_field_key_is_casefold(self):
        assert column_field_key("rH") == "rh"
        assert column_field_key("RH") == "rh"
        assert column_field_key("Rh") == "rh"
        assert column_field_key("rH_norm_s") == "rh_norm_s"
        # Idempotent for already-lowercase / non-letter content.
        assert column_field_key("foo_bar_1") == "foo_bar_1"

    def test_rh_case_collision_equivalent_collapses_to_canonical_rH(self):
        df = self._two_humidity_variants_equivalent()
        out, actions = ensure_unique_stage_columns(df)
        # Final payload has BigQuery-unique field keys.
        keys = [column_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)
        # The kept humidity column is canonicalized to ``rH``.
        assert "rH" in out.columns
        assert "RH" not in out.columns
        assert HUMIDITY_DERIVED_RENAME not in out.columns
        # Two actions: canonicalization + suppression of equivalent dup.
        assert any(
            a["action"] == "renamed_to_canonical_humidity"
            and a["renamed_to"] == HUMIDITY_SOURCE_COLUMN
            for a in actions
        )
        assert any(
            a["action"] == "suppressed_equivalent_duplicate"
            for a in actions
        )

    def test_rh_case_collision_divergent_keeps_rH_and_rH_norm_s(self):
        df = self._two_humidity_variants_diverge()
        out, actions = ensure_unique_stage_columns(df)
        keys = [column_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)
        # Source ``RH`` is canonicalized to ``rH`` (the first
        # occurrence wins); the divergent derived ``rH`` is renamed.
        assert "rH" in out.columns
        assert HUMIDITY_DERIVED_RENAME in out.columns
        assert "RH" not in out.columns
        # Source values are preserved under the canonical name.
        assert list(out["rH"]) == [60.0, 70.0, 80.0]
        assert list(out[HUMIDITY_DERIVED_RENAME]) == [55.0, 75.0, 85.0]
        assert any(
            a["action"] == "renamed_to_canonical_humidity"
            for a in actions
        )
        assert any(
            a["action"] == "renamed_divergent_duplicate"
            and a["renamed_to"] == HUMIDITY_DERIVED_RENAME
            for a in actions
        )

    def test_payload_has_no_case_insensitive_duplicate_keys(self):
        """Stronger than ``payload.columns.is_unique``: cover the
        BigQuery uniqueness semantics that surfaced as the cloud
        failure mode."""
        df = self._two_humidity_variants_diverge()
        out, _ = ensure_unique_stage_columns(df)
        # The pandas-level uniqueness check passes both before and
        # after the fix; the case-insensitive check did not pass
        # before M31.
        assert out.columns.is_unique
        keys = [column_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)

    def test_existing_rH_norm_s_still_uses_suffix_under_case_insensitivity(
        self,
    ):
        """The deterministic suffix logic must also consult
        case-folded keys when picking the next available rename
        target, so an existing ``rH_norm_s`` (or a hypothetical
        ``RH_norm_s``) cannot collide with the new derived rename."""
        base = pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                HUMIDITY_DERIVED_RENAME: [10.0, 20.0, 30.0],
            }
        )
        df = pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),
            ],
            axis=1,
        )
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        # Source ``RH`` becomes canonical ``rH``; existing
        # ``rH_norm_s`` is preserved; divergent derived gets the
        # numeric suffix.
        assert cols.count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME in cols
        assert f"{HUMIDITY_DERIVED_RENAME}_2" in cols
        keys = [column_field_key(c) for c in cols]
        assert len(set(keys)) == len(keys), cols
        assert any(
            a["renamed_to"] == f"{HUMIDITY_DERIVED_RENAME}_2"
            for a in actions
        )

    def test_non_humidity_case_insensitive_collision_raises(self):
        """``NEE`` + ``nee`` is a case-insensitive duplicate but
        not in the humidity family: the helper must raise
        ``DuplicateStageColumnsError`` so the upstream pipeline is
        fixed before any BigQuery write."""
        base = pd.DataFrame(
            {"DateTime": pd.date_range(
                "2025-08-01", periods=3, freq="30min", tz="UTC"
            )}
        )
        df = pd.concat(
            [
                base,
                pd.Series([0.1, 0.2, 0.3]).rename("NEE"),
                pd.Series([0.11, 0.22, 0.33]).rename("nee"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            ensure_unique_stage_columns(df)
        msg = str(exc_info.value)
        # Both the affected logical key and the source variants are
        # named so the operator can locate the upstream defect.
        assert "nee" in msg
        assert "NEE" in msg
        # The error preserves the source column names for tooling.
        assert set(exc_info.value.duplicate_columns) >= {"NEE", "nee"}

    def test_multiple_non_humidity_case_collisions_all_reported(self):
        base = pd.DataFrame(
            {"x": [1, 2, 3]}
        )
        df = pd.concat(
            [
                base,
                pd.Series([0.1, 0.2, 0.3]).rename("NEE"),
                pd.Series([0.11, 0.22, 0.33]).rename("nee"),
                pd.Series([1, 2, 3]).rename("Tair"),
                pd.Series([4, 5, 6]).rename("tair"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            ensure_unique_stage_columns(df)
        names = set(exc_info.value.duplicate_columns)
        assert {"NEE", "nee", "Tair", "tair"} <= names

    def test_three_way_humidity_collision_canonicalizes_and_suffixes(self):
        """``RH`` + ``Rh`` + ``rH`` should collapse to ``rH`` with the
        right policy applied to each subsequent occurrence."""
        base = pd.DataFrame({"x": [1, 2, 3]})
        df = pd.concat(
            [
                base,
                pd.Series([60.0, 70.0, 80.0]).rename("RH"),
                pd.Series([60.0, 70.0, 80.0]).rename("Rh"),  # equiv
                pd.Series([55.0, 75.0, 85.0]).rename("rH"),  # diverges
            ],
            axis=1,
        )
        out, actions = ensure_unique_stage_columns(df)
        cols = list(out.columns)
        keys = [column_field_key(c) for c in cols]
        assert len(set(keys)) == len(keys), cols
        assert "rH" in cols
        assert HUMIDITY_DERIVED_RENAME in cols
        # First variant renamed to canonical; second equivalent
        # suppressed; third divergent renamed to rH_norm_s.
        actions_by_kind = [a["action"] for a in actions]
        assert "renamed_to_canonical_humidity" in actions_by_kind
        assert "suppressed_equivalent_duplicate" in actions_by_kind
        assert "renamed_divergent_duplicate" in actions_by_kind


class TestPrepareSilverStagePayloadCaseInsensitiveM31:
    """M31 was a case-insensitive collision band-aid for ``RH`` + ``rH``
    in silver. M32 supersedes the band-aid: stage 1's internal
    biomet ``rH`` is renamed to its source-truth final name
    ``RH_1_1_1`` before the unique-column guard, so flux-side ``RH``
    and biomet ``RH_1_1_1`` are case-insensitively distinct and both
    survive. The legacy ``rH`` / ``rH_norm_s`` policy still applies
    to the upstream-defect case where stage 1 itself emits duplicate
    internal ``rH`` columns; that case is covered separately in
    ``TestPrepareSilverStagePayload`` and via
    :func:`ensure_unique_stage_columns` directly.
    """

    @staticmethod
    def _silver_with_RH_and_rH(*, diverge: bool) -> pd.DataFrame:
        rh = [55.0, 75.0, 85.0] if diverge else [60.0, 70.0, 80.0]
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
                "QC_NEE": [0, 0, 0],
                "Tair": [20.0, 21.0, 22.0],
                "USTAR": [0.2, 0.3, 0.4],
                "RH": [60.0, 70.0, 80.0],
                "rH": rh,
            }
        )

    def test_equivalent_RH_and_rH_kept_separately_under_m32(self):
        """M32: equivalent flux ``RH`` and biomet ``rH`` no longer
        collide on the case-insensitive BigQuery field key because
        biomet ``rH`` is renamed to its source-truth final name
        ``RH_1_1_1``. Both series survive distinctly."""
        silver = self._silver_with_RH_and_rH(diverge=False)
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        keys = [column_field_key(c) for c in payload.columns]
        assert len(set(keys)) == len(keys), list(payload.columns)
        assert "RH" in payload.columns
        assert "RH_1_1_1" in payload.columns
        # Flux-side ``RH`` and biomet ``RH_1_1_1`` carry their
        # respective source series even when the values happen to
        # match in this fixture.
        assert list(payload["RH"]) == [60.0, 70.0, 80.0]
        assert list(payload["RH_1_1_1"]) == [60.0, 70.0, 80.0]
        # No legacy humidity collision action fires because the
        # case-insensitive collision no longer exists post-rename.
        assert not any(
            a["action"]
            in (
                "renamed_to_canonical_humidity",
                "suppressed_equivalent_duplicate",
            )
            for a in actions
        )
        # Backend-only inherited names are not present in the
        # source-truth payload.
        for backend in ("NEE", "QC_NEE", "Tair", "USTAR", "rH"):
            assert backend not in payload.columns, backend

    def test_divergent_RH_and_rH_preserved_separately_under_m32(self):
        """M32: divergent flux ``RH`` and biomet ``rH`` are
        preserved as ``RH`` and ``RH_1_1_1`` respectively, carrying
        their own values. The legacy ``rH``/``rH_norm_s`` workaround
        no longer applies because the source-truth final names are
        already case-insensitively distinct."""
        silver = self._silver_with_RH_and_rH(diverge=True)
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        keys = [column_field_key(c) for c in payload.columns]
        assert len(set(keys)) == len(keys), list(payload.columns)
        assert "RH" in payload.columns
        assert "RH_1_1_1" in payload.columns
        assert HUMIDITY_DERIVED_RENAME not in payload.columns
        # Flux RH keeps flux values, biomet rH (now RH_1_1_1) keeps
        # the divergent biomet values.
        assert list(payload["RH"]) == [60.0, 70.0, 80.0]
        assert list(payload["RH_1_1_1"]) == [55.0, 75.0, 85.0]


class TestGoldStagePayloadLowercaseMappingM31:
    """M31: gold ``prepare_stage_dataframe`` renames source -> target
    rather than duplicating because BigQuery field keys are
    case-insensitive. Keeping both ``NEE_f`` and ``nee_f`` would
    surface as ``Field nee_f already exists in schema`` at the live
    stage load."""

    @staticmethod
    def _gold_with_canonical_backend_outputs() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE_f": [0.11, 0.21, 0.31],
                "NEE_fqc": [0, 0, 0],
                "Rg_f": [0.0, 100.0, 200.0],
                "Tair_f": [20.5, 21.5, 22.5],
                "VPD_f": [5.1, 6.1, 7.1],
            }
        )

    def test_lowercase_targets_present_uppercase_backend_names_dropped(
        self,
    ):
        out = prepare_gold_stage_payload(
            self._gold_with_canonical_backend_outputs(),
            site_id="RBRL",
        )
        for col in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f"):
            assert col in out.columns, col
        for col in ("NEE_f", "NEE_fqc", "Rg_f", "Tair_f", "VPD_f"):
            assert col not in out.columns, col
        keys = [column_field_key(c) for c in out.columns]
        assert len(set(keys)) == len(keys), list(out.columns)

    def test_values_carry_through_rename(self):
        out = prepare_gold_stage_payload(
            self._gold_with_canonical_backend_outputs(),
            site_id="RBRL",
        )
        assert out["nee_f"].iloc[0] == 0.11
        assert out["sw_in_f"].iloc[1] == 100.0
        assert out["ta_f"].iloc[2] == 22.5


class TestValidateSourceColumnsUnique:
    def test_none_is_noop(self):
        validate_source_columns_unique(None, side="bronze")  # no raise

    def test_empty_is_noop(self):
        validate_source_columns_unique(pd.DataFrame(), side="bronze")

    def test_unique_columns_pass(self):
        validate_source_columns_unique(
            pd.DataFrame({"a": [1], "b": [2]}), side="bronze"
        )

    def test_duplicate_source_columns_raise(self):
        a = pd.Series([1, 2]).rename("rH")
        b = pd.Series([3, 4]).rename("rH")
        df = pd.concat([a, b], axis=1)
        with pytest.raises(DuplicateStageColumnsError) as exc_info:
            validate_source_columns_unique(df, side="bronze flux source")
        assert "bronze flux source" in str(exc_info.value)
        assert "rH" in str(exc_info.value)
        assert exc_info.value.duplicate_columns == ("rH",)


class TestPrepareSilverStagePayload:
    def test_preserves_bronze_sentinel_column(self):
        silver = _silver_with_bronze_sentinel()
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        assert "bronze_only_flag" in payload.columns
        assert actions == []
        # Identity triple is present and ordered first.
        assert list(payload.columns)[:3] == [
            "primary_key", "site_id", "timestamp"
        ]
        # Source values preserved.
        assert list(payload["bronze_only_flag"]) == [1, 0, 1]
        # M32: silver columns survive under their source-truth final
        # names (``co2_flux``, ``u_star``, ``air_temperature_c``,
        # ``VPD_hpa``, ``SWIN_1_1_1``, ``RH_1_1_1``, ``H``, ``LE``).
        # Backend-only inherited names must not appear.
        for col in (
            "co2_flux",
            "u_star",
            "air_temperature_c",
            "VPD_hpa",
            "SWIN_1_1_1",
            "RH_1_1_1",
            "H",
            "LE",
        ):
            assert col in payload.columns, col
        for backend in ("NEE", "USTAR", "Tair", "VPD", "Rg", "rH", "QC_NEE"):
            assert backend not in payload.columns, backend

    def test_payload_columns_are_unique(self):
        silver = _silver_with_duplicate_rh()
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        # The whole point of M28: the BigQuery client must never see
        # duplicate column names.
        assert payload.columns.is_unique, list(payload.columns)
        assert any(
            a["column"] == HUMIDITY_SOURCE_COLUMN for a in actions
        )

    def test_equivalent_rH_duplicate_suppressed(self):
        silver = _silver_with_bronze_sentinel()
        extra = pd.DataFrame({"rH": silver["rH"].copy()})
        silver_dup = pd.concat([silver, extra], axis=1)
        payload, actions = prepare_silver_stage_payload(
            silver_dup, site_id="RBRL"
        )
        assert list(payload.columns).count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME not in payload.columns
        assert any(
            a["action"] == "suppressed_equivalent_duplicate" for a in actions
        )

    def test_divergent_rH_duplicate_renamed_to_rH_norm_s(self):
        silver = _silver_with_duplicate_rh()
        payload, actions = prepare_silver_stage_payload(
            silver, site_id="RBRL"
        )
        cols = list(payload.columns)
        assert cols.count("rH") == 1
        assert HUMIDITY_DERIVED_RENAME in cols
        assert list(payload["rH"]) == [60.0, 70.0, 80.0]
        assert list(payload[HUMIDITY_DERIVED_RENAME]) == [55.0, 75.0, 85.0]
        assert any(
            a["action"] == "renamed_divergent_duplicate"
            and a["renamed_to"] == HUMIDITY_DERIVED_RENAME
            for a in actions
        )

    def test_source_flux_with_duplicate_columns_raises(self):
        silver = _silver_with_bronze_sentinel()
        bad_src = pd.concat(
            [
                pd.Series([1, 2, 3]).rename("rH"),
                pd.Series([4, 5, 6]).rename("rH"),
            ],
            axis=1,
        )
        with pytest.raises(DuplicateStageColumnsError):
            prepare_silver_stage_payload(
                silver, site_id="RBRL", source_flux_df=bad_src,
            )

    def test_missing_datetime_raises(self):
        # M32A: the helper accepts either internal ``DateTime`` or
        # source-truth ``timestamp``. The error must name both so
        # the operator sees the realistic source-truth option.
        with pytest.raises(
            ValueError, match=r"missing both 'DateTime' and 'timestamp'"
        ):
            prepare_silver_stage_payload(
                pd.DataFrame({"x": [1]}), site_id="RBRL"
            )

    def test_caller_frame_not_mutated(self):
        silver = _silver_with_bronze_sentinel()
        before = silver.copy(deep=True)
        _ = prepare_silver_stage_payload(silver, site_id="RBRL")
        pd.testing.assert_frame_equal(silver, before)

    def test_identity_triple_overwrite_recorded_in_actions(self):
        # M32A: when the silver frame provides a pre-existing
        # ``timestamp`` column, that column IS the source-truth
        # time series the function uses to derive the new identity
        # ``timestamp`` (so the replacement is a benign dtype
        # normalization and is intentionally not audited). Stale
        # ``site_id`` / ``primary_key`` overwrites still surface in
        # the action list because they are not the time source.
        silver = _silver_with_bronze_sentinel()
        silver_with_ids = silver.copy()
        silver_with_ids["site_id"] = "STALE"
        silver_with_ids["primary_key"] = "STALE_PK"
        payload, actions = prepare_silver_stage_payload(
            silver_with_ids, site_id="RBRL"
        )
        assert (payload["site_id"] == "RBRL").all()
        names = {
            a["column"] for a in actions
            if a["action"] == "identity_overwrite"
        }
        assert "site_id" in names
        assert "primary_key" in names
        # ``timestamp`` is the M32A source-truth time column; using
        # it for the synthesized identity is not an overwrite of a
        # stale value.
        assert "timestamp" not in names


class TestGoldStagePayloadM28Preservation:
    """M28: every silver column survives into the gold stage payload
    (no final-table target filter exists in the storage-neutral path)."""

    def _gold_with_silver(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=3, freq="30min", tz="UTC"
                ),
                "NEE_f": [0.11, 0.21, 0.31],
                "NEE_fqc": [0, 0, 0],
                "Rg_f": [0.0, 100.0, 200.0],
                "Tair_f": [20.5, 21.5, 22.5],
                "VPD_f": [5.1, 6.1, 7.1],
                "GPP": [0.0, 1.0, 2.0],
                "Reco": [1.0, 1.0, 1.0],
                # Silver-only sentinel — must survive into the stage
                # payload under the M28 preservation contract.
                "silver_only_flag": [True, False, True],
                "H": [10.0, 20.0, 30.0],
                "LE": [50.0, 60.0, 70.0],
                "rH": [60.0, 70.0, 80.0],
            }
        )

    def test_preserves_silver_only_columns(self):
        out = prepare_gold_stage_payload(
            self._gold_with_silver(),
            site_id="RBRL",
        )
        # Silver-only sentinel is preserved (no final-table target
        # filter exists in the storage-neutral path).
        assert "silver_only_flag" in out.columns
        assert "rH" in out.columns
        assert "H" in out.columns
        # Gold analytical outputs are still there.
        for col in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f"):
            assert col in out.columns, col
        # Identity is preserved.
        for col in ("primary_key", "site_id", "timestamp"):
            assert col in out.columns, col


    def test_collision_actions_recorded_in_attrs(self):
        # Build a gold-with-silver frame that carries a duplicate rH.
        df = self._gold_with_silver()
        df = pd.concat(
            [df, pd.Series([55.0, 75.0, 85.0]).rename("rH")], axis=1
        )
        out = prepare_gold_stage_payload(
            df,
            site_id="RBRL",
        )
        assert out.columns.is_unique
        assert HUMIDITY_DERIVED_RENAME in out.columns
        actions = out.attrs.get(COLUMN_COLLISION_ATTRS_KEY, [])
        assert any(
            a["column"] == HUMIDITY_SOURCE_COLUMN for a in actions
        )

    def test_other_duplicate_raises_before_load(self):
        df = self._gold_with_silver()
        # ``GPP`` is not in the lowercase rename map and not part of
        # the identity triple, so a literal duplicate survives all the
        # internal mutation and reaches ``ensure_unique_stage_columns``.
        df = pd.concat(
            [df, pd.Series([4.0, 5.0, 6.0]).rename("GPP")], axis=1
        )
        with pytest.raises(DuplicateStageColumnsError):
            prepare_gold_stage_payload(
                df, site_id="RBRL",
            )


class TestGoldStagePayload:
    """Neutral gold payload (``prepare_gold_stage_payload``): identity,
    lineage ``gold_derived`` lowercase names and ``dateAndTime`` derived
    from the backend output (formerly asserted on the BigQuery
    ``prepare_stage_dataframe``)."""

    def _processed(self, n: int = 3) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "DateTime": pd.date_range(
                    "2025-08-01", periods=n, freq="30min", tz="UTC"
                ),
                "NEE": [0.1, 0.2, 0.3],
                "NEE_f": [0.11, 0.21, 0.31],
                "NEE_fqc": [0, 0, 0],
                "GPP": [0.0, 1.0, 2.0],
                "Reco": [1.0, 1.0, 1.0],
                "Tair": [20.0, 21.0, 22.0],
                "Tair_f": [20.5, 21.5, 22.5],
                "Rg": [0.0, 100.0, 200.0],
                "Rg_f": [0.0, 100.0, 200.0],
                "VPD": [5.0, 6.0, 7.0],
                "VPD_f": [5.1, 6.1, 7.1],
                "USTAR": [0.2, 0.3, 0.4],
            }
        )


    def test_lowercase_analytical_columns_and_dateAndTime(self):
        out = prepare_gold_stage_payload(self._processed(), site_id="RBRL")
        for col in ("nee_f", "nee_fqc", "sw_in_f", "ta_f", "vpd_f", "dateAndTime"):
            assert col in out.columns, f"missing column {col}"
        assert out["nee_f"].iloc[0] == 0.11  # from NEE_f
        assert out["sw_in_f"].iloc[1] == 100.0  # from Rg_f
        assert out["ta_f"].iloc[2] == 22.5  # from Tair_f
        # dateAndTime is the ISO-style STRING form.
        assert out["dateAndTime"].iloc[0] == "2025-08-01 00:00:00"
        # Identity is materialized.
        assert (out["site_id"] == "RBRL").all()
        assert out["primary_key"].is_unique



    def test_caller_frames_are_not_mutated(self):
        proc = self._processed()
        proc_before = proc.copy(deep=True)
        _ = prepare_gold_stage_payload(proc, site_id="RBRL")
        pd.testing.assert_frame_equal(proc, proc_before)

    def test_missing_datetime_raises(self):
        with pytest.raises(ValueError, match="missing the 'DateTime' column"):
            prepare_gold_stage_payload(
                pd.DataFrame({"NEE_f": [0.1]}), site_id="RBRL"
            )


class TestLocalGoldSourceIdentity:
    """Source identity through the file-workflow gold payload.

    Replaces the BigQuery source-flux-join identity assertion: rows with
    a source record keep the source ``primary_key`` (strings and integer
    IDs above 2**53 without precision loss), the regularized gap row has
    a valid regular-grid ``timestamp`` and the uniform group ``site_id``,
    and its source measurements stay missing (never zero or fabricated).
    """

    @pytest.mark.parametrize(
        "keys",
        [
            ["src-pk-A", "src-pk-B", None],
            pd.array([2**53 + 1, 2**53 + 3, pd.NA], dtype="Int64"),
        ],
        ids=["string_keys", "int64_keys_above_2_53"],
    )
    def test_source_primary_key_carried_and_gap_row_measurements_missing(self, keys):
        grid = pd.date_range("2025-08-01", periods=3, freq="30min", tz="UTC")
        gold = pd.DataFrame(
            {
                "primary_key": keys,
                "site_id": ["RBRL", "RBRL", "RBRL"],
                "timestamp": grid,
                "DateTime": grid,
                "co2_flux": [0.1, 0.2, np.nan],
                "air_temperature_c": [20.0, 21.0, np.nan],
                "u_star": [0.21, 0.22, np.nan],
                "NEE": [0.1, 0.2, np.nan],
                "NEE_f": [0.11, 0.21, 0.31],
                "Reco": [1.0, 1.0, 1.0],
                "GPP": [0.0, 1.0, 2.0],
            }
        )
        before = gold.copy(deep=True)
        out, _ = prepare_local_gold_payload(gold)
        pd.testing.assert_frame_equal(gold, before)
        assert list(out.columns[:3]) == ["primary_key", "site_id", "timestamp"]
        assert out["primary_key"].iloc[0] == keys[0]
        assert out["primary_key"].iloc[1] == keys[1]
        # Gap row: nothing synthesized for the key, valid grid timestamp,
        # uniform site identity, missing measurements stay missing.
        assert pd.isna(out["primary_key"].iloc[2])
        assert out["timestamp"].iloc[2] == pd.Timestamp("2025-08-01 01:00", tz="UTC")
        assert (out["site_id"] == "RBRL").all()
        assert out["co2_flux"].iloc[0] == 0.1
        for col in ("co2_flux", "air_temperature_c", "u_star"):
            assert pd.isna(out[col].iloc[2]), col
        # Backend gap-filled output is kept under its lowercase name.
        assert out["nee_f"].iloc[2] == 0.31
        assert list(out.columns).count("timestamp") == 1
        assert "DateTime" not in out.columns
