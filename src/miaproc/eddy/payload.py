"""Transport-neutral source-facing payload helpers for eddy silver/gold.

This module owns the pure DataFrame logic shared by every silver/gold
storage path (extracted in M002-S01 from the former BigQuery writeback
module, which M002-S04 removed):

- M32 / M32A source-truth renaming of the internal stage-1 names to
  the inherited final names (:func:`apply_silver_source_truth_rename`)
  and the reverse reconstruction of the internal calculation frame,
  preferring the source-truth ``timestamp`` over a leaked ``DateTime``
  (:func:`silver_to_internal_calc_frame`);
- M28 / M31 column-uniqueness and humidity-collision policy
  (:func:`validate_source_columns_unique`,
  :func:`ensure_unique_stage_columns`, :func:`column_field_key`);
- silver stage-payload preparation with the
  ``(primary_key, site_id, timestamp)`` identity triple
  (:func:`prepare_silver_stage_payload`);
- the transport-neutral gold stage-payload subset
  (:func:`prepare_gold_stage_payload`, built from
  :func:`derive_gold_stage_columns` and
  :func:`finalize_gold_stage_payload`): ``DateTime``-derived identity,
  the lineage-CSV ``gold_derived`` names (:data:`GOLD_DERIVED_RENAME`
  plus ``dateAndTime``), the M32 drop of redundant internal
  passthroughs and identity-first ordering.

Behavior, column order, dtypes, exceptions and messages are unchanged
from the former BigQuery-module originals; the case-insensitive field
key and the identity triple are inherited contracts of the deployed
source-facing payload. ``miaproc.eddy`` re-exports the silver/uniqueness
names. The BigQuery-only source-flux join, target-column filter and
target-type cast were removed with the BigQuery transport (M002-S04).
This module imports only pandas and :mod:`miaproc.eddy.constants`; it
never imports a Google/BigQuery client, constructs a client, or looks
up credentials.
"""
from __future__ import annotations

from typing import Any, Callable, Optional

import pandas as pd


__all__ = [
    "COLUMN_COLLISION_ATTRS_KEY",
    "DuplicateStageColumnsError",
    "GOLD_DERIVED_RENAME",
    "HUMIDITY_DERIVED_RENAME",
    "HUMIDITY_SOURCE_COLUMN",
    "apply_silver_source_truth_rename",
    "column_field_key",
    "derive_gold_stage_columns",
    "ensure_unique_stage_columns",
    "finalize_gold_stage_payload",
    "prepare_gold_stage_payload",
    "prepare_local_gold_payload",
    "prepare_local_silver_payload",
    "prepare_silver_stage_payload",
    "silver_to_internal_calc_frame",
    "validate_source_columns_unique",
]


# M28: humidity-collision policy. The bronze/source flux column
# carries the source-side humidity record under the canonical name
# ``rH``; if a stage-1 / silver derivation produces a second column
# also named ``rH`` and it diverges from source, it is renamed to
# ``rH_norm_s`` rather than being dropped or fused. The rH-equivalent
# duplicate is suppressed in favor of the source. Any duplicate column
# name that is not humidity has no deterministic policy and is
# surfaced as ``DuplicateStageColumnsError`` so the operator can fix
# the upstream pipeline before any stage output is written.
HUMIDITY_SOURCE_COLUMN = "rH"
HUMIDITY_DERIVED_RENAME = "rH_norm_s"
# Key used to attach the M28 collision-action list to ``df.attrs`` so
# downstream callers (the CLI, in particular) can surface the actions
# in run-metadata JSON without re-derivation.
COLUMN_COLLISION_ATTRS_KEY = "miaproc_column_collision_actions"


class DuplicateStageColumnsError(ValueError):
    """Raised when a stage payload has unresolvable duplicate column names.

    Pandas allows duplicate column names; Parquet (and the former
    BigQuery tables) need unique field names. ``ensure_unique_stage_columns``
    handles the known ``rH`` source-vs-derived case deterministically; any
    other duplicate is a real upstream defect and raises this error so the
    package fails loudly before any stage output is written.
    """

    def __init__(
        self,
        message: str,
        *,
        duplicate_columns: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.duplicate_columns = tuple(duplicate_columns)


# ----------------------------------------------------------------------
# M28: stage-payload column-uniqueness guard + silver payload builder
# ----------------------------------------------------------------------


def validate_source_columns_unique(
    df: Optional[pd.DataFrame], *, side: str
) -> None:
    """Raise :class:`DuplicateStageColumnsError` if ``df`` has duplicate names.

    Used to guard the M28 contract that the bronze/source flux frame
    handed to silver payload preparation has unique column names. If a
    source itself carries duplicates we cannot guess which one is
    authoritative; the caller must fix the upstream pipeline.

    A ``None`` or empty frame is a no-op so the helper is safe for
    paths that may not always pass a source frame.
    """
    if df is None:
        return
    columns_attr = getattr(df, "columns", None)
    if columns_attr is None:
        return
    cols = list(columns_attr)
    if not cols:
        return
    seen: dict[str, int] = {}
    for c in cols:
        seen[c] = seen.get(c, 0) + 1
    dups = sorted(c for c, n in seen.items() if n > 1)
    if dups:
        raise DuplicateStageColumnsError(
            f"{side} input has duplicate column names: {dups}. "
            "Stage payload preparation cannot guess which one is "
            "authoritative; fix the upstream pipeline before staging.",
            duplicate_columns=tuple(dups),
        )


def _series_equivalent_nan_aware(a: pd.Series, b: pd.Series) -> bool:
    """Return True iff ``a`` and ``b`` carry the same values, NaN ≡ NaN.

    Conservative: returns False on any shape mismatch, any null-mask
    mismatch, or any non-null value mismatch. Used by
    :func:`ensure_unique_stage_columns` to decide whether two same-named
    columns can be merged (one suppressed) or must be preserved as
    ``rH`` + ``rH_norm_s``.
    """
    if a is None or b is None:
        return False
    if len(a) != len(b):
        return False
    try:
        a_na = a.isna().to_numpy()
        b_na = b.isna().to_numpy()
    except Exception:
        return False
    if not bool((a_na == b_na).all()):
        return False
    mask = ~a_na
    if not bool(mask.any()):
        return True
    try:
        return bool((a.to_numpy()[mask] == b.to_numpy()[mask]).all())
    except Exception:
        return False


def column_field_key(name: Any) -> str:
    """Return the case-insensitive logical field key for a column name.

    BigQuery treats schema field names case-insensitively, so
    ``RH`` and ``rH`` collide as the same logical field even though
    pandas considers them distinct (M31). The package-level
    duplicate-column policy operates on this key, not the literal
    pandas column name, so the staged payload stays compatible with
    case-insensitive consumers (formerly ``bigquery_field_key``).
    """
    return str(name).casefold()


def ensure_unique_stage_columns(
    df: pd.DataFrame,
    *,
    key: Callable[[Any], str] = column_field_key,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Return ``(df_unique, collision_actions)`` deterministically.

    Pandas allows duplicate column names; BigQuery does not, and
    BigQuery's schema field uniqueness is **case-insensitive** (M31).
    So columns named ``RH`` and ``rH`` are unique to pandas but
    collide on BigQuery key ``rh``. The M28 + M31 contract is:

    - For every collision on BigQuery key ``rh`` (the known humidity
      family), the first occurrence is canonicalized to ``rH`` and
      becomes the source-side kept column. Subsequent occurrences are
      compared NaN-aware against the kept series:

      * If equivalent, the duplicate is dropped and a
        ``suppressed_equivalent_duplicate`` action is recorded.
      * Otherwise the duplicate is renamed to ``rH_norm_s`` (with
        deterministic numeric suffixes ``rH_norm_s_2`` etc. if
        ``rH_norm_s`` is already taken) and a
        ``renamed_divergent_duplicate`` action is recorded.

      If the first humidity variant arrives under a non-canonical
      case (``RH``, ``Rh``, ...) it is renamed to ``rH`` and a
      ``renamed_to_canonical_humidity`` action is recorded so the
      operator can audit the canonicalization.

    - Every other case-insensitive duplicate raises
      :class:`DuplicateStageColumnsError` naming the affected logical
      field keys and source column names.

    The returned frame is a new object; the input is not mutated. The
    actions list records every collision the helper resolved so
    downstream run-metadata JSON can surface non-silent handling.

    The ``COLUMN_COLLISION_ATTRS_KEY`` attribute on the returned frame
    carries the same actions list for callers that prefer
    ``df.attrs``-based introspection.

    ``key`` defaults to the case-insensitive :func:`column_field_key`
    (the inherited M31 contract). The local Parquet payloads pass ``key=str``: Parquet
    field names are case-sensitive, so only literal duplicates collide
    (the same humidity policy on ``rH``; any other literal duplicate
    raises).
    """
    if df is None:
        return df, []
    cols = list(df.columns)
    keys = [key(c) for c in cols]
    if len(set(keys)) == len(keys):
        out = df.copy()
        out.attrs[COLUMN_COLLISION_ATTRS_KEY] = []
        return out, []

    # Bucket positional indices by logical key (``key``; case-insensitive by default).
    key_to_indices: dict[str, list[int]] = {}
    for i, k in enumerate(keys):
        key_to_indices.setdefault(k, []).append(i)

    humidity_key = key(HUMIDITY_SOURCE_COLUMN)

    # Reject any case-insensitive duplicate we have no policy for
    # (anything other than the humidity family).
    other_dup_keys = sorted(
        k
        for k, idxs in key_to_indices.items()
        if len(idxs) > 1 and k != humidity_key
    )
    if other_dup_keys:
        affected_names = sorted(
            {
                str(cols[i])
                for k in other_dup_keys
                for i in key_to_indices[k]
            }
        )
        raise DuplicateStageColumnsError(
            "Stage payload has duplicate column names with no "
            "deterministic resolution policy. "
            f"Affected logical keys: {other_dup_keys}. "
            f"Affected column names: {affected_names}. The package "
            "refuses to write a duplicate-column payload; the upstream "
            "pipeline must be fixed before the stage output is written.",
            duplicate_columns=tuple(affected_names),
        )

    actions: list[dict[str, Any]] = []
    rh_idxs = key_to_indices.get(humidity_key, [])
    keep_mask = [True] * len(cols)
    rename_at: dict[int, str] = {}

    if len(rh_idxs) >= 2:
        first_idx = rh_idxs[0]
        first_series = df.iloc[:, first_idx]
        first_name = str(cols[first_idx])
        # Canonicalize the kept humidity column to ``rH`` (the
        # source-side canonical name from
        # ``constants.BIOMET_OUT_RENAME``) so the payload schema
        # field is deterministic regardless of which case variant
        # arrived first from upstream.
        if first_name != HUMIDITY_SOURCE_COLUMN:
            rename_at[first_idx] = HUMIDITY_SOURCE_COLUMN
            actions.append(
                {
                    "column": first_name,
                    "action": "renamed_to_canonical_humidity",
                    "renamed_to": HUMIDITY_SOURCE_COLUMN,
                    "reason": (
                        "field keys are compared case-insensitively; "
                        f"renamed humidity column {first_name!r} to "
                        f"{HUMIDITY_SOURCE_COLUMN!r} to match the "
                        "canonical source name."
                    ),
                }
            )
        # Reserved keys: case-insensitive view of every column name
        # already in the frame or already chosen as a rename target,
        # so a new derived name cannot collide with anything.
        reserved_keys: set[str] = {
            key(c) for c in cols
        }
        for new_name in rename_at.values():
            reserved_keys.add(key(new_name))
        for dup_idx in rh_idxs[1:]:
            dup_series = df.iloc[:, dup_idx]
            dup_name = str(cols[dup_idx])
            if _series_equivalent_nan_aware(first_series, dup_series):
                keep_mask[dup_idx] = False
                actions.append(
                    {
                        "column": HUMIDITY_SOURCE_COLUMN,
                        "action": "suppressed_equivalent_duplicate",
                        "renamed_to": None,
                        "source_column_name": dup_name,
                        "reason": (
                            f"derived duplicate {dup_name!r} equivalent "
                            "to source; kept the first occurrence as "
                            f"{HUMIDITY_SOURCE_COLUMN!r}."
                        ),
                    }
                )
            else:
                new_name = HUMIDITY_DERIVED_RENAME
                n = 2
                while key(new_name) in reserved_keys:
                    new_name = f"{HUMIDITY_DERIVED_RENAME}_{n}"
                    n += 1
                reserved_keys.add(key(new_name))
                rename_at[dup_idx] = new_name
                actions.append(
                    {
                        "column": HUMIDITY_SOURCE_COLUMN,
                        "action": "renamed_divergent_duplicate",
                        "renamed_to": new_name,
                        "source_column_name": dup_name,
                        "reason": (
                            f"derived duplicate {dup_name!r} diverges "
                            "from source; preserved both as "
                            f"{HUMIDITY_SOURCE_COLUMN!r} and "
                            f"{new_name!r}."
                        ),
                    }
                )

    # Materialize the deduplicated frame. Use iloc-based positional
    # selection so the duplicate-named columns can be addressed
    # individually without ambiguity.
    keep_positions = [i for i, keep in enumerate(keep_mask) if keep]
    new_cols = [rename_at.get(i, cols[i]) for i in keep_positions]
    new_data = df.iloc[:, keep_positions].copy()
    new_data.columns = new_cols
    new_data.attrs[COLUMN_COLLISION_ATTRS_KEY] = list(actions)
    # Final paranoia check: case-insensitive uniqueness. If something
    # slipped past the deterministic policy, surface it loudly so a
    # duplicate-key payload is never written.
    final_keys = [key(c) for c in new_cols]
    if len(set(final_keys)) != len(final_keys):
        seen: dict[str, int] = {}
        for k in final_keys:
            seen[k] = seen.get(k, 0) + 1
        post = sorted(k for k, v in seen.items() if v > 1)
        raise DuplicateStageColumnsError(
            "ensure_unique_stage_columns failed to resolve duplicates "
            f"(logical keys): {post}. This is a package bug; "
            "please report it.",
            duplicate_columns=tuple(post),
        )
    return new_data, actions


def _rename_unique_columns(
    df: pd.DataFrame, rename_map: dict[str, str]
) -> pd.DataFrame:
    """Return ``df`` with every column whose name is a unique key in
    ``rename_map`` renamed to its mapped value.

    A column is renamed only when its internal name appears exactly
    once in the frame. If upstream produced duplicates of an internal
    name (e.g. two literal ``rH`` columns from a malformed silver
    construction), those duplicates are left untouched so the
    existing :func:`ensure_unique_stage_columns` humidity policy can
    resolve them deterministically (``rH`` + ``rH_norm_s``) rather
    than collapsing into a literal-duplicate source-truth name and
    losing the second series silently.

    The caller's frame is not mutated.
    """
    cols = list(df.columns)
    if not cols:
        return df
    counts: dict[str, int] = {}
    for c in cols:
        counts[c] = counts.get(c, 0) + 1
    new_cols = list(cols)
    changed = False
    for i, c in enumerate(cols):
        if c in rename_map and counts[c] == 1:
            new_cols[i] = rename_map[c]
            changed = True
    if not changed:
        return df
    out = df.copy()
    out.columns = new_cols
    return out


def apply_silver_source_truth_rename(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Rename internal eddy column names to their M32 / M32A source-
    truth final names.

    Maps the backend/canonical names used by stage 1 (``DateTime``,
    ``NEE``, ``QC_NEE``, ``Tair``, ``USTAR``, ``VPD``, ``Rg``,
    ``P_RAIN``, ``rH``) to the inherited final names exposed in
    silver / gold payloads (``timestamp``, ``co2_flux``,
    ``qc_co2_flux``, ``air_temperature_c``, ``u_star``, ``VPD_hpa``,
    ``SWIN_1_1_1``, ``P_RAIN_1_1_1``, ``RH_1_1_1``). Columns not in
    :data:`~miaproc.eddy.constants.SILVER_INTERNAL_TO_FINAL_RENAME`
    are returned unchanged, so passing an already-source-truth frame
    is a no-op. The caller's frame is not mutated.

    When an internal name appears more than once in ``df`` (an
    upstream defect — stage 1 emits at most one of each internal
    column), the rename is skipped for that name so the existing
    :func:`ensure_unique_stage_columns` humidity policy can still
    resolve the M28 ``rH`` / ``rH_norm_s`` fallback.

    M32A: if both internal ``DateTime`` and source-truth
    ``timestamp`` are present, the internal ``DateTime`` is dropped
    so the rename does not synthesize a duplicate ``timestamp``
    field. The source-truth ``timestamp`` value wins at the silver
    output boundary by contract; internal ``DateTime`` is recomputed
    on the gold side via :func:`silver_to_internal_calc_frame`.
    """
    from .constants import SILVER_INTERNAL_TO_FINAL_RENAME

    out = df
    # M32A: drop the internal ``DateTime`` when the source-truth
    # ``timestamp`` is already attached to the frame so the rename
    # cannot create a literal-duplicate ``timestamp`` column.
    if (
        out is not None
        and "DateTime" in out.columns
        and "timestamp" in out.columns
    ):
        out = out.drop(columns=["DateTime"])
    return _rename_unique_columns(out, SILVER_INTERNAL_TO_FINAL_RENAME)


def silver_to_internal_calc_frame(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Map a source-truth silver frame back to the internal eddy
    backend names so :func:`postproc` / :func:`prepare_reddyproc_input`
    can run unchanged.

    Used by the gold CLI after reading a source-truth silver table
    (``timestamp``, ``co2_flux``, ``qc_co2_flux``,
    ``air_temperature_c``, ...) to rebuild the calculation frame
    (``DateTime``, ``NEE``, ``QC_NEE``, ``Tair``, ...) the
    hesseflux / REddyProc backends expect. Columns absent from
    :data:`~miaproc.eddy.constants.FINAL_TO_INTERNAL_RENAME` are
    returned unchanged. The caller's frame is not mutated.

    Like :func:`apply_silver_source_truth_rename`, only unique
    source-truth column names are renamed so a hypothetical upstream
    duplicate is preserved rather than fused.

    M32A: if both source-truth ``timestamp`` and a leaked internal
    ``DateTime`` are present in the same frame (upstream defect or a
    legacy non-source-truth silver table), the leaked ``DateTime``
    is dropped so the source-truth ``timestamp`` value wins on the
    rename. The BigQuery silver table the gold CLI reads back under
    the M32A contract will only carry ``timestamp``, so the
    source-truth value is the authoritative one to feed into the
    backend.
    """
    from .constants import FINAL_TO_INTERNAL_RENAME

    out = df
    # M32A: prefer the source-truth ``timestamp`` over any leaked
    # internal ``DateTime`` so the reconstructed calc frame carries
    # the source-truth time series.
    if (
        out is not None
        and "timestamp" in out.columns
        and "DateTime" in out.columns
    ):
        out = out.drop(columns=["DateTime"])
    return _rename_unique_columns(out, FINAL_TO_INTERNAL_RENAME)


def prepare_silver_stage_payload(
    silver_df: pd.DataFrame,
    *,
    site_id: str,
    source_flux_df: Optional[pd.DataFrame] = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Build a BigQuery-ready silver stage payload (M28).

    Contract:

    - Validate that ``source_flux_df`` (when provided) has unique
      column names; fail loudly via
      :class:`DuplicateStageColumnsError` if not.
    - Preserve every column the silver frame carries. ``silver_df`` is
      the output of ``stage1_from_raw_frames(...)`` and already
      contains the joined bronze/source flux columns plus the
      biomet-derived stage-1 additions, in source-first order.
    - Materialize the operational identity triple
      ``(primary_key, site_id, timestamp)`` from
      ``silver_df['DateTime']`` so the writeback validation SQL
      (``site_id``/``timestamp`` non-null + uniqueness on
      ``primary_key`` and ``(site_id, timestamp)``) can succeed
      without renaming source columns.
    - Run :func:`ensure_unique_stage_columns` so the payload sent to
      ``load_table_from_dataframe`` has unique column names. Resolves
      the M28 ``rH`` / ``rH_norm_s`` collision deterministically.

    Returns ``(payload, collision_actions)``. ``payload.attrs`` also
    carries the actions under :data:`COLUMN_COLLISION_ATTRS_KEY` so
    callers can recover them without holding the tuple.

    The caller's frames are not mutated. ``source_flux_df`` is only
    consulted for the uniqueness guard — silver column order/values
    are taken from ``silver_df`` because stage-1 has already done the
    bronze-preserving join.
    """
    # M32A: accept either the internal ``DateTime`` column (stage 1
    # output, before the source-truth rename) or the source-truth
    # ``timestamp`` column (silver frame that has already passed
    # through :func:`apply_silver_source_truth_rename`, or a silver
    # table read back from BigQuery under the M32A contract). Prefer
    # ``timestamp`` if both are present so the source-truth value
    # wins at the silver output boundary.
    if "timestamp" in silver_df.columns:
        ts_source_col = "timestamp"
    elif "DateTime" in silver_df.columns:
        ts_source_col = "DateTime"
    else:
        raise ValueError(
            "prepare_silver_stage_payload: silver frame missing both "
            "'DateTime' and 'timestamp' columns required for stage-"
            "identity derivation."
        )
    validate_source_columns_unique(source_flux_df, side="bronze flux source")

    out = silver_df.copy()
    dt = pd.to_datetime(out[ts_source_col], utc=True, errors="coerce")
    iso = dt.dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    # M32A: drop the legacy internal ``DateTime`` column at the silver
    # output boundary. The source-truth payload exposes only
    # ``timestamp`` per the lineage CSV; the canonical ``timestamp``
    # is reinjected from ``dt`` below.
    if "DateTime" in out.columns:
        out = out.drop(columns=["DateTime"])

    # Identity-triple assignment via DataFrame insertion would create
    # a duplicate when the silver frame already carries one of those
    # names; we resolve it explicitly to avoid silently overwriting a
    # source-side column with a synthesized value.
    #
    # M32A: when the silver frame provided ``timestamp`` as the
    # time source (``ts_source_col == "timestamp"``), the synthesized
    # ``timestamp`` is derived from that exact column. Re-injecting
    # it is not a substitution and should not surface as an
    # ``identity_overwrite`` action — the audit signal is reserved
    # for the case where the caller supplied a *stale* identity
    # column we replaced.
    identity_cols = ("primary_key", "site_id", "timestamp")
    pre_existing = [c for c in identity_cols if c in out.columns]
    if ts_source_col == "timestamp" and "timestamp" in pre_existing:
        pre_existing = [c for c in pre_existing if c != "timestamp"]
    if "timestamp" in out.columns:
        out = out.drop(columns=["timestamp"])
    if pre_existing:
        # Drop pre-existing identity columns so the freshly synthesized
        # values are the only ones present. Record this in actions so
        # the CLI run-metadata makes the substitution visible.
        out = out.drop(columns=pre_existing)

    # M32: rename internal eddy aliases (NEE, USTAR, Tair, VPD, Rg,
    # P_RAIN, rH, QC_NEE) to source-truth final names (co2_flux,
    # u_star, air_temperature_c, VPD_hpa, SWIN_1_1_1, P_RAIN_1_1_1,
    # RH_1_1_1, qc_co2_flux). Applied here, before the unique-column
    # guard, so a preserved flux-side ``RH`` and the renamed biomet
    # ``rH`` -> ``RH_1_1_1`` no longer collide on the case-insensitive
    # BigQuery field key. A no-op when the silver frame already
    # carries source-truth names. Under M32A the rename map also
    # includes ``DateTime -> timestamp``, but ``DateTime`` is dropped
    # above so the rename never synthesizes a literal-duplicate
    # ``timestamp``.
    out = apply_silver_source_truth_rename(out)

    out["timestamp"] = dt
    out["site_id"] = site_id
    out["primary_key"] = site_id + "|" + iso

    # Order: identity triple first, then every preserved silver column
    # in its original order. This puts the BigQuery merge keys at the
    # front of the staged table which matches the M10 convention.
    ordered = list(identity_cols) + [
        c for c in out.columns if c not in identity_cols
    ]
    out = out[ordered]

    payload, actions = ensure_unique_stage_columns(out)

    if pre_existing:
        for ident in pre_existing:
            actions.append(
                {
                    "column": ident,
                    "action": "identity_overwrite",
                    "renamed_to": None,
                    "reason": (
                        "silver frame carried a pre-existing "
                        f"{ident!r} column; replaced with the "
                        "writeback-synthesized identity value"
                    ),
                }
            )
        payload.attrs[COLUMN_COLLISION_ATTRS_KEY] = list(actions)

    return payload, actions


# ----------------------------------------------------------------------
# Gold stage-payload subset (M10 / M28 / M32), transport-neutral
# ----------------------------------------------------------------------


# Lineage CSV ``gold_derived`` rows: backend gap-filled outputs are
# carried to gold under these lowercase final names (originally the
# live ``_s2_filt_1`` analytical columns, guide 001 §2.1). The sixth
# ``gold_derived`` name, ``dateAndTime``, is built directly from
# ``DateTime`` by :func:`derive_gold_stage_columns` and is not part of
# this rename map. Formerly ``S2_FILT_1_RENAME_MAP`` of the removed
# BigQuery writeback module.
GOLD_DERIVED_RENAME: dict[str, str] = {
    "NEE_f": "nee_f",
    "NEE_fqc": "nee_fqc",
    "Rg_f": "sw_in_f",
    "Tair_f": "ta_f",
    "VPD_f": "vpd_f",
}


def derive_gold_stage_columns(
    processed: pd.DataFrame,
    *,
    site_id: str,
) -> pd.DataFrame:
    """Return a copy of ``processed`` with the gold identity and
    ``gold_derived`` columns added.

    - ``timestamp`` is ``DateTime`` parsed as UTC (``errors="coerce"``);
      any existing ``timestamp`` column is overwritten.
    - ``site_id`` is the caller's value on every row, including
      regularized gap rows.
    - ``primary_key`` is synthesized as
      ``"<site_id>|<YYYY-MM-DDTHH:MM:SS+0000>"`` from that timestamp;
      any existing ``primary_key`` is overwritten.
    - Backend outputs in :data:`GOLD_DERIVED_RENAME` are renamed in
      place (an existing target column is dropped first) and
      ``dateAndTime`` is added as ``"YYYY-MM-DD HH:MM:SS"`` (UTC).

    This is the first half of the gold payload preparation;
    :func:`finalize_gold_stage_payload` is the second (the removed
    BigQuery adapter ran its source-flux join, target filter and dtype
    cast between the two). The caller's frame is not mutated.
    """
    if "DateTime" not in processed.columns:
        raise ValueError(
            "gold stage payload: processed output is missing the "
            "'DateTime' column required for stage-identity derivation."
        )

    out = processed.copy()
    dt = pd.to_datetime(out["DateTime"], utc=True, errors="coerce")
    out["timestamp"] = dt
    out["site_id"] = site_id
    iso_utc = dt.dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Synthesized fallback; the BigQuery adapter's source-flux join may
    # replace it with the source primary_key on matched rows.
    out["primary_key"] = site_id + "|" + iso_utc

    # Lowercase analytical mapping (guide 001 §2.1).
    #
    # M31: rename source -> target rather than duplicating. BigQuery
    # field keys are case-insensitive, so keeping both ``NEE_f`` and
    # ``nee_f`` would fail at ``load_table_from_dataframe`` with
    # ``Field nee_f already exists in schema``. The lowercase
    # analytical names are the user-facing canonical names in the
    # ``_s2_filt_1`` schema; the uppercase backend-output names are
    # an internal scientific convention that does not need to survive
    # into the staged BigQuery payload.
    for src_col, dst_col in GOLD_DERIVED_RENAME.items():
        if src_col in out.columns:
            if dst_col in out.columns and dst_col != src_col:
                out = out.drop(columns=[dst_col])
            out = out.rename(columns={src_col: dst_col})

    # dateAndTime STRING column.
    out["dateAndTime"] = dt.dt.strftime("%Y-%m-%d %H:%M:%S")
    return out


def finalize_gold_stage_payload(df: pd.DataFrame) -> pd.DataFrame:
    """Drop redundant internal passthroughs, order identity first and
    enforce column uniqueness.

    ``df`` must carry ``primary_key``, ``site_id`` and ``timestamp``
    (see :func:`derive_gold_stage_columns`). Returns a new frame with
    the identity triple first and every other column in sorted order;
    the resolved collision actions are attached to
    ``attrs[COLUMN_COLLISION_ATTRS_KEY]``. The caller's frame is not
    mutated.
    """
    identity = ("primary_key", "site_id", "timestamp")
    out = df

    # M32: drop redundant internal-name passthroughs in the gold
    # stage payload whenever the corresponding source-truth final
    # column is also present. The hesseflux/REddyProc backends emit
    # raw NEE/Tair/USTAR/VPD/Rg passthroughs alongside the genuinely
    # new gap-filled outputs (NEE_f, Tair_f, ...); under the M32
    # contract those passthroughs duplicate the source-truth silver
    # columns (co2_flux, air_temperature_c, u_star, VPD_hpa,
    # SWIN_1_1_1) that ``_attach_silver_columns_to_gold`` carries
    # forward. We keep the new processing outputs and drop only the
    # redundant passthroughs to avoid ambiguous backend-vs-source-truth
    # semantics in the staged payload. Because ``timestamp`` is always
    # present here, this is also the step that removes ``DateTime``.
    from .constants import SILVER_INTERNAL_TO_FINAL_RENAME

    cols_to_drop = [
        internal
        for internal, final in SILVER_INTERNAL_TO_FINAL_RENAME.items()
        if internal in out.columns and final in out.columns
    ]
    if cols_to_drop:
        out = out.drop(columns=cols_to_drop)

    # Order: identity first, then everything else in stable alphabetical order.
    rest = sorted(c for c in out.columns if c not in identity)
    out = out[list(identity) + rest]

    # M28: enforce unique column names before any caller can hand the
    # frame to a writer. Resolves the known ``rH`` source-vs-derived
    # collision deterministically and raises
    # ``DuplicateStageColumnsError`` for anything else.
    out, _actions = ensure_unique_stage_columns(out)
    return out


def prepare_gold_stage_payload(
    processed: pd.DataFrame,
    *,
    site_id: str,
) -> pd.DataFrame:
    """Build the transport-neutral gold stage payload.

    Equivalent to the removed BigQuery ``prepare_stage_dataframe(processed,
    site_id=site_id)`` with no source-flux frame, target columns or
    target types: :func:`derive_gold_stage_columns` followed by
    :func:`finalize_gold_stage_payload`. The result carries exactly one
    ``timestamp`` (derived from ``DateTime``, which is dropped), the
    identity triple first, the ``gold_derived`` names and every other
    column sorted. Identity is synthesized, not taken from source
    ``primary_key``/``site_id`` columns. The caller's frame is not
    mutated.
    """
    return finalize_gold_stage_payload(
        derive_gold_stage_columns(processed, site_id=site_id)
    )


# ----------------------------------------------------------------------
# M002-S02: local source-facing Parquet silver/gold payloads
# ----------------------------------------------------------------------

# Source identity columns carried unchanged (never synthesized or
# overwritten) by the local payloads; ``timestamp`` follows them.
LOCAL_IDENTITY_COLUMNS = ("primary_key", "site_id")


def _as_timestamp(series: pd.Series) -> pd.Series:
    """Return ``series`` as tz-aware datetimes (strings parsed as UTC)."""
    if isinstance(series.dtype, pd.DatetimeTZDtype):
        return series
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.tz_localize("UTC")
    return pd.to_datetime(series, utc=True, errors="coerce", format="mixed")


def _identity_first(df: pd.DataFrame, rest: list[int]) -> pd.DataFrame:
    """Positional reorder: present identity columns, ``timestamp``, then ``rest``.

    Positional (``iloc``) selection never multiplies a duplicated label
    (M002-S01-F3/F5); callers resolve duplicates before ordering.
    """
    cols = list(df.columns)
    head = [cols.index(c) for c in (*LOCAL_IDENTITY_COLUMNS, "timestamp") if c in cols]
    return df.iloc[:, head + [i for i in rest if i not in head]]


def _require_unique(df: pd.DataFrame, *, stage: str) -> None:
    """Fail when a rename produced a literal duplicate (for example a source
    ``air_temperature_c`` next to the stage-1 ``Tair`` renamed to it)."""
    names = [str(c) for c in df.columns]
    dups = sorted({c for c in names if names.count(c) > 1})
    if dups:
        raise DuplicateStageColumnsError(
            f"local {stage} payload: renaming to the source-facing names produced "
            f"duplicate columns {dups}; the input already carries a column with a "
            "source-facing name. Remove or rename it upstream.",
            duplicate_columns=tuple(dups),
        )


def _drop_unique(df: pd.DataFrame, names: list[str]) -> pd.DataFrame:
    keep = [i for i, c in enumerate(df.columns) if c not in set(names)]
    return df.iloc[:, keep]


def prepare_local_silver_payload(
    stage1: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Build the source-facing silver payload written to local Parquet.

    - Literal duplicate column names are resolved first with the M28
      humidity policy (``key=str``): duplicate ``rH`` -> kept ``rH`` plus
      ``rH_norm_s`` (or suppressed when equivalent); any other literal
      duplicate raises :class:`DuplicateStageColumnsError`.
    - ``timestamp`` is the stage-1 observation instant ``DateTime``
      (built from the source ``timestamp`` or ``date`` + ``time`` with the
      caller's tz settings, regular-grid value on inserted gap rows); the
      raw source ``timestamp`` and ``DateTime`` are replaced by it, so the
      payload has exactly one ``timestamp`` and no ``DateTime``. Without
      ``DateTime`` an existing ``timestamp`` is used.
    - Internal names are renamed to the M32 source-facing names
      (``air_temperature_c``, ``VPD_hpa``, ``u_star``, ``RH_1_1_1``, ...);
      every other column is preserved unchanged and in order.
    - Source ``primary_key`` / ``site_id`` are carried unchanged (dtype
      and values); nothing is synthesized. Gap rows keep the uniform
      group ``site_id`` stage 1 propagated and a null ``primary_key``.
    - Column order: present identity columns, ``timestamp``, the rest.

    Returns ``(payload, collision_actions)``; the actions are also on
    ``payload.attrs[COLUMN_COLLISION_ATTRS_KEY]``. The input is not
    mutated.
    """
    out, actions = ensure_unique_stage_columns(stage1, key=str)
    if "DateTime" in out.columns:
        ts = _as_timestamp(out["DateTime"])
    elif "timestamp" in out.columns:
        ts = _as_timestamp(out["timestamp"])
    else:
        raise ValueError(
            "local silver payload: stage-1 frame has neither 'DateTime' nor 'timestamp'."
        )
    out = _drop_unique(out, ["DateTime", "timestamp"])
    out = apply_silver_source_truth_rename(out)
    _require_unique(out, stage="silver")
    out = out.copy()
    out.insert(0, "timestamp", ts.array)
    payload = _identity_first(out, list(range(out.shape[1]))).copy()
    payload.attrs[COLUMN_COLLISION_ATTRS_KEY] = list(actions)
    return payload, actions


def prepare_local_gold_payload(
    gold: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Build the source-facing gold payload written to local Parquet.

    ``gold`` is the backend output with the silver-only columns attached
    (``_attach_silver_columns_to_gold``). Applies the neutral gold
    contract without the BigQuery identity synthesis:

    - literal duplicates resolved first exactly as for silver;
    - ``timestamp``: the silver ``timestamp`` when present (historical
      precedence), else the backend ``DateTime``; ``DateTime`` is
      dropped (exactly one ``timestamp``);
    - backend gap-filled outputs get the lineage-CSV ``gold_derived``
      names (:data:`GOLD_DERIVED_RENAME`) plus ``dateAndTime`` (UTC
      ``YYYY-MM-DD HH:MM:SS``);
    - the M32 rule drops an internal passthrough (``NEE``, ``Tair``,
      ``USTAR``, ``VPD``, ``Rg``, ``QC_NEE``, ``P_RAIN``, ``rH``) when its
      source-facing name is present; every other column is kept;
    - source ``primary_key`` / ``site_id`` are carried unchanged;
    - order: present identity columns, ``timestamp``, the rest sorted.

    Returns ``(payload, collision_actions)``; the input is not mutated.
    """
    from .constants import SILVER_INTERNAL_TO_FINAL_RENAME

    out, actions = ensure_unique_stage_columns(gold, key=str)
    if "timestamp" in out.columns:
        ts = _as_timestamp(out["timestamp"])
        if "DateTime" in out.columns:
            ts = ts.fillna(_as_timestamp(out["DateTime"]))
    elif "DateTime" in out.columns:
        ts = _as_timestamp(out["DateTime"])
    else:
        raise ValueError(
            "local gold payload: backend output has neither 'timestamp' nor 'DateTime'."
        )
    out = _drop_unique(out, ["DateTime", "timestamp"]).copy()
    for src_col, dst_col in GOLD_DERIVED_RENAME.items():
        if src_col in out.columns:
            if dst_col in out.columns:
                out = _drop_unique(out, [dst_col])
            out = out.rename(columns={src_col: dst_col})
    out["timestamp"] = ts.array
    out["dateAndTime"] = ts.dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S").values
    redundant = [
        internal
        for internal, final in SILVER_INTERNAL_TO_FINAL_RENAME.items()
        if internal in out.columns and final in out.columns
    ]
    out = _drop_unique(out, redundant)
    _require_unique(out, stage="gold")
    cols = list(out.columns)
    rest = sorted(range(len(cols)), key=lambda i: str(cols[i]))
    payload = _identity_first(out, rest).copy()
    payload.attrs[COLUMN_COLLISION_ATTRS_KEY] = list(actions)
    return payload, actions
