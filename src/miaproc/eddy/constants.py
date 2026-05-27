from __future__ import annotations

# Mirrors R: na.strings = c("-9999", "NA", "","NaN")
NA_VALUES = ["-9999", "NA", "", "NaN"]

UNIT_ROW_TIME_MARKER = "[HH:MM]"

# R renames:
# NEE    = co2_flux
# Tair   = air_temperature
# USTAR  = `u.`
# QC_NEE = qc_co2_flux
FULL_OUTPUT_RENAME_MAP = {
    "co2_flux": "NEE",
    "air_temperature": "Tair",
    "u.": "USTAR",
    "qc_co2_flux": "QC_NEE",
}

# Biomet selection keys (R transmute)
BIOMET_SW_IN = "SWIN_1_1_1"
BIOMET_RAIN = "P_RAIN_1_1_1"
BIOMET_RH = "RH_1_1_1"

# Output names (R transmute)
BIOMET_OUT_RENAME = {
    BIOMET_SW_IN: "Rg",
    BIOMET_RAIN: "P_RAIN",
    BIOMET_RH: "rH",
}

# M32: source-truth column contract.
#
# Stage 1 keeps backend/canonical names (DateTime, NEE, QC_NEE, Tair,
# USTAR, VPD, Rg, P_RAIN, rH) internally so
# hesseflux/REddyProc/dynamic-u*/QC/rain/sigma processing and
# ``miaproc.eddy.stage2.prepare_reddyproc_input`` remain unchanged.
# Silver and gold *outputs* carry source-facing final names so the
# BigQuery silver/gold tables look like source-truth products instead
# of renamed processing tables.
#
# ``air_temperature_c`` and ``VPD_hpa`` bake the processed unit into
# the final name; this avoids carrying both raw-unit and processed-unit
# copies of the same physical concept in the staged payload. ``rH``
# becomes ``RH_1_1_1`` (the biomet source name), which is
# case-insensitively distinct from a preserved flux-side ``RH`` so the
# two humidity series can coexist without colliding on the BigQuery
# field key (M32 supersedes the M31 ``rH``/``rH_norm_s`` band-aid for
# the ``RH``/``rH`` case-collision because the final names are no
# longer ambiguous).
#
# M32A adds the ``DateTime -> timestamp`` row that the lineage CSV
# (``06_infra/schemas/eddy_bronze_to_stage_column_lineage_contract.csv``
# lines 3 and 18) requires: the internal processing column is
# ``DateTime`` and the inherited final time column is ``timestamp``.
# The BigQuery writeback identity triple already uses ``timestamp``,
# so the silver / gold output boundary collapses to a single
# ``timestamp`` column and the internal ``DateTime`` is recomputed
# on the gold side via :func:`silver_to_internal_calc_frame` before
# the backend dispatches.
SILVER_INTERNAL_TO_FINAL_RENAME: dict[str, str] = {
    "DateTime": "timestamp",
    "NEE": "co2_flux",
    "QC_NEE": "qc_co2_flux",
    "Tair": "air_temperature_c",
    "USTAR": "u_star",
    "VPD": "VPD_hpa",
    "Rg": BIOMET_SW_IN,
    "P_RAIN": BIOMET_RAIN,
    "rH": BIOMET_RH,
}

# Reverse map used by the gold command to reconstruct the internal
# calculation frame from a source-truth silver table before dispatching
# to the hesseflux / REddyProc backend or
# :func:`miaproc.eddy.stage2.prepare_reddyproc_input`.
FINAL_TO_INTERNAL_RENAME: dict[str, str] = {
    v: k for k, v in SILVER_INTERNAL_TO_FINAL_RENAME.items()
}

# Raw bronze column -> source-truth final name aliases consulted by
# the M29 dry-run preservation check. Only the unit-transformed
# variables and the legacy ``u.`` flux name require an explicit alias
# under the M32 contract: every other bronze name (``co2_flux``,
# ``qc_co2_flux``, ``u_star``, ``RH``, ``SWIN_1_1_1``,
# ``P_RAIN_1_1_1``, ``RH_1_1_1``) survives into silver under its
# bronze name and is detected by exact match. Identity mappings are
# omitted intentionally so ``input_column_payload_aliases`` only
# records resolutions that actually changed a column name.
SILVER_BRONZE_TO_FINAL_ALIASES: dict[str, str] = {
    "air_temperature": "air_temperature_c",
    "VPD": "VPD_hpa",
    "u.": "u_star",
}
