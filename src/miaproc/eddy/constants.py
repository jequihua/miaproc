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
