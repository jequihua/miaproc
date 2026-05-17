# Eddy BigQuery layer schemas

BigQuery TableSchema JSON files for the four tables the eddy
pipeline reads from / writes to in BigQuery. They document the
load-bearing column contract that the package depends on; pass-through
columns the package preserves but does not interpret are not
enumerated here.

| Layer | File | Producer | Consumer |
|---|---|---|---|
| bronze (flux) | [`eddy_bronze_flux.schema.json`](eddy_bronze_flux.schema.json) | EddyPro (operator-owned) | `miaproc eddy run-bigquery` / `run-bigquery-silver` |
| bronze (biomet) | [`eddy_bronze_biomet.schema.json`](eddy_bronze_biomet.schema.json) | biomet sensor exports (operator-owned) | `miaproc eddy run-bigquery` / `run-bigquery-silver` |
| silver | [`eddy_silver.schema.json`](eddy_silver.schema.json) | `miaproc eddy run-bigquery-silver` | `miaproc eddy run-bigquery-gold` |
| gold | [`eddy_gold.schema.json`](eddy_gold.schema.json) | `miaproc eddy run-bigquery-gold` | downstream lakehouse / analytics |

Format is BigQuery TableSchema JSON — usable with
`bq load --schema=...` and `bq mk --schema=...`. Each entry has
`name`, `type`, `mode`, and `description`. Types are the BigQuery
SQL types (`STRING`, `TIMESTAMP`, `FLOAT64`, `INT64`); modes are
`REQUIRED` or `NULLABLE`.

## What "load-bearing" means here

The schemas list the columns `miaproc.eddy` actually reads or writes
by name, plus the M8 stage-identity contract
(`primary_key`, `site_id`, `timestamp`). EddyPro flux exports carry
many additional columns (full-output shape: ~50 columns including
`h2o_flux`, `H_strg`, `LE_strg`, `wind_speed`, fetch percentiles,
etc.). Those pass through silver and gold under the M14
column-preservation contract; cloud engineers can let BigQuery
auto-detect them at load time, or extend the bronze schema with
the operator-owned column set.

The silver schema lists columns the stage-1 pipeline produces by
name; the silver BigQuery stage table additionally carries the
EddyPro pass-through columns (`filename`, `DOY`, `daytime`,
`h2o_flux`, `qc_h2o_flux`, `RH`, `Tdew`, ..., site-specific) that
were present on the bronze flux read. Those are intentionally not
listed here because they are operator-defined.

The gold schema lists the 13-column backend contract from
[`08_pkg/backend_contract.md`](../../08_pkg/backend_contract.md),
the lowercase mirrors built by `prepare_stage_dataframe` for parity
with the legacy
`manglaria-staging.manglaria_lakehouse_ds.carbon_flux_eddycovariance_s2_filt_1`
target (guide 001 section 2.1), the M22 stage identity columns,
and the silver-only columns the M14
`_attach_silver_columns_to_gold` helper reattaches onto the backend
output.

## Column rename chain (bronze -> silver)

| Bronze flux name | Silver/gold name |
|---|---|
| `co2_flux` | `NEE` |
| `qc_co2_flux` | `QC_NEE` |
| `air_temperature` | `Tair` (also unit-converted K -> degrees C) |
| `u_star` (or legacy `u.`) | `USTAR` |

| Bronze biomet name | Silver/gold name |
|---|---|
| `SWIN_1_1_1` | `Rg` |
| `P_RAIN_1_1_1` | `P_RAIN` |
| `RH_1_1_1` | `rH` |

Source: [`08_pkg/src/miaproc/eddy/constants.py`](../../08_pkg/src/miaproc/eddy/constants.py)
(`FULL_OUTPUT_RENAME_MAP`, `BIOMET_OUT_RENAME`).

## Column rename chain (gold -> lakehouse `_s2_filt_1` parity)

For deployments that want the gold output to drop directly into the
legacy `_s2_filt_1` table shape, `prepare_stage_dataframe` builds
lowercase mirrors:

| Gold name | Lowercase mirror |
|---|---|
| `NEE_f` | `nee_f` |
| `NEE_fqc` | `nee_fqc` |
| `Rg_f` | `sw_in_f` |
| `Tair_f` | `ta_f` |
| `VPD_f` | `vpd_f` |
| `DateTime` | `dateAndTime` (formatted `YYYY-MM-DD HH:MM:SS` string) |

Source: `S2_FILT_1_RENAME_MAP` and the dateAndTime construction in
[`08_pkg/src/miaproc/eddy/bigquery_writeback.py`](../../08_pkg/src/miaproc/eddy/bigquery_writeback.py).

## Stage identity contract

For any silver or gold writeback, `miaproc.cli` materializes three
identity columns before staging:

- `primary_key` — `STRING REQUIRED`, deterministic
  `<site_id>|<iso_utc_timestamp>` synthesized from `site_id` +
  `DateTime`. For gold-from-BQ-silver under M22, source-flux
  pass-through is intentionally not used (silver-from-BQ is
  processed stage-1 output, not raw bronze flux), so the synthesized
  form is canonical.
- `site_id` — `STRING REQUIRED`, **per-row** category value. Under
  the M24 all-data grouped CLI, the eddy CLI no longer accepts a
  single-site flag; instead `--group-column site_id` partitions the
  all-data BigQuery read into per-category runs. Each per-category
  run uses that group's category as its stage-identity `site_id`,
  and `prepare_stage_dataframe` is called once per group; the
  per-group stage frames are then concatenated into one stacked
  payload before writeback. Legacy single-site programmatic callers
  may still pass an explicit `site_id=<value>` to the package-level
  helpers, but the CLI no longer drives that path.
- `timestamp` — `TIMESTAMP REQUIRED`, equal to `DateTime` cast to
  tz-aware UTC.

`validate_stage_table` aborts the writeback if `row_count<=0`, any
of those three columns is null, or `(site_id, timestamp)` /
`primary_key` carry duplicates. The MERGE keys on
`(site_id, timestamp)`; non-key columns including `primary_key`
are updated on match. The watermark contract advances per-site
after a successful explicit MERGE — one `cf_s2_watermark` row per
distinct `site_id` present in the staged frame, each carrying that
site's max timestamp; stage-only and failed runs do not advance
any watermark.

## Status

These schema files are **handoff documentation**. They have not
been used to mint live BigQuery tables in this repo. Operators are
free to:

- create real BigQuery tables with `bq mk --schema=eddy_silver.schema.json silver_stage`,
- extend each schema with operator-owned EddyPro pass-through columns,
- add `clustering` / `partitioning` directives at table-creation time
  (the writeback itself uses `WRITE_TRUNCATE` for stage and a MERGE
  on `(site_id, timestamp)` for final, both of which work cleanly
  with day-partitioning on `timestamp`).

No live BigQuery validation against these schemas has been run
through M25; the schemas describe the post-M24A grouped stage /
final output shape and the bronze schemas remain operator-owned
source contracts.
