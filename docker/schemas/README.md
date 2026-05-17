# Eddy BigQuery layer schemas

BigQuery TableSchema JSON files for the four eddy tables the package reads from or
writes to in BigQuery. They document the canonical column contract for the current
package, including the M28/M29 rule that stage payload columns accumulate from
bronze to silver and from silver to gold while remaining BigQuery-unique.

| Layer | File | Producer | Consumer |
|---|---|---|---|
| bronze (flux) | [`eddy_bronze_flux.schema.json`](eddy_bronze_flux.schema.json) | EddyPro flux export (operator-owned) | `miaproc eddy run-bigquery` / `run-bigquery-silver` |
| bronze (biomet) | [`eddy_bronze_biomet.schema.json`](eddy_bronze_biomet.schema.json) | biomet sensor export (operator-owned) | `miaproc eddy run-bigquery` / `run-bigquery-silver` |
| silver | [`eddy_silver.schema.json`](eddy_silver.schema.json) | `miaproc eddy run-bigquery-silver` | `miaproc eddy run-bigquery-gold` |
| gold | [`eddy_gold.schema.json`](eddy_gold.schema.json) | `miaproc eddy run-bigquery-gold` | downstream lakehouse / analytics |

Format is BigQuery TableSchema JSON, usable with `bq load --schema=...` and
`bq mk --schema=...`. Each entry has `name`, `type`, `mode`, and `description`.
Types use BigQuery SQL types (`STRING`, `TIMESTAMP`, `FLOAT64`, `INT64`); modes
are `REQUIRED` or `NULLABLE`.

## Accumulation contract

The package now treats the stage payload as an accumulating table shape:

- bronze flux columns that survive stage-1 normalization are preserved into silver;
- silver columns are preserved into gold even when the final table's discovered schema is narrow;
- newly computed silver/gold columns are appended without dropping earlier columns;
- every stage payload is validated to have BigQuery-unique column names before writeback or dry-run artifact creation.

The schemas enumerate the canonical columns known to the current package and the
checked-in bronze source schemas. Operator-specific EddyPro columns that are not
listed here may still be added deliberately to the bronze/silver/gold schemas if
a deployment needs them, but they should be treated as explicit schema extensions
rather than invisible extras.

## Humidity collision policy

M28 fixed the live BigQuery failure `Field rH already exists in schema` by making
the humidity collision policy deterministic:

- source `rH` is never renamed;
- equivalent duplicate `rH` columns are suppressed;
- divergent duplicate humidity is preserved as `rH_norm_s` at silver level;
- if `rH_norm_s` already exists, later divergent humidity columns receive numeric suffixes such as `rH_norm_s_2`, which require a deliberate schema extension before a real writeback;
- duplicate non-humidity column names raise `DuplicateStageColumnsError` instead of being silently fused.

Because `rH_norm_s` is the canonical derived humidity name, it is listed in both
the silver and gold schemas as `NULLABLE`.

## Column rename chain: bronze to silver

| Bronze flux name | Silver/gold name |
|---|---|
| `co2_flux` | `NEE` |
| `qc_co2_flux` | `QC_NEE` |
| `air_temperature` | `Tair` (also unit-converted K to degrees C) |
| `u_star` (or legacy `u.`) | `USTAR` |

| Bronze biomet name | Silver/gold name |
|---|---|
| `SWIN_1_1_1` | `Rg` |
| `P_RAIN_1_1_1` | `P_RAIN` |
| `RH_1_1_1` | `rH` |

Source: [`src/miaproc/eddy/constants.py`](../../src/miaproc/eddy/constants.py)
(`FULL_OUTPUT_RENAME_MAP`, `BIOMET_OUT_RENAME`).

## Gold lakehouse parity columns

For deployments that want gold output to drop into the legacy `_s2_filt_1` table
shape, `prepare_stage_dataframe` also builds lowercase mirrors:

| Gold name | Lowercase mirror |
|---|---|
| `NEE_f` | `nee_f` |
| `NEE_fqc` | `nee_fqc` |
| `Rg_f` | `sw_in_f` |
| `Tair_f` | `ta_f` |
| `VPD_f` | `vpd_f` |
| `DateTime` | `dateAndTime` (formatted `YYYY-MM-DD HH:MM:SS` string) |

Source: `S2_FILT_1_RENAME_MAP` and the `dateAndTime` construction in
[`src/miaproc/eddy/bigquery_writeback.py`](../../src/miaproc/eddy/bigquery_writeback.py).

## Stage identity contract

For silver or gold writeback, `miaproc.cli` materializes three identity columns
before staging:

- `primary_key`: `STRING REQUIRED`, deterministic `<site_id>|<iso_utc_timestamp>` synthesized from `site_id` and `DateTime`. If a source frame already carried `primary_key`, the M28 silver payload builder audits the overwrite as `identity_overwrite`.
- `site_id`: `STRING REQUIRED`, per-row category value. The grouped CLI uses `--group-column site_id` to partition all-data BigQuery reads into per-category runs, then stacks the per-group stage frames into one payload before writeback.
- `timestamp`: `TIMESTAMP REQUIRED`, equal to `DateTime` cast to tz-aware UTC.

`validate_stage_table` aborts writeback if `row_count <= 0`, any identity column
is null, or `(site_id, timestamp)` / `primary_key` carry duplicates. The MERGE
keys on `(site_id, timestamp)`, and non-key columns, including `primary_key`, are
updated on match. Stage-only, dry-run, and failed runs do not advance watermarks.

## Local dry-run verification

The package exposes `--stage-payload-dry-run-dir` on both
`miaproc eddy run-bigquery-silver` and `miaproc eddy run-bigquery-gold`. A dry
run writes:

- `stage_payload.csv`
- `stage_payload_metadata.json`

The metadata records `columns_unique`, `duplicate_columns`,
`column_collision_actions`, `preserved_input_columns`, `missing_input_columns`,
`appended_payload_columns`, and the four safety flags `bigquery_write_attempted`,
`validation_sql_attempted`, `merge_attempted`, and `watermark_advanced`. This is
the preferred operator check before a real BigQuery writeback.

## Docker location

The Docker runtime used for the eddy CLI is defined at
[`docker/Dockerfile.miaproc-r45-reddyproc`](../../docker/Dockerfile.miaproc-r45-reddyproc),
with usage notes in [`docker/README.md`](../../docker/README.md). Build/publish
decisions are separate from these schema files; the schemas describe table shape
only.

## Status

These schema files are handoff documentation. They have not been used from this
repo to mint live BigQuery tables. They now describe the post-M30 package
contract: M28 unique-column writeback payloads, M29 local dry-run artifacts, and
M30 host/container dry-run validation. The remaining cloud-owned validation step
is to run the dry-run mode against real BigQuery reads and inspect the emitted
payload before any real stage write or MERGE.
