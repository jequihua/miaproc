# Eddy BigQuery Schema Examples

These files are BigQuery TableSchema JSON examples for the eddy bronze,
silver, and gold layers. They are handoff material for cloud engineers, not
proof that live tables have already been minted from these exact files.

M31 status:

- The package runs eddy BigQuery processing in two explicit stages:
  `miaproc eddy run-bigquery-silver` and then
  `miaproc eddy run-bigquery-gold`.
- Stage payload dry-run metadata is the authoritative way to inspect the exact
  columns for a real site/table/date window before mutating BigQuery.
- Silver preserves unique bronze/source information. Some source columns are
  represented under stage-1 aliases, for example `co2_flux -> NEE`,
  `qc_co2_flux -> QC_NEE`, `air_temperature -> Tair`, and
  `u_star -> USTAR`; M31 records these as `input_column_payload_aliases` in the
  dry-run metadata.
- Gold preserves silver columns and appends gold/backend columns.
- BigQuery field names are treated case-insensitively. The humidity policy is:
  source humidity is canonicalized to `rH`; if a divergent derived/normalized
  humidity column is present, it is written as `rH_norm_s`; non-humidity
  duplicate field keys fail loudly.

## Files

| Layer | File | Purpose |
|---|---|---|
| Bronze flux | `eddy_bronze_flux.schema.json` | Example source-table contract for flux variables used by miaproc. |
| Bronze biomet | `eddy_bronze_biomet.schema.json` | Example source-table contract for biomet variables used by miaproc. |
| Silver | `eddy_silver.schema.json` | Example silver stage/final payload schema after source aliasing and humidity deduplication. |
| Gold | `eddy_gold.schema.json` | Example gold payload schema after preserving silver columns and appending backend/lakehouse columns. |

## Recommended Operator Workflow

Before pre-creating or altering BigQuery tables, run the Docker/CLI dry-run
against the actual cloud source tables and inspect:

```text
stage_payload_metadata.json
```

Load-bearing fields should show:

```text
columns_unique: true
duplicate_columns: []
missing_input_columns: []
bigquery_write_attempted: false
validation_sql_attempted: false
merge_attempted: false
watermark_advanced: false
```

Use `columns` and `dtypes` in that metadata as the final source of truth for a
specific deployment. These JSON schema files are useful templates, but real
EddyPro exports can carry site-specific pass-through columns beyond this core
set.

## Identity Contract

Silver and gold writeback payloads include:

- `primary_key` - deterministic `<site_id>|<iso_utc_timestamp>` string.
- `site_id` - per-row site/category value, usually grouped with
  `--group-column site_id`.
- `timestamp` - UTC observation timestamp used with `site_id` as the MERGE key.

Stage validation rejects null identity values and duplicate identity keys.
Watermarks advance only after an explicit successful gold MERGE.

## Cloud Safety

The package default rejects writes to project `manglaria`. Use staging/output
projects for stage/final/control tables unless governance explicitly authorizes
otherwise.
