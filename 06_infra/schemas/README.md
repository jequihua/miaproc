# BigQuery Schema Examples

These files are BigQuery TableSchema JSON examples for `miaproc` cloud handoff.
They are engineer-facing templates, not proof that live tables have already
been created from these exact files.

Use these schemas as a starting point, then verify against the exact payload
produced by the package for the source table and flags your team is running.

## Files

| Domain | Layer | File | Purpose |
|---|---|---|---|
| Eddy | Bronze flux | `eddy_bronze_flux.schema.json` | Example source-table contract for flux variables used by miaproc. |
| Eddy | Bronze biomet | `eddy_bronze_biomet.schema.json` | Example source-table contract for biomet variables used by miaproc. |
| Eddy | Silver | `eddy_silver.schema.json` | Example silver payload schema after source aliasing and humidity deduplication. |
| Eddy | Gold | `eddy_gold.schema.json` | Example gold payload schema after preserving silver columns and appending backend/lakehouse columns. |
| Biomass | Forest-structure source | `biomass_forest_structure_source.schema.json` | Canonical individual-tree source schema from `08_pkg/docs/forest_data_schema.csv`. |
| Biomass | Estimation output | `biomass_estimation.schema.json` | Row-preserving biomass product: source columns plus `biomass_estimate` and `equation_used`. |
| Biomass | Runs control | `biomass_runs.schema.json` | Control/audit schema for `cf_biomass_runs`, created by biomass BigQuery writeback. |

## Eddy Notes

M31 status:

- Eddy BigQuery processing runs in two explicit stages:
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

Before pre-creating or altering eddy BigQuery tables, run the Docker/CLI dry-run
against the actual cloud source tables and inspect `stage_payload_metadata.json`.
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
specific eddy deployment. Real EddyPro exports can carry site-specific
pass-through columns beyond the core set listed here.

## Biomass Notes

The biomass product is intentionally row-preserving:

- `miaproc biomass enrich-table` reads a local table and writes the same rows
  plus exactly two appended columns by default: `biomass_estimate` and
  `equation_used`.
- `miaproc biomass run-bigquery` reads one BigQuery source table, applies the
  same enrichment contract in memory, writes local output, and optionally stages
  / merges the enriched rows when writeback flags are supplied.
- The default equation dataset is `dina`, the direct-biomass mangrove equation
  set. Under this default, adult rows with non-null `dbh_cm` and matched species
  can receive biomass estimates in kg; ineligible rows are preserved with null
  `biomass_estimate` and null `equation_used`.
- The default merge key for BigQuery writeback is `primary_key`. Deployments can
  override it with `--bq-merge-key`, but the chosen key must be present, non-null,
  and unique in the staged frame.
- Biomass has no watermark table by design. The control table is
  `cf_biomass_runs` only.

`biomass_estimation.schema.json` uses the canonical source-field names from
`08_pkg/docs/forest_data_schema.csv`. If an operator-owned source table uses
legacy or survey-specific names, normalize them before enrichment or pass the
CLI column-mapping flags (`--species-col`, `--dbh-col`, `--height-col`,
`--life-stage-col`) and adjust the table schema accordingly.

## Cloud Safety

The package default rejects writes to project `manglaria`. Use staging/output
projects for stage/final/control tables unless governance explicitly authorizes
otherwise. Do not commit service-account keys or local Google credential files.
