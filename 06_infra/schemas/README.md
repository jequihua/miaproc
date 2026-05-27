# BigQuery Schema Examples

These files are BigQuery TableSchema JSON examples for `miaproc` cloud handoff.
They are engineer-facing templates, not proof that live tables have already
been created from these exact files. M33 updates the eddy silver/gold schemas
to the accepted M32A source-truth column contract but does not alter any live
BigQuery table. The post-M33 source-table refresh widens the eddy examples to
the full 54-column case-study carbon-flux source shape seen in
`01_data/case_study/flux/{flux,flux_staging}.csv`.

Use these schemas as a starting point, then verify against the exact payload
produced by the package for the source table and flags your team is running.
Cloud engineers may adapt schemas to fit the exact table-management workflow
their deployment uses (CREATE TABLE / partitioning / clustering / column
descriptions).

## Files

| Domain | Layer | File | Purpose |
|---|---|---|---|
| Eddy | Bronze flux | `eddy_bronze_flux.schema.json` | Example source-table contract for the full 54-column case-study carbon-flux table. |
| Eddy | Bronze biomet | `eddy_bronze_biomet.schema.json` | Minimal source-table contract for the biomet columns miaproc currently uses: join/group identifiers plus `SWIN_1_1_1`, `P_RAIN_1_1_1`, and `RH_1_1_1`. |
| Eddy | Silver | `eddy_silver.schema.json` | Source-truth silver payload schema: all carbon-flux source columns carried forward, with `air_temperature -> air_temperature_c` and `VPD -> VPD_hpa`, plus used biomet columns and optional `rH_norm_s`. |
| Eddy | Gold | `eddy_gold.schema.json` | Source-truth gold stage payload: preserves the widened silver payload and adds gold-only outputs (`dateAndTime`, `nee_f`, `nee_fqc`, `sw_in_f`, `ta_f`, `vpd_f`, `GPP`, `Reco`). |
| Biomass | Forest-structure source | `biomass_forest_structure_source.schema.json` | Canonical individual-tree source schema from `08_pkg/docs/forest_data_schema.csv`. |
| Biomass | Estimation output | `biomass_estimation.schema.json` | Row-preserving biomass product: source columns plus `biomass_estimate` and `equation_used`. |
| Biomass | Runs control | `biomass_runs.schema.json` | Control/audit schema for `cf_biomass_runs`, created by biomass BigQuery writeback. |

## Eddy Notes

M32A source-truth status:

- Eddy BigQuery processing runs in two explicit stages:
  `miaproc eddy run-bigquery-silver` and then
  `miaproc eddy run-bigquery-gold`.
- Silver/gold payloads carry the source-truth final names and a single
  `timestamp` column. The internal backend processing column `DateTime` is
  reconstructed inside the gold step from `timestamp` and is not part of the
  final silver or gold payload.
- The product rule for the split BigQuery path is now: preserve every
  carbon-flux bronze/source column into silver and gold, preserving the source
  name unless miaproc changes the units. The two current unit-baked names are
  `air_temperature_c` (from source `air_temperature`) and `VPD_hpa` (from
  source `VPD`). Only the biomet columns used by processing are carried forward.
- The accepted M32A internal -> final mapping is:

  ```text
  DateTime       -> timestamp
  NEE            -> co2_flux
  QC_NEE         -> qc_co2_flux
  Tair           -> air_temperature_c
  USTAR          -> u_star
  VPD            -> VPD_hpa
  Rg             -> SWIN_1_1_1
  P_RAIN         -> P_RAIN_1_1_1
  rH             -> RH_1_1_1
  ```

- The authoritative column mapping is
  `06_infra/schemas/eddy_bronze_to_stage_column_lineage_contract.csv`. The CSV
  is the source of truth; the JSON schemas mirror it for cloud engineers.
- Current example counts: `eddy_bronze_flux.schema.json` has 54 columns,
  `eddy_bronze_biomet.schema.json` has 5 used columns,
  `eddy_silver.schema.json` has 58 columns, and
  `eddy_gold.schema.json` has 66 columns.
- Flux-side `RH` and biomet-side `RH_1_1_1` are case-insensitively distinct
  and must both survive when present.
- `eddy_timestamp_precision_note.md` records the current warning that the two
  case-study flux CSVs use different timestamp string precision. Each file
  parses cleanly today, but mixed precision inside a single table/file should
  trigger a revisit of `miaproc.eddy.time.create_datetime()`.
- BigQuery field names are case-insensitive. `casefold()` uniqueness of column
  names is enforced before any stage write.
- The M28 defensive `rH_norm_s` fallback is still preserved when a divergent
  derived humidity column appears; equivalent humidity duplicates are
  suppressed; non-humidity case-insensitive duplicates raise.
- Stage payload dry-run metadata is the authoritative way to inspect the exact
  columns for a real site/table/date window before mutating BigQuery.

Operators can locally assert the schema-level invariants of the example files
with a one-line Python check:

```python
import json
import pathlib

root = pathlib.Path("06_infra/schemas")
for name in ("eddy_silver.schema.json", "eddy_gold.schema.json"):
    fields = json.loads((root / name).read_text(encoding="utf-8"))
    cols = [f["name"] for f in fields]
    assert "timestamp" in cols, (name, cols)
    assert "DateTime" not in cols, (name, cols)
    assert len({c.casefold() for c in cols}) == len(cols), (name, cols)
```

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
M33 does not perform any live BigQuery DDL/DML, IAM, Scheduler, or Cloud Run
mutation; the schemas in this directory are example templates only.
