# Eddy Column Contracts

This directory holds the authoritative column lineage of the eddy stages
and a timestamp note. The former `*.schema.json` files (BigQuery
TableSchema lists with `STRING`/`FLOAT64`/`TIMESTAMP` types and `REQUIRED`
modes for the removed BigQuery handoff) were deleted in M002-S05: their
types and modes did not describe the Parquet files (for example, Parquet
silver/gold give regularized gap rows a null `primary_key`), and miaproc
no longer creates, reads or writes tables. The package does not read any
file of this directory at runtime.

The current file contract for Parquet inputs and the source-facing Parquet
silver/gold outputs (Arrow types, identity, timestamps, duplicates, groups,
case-sensitive names, D014) is
[`../../docs/PARQUET_FILE_SCHEMA.md`](../../docs/PARQUET_FILE_SCHEMA.md).

## Files

| Domain | Layer | File | Purpose |
|---|---|---|---|
| Eddy | Lineage | `eddy_bronze_to_stage_column_lineage_contract.csv` | **Authoritative** bronze -> silver -> gold column mapping (read by the package tests, not at runtime). |
| Eddy | Note | `eddy_timestamp_precision_note.md` | Mixed timestamp string precision in source CSVs. |

## Eddy Notes

- Processing runs in two explicit stages, `miaproc eddy run-silver` and then
  `miaproc eddy run-gold`, over local or mounted files.
- Parquet silver/gold carry the source-truth final names and a single
  `timestamp` column. The internal backend column `DateTime` is reconstructed
  inside the gold step from `timestamp` and is not part of Parquet silver or
  gold.
- Every carbon-flux bronze/source column is preserved into silver and gold
  under its source name unless miaproc changes the units. The two current
  unit-baked names are `air_temperature_c` (from source `air_temperature`)
  and `VPD_hpa` (from source `VPD`). Only the biomet columns used by
  processing are carried forward.
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

- Gold files carry backend-specific columns beyond the lineage contract
  (for example hesseflux and REddyProc diagnostics columns); they differ by
  engine.
- Flux-side `RH` and biomet-side `RH_1_1_1` are distinct and must both survive
  when present.
- A literal duplicate `rH` is resolved with the M28 humidity policy
  (equivalent copy suppressed, divergent copy renamed `rH_norm_s`); any other
  duplicate name fails the run.
- `eddy_timestamp_precision_note.md` records that the two case-study flux CSVs
  use different timestamp string precision; mixed precision inside one file
  should trigger a revisit of `miaproc.eddy.time.create_datetime()`.

To inspect the exact columns of a real deployment, run silver/gold into a
throwaway run directory and read the published Parquet schema and the run
JSON (`outputs`, `column_collision_actions`); validate it first with
`miaproc eddy validate-run`.

Downstream engineers who load published Parquet into another system own
that system's table definitions; derive them from the published Parquet
schema (for example `pyarrow.parquet.read_schema(path)`), not from a copied
example.
