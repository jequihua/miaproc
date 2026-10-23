# Parquet file schema for `eddy run-silver` / `eddy run-gold` (M002-S02)

This page documents the local Parquet inputs and the source-facing Parquet
silver/gold outputs of the two familiar stage commands. The CSV-directory
inputs (`--flux-dir` / `--biomet-dir`) and `.csv` outputs remain the legacy
compatibility path and keep their M001 shape (D014): a `.csv` silver/gold
does not follow the source-facing contract below (for example CSV gold
still carries the internal `DateTime` and `NEE_f`-style names). New
workflows should write `.parquet` silver and gold.

Scope: ordinary local or mounted filesystem paths only. Publication safety
on mounted object storage (scratch staging, completion manifest, fresh run
directories) is **not** part of this page; it is delivered separately and is
not claimed here.

## Inputs

| Flag | Meaning |
|---|---|
| `--flux-dir DIR` / `--flux-parquet FILE` | Exactly one flux source (argparse rejects both or neither). |
| `--biomet-dir DIR` / `--biomet-parquet FILE` | Exactly one biomet source. |
| `--silver-table FILE` (gold) | `.csv` or one `.parquet`/`.pq` silver file. |

Rules for every Parquet input file:

- one file with suffix `.parquet` or `.pq`; directories / partitioned
  datasets are rejected;
- URI schemes (`gs://`, `s3://`, `http(s)://`, `file://`, ...) are rejected
  with exit 3 on every path flag; mount the bucket and pass the mounted
  path (no cloud SDK is used);
- read with PyArrow (`pyarrow.parquet.ParquetFile`); a corrupt or
  truncated file, zero rows, duplicate field names or nested
  (list/struct/map) columns fail with exit 3 before any processing;
- `--skip-flux` / `--skip-biomet` apply to CSV only and are rejected with
  a Parquet source.

Required columns (names are case-sensitive):

| Role | Time | Required |
|---|---|---|
| flux | `timestamp` (timestamp or ISO-8601 string) or legacy `date` + `time` | `co2_flux`, `qc_co2_flux`, `air_temperature` (K), `VPD` (Pa), `u_star` or `u.` |
| biomet | same | `SWIN_1_1_1`, `P_RAIN_1_1_1`, `RH_1_1_1` |
| silver (gold input) | `timestamp` (or legacy `DateTime`) | none beyond time; the backend validates its own inputs |

Every other column (for example `H`, `LE`, flux `RH`, `wind_speed`,
`primary_key`, `site_id`) is carried through unchanged.

Type policy on read (independent of the writer's pandas metadata):

- integers become pandas nullable `Int64` (`UInt64` for uint64), so
  identifiers above 2**53 stay exact, also after gap rows add nulls; the
  measurement columns stage 1 computes with (`co2_flux`, `qc_co2_flux`,
  `H`, `qc_H`, `LE`, `qc_LE`, `air_temperature`, `VPD`, `u_star`/`u.`,
  `SWIN_1_1_1`, `P_RAIN_1_1_1`, `RH_1_1_1`, and the silver names
  `air_temperature_c`, `VPD_hpa`) become `float64`, as the CSV reader
  produces them;
- timestamps of any stored unit (s/ms/us/ns) become nanosecond pandas
  timestamps without truncation; the stored timezone is kept. Naive
  timestamps are interpreted in `--tz-in` (as for CSV);
- time strings (a string `timestamp` column in Parquet or CSV, or legacy
  `date` + `time`) may mix precisions within one input, for example
  `2025-12-02 11:00:00 UTC` next to `2025-12-02 11:30:00.000000 UTC`, or
  CSV files of a `--flux-dir`/`--biomet-dir` written at different
  precisions. Strings the first, inferred format does not match are parsed
  again element by element (pandas `format="mixed"`), and legacy `time`
  values may carry fractional seconds (`HH:MM:SS.ffffff`), so every such
  observation keeps its exact instant. Rows whose time still does not parse
  are dropped by stage 1 with a warning and counted in the silver run JSON
  (`row_counts.unparsed_time_rows_dropped`);
- strings stay strings with nulls as missing; floats keep NaN.

## Outputs

The output format is chosen by the `--output-table` suffix. `.parquet`/`.pq`
writes the source-facing contract below with PyArrow and no index column;
`.csv` writes the legacy shape. Grouped runs write per-group files of the
same format under `--output-groups-dir`.

### Silver (`.parquet`)

- exactly one `timestamp` (the stage-1 observation instant; gap rows
  inserted by the 30-minute regularization get the grid timestamp) and no
  `DateTime`;
- processed names and units: `co2_flux`, `qc_co2_flux`,
  `air_temperature_c` (deg C), `VPD_hpa` (hPa), `u_star`, `SWIN_1_1_1`,
  `P_RAIN_1_1_1`, `RH_1_1_1` (biomet humidity); the flux-side `RH` stays
  separate; every other input column is preserved in input order;
- source `primary_key` / `site_id` are carried unchanged (type and values,
  never synthesized); gap rows keep the uniform group `site_id`, a null
  `primary_key` and missing measurements;
- repeated input instants within one input (or one group) are resolved as
  for CSV: stage 1 keeps the first row in file order (its `primary_key`
  and values) and drops the later ones with a warning; the silver run JSON
  records the number dropped per side in
  `row_counts.duplicate_instants_dropped` (`{"flux": n, "biomet": m}`), and
  unparsed time rows in `row_counts.unparsed_time_rows_dropped`. The
  top-level values count each source row once; each group record shows
  what that group's stage-1 run dropped, so a biomet input without the
  group column (shared by every group) shows its drops in every group but
  once at top level. Both are `null` for the ungrouped CSV-directory path,
  where the files are read inside stage 1 (the parser fix above applies
  there too; the drop warnings remain on stderr). The same instant at two
  different groups is not a duplicate;
- column order: present identity columns (`primary_key`, `site_id`),
  `timestamp`, then the rest.

### Gold (`.parquet`)

- exactly one `timestamp` (the silver `timestamp` wins over the backend
  `DateTime`), no `DateTime`;
- every silver column is retained; backend gap-filled outputs use the
  lineage names `nee_f`, `nee_fqc`, `sw_in_f`, `ta_f`, `vpd_f`, plus
  `dateAndTime` (UTC `YYYY-MM-DD HH:MM:SS`); `GPP`, `Reco` and other
  backend columns keep their names;
- a backend passthrough (`NEE`, `QC_NEE`, `Tair`, `USTAR`, `VPD`, `Rg`,
  `P_RAIN`, `rH`) is dropped when its source-facing silver column is
  present (for REddyProc this means its prepared `Rg` is represented by
  the silver `SWIN_1_1_1`);
- order: identity columns, `timestamp`, then the rest sorted by name.
- gold has exactly one row per silver row; timestamps stay unique per group.

Identity precision note: a Parquet silver keeps `primary_key` exact. A
legacy `.csv` silver is read with the pandas CSV parser, so an integer
`primary_key` with blank gap rows becomes `float64` and identifiers above
2**53 are rounded before they reach a Parquet gold; use Parquet silver
when identifiers must stay exact.

Internal names used by the backends are reconstructed from these names on
read (`timestamp -> DateTime`, `co2_flux -> NEE`, `air_temperature_c ->
Tair`, `VPD_hpa -> VPD`, `u_star -> USTAR`, `SWIN_1_1_1 -> Rg`,
`P_RAIN_1_1_1 -> P_RAIN`, `RH_1_1_1 -> rH`, `qc_co2_flux -> QC_NEE`).

### Duplicate and colliding columns

Parquet names are case-sensitive. Gold may therefore carry backend names
that differ only by case, for example hesseflux `TA_f` next to the lineage
`ta_f`, and `RECO` next to `Reco` (D014; no column is renamed to hide
this). **Consumer caveat:** case-insensitive readers (for example BigQuery
external tables or Spark with default case-insensitive resolution) cannot
address both columns of such a pair; select them by exact name or rename
on load.

A literal duplicate `rH` is resolved deterministically (equivalent copy
suppressed; divergent copy renamed `rH_norm_s`, `rH_norm_s_2`, ...); any
other literal duplicate fails the run, including one produced by the
source-facing rename (an input that already carries, say,
`air_temperature_c` next to `air_temperature`), which fails with a
`DuplicateStageColumnsError` naming the column (exit 4). Resolved actions
are written to the run JSON as `column_collision_actions` (top level and
per group).

Published name of the kept humidity column: the collision policy runs on
the internal names, before the source-facing rename, so an action recorded
as `column: rH` (for example `renamed_divergent_duplicate` with
`renamed_to: rH_norm_s`, reason "preserved both as 'rH' and 'rH_norm_s'")
describes a kept column that the published silver/gold carry as
`RH_1_1_1` (the `rH -> RH_1_1_1` mapping above). A published table
therefore holds `RH_1_1_1` plus `rH_norm_s` (divergent copy) or `RH_1_1_1`
alone (equivalent copy suppressed), never a column named `rH`.

### Groups

With `--group-column`, every non-null category runs independently in sorted
order and the stacked output equals the concatenation of the per-group
files; null-category rows are counted in
`row_counts.null_category_rows_skipped`. A group column with no non-null
value fails with exit 4 and writes no table, run JSON or groups directory.
`eddy run-silver` and `eddy run-gold` refuse categories whose sanitized
file names collide (for example `S/1` and `s 1`) with exit 4 before any
group runs and before any output is written (D014; this applies to CSV
and Parquet inputs alike). Caveat: `miaproc run` keeps its earlier
behavior, where colliding labels share and overwrite one per-group file.

### Persisted diagnostics

- gold diagnostics JSON: the backend diagnostics (ungrouped) or the list of
  per-group diagnostics files (grouped);
- run JSONs: input formats and paths, input/output row counts,
  `outputs.contract` (`source-facing` or `legacy-csv`), output columns and
  collision actions; gold also records the R preflight for
  `reddyproc-reference`.

## Example

```bash
miaproc eddy run-silver \
    --flux-parquet /data/in/flux.parquet \
    --biomet-parquet /data/in/biomet.parquet \
    --group-column site_id \
    --output-table /data/out/silver-0001/silver.parquet \
    --output-run-json /data/out/silver-0001/silver_run.json

miaproc eddy run-gold \
    --silver-table /data/out/silver-0001/silver.parquet \
    --engine hesseflux-native \
    --group-column site_id \
    --output-table /data/out/gold-0001/gold.parquet \
    --output-diagnostics-json /data/out/gold-0001/gold_diag.json \
    --output-run-json /data/out/gold-0001/gold_run.json
```

M002-S03: each invocation publishes into its own fresh run directory with a
completion manifest written last; gold validates the silver run's manifest.
See [RUN_PUBLICATION.md](RUN_PUBLICATION.md).

Exit codes: 0 success; 2 `reddyproc-reference` preflight not approved, or
an argparse usage error such as two or no sources for one side; 3 argument
or input validation (including every Parquet input failure above); 4
runtime processing failure (including colliding group file names, an
all-null group column, duplicate columns after the source-facing rename,
and an unreadable or empty CSV directory, also when the other side is a
Parquet file); 3 also for run-directory reuse/overlap/traversal and an
incomplete or mismatched silver run; 4 also for any publication fault before
the manifest; 5 when the manifest was written but the finish could not be
confirmed (the run is published; see RUN_PUBLICATION.md).

## Verification scope note

The frozen same-engine comparisons (verifier lanes `parquet_cli` and
`parquet_cli_reddyproc`) compare the Parquet CLI path with a baseline that
was recorded with `drop_rain_rows=True`, while `eddy run-silver` masks rain
rows (`drop_rain_rows=False`). Those lanes therefore leave the flux rows at
the frozen rain instants out of the converted input, and the CLI rain-masking
branch itself has no same-engine frozen oracle; it is unchanged code covered
by unit tests and coverage checks. A CLI-policy reference could be frozen at
the next re-baseline.
