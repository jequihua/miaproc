# Eddy Timestamp Precision Parsing Note

The two case-study carbon-flux CSV files currently use the same 54-column
schema, but their timestamp string precision differs:

- `01_data/case_study/flux/flux.csv`: examples look like
  `2025-10-25 08:00:00 UTC`.
- `01_data/case_study/flux/flux_staging.csv`: examples look like
  `2025-12-02 11:00:00.000000 UTC`.

Checked on 2026-05-27: parsing each file independently with
`pandas.to_datetime(..., errors="coerce")` produced zero `NaT` values and both
normalized to `datetime64[ns, UTC]`, so the precision difference does not block
the current workflow.

Warning to revisit if future data fails timestamp parsing: pandas can infer one
format for a mixed-format Series and coerce the other format to `NaT`. If a
single incoming table/file mixes second-precision and microsecond-precision UTC
strings, harden `miaproc.eddy.time.create_datetime()` to parse timestamp strings
with a mixed-format strategy (for example `format="mixed"` where supported, or a
small two-pass parser) before treating the failure as a data-quality issue.
