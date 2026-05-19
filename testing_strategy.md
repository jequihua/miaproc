# Testing Strategy

## Test Markers

Use markers to keep default tests fast and portable:

- default: Python-only tests.
- `reddyproc`: tests requiring R, REddyProc, and `rpy2`.
- `integration`: larger case-study or file-based tests.
- `slow`: long-running comparisons.

Default CI should run Python-only tests. R-backed tests are opt-in.

## Data-Backed Test Fixture Policy

For all future tests that require real eddy covariance data, use the case-study
datasets under `01_data/case_study`:

- flux data: `01_data/case_study/flux/flux.csv`
- biomet data: `01_data/case_study/biomet/biomet.csv`

These files contain multiple sites in one table. Unless a test is explicitly
about site-selection behavior, tests must filter both files to:

```text
site_id == "RBMNN"
```

before stage-1 loading, stage-2 preparation, backend execution, or scientific
comparison. Do not run package tests against the combined two-site frame because
duplicate timestamps across sites would mix independent towers.

The current case-study files use a flattened export shape:

- `timestamp` instead of legacy `date` + `time`.
- `u_star` instead of legacy `u.`.

Future data-backed tests should either use a package adapter that supports this
shape directly or create an explicit temporary adaptation in the test setup. Do
not rewrite or overwrite the raw case-study files.

## Milestone 1 Tests

Required:

- Legacy import tests.
- Legacy biomass tests.
- Legacy stage-1 eddy tests.
- Legacy hesseflux fixed-mode tests.

Rule: do not change expected behavior in this milestone.

## Milestone 2 Tests

Required:

- Stage-2 preparation returns required columns.
- `Year`, `DoY`, and `Hour` are computed correctly.
- Negative `Rg` is clamped to zero.
- `USTAR` maps to `Ustar` for REddyProc input.
- Missing required columns produce clear errors.

## Milestone 3 Tests

Required:

- Missing `rpy2` or R produces actionable skip/error behavior.
- Backend output contains all required common columns.
- Backend diagnostics include u* scenarios and selected scenario.
- A tiny synthetic or fixture dataset can exercise the adapter where R is
  available.

Recommended:

- Optional parity fixture generated from `R_manglaria.R`.

## Milestone 4 Tests

Required:

- Fixed mode still uses explicit caller threshold.
- Dynamic mode does not read or default to the old arbitrary fixed threshold.
- Dynamic mode produces u* diagnostics.
- Low nighttime `USTAR` values are filtered before NEE gap filling.
- Sparse nighttime data fail explicitly.
- Lasslop partitioning path returns finite GPP and Reco where inputs permit.
- Any data-backed integration test uses `01_data/case_study` filtered to
  `site_id == "RBMNN"`.

## Milestone 5 Tests

Required:

- R environment preflight runs before any live `reddyproc-rpy2` comparison and
  records R executable or launch path, `R.home()`, R version, `.libPaths()`,
  `REddyProc` version, and `rpy2` version.
- Live R tests must not silently use an arbitrary global R installation. If no
  controlled or explicitly approved R environment is available, the preflight
  must stop with an actionable skip/failure before scientific comparison.
- Case-study pipeline can run end-to-end for both available backends using the
  `RBMNN` subset of `01_data/case_study`.
- Comparison report includes:
  - u* threshold scenarios.
  - NEE_f coverage.
  - GPP and Reco summary statistics.
  - daily totals.
  - aggregate carbon balance.
  - correlations and bias against REddyProc-backed output.

## Acceptance Philosophy

Use exact assertions for schema, configuration, and deterministic transformations.
Use tolerances for scientific numeric comparisons between REddyProc and hesseflux.

Never approve a change solely because plots look reasonable. Plots are supporting
evidence; tests and diagnostics are required.
