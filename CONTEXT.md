# Workspace: 08_pkg

## Purpose

This is the active package-development workspace for `miaproc`.

The package currently exists in `90_legacy_review/miaproc-main`. The first coding
milestone is to migrate that package here without changing behavior.

## Rules For Coding Agents

- Read `08_pkg/development_roadmap.md`, `08_pkg/backend_contract.md`, and
  `08_pkg/testing_strategy.md` before editing package code.
- Do not rewrite stable modules without documenting the defect that requires it.
- Keep `reddyproc-rpy2` optional.
- Keep Python-only tests runnable without R.
- Add tests with behavior changes.
- For tests that require real case-study data, use
  `01_data/case_study/flux/flux.csv` and
  `01_data/case_study/biomet/biomet.csv`, filtered to
  `site_id == "RBMNN"` unless the test is explicitly about site selection.
- Update governance logs when changing architecture or scientific assumptions.

## Active Scientific Target

Implement the standard long-series eddy workflow reflected in
`90_legacy_review/R/R_manglaria.R`:

- REddyProc-style dynamic u* scenarios.
- MDS gap filling after u* filtering.
- Lasslop-style partitioning.
- Common normalized output schema.
