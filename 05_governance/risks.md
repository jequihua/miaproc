# Risks

## R1: rpy2 / R Setup Friction

- Severity: high.
- Likelihood: medium.
- Risk: Windows users and CI environments may have difficulty installing compatible
  R, REddyProc, and `rpy2` versions.
- Mitigation: keep `reddyproc-rpy2` optional; provide clear setup docs; skip R
  tests by default when dependencies are missing.

## R2: False Parity Between hesseflux And REddyProc

- Severity: high.
- Likelihood: high.
- Risk: hesseflux and REddyProc may produce similar-looking but scientifically
  different outputs.
- Mitigation: use the R backend as a parity reference; compare outputs with
  explicit diagnostics and tolerances.

## R3: Misuse Of hesseflux ustarfilter

- Severity: high.
- Likelihood: medium.
- Risk: hesseflux's u* filter expects at least one full year and may be misapplied
  to the 90-day case.
- Mitigation: document this limitation; do not use it as the sole dynamic u*
  implementation for standard >90-day ManglarIA processing.

## R4: Silent Fixed-Threshold Fallback

- Severity: high.
- Likelihood: low (behaviorally prevented since M4).
- Risk: dynamic mode could fail and silently fall back to the old arbitrary fixed
  threshold.
- Status: **mitigated in M4**. ``HessefluxConfig.ustar_mode`` is explicit,
  defaulting to ``"fixed"``. ``run_hesseflux_engine`` resolves the threshold
  through ``_resolve_ustar_threshold`` which raises
  ``DynamicUstarEstimationError`` on any dynamic-mode failure; there is no
  code path that reads ``ustar_fixed`` when
  ``ustar_mode="dynamic"``. Covered by
  ``tests/test_eddy_hesseflux_dynamic_ustar.py::TestDynamicModeOnSyntheticData::test_dynamic_threshold_differs_from_ustar_fixed``
  and ``::test_dynamic_sparse_input_raises_no_silent_fallback``.
- Residual concern: scientific accuracy of the selected threshold is still
  unvalidated against REddyProc; that belongs to R2/R5 and Milestone 5.

## R5: Unit And Sign Convention Drift

- Severity: medium.
- Likelihood: medium.
- Risk: Tair, VPD, GPP, Reco, or NEE conventions may differ between R and Python.
- Mitigation: add schema tests, backend contract tests, and comparison reports.

## R6: Over-Refactoring

- Severity: medium.
- Likelihood: medium.
- Risk: a coding agent may redesign stable package areas while implementing the new
  backend.
- Mitigation: milestones must keep scope narrow; reviewer approval required for
  broad refactors.

## R7: Python 3.14 Forward-Compatibility Of Legacy Dependencies

- Severity: low.
- Likelihood: low (reduced after M1).
- Risk: the legacy package was authored against Python 3.10–3.11; running under
  Python 3.14.3 might surface wheel or behavioral regressions in `numpy`,
  `pandas`, `scipy`, `hesseflux`, or transitive deps.
- Status: M1 editable install resolved `hesseflux 5.0`, `pandas 2.3.3`,
  `numpy 2.4.4`, `scipy 1.17.1`, `pyarrow 24.0.0` as cp314 wheels; all four
  migrated tests passed without modification.
- Mitigation: keep `requires-python = ">=3.10"` so 3.11/3.12 users are not
  locked out; revisit for `rpy2` specifically when Milestone 3 begins (R
  bridge is the most likely place 3.14 compatibility will matter).

## R8: CI Drift From Local Environment

- Severity: low.
- Likelihood: medium.
- Risk: `.github/workflows/ci.yml` still targets Python 3.11, does not run
  from `08_pkg/`, and uses `|| true` for both `ruff` and `pytest`, so CI is
  currently non-blocking and not aligned with the migrated layout.
- Mitigation: align CI during Milestone 6 (Documentation And CI Hardening)
  per the roadmap. Until then, test results in `03_experiments/run_summary.md`
  are the authoritative signal, not CI.

## R9: Unvalidated rpy2/R6 Call Sequence In reddyproc-rpy2 Backend

- Severity: medium.
- Likelihood: medium.
- Risk: `engine_reddyproc.py` contains the full REddyProc call sequence
  (`sEddyProc$new`, `sEstimateUstarScenarios`, `sMDSGapFill`,
  `sMDSGapFillUStarScens`, `sGLFluxPartition`) dispatched through
  `ro.r("$")(obj, "method")`. This code was written following published
  rpy2/R6 conventions but has not been executed during the M3 pass because
  R/REddyProc/`rpy2` are not installed on the implementing workstation.
  Subtle mismatches (argument names, R6 return-handle semantics, NA-aware
  type coercion) may only surface in a live R environment.
- Mitigation: the opt-in `@pytest.mark.reddyproc` suite in
  `tests/test_eddy_reddyproc_live.py` exists specifically to validate the
  live path. Reviewers or users with an R-capable environment should run
  `pytest -v -m reddyproc` and file corrections. Scientific accuracy is
  separately gated by Milestone 5 (case study). Until at least one live
  run passes, the backend should be treated as "plumbed, not verified."

## R10: pandas `DataFrame.attrs` Loss For Backend Diagnostics

- Severity: low.
- Likelihood: low.
- Risk: M3 attaches backend diagnostics via
  `df.attrs["miaproc_diagnostics"]`. Some pandas operations (concat,
  groupby-apply, certain arithmetic paths) are known to drop `attrs`
  silently, which would strip diagnostics from downstream frames.
- Mitigation: callers who need durable diagnostics should copy them
  immediately after `postproc()` returns, e.g.
  `diag = out.attrs["miaproc_diagnostics"]`. Decision 007 notes this
  trade-off. If a durable sidecar becomes necessary, consider returning
  a small dataclass wrapping `(df, diagnostics)` in a later milestone
  rather than mutating the current contract.

## R11: Uncontrolled R Runtime Capture By rpy2

- Severity: high.
- Likelihood: medium.
- Risk: `rpy2` binds to an existing R installation; it does not install R or
  create an isolated R runtime. Milestone 5 could accidentally use a developer's
  personal/global R installation and produce REddyProc reference output that
  depends on unrecorded local package libraries or R version.
- Mitigation: Decision 010 requires M5 to begin with an R environment
  isolation/preflight gate. The preflight must record the selected R executable
  or launch path, `R.home()`, R version, `.libPaths()`, `REddyProc` version, and
  `rpy2` version. If the selected R environment is not project-scoped or
  explicitly reviewer-approved, M5 must stop before scientific comparison.
