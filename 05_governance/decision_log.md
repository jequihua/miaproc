# Decision Log

## Decision 001: Preserve The Existing Package And Develop Incrementally

## Context

`90_legacy_review/miaproc-main` is a working package and has served the project
well. The new effort is to formalize development and add the long-series dynamic
u* workflow.

## Options Considered

- Rewrite the package from scratch.
- Migrate the existing package and extend it in milestones.

## Chosen Option

Migrate the existing package and extend it in milestones.

## Rationale

The current package already solves stage-1 processing and has tests. A rewrite
would add risk without improving the immediate scientific requirement.

## Impact

The first coding milestone must be a behavior-preserving migration. Major
refactors require a documented defect or reviewer approval.

## Date

2026-04-22

---

## Decision 002: Use REddyProc Through rpy2 As The Parity Backend

## Context

`R_manglaria.R` is the validated long-series workflow. It relies on REddyProc's
u* scenarios, MDS gap filling, and Lasslop partitioning.

## Options Considered

- Only implement a Python approximation.
- Shell out to R scripts.
- Add a thin `rpy2` backend around REddyProc.

## Chosen Option

Add an optional `reddyproc-rpy2` backend.

## Rationale

`rpy2` keeps parity close to the validated R workflow while allowing the package
API to remain Python-first.

## Impact

R, REddyProc, and `rpy2` are optional dependencies. Default CI must not require
them. Optional parity tests should be marked separately.

## Date

2026-04-22

---

## Decision 003: Keep hesseflux As The Portable Python Backend

## Context

The legacy package already uses hesseflux in Python.

## Options Considered

- Remove hesseflux and rely only on R.
- Keep hesseflux and add dynamic u* behavior around it.

## Chosen Option

Keep hesseflux as the portable backend.

## Rationale

Python-only users should not need an R installation. hesseflux remains useful for
gap filling and partitioning, but its u* filter must not be treated as sufficient
for the 90-day case.

## Impact

The hesseflux backend must expose fixed and dynamic u* modes. Dynamic mode must be
validated against the REddyProc-backed path.

## Date

2026-04-22

---

## Decision 004: Use Python 3.14.3 In A Local Venv

## Context

Development environment for active work in `08_pkg` needs a pinned interpreter so
migration, tests, and optional `rpy2` wiring are reproducible. The `py` launcher
exposes 3.14, 3.11, and 3.9 on this workstation.

## Options Considered

- Reuse the workstation's Python 3.11 (matches CI default).
- Use Python 3.14.3 in a local `.venv`.
- Use conda/mamba environment.

## Chosen Option

Python 3.14.3 in a local `.venv` at the repo root.

## Rationale

3.14 is the latest available interpreter on this machine; using it surfaces any
forward-compatibility issues in the legacy package early, before migration.
`venv` keeps the toolchain lightweight and gitignored. The existing CI
configuration pins 3.11; a follow-up decision will align CI if 3.14 proves
stable.

## Impact

- All commands in this project should run under `.venv/Scripts/python.exe`.
- `pyproject.toml` currently declares `requires-python = ">=3.10"`, which
  remains compatible.
- `.github/workflows/ci.yml` still uses `python-version: "3.11"`; revisit
  before the migration milestone closes.
- Any dependency that is not yet wheel-available for 3.14 (notably scientific
  stack such as `rpy2`, `geopandas` transitives, or numeric extensions) may
  require falling back to 3.11; if that happens, record a follow-up decision
  rather than silently switching.

## Date

2026-04-22

---

## Decision 005: Merge Legacy pyproject.toml With Framework Scaffolding

## Context

The legacy `90_legacy_review/miaproc-main/pyproject.toml` is the source of truth
for package metadata per Decision 001 and Migration Decision M001. However, the
pre-existing `08_pkg/pyproject.toml` had already been adapted during framework
setup to include:

- pytest markers (`reddyproc`, `integration`, `slow`) aligned with
  `08_pkg/testing_strategy.md`.
- a `reddyproc` optional-dependency entry aligned with Decision 002.

A strict byte-for-byte copy would erase those framework-level additions.

## Options Considered

- Overwrite `08_pkg/pyproject.toml` verbatim from the legacy file.
- Discard the legacy file and keep the adapted stub.
- Merge: legacy metadata (description, readme, license, authors, optional-deps
  for `hesseflux` and `biomass`, package-data, explicit `package-dir`) plus the
  framework additions (pytest markers, `reddyproc` optional-dep).

## Chosen Option

Merge.

## Rationale

- Metadata and dependency specifications come from the legacy file (source of
  truth for the package).
- Framework additions that encode Decisions 001–003 and the testing strategy
  are preserved.
- `hesseflux` is kept as an optional dependency to match legacy exactly;
  development installs use `pip install -e ".[dev,hesseflux,biomass]"` to run
  the migration test suite.

## Impact

- No change to scientific behavior; only install surface and test-marker
  vocabulary.
- Reviewer of Gate M1 should confirm the merge does not introduce hidden
  behavior changes.
- If CI is updated for M1, it must install with the `[dev,hesseflux]` extras
  (plus `biomass` once biomass tests are added).

## Date

2026-04-22

---

## Decision 006: Optional `local_tz` For REddyProc Calendar Fields

## Context

`R_manglaria.R` computes `Year`, `DoY`, and `Hour` from
`with_tz(DateTime, tzone = "America/Mazatlan")` — i.e. the local wall-clock
time at the Marismas Nacionales site — while keeping the original `DateTime`
column in the selected output. This matters because REddyProc interprets
`DoY` and `Hour` as local-time calendar fields for solar-position
calculations during partitioning. A UTC-valued `DoY`/`Hour` at a
UTC-7 site will systematically shift day boundaries relative to the R
reference.

The Python stage-2 helper is site-agnostic package code and should not
hard-code `America/Mazatlan`, but callers running the Marismas workflow
(and any comparison against REddyProc output) will need the local-time
semantics to reproduce the R result.

## Options Considered

- Always compute calendar fields from `DateTime` as given, and require
  callers to tz-convert upstream.
- Always localize using a fixed default timezone.
- Accept an optional `local_tz` keyword argument.

## Chosen Option

Optional `local_tz` keyword argument.

## Rationale

- Keeps the helper site-agnostic by default (no hidden bias toward a
  specific site).
- Lets the Marismas long-series workflow request the same local-time
  semantics as `R_manglaria.R` with a single keyword.
- Keeps the returned `DateTime` column as the original instant, matching
  the R `select(DateTime, ...)` behavior — only the derived calendar
  fields reflect `local_tz`.
- If the input `DateTime` is timezone-naive, the function explicitly
  localizes it as UTC before converting (documented in the docstring).

## Impact

- Any backend that reproduces the R workflow (notably the future
  `reddyproc-rpy2` backend in Milestone 3) should pass
  `local_tz="America/Mazatlan"` (or the relevant site tz) when preparing
  the stage-2 input for Marismas Nacionales.
- Tests assert the UTC-default behavior and the `local_tz` behavior
  independently; breaking either in later milestones will surface in
  `test_eddy_stage2.py::TestCalendarFields`.

## Date

2026-04-22

---

## Decision 007: REddyProc rpy2 Backend — Integration Shape

## Context

Milestone 3 introduces the optional REddyProc-through-rpy2 parity backend.
Several small integration choices needed to be made: the canonical backend
name (with or without a legacy alias), how diagnostics are exposed,
how R-side R6 class methods are dispatched from rpy2, and how the
opt-in live-R tests are guarded.

## Options Considered

1. Name canonicalization:
   - (a) rename only: `"reddyproc-rpy2"` and drop the migrated
     `"reddyproc"` legacy placeholder;
   - (b) keep both names, route to one implementation.
2. Diagnostics delivery:
   - (a) return a `(df, diagnostics)` tuple (would differ from the
     hesseflux backend's single-DataFrame return);
   - (b) attach diagnostics via `DataFrame.attrs["miaproc_diagnostics"]`
     (stays compatible with the existing `postproc()` return type).
3. R6 method dispatch through rpy2:
   - (a) attribute-style calls (`obj.sMDSGapFill(...)`);
   - (b) explicit `ro.r("$")(obj, "method")(*args, **kwargs)`.
4. Opt-in live tests:
   - (a) marker alone (`@pytest.mark.reddyproc`);
   - (b) marker + `pytest.importorskip("rpy2")` + probed REddyProc
     package load.

## Chosen Option

1. Keep both names. `"reddyproc-rpy2"` is the contract; `"reddyproc"` is
   an explicit deprecated alias that routes to the same implementation.
2. Attach diagnostics via `df.attrs["miaproc_diagnostics"]`.
3. Use `ro.r("$")(obj, "method")` for R6 method dispatch throughout.
4. Combine `@pytest.mark.reddyproc` with `pytest.importorskip("rpy2")`
   and a probed REddyProc-load gate.

## Rationale

1. The migrated `engines.py` already exposed `"reddyproc"`. Removing it
   would break any caller that may have wired to that name during the
   legacy phase. Keeping both is trivial (one alias tuple) and the two
   names share exactly one dispatch branch, so they cannot drift.
2. `df.attrs` keeps the `postproc()` return type stable across
   backends, which downstream consumers rely on. The attribute is a
   standard pandas feature and survives indexing/assignment for our
   use. Trade-off: `attrs` is known to be cleared by some pandas
   operations; callers who need durable diagnostics should copy them
   to their own dict immediately after the call.
3. Attribute-style R6 dispatch in rpy2 is ambiguous (it clashes with
   Python method resolution). The explicit `ro.r("$")(obj, "method")`
   form is portable across rpy2 versions and is the pattern
   recommended in recent REddyProc examples. The cost is slightly
   more verbose code.
4. The marker alone would still execute the tests in a default
   environment that lacks rpy2, causing hard collection errors. The
   `importorskip` gate makes the default `pytest -v` invocation clean.
   The marker remains the correct tool for running only these tests
   on an R-capable machine (`pytest -m reddyproc`).

## Impact

- Install surface unchanged: `miaproc[reddyproc]` still opts in to
  `rpy2`. R and REddyProc are user-installed separately (documented
  in the error message and in `08_pkg/backend_contract.md`).
- `postproc()` return shape is unchanged. Diagnostics are on
  `df.attrs["miaproc_diagnostics"]`.
- The R-side call sequence in `engine_reddyproc.py` has **not** been
  executed during the M3 pass (no R/rpy2 in this environment). The
  opt-in live-R suite is the mechanism to validate rpy2/R6 syntax in
  real environments; any issues discovered there are to be fixed
  without further scope creep.
- Milestone 5 (case study) is the first place scientific accuracy of
  the backend is assessed. M3 only asserts routing, configuration,
  error surfaces, scenario selection, output normalization, and
  diagnostics.

## Date

2026-04-22

---

## Decision 008: Use RBMNN As The Default Real-Data Test Site

## Context

The repository now includes case-study eddy covariance data under
`01_data/case_study`:

- flux: `01_data/case_study/flux/flux.csv`
- biomet: `01_data/case_study/biomet/biomet.csv`

These files contain multiple sites stacked together (`RBMNN` and `RBRL` were
observed during inspection). The combined files therefore have duplicated UTC
timestamps across sites, even though timestamps are unique within each site.
The current package loader also expects the legacy `date` + `time` and `u.`
shape, while the case-study files use `timestamp` and `u_star`.

## Options Considered

- Treat the combined case-study files as one test series.
- Let each future coding prompt choose an arbitrary site.
- Standardize all future real-data tests on one explicit site subset.

## Chosen Option

All future tests and validation runs that require repository case-study data
must use the `RBMNN` subset of both flux and biomet files, unless the test is
explicitly about site-selection or multi-site behavior.

## Rationale

`RBMNN` provides the longer observed case-study series and avoids duplicate
timestamps caused by stacked sites. Standardizing on one site keeps tests
repeatable and prevents accidental mixing of independent tower records.

## Impact

- Test setup must filter both `01_data/case_study/flux/flux.csv` and
  `01_data/case_study/biomet/biomet.csv` to `site_id == "RBMNN"` before
  stage-1 loading, stage-2 preparation, backend execution, or case-study
  comparison.
- Future ingestion work should support or explicitly adapt the case-study
  flattened schema (`timestamp`, `u_star`) without modifying the raw CSVs.
- The combined multi-site files should only be used directly in tests that
  assert the package detects or handles multi-site input deliberately.

## Date

2026-04-22

---

## Decision 009: Dynamic u* Algorithm For The hesseflux Backend

## Context

Milestone 4 requires that ``engine="hesseflux"`` support a data-driven u*
threshold instead of forcing every caller to supply ``ustar_fixed``. REddyProc
uses a bootstrap-resampled, seasonal, per-temperature-class plateau test
(``90_legacy_review/REddyProc-master/R/EddyUStarFilterDP.R``). A full
seasonal+bootstrap port is out of scope for M4 both because it is a large
scientific surface and because scientific parity is Milestone 5's job; a
simpler deterministic estimator would still satisfy the M4 definition of done.

## Options Considered

1. Full REddyProc port (seasons + bootstrap) in M4.
2. Call REddyProc through ``rpy2`` for u* estimation only.
3. Deterministic REddyProc-inspired plateau estimator with
   temperature-quantile pseudo-seasons and no bootstrap.
4. Simple fixed-quantile of nighttime ``USTAR`` as the threshold.

## Chosen Option

Option 3: deterministic REddyProc-inspired plateau estimator.

## Rationale

- Option 1 would bundle M4 and M5 into one pass; the roadmap treats them
  separately so scientific comparison has a stable target.
- Option 2 breaks R3 (Python-only users should not need R) and reintroduces
  the ``rpy2`` optional-dependency surface inside the portable backend.
- Option 4 does not actually implement the u* plateau concept; it collapses
  to a cosmetic change with no scientific defensibility.
- Option 3 keeps the hesseflux backend Python-native, produces the same
  scenario labels (``U05``/``U50``/``U95``) as the REddyProc backend so M5
  can compare them directly, and is small enough to test with synthetic
  plateau-shaped data (``test_selected_threshold_near_true_plateau``).

## Implementation Notes

- Module: ``08_pkg/src/miaproc/eddy/ustar.py``.
- Method string exposed in diagnostics: ``"hesseflux-plateau-v1"``. A future
  bootstrap or true-seasonal pass would advance this to ``v2``, preserving
  the M4 baseline for comparison.
- "Seasons" are currently temperature-quantile bins
  (``ustar_temp_bins=4`` by default). This is not equivalent to REddyProc's
  true calendar seasons; the difference is documented and intentional.
- No RNG is used; the estimator is deterministic. ``ustar_bootstrap_samples``
  is reserved in ``HessefluxConfig`` for a future pass.

## Impact

- M4 dynamic-mode outputs are defensible against synthetic plateau datasets
  but are **not** a scientific substitute for REddyProc. Callers needing
  validated REddyProc parity should use the ``reddyproc-rpy2`` backend.
- The chosen algorithm may be replaced in M5 if the comparison against the
  R backend shows material bias; if that happens, a new decision must be
  recorded before changing ``"hesseflux-plateau-v1"`` to a new method
  string.
- Diagnostics exposes ``method``, ``available_scenarios``,
  ``selected_threshold``, ``thresholds_by_scenario``,
  ``thresholds_by_season``, ``night_sample_count``, and
  ``fraction_nee_filtered``, matching the vocabulary used by the
  ``reddyproc-rpy2`` backend so M5 can compare diagnostics side by side.

## Date

2026-04-22

---

## Decision 010: Gate M5 Starts With R Environment Isolation Preflight

## Context

The `reddyproc-rpy2` backend uses `rpy2` to call REddyProc, but `rpy2` does not
install R or create an isolated R runtime. It binds to an existing R installation
found through the local environment. If Milestone 5 silently uses a developer's
personal or global R installation, the REddyProc reference output may depend on
unrecorded local state.

## Options Considered

- Let `rpy2` discover any available R installation and record versions after the
  fact.
- Require a project-scoped R environment such as `renv` before any live run.
- Start M5 with an explicit R environment preflight gate that records the
  selected R runtime and stops before scientific comparison unless the runtime is
  project-scoped or explicitly reviewer-approved.

## Chosen Option

Start M5 with an explicit R environment isolation/preflight gate.

## Rationale

The project needs the REddyProc output to be a controlled reference, not an
accidental artifact of whichever R installation `rpy2` finds first. A preflight
gate keeps the current optional-backend design while making environment capture
auditable before scientific comparison begins.

## Impact

- The first M5 coding prompt must implement or run a preflight that records:
  R executable or launch path, `R.home()`, R version, `.libPaths()`,
  `packageVersion("REddyProc")`, and `rpy2` version.
- The M5 comparison must not run against an unapproved personal/global R
  installation. If no controlled or explicitly approved R environment is
  available, M5 should produce a setup/preflight report and stop before
  scientific comparison.
- The live `pytest -m reddyproc` smoke remains useful, but its result is not
  sufficient for M5 scientific acceptance unless the R environment preflight is
  recorded with it.

## Date

2026-04-22

---

## Decision 011: Close Gate M5 Under Non-Parity Framing With Opt-In Lloyd-Taylor Wrapper

## Context

Review 022 accepted the H2 Lloyd-Taylor corrective pass as scope-clean and
recommended owner-authorized closure. Under the fixed four-gate rule in Coding
Prompt 022 the outcome classified as `h2_supported`:

- wrapper Reco Pearson r `0.397` (>= `0.30`),
- Reco r improvement over native `+0.324` (>= `+0.10`),
- wrapper NEE_f r `0.968` (>= `0.965`),
- wrapper GPP r `0.873` (>= `0.70`).

Residual non-parity signals remain and must be recorded rather than hidden.

## Options Considered

- Open one further corrective coding pass (gap-fill wrapper, longer window,
  or second-site validation) before closure.
- Accept owner-authorized closure under Decision 009's non-parity framing,
  recording the residual limitations explicitly.

## Chosen Option

Close Gate M5 under Decision 009's non-parity framing, with the closure-track
defaults and limitations recorded below.

## Closure-Track Defaults

The supported RBMNN closure configuration is:

- `drop_rain_rows=False`
- `HessefluxConfig.partition_method="lasslop"`
- `HessefluxConfig.swthr=20.0`
- `HessefluxConfig.nogppnight=False`
- `HessefluxConfig.reco_fit_mode="native"` (default; unchanged)

## Reco-Fit-Mode Policy

- `reco_fit_mode="native"` remains the default for backwards compatibility.
- `reco_fit_mode="lt_reddyproc_aligned"` is the **recommended opt-in** for
  REddyProc-comparability studies. It replaces `hf.nee2gpp` with a deterministic
  Lloyd-Taylor fit on nighttime `NEE_fqc==0` rows, using REddyProc's standard
  constants (`Tref=15 °C`, `T0=-46.02 °C`).
- No silent fallback: `LTWrapperError` propagates on every failure mode
  (insufficient samples, invalid temperature domain, optimizer failure,
  boundary-bound solution).

## Accepted Limitations

1. **Reco magnitude non-parity**. Wrapper-vs-REddyProc OLS slope ~`2.15` on the
   RBMNN closure track — wrapper Reco underestimates REddyProc Reco by roughly
   half. Pearson correlation is improved; absolute magnitude scaling is not
   parity. Decision 009's "not parity" framing continues to apply.
2. **NaN-outside-LT-domain contract**. In `lt_reddyproc_aligned` mode the
   wrapper returns NaN for Reco where `Tair_f` is outside the LT temperature
   domain (observed ~`23%` of rows in the RBMNN run — finite paired Reco rows
   drop from `7552` to `5783`). Downstream consumers must handle NaN
   explicitly; the wrapper intentionally does not extrapolate.
3. **Dataset/window-specific LT fit**. The fit on the RBMNN closure track used
   only `149` nighttime high-quality samples (Rref=`4.13`, E0=`140.2`).
   Parameters are not portable across sites or windows and must be refit per
   new deployment.
4. **Diagnostic API change** (hesseflux backend only). `diag["partitioning"]`
   is now a dict with keys `method`, `reco_fit_mode`, `lt_wrapper` (was a
   string). External consumers must read `diag["partitioning"]["method"]` for
   the legacy string. The reddyproc backend's diagnostic shape is unchanged.

## Operational Guidance

When a REddyProc reference is available for a given deployment, callers
consuming hesseflux Reco should compute a comparability health-check and
surface a warning if Reco scaling (OLS slope, Pearson r against REddyProc)
drifts materially from the recorded RBMNN closure-track band (`r ~ 0.40`,
`OLS slope ~ 2.15`). This is guidance, not a mandatory guard; the hesseflux
backend does not call REddyProc internally.

## Impact

- Gate M5 is closed. Subsequent M5 artifact regeneration is not a gate
  prerequisite; it is a routine maintenance concern governed by M6.
- M6 (Documentation and CI Hardening) is the next active milestone and may
  open immediately.
- No further scientific corrective loops are scheduled against the RBMNN
  single-site, single-window comparison. Additional deployments or longer
  windows would trigger a reassessment, not a reopen of this closure.

## Date

2026-04-24

---

## Decision 012: Prompt Length Soft Cap (~300 Lines, Short And Specific)

## Context

Coding and review prompts under `prompts/` are accumulating bulk as the
project moves through more complex milestones. Recent review prompts
have drifted past 400–600 lines (review 034 ~500, review 035 ~620).
Long prompts dilute the load-bearing invariants, force reviewers to
skim, and turn working checklists into read-only documents that the
prompt-author cannot easily edit again later.

Some prompts genuinely need extra detail — multi-classification
fallbacks, multi-backend evidence, IAM tables, copy-pasteable
operator commands — and a hard cap would do more harm than good.

## Options Considered

- No length guidance — let prompts grow as needed.
- Hard cap (reject prompts over N lines).
- Soft cap with documented exceptions.

## Chosen Option

Soft cap of approximately **300 lines** for both coding and review
prompts. Prompts may exceed the cap when the milestone genuinely
requires the extra surface area — in which case the prompt should
be visibly bigger because of evidence, not because of throat-clearing.

## Rationale

- 300 lines fits comfortably in a single editor scroll and stays
  scannable. A reviewer or coding agent can pick up cold without
  wading through redundant project-context recap.
- "Soft" leaves room for honest exceptions (multi-classification
  passes, multi-backend evidence, IAM rollout docs).
- "Specific" is the working principle: prompt authors should drop
  redundant restatement of `CLAUDE.md` / handoff docs / decision-log
  framing and lean on those existing artifacts. The prompt should
  carry only what is unique to *this* milestone.
- The cap targets prompts, not the artifacts the prompts cite —
  `03_experiments/run_summary.md`, `05_governance/review_log.md`, and
  individual review records under `05_governance/reviews/` may still
  carry full evidence detail; the prompt only needs to point at them.

## Acceptable Reasons To Exceed The Cap

- The milestone has multiple classification targets / fallbacks that
  each need explicit framing (M12 / M13 / M14 patterns).
- A load-bearing safety contract needs verbatim listing inline (e.g.
  Decision 010 preflight, IAM table, multi-backend column-preservation
  evidence).
- Operator-runnable commands or copy-pasteable evidence that a
  reviewer cannot reconstruct from the citation alone.

If a prompt exceeds the cap, the prompt author should briefly note
**why** near the top so the reviewer is not surprised — for example:
"Multi-backend evidence; expected ~500 lines."

## Reasons That Do *Not* Justify Exceeding The Cap

- Re-stating `CLAUDE.md` framing the reviewer already loads.
- Restating the framework / artifact-first principles.
- Restating decision-log entries that the prompt could simply cite.
- Verbose introductions or pre-amble before the actual review object.
- Carrying every flag-condition variant when one well-named flag
  covers the same surface.

## Impact

- Future coding/review prompts under `prompts/` should aim for
  ≤300 lines and flag the exception explicitly when they exceed it.
- Existing prompts above 300 lines (review prompts 032 / 033 / 034 /
  035, several earlier corrective prompts) are **not retroactively
  trimmed** — this decision is forward-looking guidance.
- `prompts/README.md` carries a short "Length discipline" reference
  to this decision under its conventions section so prompt authors
  see the rule where they work.
- Verbose prompts can still be accepted by the reviewer; the cap is
  a writing discipline, not a verdict gate.

## Date

2026-05-06
