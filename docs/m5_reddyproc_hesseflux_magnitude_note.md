# M5 Note: REddyProc vs hesseflux Magnitude Differences

## Purpose

This note summarizes:

1. How the M5 comparison was executed.
2. What magnitude differences were observed.
3. Which implementation/scientific differences plausibly explain those gaps.

Scope is the RBMNN comparison track used in the latest H2 corrective artifacts.

## Comparison Setup

### Data and frame

- Site: `RBMNN`
- Frame size: `7552` rows on 30-min grid
- DateTime range: `2025-08-27T23:30:00+00:00` to `2026-02-01T07:00:00+00:00`
- Duplicate DateTime rows: `0`
- Track setting used: `drop_rain_rows=False` (extended-support track)

### Environment gate

Preflight was run before execution and recorded:

- `Status: ok`
- `Approval source: project-scoped`
- `R: 4.5.3`
- `REddyProc: 1.3.4`
- `rpy2: 3.6.7`

### Backends and alignment protocol

The H2 runner (`03_experiments/m5_case_study_comparison/run_h2_ltwrapper_corrective.py`) computed one REddyProc reference and two hesseflux variants:

1. `native` (`reco_fit_mode="native"`, `hf.nee2gpp(method="lasslop")`)
2. `lt_reddyproc_aligned` (Lloyd-Taylor wrapper for Reco)

Pairing was done by inner join on UTC `DateTime`; metrics were computed on paired-finite rows per variable (`NEE_f`, `GPP`, `Reco`), including Pearson r, Spearman rho, RMSE, bias, and OLS slope/intercept.

## Current Findings (Latest H2 Pass)

### Headline metrics vs REddyProc

Native hesseflux:

- `NEE_f`: r `0.9684`, RMSE `1.9788`, n `7552`
- `GPP`: r `0.7254`, RMSE `5.7830`, n `2562`
- `Reco`: r `0.0733`, RMSE `10.4080`, n `7552`, OLS slope `0.0246`

Wrapper hesseflux (`lt_reddyproc_aligned`):

- `NEE_f`: r `0.9684`, RMSE `1.9788`, n `7552`
- `GPP`: r `0.8733`, RMSE `4.3235`, n `5783`
- `Reco`: r `0.3973`, RMSE `4.1167`, n `5783`, OLS slope `2.1535`

Delta (wrapper minus native) on Reco:

- Pearson r: `+0.3240`
- RMSE: `-6.2913`
- OLS slope: `+2.1289`

### Gate result

Under the fixed H2 rule, outcome was `h2_supported` because:

- Wrapper Reco r `0.3973 >= 0.30`
- Reco improvement `+0.3240 >= +0.10`
- Wrapper `NEE_f` r `0.9684 >= 0.965`
- Wrapper `GPP` r `0.8733 >= 0.70`

### Important residual limitation

- Reco magnitude scaling is still non-parity: OLS slope `2.1535` implies wrapper Reco remains systematically scaled vs REddyProc.
- Wrapper Reco is finite on `5783/7552` rows (about 76.6%); remaining rows are NaN outside LT domain (intentional contract).
- LT fit used `149` nighttime samples in this dataset/window.

## Implementation Comparison (Why Outputs Differ)

### Ustar estimation

REddyProc path:

- Seasonal/bootstrap scenario machinery (`U05/U50/U95` by season).
- Reported `selected_threshold=None` when seasonal thresholds vary.

hesseflux path:

- Deterministic plateau estimator (`hesseflux-plateau-v1`) with one selected threshold.
- In this run, `U50 ~ 0.2855`, notably above REddyProc seasonal `U50` values (~0.11-0.13).

### Gap-filling path

REddyProc:

- `sMDSGapFill` + `sMDSGapFillUStarScens` sequence.

hesseflux:

- `hf.gapfill` + internal filtering conventions.

Even before partitioning, prior diagnostics showed nontrivial `NEE_f` exact-value mismatch (not correlation failure), indicating backend-internal path differences remain.

### Partitioning path

Native hesseflux:

- `hf.nee2gpp(method="lasslop")` directly.

Wrapper mode:

- Fit REddyProc-form Lloyd-Taylor constants (`Tref=15`, `T0=-46.02`) on nighttime high-quality rows.
- Predict Reco on valid LT domain; compute `GPP = Reco - NEE_f`.
- Raise on fit failures (no silent fallback).

This wrapper improved Reco comparability substantially, but not to magnitude parity.

## Backend Science Interpretation

### What is now ruled out

Earlier option-4 evidence (`rbmnn_rpy2_vs_r_only_reference_summary.json`) showed rpy2 vs independent R-only REddyProc agreement was high:

- `NEE_f` r `0.9993`
- `GPP` r `0.9957`
- `Reco` r `0.9284`

So the remaining discrepancy is not primarily an rpy2 bridge artifact.

### Most likely drivers of magnitude differences

1. Different u* threshold construction (seasonal/bootstrap vs deterministic plateau).
2. Different gap-filling mechanics feeding partitioning.
3. Different partitioning implementations and objective surfaces.
4. Limited nighttime support (`n_night_samples=149`) for LT fit in this window.
5. Domain-limited LT prediction (NaNs outside valid temperature domain), changing paired row set and effective comparison population.

## Practical Takeaways

- The comparison now supports a **comparability claim** for Reco improvement (H2 supported), not strict parity.
- `NEE_f` and `GPP` agreement are strong on this track; Reco is materially improved but still magnitude-shifted.
- For closure/operations, any decision should explicitly document:
  - non-parity in Reco magnitude (slope ~2.15),
  - wrapper NaN-outside-domain behavior,
  - dataset/window-specific fit caveat.

## Primary Artifacts

- `03_experiments/m5_case_study_comparison/rbmnn_h2_ltwrapper_summary.json`
- `03_experiments/m5_case_study_comparison/rbmnn_h2_ltwrapper_report.md`
- `03_experiments/m5_case_study_comparison/run_h2_ltwrapper_corrective.py`
- `03_experiments/m5_case_study_comparison/rbmnn_reco_root_cause_summary.json`
- `03_experiments/m5_case_study_comparison/rbmnn_h1_byte_identical_summary.json`
- `03_experiments/m5_case_study_comparison/rbmnn_rpy2_vs_r_only_reference_summary.json`
