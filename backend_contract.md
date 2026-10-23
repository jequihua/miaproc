# Backend Contract

## Public Dispatch

The package should expose a stable dispatch point similar to:

```python
postproc(df, engine="hesseflux", config=...)
```

Supported engines:

- `hesseflux`: portable Python-native backend.
- `reddyproc-rpy2`: optional REddyProc parity backend.

Backend names are part of the public API and should not change casually.

## Backend Modes

### hesseflux

Required u* modes:

- `fixed`: legacy mode; caller supplies an explicit threshold.
- `dynamic`: standard long-series mode; threshold or scenario is derived from data.

Recommended partitioning default for standard long-series dynamic mode:

- `partition_method="lasslop"`

Reco-fit modes (Decision 011):

- `reco_fit_mode="native"` (**default**): `hf.nee2gpp(method=...)` is called
  directly. Behavior unchanged from pre-M5 releases. Recommended for
  portable Python-only deployments.
- `reco_fit_mode="lt_reddyproc_aligned"` (**opt-in, comparability**): skips
  `hf.nee2gpp` entirely; fits a deterministic Lloyd-Taylor model on
  nighttime `NEE_fqc==0` rows using REddyProc's constants
  (`Tref=15 °C`, `T0=-46.02 °C`), predicts `Reco` from `Tair_f`, and
  derives `GPP = Reco - NEE_f`. Intended for REddyProc-comparability
  studies. See `docs/m5_reddyproc_hesseflux_magnitude_note.md` for the
  M5 closure-track results.

Wrapper-mode contract caveats:

- Wrapper returns `NaN` for Reco where `Tair_f` is outside the LT
  temperature domain; no extrapolation.
- On any fit failure (insufficient nighttime samples, invalid domain,
  optimizer failure, boundary-bound solution) the backend raises
  `LTWrapperError`. There is no silent fallback to native behavior.
- Fitted parameters (`Rref`, `E0`) are dataset/window-specific and must
  be refit per new site or window.

### reddyproc-rpy2

This backend should mirror `R_manglaria.R`:

- `sEstimateUstarScenarios(nSample=200, probs=c(0.05, 0.5, 0.95))`
- `sMDSGapFill("Rg", FillAll=TRUE)`
- `sMDSGapFill("VPD", FillAll=TRUE)`
- `sMDSGapFill("Tair", FillAll=TRUE)`
- `sMDSGapFillUStarScens("NEE", FillAll=TRUE)`
- `sGLFluxPartition(...)`

`rpy2` does not install or sandbox R. It binds to an existing R installation.
Any case-study reference run must therefore begin with an explicit R environment
preflight before the backend is treated as scientifically authoritative. The
preflight must record the selected R executable or launch path, `R.home()`,
R version, `.libPaths()`, `REddyProc` version, and `rpy2` version. If the
environment is a personal/global R installation rather than a project-scoped or
reviewer-approved environment, M5 must stop and report that state instead of
silently using it.

## Required Configuration

Backends must accept explicit site metadata:

- `site_name`
- `latitude`
- `longitude`
- `timezone_hour`
- `dts`, default `48`

Dynamic u* configuration:

- `ustar_probs`, default `(0.05, 0.5, 0.95)`
- `ustar_n_sample`, default `200`
- `ustar_scenario`, default median or `U50`
- `swthr`, default `10.0`
- deterministic seed or test hook where practical

## Required Output Columns

All backends must return a pandas DataFrame containing:

- `DateTime`
- `NEE`
- `NEE_f`
- `NEE_fqc`
- `GPP`
- `Reco`
- `Tair`
- `Tair_f`
- `Rg`
- `Rg_f`
- `VPD`
- `VPD_f`
- `USTAR`

Additional backend-specific columns are allowed if documented.

## Diagnostics

The backend result must expose diagnostics either as a structured attribute,
sidecar object, or returned dataclass. Diagnostics must include:

- backend name.
- package/library versions when available.
- u* mode.
- selected u* scenario.
- selected u* threshold.
- all available u* scenario thresholds.
- fraction of NEE filtered by u*.
- partitioning method.
- site metadata.
- warnings and fallback decisions.

### Hesseflux diagnostic shape (post-Decision 011)

The hesseflux backend's `diag["partitioning"]` is a **dict** with keys:

- `method`: the legacy partition method string (`"reichstein"`, `"lasslop"`,
  or `"falge"`) — consumers that previously read
  `diag["partitioning"] == "lasslop"` must now read
  `diag["partitioning"]["method"]`.
- `reco_fit_mode`: `"native"` or `"lt_reddyproc_aligned"`.
- `lt_wrapper`: `None` in native mode; in wrapper mode a dict carrying
  `fit_status`, `n_night_samples`, `rref`, `e0`, `night_fit_rmse`,
  `tref_c`, `t0_c`, and `bounds`.

The reddyproc-rpy2 backend's `diag["partitioning"]` remains a string (no
wrapper concept on that side).

Do not hide fallback behavior. If dynamic u* estimation fails, report the failure
explicitly.

## Errors

Required error behavior:

- Missing required columns: raise a clear validation error.
- Missing optional R dependencies: raise an actionable optional-dependency error.
- Dynamic u* cannot be estimated: raise a domain-specific error or return a failed
  diagnostic according to the public API design.
- Unsupported backend: raise a clear backend-selection error.

## Source-Facing Payload Helpers

The transport-neutral silver/gold payload logic lives in
`miaproc.eddy.payload`, which imports only pandas and `miaproc.eddy.constants`
(no cloud SDK client, no credential lookup):

- `apply_silver_source_truth_rename` / `silver_to_internal_calc_frame`: the
  nine internal-to-final mappings in `SILVER_INTERNAL_TO_FINAL_RENAME`
  (`DateTime -> timestamp`, `NEE -> co2_flux`, `QC_NEE -> qc_co2_flux`,
  `Tair -> air_temperature_c`, `USTAR -> u_star`, `VPD -> VPD_hpa`,
  `Rg -> SWIN_1_1_1`, `P_RAIN -> P_RAIN_1_1_1`, `rH -> RH_1_1_1`) and their
  reverse; `timestamp` wins when both time columns are present.
- `validate_source_columns_unique`, `ensure_unique_stage_columns` and
  `column_field_key`: column uniqueness with the deterministic
  `rH` / `rH_norm_s` humidity policy; other duplicates raise
  `DuplicateStageColumnsError`.
- `prepare_local_silver_payload` / `prepare_local_gold_payload`: the
  source-facing payloads that `eddy run-silver` / `eddy run-gold` write to
  Parquet. Exactly one `timestamp` and no `DateTime`; source
  `primary_key`/`site_id` are carried unchanged (type and values, never
  synthesized; gap rows keep the uniform group `site_id`, a null
  `primary_key` and missing measurements); literal duplicates are resolved
  with the humidity policy before ordering; gold uses the lineage-CSV
  `gold_derived` names (`GOLD_DERIVED_RENAME`: `NEE_f -> nee_f`,
  `NEE_fqc -> nee_fqc`, `Rg_f -> sw_in_f`, `Tair_f -> ta_f`,
  `VPD_f -> vpd_f`) plus `dateAndTime`, drops internal passthroughs whose
  source-facing name is present and keeps every silver column. The file
  contract is [`docs/PARQUET_FILE_SCHEMA.md`](docs/PARQUET_FILE_SCHEMA.md).
- `prepare_silver_stage_payload` / `prepare_gold_stage_payload` (=
  `derive_gold_stage_columns` then `finalize_gold_stage_payload`): the
  older stage-table payload shape with a synthesized
  `(primary_key = <site_id>|<UTC second>, site_id, timestamp)` identity
  triple. They remain as pure helpers with their historical behavior; the
  file workflow does not use them because C1 requires source identity to be
  carried unchanged.

M002-S04 removed the BigQuery transport (`miaproc.eddy.bigquery_runner`,
`miaproc.eddy.bigquery_writeback`, their re-exports from `miaproc.eddy`,
the `bigquery` install extra and the `eddy run-bigquery*` commands). No
backend, scientific default or payload rule changed with that removal.

## Storage and CLI Contract (M002)

The CLI stage commands keep their names and engine/site/time/group
arguments; storage is ordinary local or mounted file paths:

- Inputs: CSV directories (`--flux-dir`/`--biomet-dir`, legacy
  compatibility) or exactly one explicit Parquet file per side
  (`--flux-parquet`/`--biomet-parquet`, `.parquet`/`.pq`, read with PyArrow).
  Gold reads `--silver-table` (`.csv` or one Parquet file). URI schemes such
  as `gs://` or `s3://` are rejected; mount the storage and pass the path.
  Inputs are never modified.
- Outputs: `.parquet`/`.pq` writes the source-facing contract
  ([`docs/PARQUET_FILE_SCHEMA.md`](docs/PARQUET_FILE_SCHEMA.md)); `.csv`
  keeps the legacy CSV shape (D014).
- Publication ([`docs/RUN_PUBLICATION.md`](docs/RUN_PUBLICATION.md)): each
  `run-silver`/`run-gold` invocation owns one fresh `--run-dir`; artifacts
  are staged in bounded local scratch, copied sequentially with checked
  close, and the completion manifest `_MIAPROC_RUN_COMPLETE.json` is written
  last. The valid manifest is the publication point. `run-gold` validates
  the silver run's manifest; `miaproc eddy validate-run --run-dir DIR
  [--stage silver|gold]` is the consumer check. A legacy CSV silver without
  a manifest needs `--allow-legacy-silver-csv`.
- Exit codes: `0` success; `2` `reddyproc-reference` preflight not
  project-scoped approved (also argparse usage errors); `3` input/config
  validation, run-directory reuse/overlap/traversal, incomplete or
  mismatched silver run, and any removed BigQuery command or flag; `4`
  runtime processing failure, including any publication fault before the
  manifest; `5` the manifest was written but the finish could not be
  confirmed (the run is published and never deleted; decide with
  `validate-run`).
- Input windows, rerun scheduling and any loading of results into a
  warehouse are caller/orchestrator-owned; see
  [`docs/MIGRATING_FROM_BIGQUERY.md`](docs/MIGRATING_FROM_BIGQUERY.md).

## Compatibility

Exact equality between `reddyproc-rpy2` and `hesseflux` outputs is not required.
Column presence, units, sign conventions, and diagnostics are required.

Per Decision 009 (hesseflux is REddyProc-inspired, not parity) and
Decision 011 (M5 closure), the M5 comparison on the RBMNN case-study
track reported:

- `NEE_f`: strong agreement (Pearson r ~ 0.97, both modes).
- `GPP`: native r ~ 0.73; wrapper r ~ 0.87.
- `Reco`: native r ~ 0.07; wrapper r ~ 0.40; wrapper-vs-REddyProc OLS
  slope ~ 2.15 (magnitude still non-parity).

See [`docs/m5_reddyproc_hesseflux_magnitude_note.md`](../docs/m5_reddyproc_hesseflux_magnitude_note.md)
for the full closure-track comparison and
[`05_governance/decision_log.md`](../05_governance/decision_log.md)
Decision 011 for accepted limitations.
