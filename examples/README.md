# miaproc examples

Runnable example workflows for new developers. Three entry points, one per
run mode:

| # | Script | Run mode | Backend |
|---|---|---|---|
| 01 | [`01_python_only_hesseflux_dynamic.py`](01_python_only_hesseflux_dynamic.py) | Python-only | `hesseflux` (dynamic u*, native Reco) |
| 02 | [`02_hesseflux_ltwrapper_comparability.py`](02_hesseflux_ltwrapper_comparability.py) | Python-only | `hesseflux` (dynamic u*, **opt-in** Lloyd-Taylor Reco wrapper) |
| 03 | [`03_r_backed_reddyproc_reference.py`](03_r_backed_reddyproc_reference.py) | R-backed | `reddyproc-rpy2` (REddyProc reference) |

All examples:

- take `--flux-dir` and `--biomet-dir` as CLI arguments; no machine-specific
  paths are hard-coded,
- accept `--site-id` (default `RBMNN` per Decision 008),
- print a compact diagnostics summary at the end,
- **do not write output data files**. If you want derived outputs, pipe the
  example script or extend it yourself.

If you want a non-interactive CLI that also **writes** machine-readable
artifacts (processed table + diagnostics JSON + run-metadata JSON) for
batch or cloud execution, use the `miaproc` console script described
in [`../README.md`](../README.md) ("CLI (production, non-interactive)")
and the bundled Docker profile in [`../../docker/README.md`](../../docker/README.md).
The CLI's three `--engine` modes (`hesseflux-native`,
`hesseflux-ltwrapper`, `reddyproc-reference`) line up 1:1 with examples
01–03 below.

## Install

See [`../README.md`](../README.md) for the full install matrix. Minimum:

```bash
pip install -e "08_pkg[dev,hesseflux]"        # Python-only examples 01/02
pip install -e "08_pkg[dev,hesseflux,reddyproc]"  # adds R-backed example 03
```

## Required input shape

All examples load stage-1 via `miaproc.eddy.load_stage1`. The flux and biomet
CSV folders must have at least these columns (see
[`../backend_contract.md`](../backend_contract.md) and
[`../../01_data/schema.md`](../../01_data/schema.md)):

- flux: `DateTime` (or `date` + `time`), `NEE`, `USTAR` (or `u_star`),
  `QC_NEE`, `Tair`, `VPD`
- biomet: `DateTime` (or `date` + `time`), `Rg`, `P_RAIN`, `rH`

Units expected internally: `Tair` °C, `VPD` hPa, `Rg` W m⁻², `USTAR` m s⁻¹.

## Example invocation

```bash
# 01 — native hesseflux dynamic u*
python examples/01_python_only_hesseflux_dynamic.py \
  --flux-dir /path/to/flux \
  --biomet-dir /path/to/biomet \
  --site-id RBMNN

# 02 — opt-in Lloyd-Taylor wrapper for REddyProc-comparability studies
python examples/02_hesseflux_ltwrapper_comparability.py \
  --flux-dir /path/to/flux \
  --biomet-dir /path/to/biomet

# 03 — R-backed REddyProc reference (requires scoped R_HOME + renv)
R_HOME="/path/to/R-4.5.3" \
  python examples/03_r_backed_reddyproc_reference.py \
    --flux-dir /path/to/flux \
    --biomet-dir /path/to/biomet \
    --repo-root /path/to/miaproc-dev
```

## Scientific framing

Per [Decision 009](../../05_governance/decision_log.md) and
[Decision 011](../../05_governance/decision_log.md), the hesseflux backend is
REddyProc-**inspired**, not parity. The wrapper mode (example 02) improves
Reco correlation with REddyProc on the RBMNN closure track but does not
produce magnitude parity — wrapper-vs-REddyProc OLS slope is ~2.15. See
[`../../docs/m5_reddyproc_hesseflux_magnitude_note.md`](../../docs/m5_reddyproc_hesseflux_magnitude_note.md)
for the full comparison.

Wrapper mode:

- is **opt-in**; default `reco_fit_mode="native"` is unchanged,
- returns `NaN` for Reco outside the Lloyd-Taylor temperature domain
  (no extrapolation),
- raises `LTWrapperError` on any fit failure with **no silent fallback** to
  native.
