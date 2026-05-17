# Eddy silver/gold colleague handoff (draft)

This doc is a **non-authoritative draft** for infra colleagues who will
later wrap the accepted M14 eddy silver/gold Docker contract around
BigQuery reads/writes and Cloud Run orchestration.

**It is not a deployment asset.** The Cloud Run Job manifests under
[`cloudrun/`](cloudrun/) and the deployment + IAM material in
[`deployment.md`](deployment.md) and [`../09_ops/runbooks.md`](../09_ops/runbooks.md)
remain the source of truth for cloud rollout. This doc only sketches
the shape colleagues can base new wrappers on.

---

## Responsibility split

| What | Owner |
|---|---|
| eddy module logic (`load_stage1`, `postproc`, engine dispatch) | this repo (`08_pkg/src/miaproc/`) |
| Docker image behavior (`miaproc:cli-r45`) | this repo (`docker/Dockerfile.miaproc-r45-reddyproc`) |
| `miaproc eddy run-silver` / `miaproc eddy run-gold` CLI contract | this repo (`08_pkg/src/miaproc/cli.py`) |
| File-in / file-out I/O contract (CSV / parquet) | this repo (M14) |
| Column-preservation behavior (silver columns survive into gold) | this repo (M14) |
| Cloud Run job definitions, scheduling | infra colleagues |
| IAM, service accounts, project boundaries | infra colleagues |
| BigQuery orchestration around the container | infra colleagues |
| Terraform / IaC codification | infra colleagues |

If a future change pulls cloud responsibility back into this repo, that
needs an explicit decision, not a quiet PR.

---

## Two-stage eddy contract (summary)

The full per-flag contract lives in `miaproc eddy run-silver --help` and
`miaproc eddy run-gold --help`. Treat those as the authoritative
reference; the table below is a quick orientation only.

| Stage | Command | Inputs | Outputs | Engine | Preflight |
|---|---|---|---|---|---|
| **silver** | `miaproc eddy run-silver` | `--flux-dir` + `--biomet-dir` (CSV directories); optional `--group-column site_id` to partition multi-site input | `--output-table` (CSV / parquet) + `--output-run-json` | none — stage-1 only | none — silver does not invoke R |
| **gold** | `miaproc eddy run-gold` | `--silver-table` (CSV / parquet) | `--output-table` (CSV / parquet) + `--output-diagnostics-json` + `--output-run-json` | `--engine` (default `reddyproc-reference`) | project-scoped preflight gates `reddyproc-reference` per Decision 010 / R11 |

Key contract points:

- Silver runs `load_stage1(...)`-equivalent work (cleaning, QC, rain
  filter, 3-sigma mask, regularize to 30-minute grid). It is the input
  contract for gold and for any future cloud wrapper.
- Gold runs `postproc(...)` for the selected engine. The default
  `reddyproc-reference` is the production priority engine
  (rpy2 → R → REddyProc 1.3.4) per Decision 002 / 010 and the M11–M13
  Docker-runtime story; `hesseflux-native` and `hesseflux-ltwrapper`
  are the portable Python-only fallbacks per Decision 003 / 009 / 011.
- Gold preserves silver columns and appends backend analytical
  outputs. The silver-attach helper is idempotent for hesseflux
  (which preserves natively) and load-bearing for `reddyproc-rpy2`
  (which returns a strict ~13-column backend contract). M14 evidence
  is recorded in `03_experiments/run_summary.md` (M14 priority-engine
  block) — do not restate the column-set numbers in cloud wrappers;
  cite that block.
- Decision 010 / R11: `reddyproc-reference` requires
  `--repo-root /app` so the in-image project-scoped preflight can
  resolve the baked `/app/renv.lock` + project-scoped R library.
  **No bypass flag exists; do not add one.**

---

## Local file-based mimic (case-study anchored)

The owner-driven workflow is to mimic the future cloud flow locally
with file inputs/outputs. The realistic source for this is the
case-study data already in the repo at
[`../01_data/case_study/`](../01_data/case_study/) (the same
EddyPro-shaped layout the BigQuery source tables use:
`primary_key`, `timestamp`, `site_id`, plus the standard flux /
biomet columns; multi-site, so `--group-column site_id` partitions
every site present per Decision 008 — M24 removes the prior
`--site-id` CLI flag).

### Step 1: silver from the case-study CSVs

```bash
mkdir -p .runs/m15_handoff
miaproc eddy run-silver \
    --flux-dir 01_data/case_study/flux \
    --biomet-dir 01_data/case_study/biomet \
    --group-column site_id \
    --output-table .runs/m15_handoff/silver.parquet \
    --output-run-json .runs/m15_handoff/silver_run.json
```

M24 grouped run: every non-null site present in the case-study CSVs
is processed independently and the silver outputs are stacked under
`silver.parquet`. Single-site experiments are no longer a CLI
option — pre-filter the CSVs or call package functions
programmatically when you need exactly one site.

### Step 2: gold from the silver above

The Docker default (R-backed REddyProc, the production priority
engine) needs `--repo-root /app` and the in-image project-scoped
preflight:

```bash
docker run --rm \
    -v "$PWD/.runs/m15_handoff:/work" \
    miaproc:cli-r45 \
    miaproc eddy run-gold \
      --silver-table /work/silver.parquet \
      --repo-root /app \
      --output-table /work/gold.parquet \
      --output-diagnostics-json /work/gold_diag.json \
      --output-run-json /work/gold_run.json
```

For Python-only local testing without R, override the engine:

```bash
miaproc eddy run-gold \
    --silver-table .runs/m15_handoff/silver.parquet \
    --engine hesseflux-native \
    --output-table .runs/m15_handoff/gold.parquet \
    --output-diagnostics-json .runs/m15_handoff/gold_diag.json \
    --output-run-json .runs/m15_handoff/gold_run.json
```

`.csv` and `.parquet` work on both stages. Parquet preserves tz-aware
`DateTime` cleanly; CSV roundtrips re-normalize `DateTime` to tz-aware
UTC on read.

---

## Draft cloud wrapper shapes (non-authoritative examples)

These are **commented sketches** for colleagues. They are not Cloud
Run YAML manifests, not deployment commits, and not tested in cloud.
Use them as a starting point for an actual `google_cloud_run_v2_job`
or equivalent.

### Draft: silver wrapper (BigQuery source tables → silver staging artifact)

```bash
# DRAFT — colleague-facing example. Not a deployment asset.
#
# Wrapper responsibilities (owned by infra colleagues):
#   - decide whether silver inputs are exported from BigQuery to
#     CSV/parquet first (GCS-backed, gcsfuse-mounted) or fed in via
#     a future BigQuery-native silver path (would require this repo
#     to add load_stage1_from_bigquery — out of scope today).
#   - mount the staged inputs into the container.
#   - mount a writable staging bucket / volume for the silver
#     artifact + run JSON.
#   - capture exit code; non-zero = job failure.
#
# Container responsibilities (this repo):
#   - run miaproc eddy run-silver against the mounted inputs.
#   - write silver table + run JSON to the mounted output dir.
#
docker run --rm \
    -v "/mnt/inputs:/inputs:ro" \
    -v "/mnt/silver_stage:/out" \
    miaproc:cli-r45 \
    miaproc eddy run-silver \
      --flux-dir /inputs/flux \
      --biomet-dir /inputs/biomet \
      --group-column site_id \
      --output-table /out/silver.parquet \
      --output-run-json /out/silver_run.json
```

### Draft: gold wrapper (silver staging artifact → gold final artifact)

```bash
# DRAFT — colleague-facing example. Not a deployment asset.
#
# Wrapper responsibilities (owned by infra colleagues):
#   - mount the silver artifact written by the silver wrapper above
#     (read-only).
#   - mount a writable final bucket / volume for the gold artifact +
#     diagnostics + run JSON.
#   - capture exit code (0 success, 2 preflight not project-scoped
#     approved, 3 input/config validation failure, 4 runtime
#     processing failure — same M6+ exit-code semantics).
#   - propagate the run JSON + diagnostics to the orchestrator's
#     metadata channel for downstream auditing.
#
# Container responsibilities (this repo):
#   - run miaproc eddy run-gold against the mounted silver artifact.
#   - default engine is reddyproc-reference; project-scoped preflight
#     gates this per Decision 010 / R11 — do not bypass.
#   - --repo-root /app keeps the in-image baked renv.lock +
#     project-scoped R library in scope for the preflight.
#
docker run --rm \
    -v "/mnt/silver_stage:/silver:ro" \
    -v "/mnt/gold_final:/out" \
    miaproc:cli-r45 \
    miaproc eddy run-gold \
      --silver-table /silver/silver.parquet \
      --repo-root /app \
      --output-table /out/gold.parquet \
      --output-diagnostics-json /out/gold_diag.json \
      --output-run-json /out/gold_run.json
```

### Pseudo-Cloud-Run shape (orientation only)

A future `google_cloud_run_v2_job` per stage might look roughly like
the existing M11/M12 single-job manifests under
[`cloudrun/`](cloudrun/), but with a **single CLI command per job**
(`run-silver` for the silver job, `run-gold` for the gold job)
instead of the current single-job `run-bigquery` pattern. The image
pin, dedicated service account, and preflight gating from M12/M13
carry over unchanged. IAM remains
[`cloudrun/iam-grants-required.md`](cloudrun/iam-grants-required.md)
plus whatever read/write surface the colleagues' chosen storage
boundary (GCS bucket vs. BigQuery stage table) requires.

This repo does **not** ship those two-job manifests. Authoring
them is on the colleagues' track.

---

## Column-preservation contract

Silver-to-gold column preservation is the load-bearing M14 product
guarantee:

- Silver output preserves the joined input columns produced by
  `load_stage1(...)` (the existing accepted stage-1 contract).
- Gold output preserves all silver columns (LEFT-joined on
  `DateTime`) and appends backend analytical outputs. The
  silver-attach helper preserves `df.attrs["miaproc_diagnostics"]`
  across the merge.
- For the priority `reddyproc-reference` engine, the helper does
  load-bearing work (the rpy2 backend returns a strict ~13-column
  contract; the helper appends the remaining silver-only columns
  on top). For the hesseflux backends, the helper is an idempotent
  no-op (hesseflux preserves natively).

Recorded numerical evidence lives in
[`../03_experiments/run_summary.md`](../03_experiments/run_summary.md)
under the M14 block ("Priority-engine end-to-end evidence" addendum).
Cloud wrappers do not need to re-prove this; cite the M14 evidence.

---

## Cross-references

- M14 implementation + evidence:
  [`../03_experiments/run_summary.md`](../03_experiments/run_summary.md),
  [`../05_governance/review_log.md`](../05_governance/review_log.md),
  [`../05_governance/reviews/review_m14_eddy_docker_silver_gold_split.md`](../05_governance/reviews/review_m14_eddy_docker_silver_gold_split.md)
  (when filed).
- Docker runtime + smoke recipes:
  [`../docker/README.md`](../docker/README.md).
- Package / CLI reference:
  [`../08_pkg/README.md`](../08_pkg/README.md).
- Cloud Run Job manifests + helper scripts (M11–M13 single-job
  shape, the colleagues' starting reference for SA / image /
  preflight): [`cloudrun/`](cloudrun/),
  [`deployment.md`](deployment.md),
  [`cloudrun/iam-grants-required.md`](cloudrun/iam-grants-required.md),
  [`../09_ops/runbooks.md`](../09_ops/runbooks.md).
- Decisions to preserve in any cloud wrapper:
  [`../05_governance/decision_log.md`](../05_governance/decision_log.md)
  (Decisions 009, 010, 011);
  [`../05_governance/risks.md`](../05_governance/risks.md)
  (R2, R9, R10, R11).
