# Biomass colleague handoff (draft)

This doc is a **non-authoritative draft** for infra colleagues wrapping
the accepted biomass Docker contract into Cloud Run orchestration. The
current cloud-shaped entry point is `miaproc biomass run-bigquery`:
M19 reads a forest-structure table directly from BigQuery and M20 can
optionally write a validated stage table, then MERGE only when the
operator passes `--bq-allow-final-merge`.

**It is not a deployment asset.** The deployment + IAM material in
[`deployment.md`](deployment.md), the Cloud Run Job manifests under
[`cloudrun/`](cloudrun/), and the operator runbook in
[`../09_ops/runbooks.md`](../09_ops/runbooks.md) remain the source of
truth for cloud rollout. This doc only sketches the shape colleagues
can base new biomass wrappers on.

This is the biomass parallel to the eddy handoff in
[`eddy_colleague_handoff_drafts.md`](eddy_colleague_handoff_drafts.md).

---

## Responsibility split

| What | Owner |
|---|---|
| biomass equation selection (M16 unified parquet, dataset filter) | this repo (`08_pkg/src/miaproc/biomass/`) |
| species normalization + alias map (M17A two known typos) | this repo (`equations.py`) |
| adult-life-stage + non-null `dbh_cm` eligibility behavior | this repo (`api.py`) |
| row-preserving `enrich_table` library helper | this repo (`api.py`) |
| `miaproc biomass enrich-table` CLI contract | this repo (`cli.py`) |
| `miaproc biomass run-bigquery` read + optional stage/merge contract | this repo (`cli.py`, `biomass/bigquery_io.py`, `biomass/bigquery_writeback.py`) |
| Docker image behavior (`miaproc:cli-r45`) | this repo (`docker/Dockerfile.miaproc-r45-reddyproc`) |
| Output column contract (`biomass_estimate`, `equation_used`) | this repo (M17) |
| Cloud Run job definitions, scheduling | infra colleagues |
| IAM, service accounts, project boundaries | infra colleagues |
| Establishing source, stage, final, and merge-key table names | infra colleagues + operators |
| Terraform / IaC codification | infra colleagues |

If a future change pulls cloud responsibility back into this repo,
that needs an explicit decision, not a quiet PR.

Biomass needs **no R** — the M16 / M17 / M17A code is pure Python.
The Decision 010 project-scoped R preflight does **not** apply to
biomass; it stays gated only for `reddyproc-reference` eddy gold.

---

## Accepted biomass contract (summary)

The full per-flag contracts live in
`miaproc biomass enrich-table --help` and
`miaproc biomass run-bigquery --help`. Treat those as authoritative;
the table below is a quick orientation only.

| Stage | Command | Inputs | Outputs |
|---|---|---|---|
| **enrich-table** | `miaproc biomass enrich-table` | `--input-table` (CSV / parquet / pq) | `--output-table` (CSV / parquet / pq) + `--output-run-json` |
| **run-bigquery** | `miaproc biomass run-bigquery` | `--bq-input-project` / `--bq-input-dataset` / `--bq-input-table` | local enriched table + run JSON; optional BigQuery stage write; optional explicit MERGE |

Required flags: `--input-table`, `--output-table`, `--output-run-json`.
For `run-bigquery`, required read/local flags are
`--bq-input-project`, `--bq-input-dataset`, `--bq-input-table`,
`--output-table`, and `--output-run-json`.

Optional flags (defaults shown): `--dataset dina` (use `"infys"` for
volume rows or empty string to disable), `--equations-path` (omit
for packaged M16), `--state`, `--response-variable`, the
BiomassColumns input mapping `--species-col species` /
`--dbh-col dbh_cm` / `--height-col tree_height_m` /
`--life-stage-col life_stage`, and the output naming
`--biomass-estimate-col biomass_estimate` /
`--equation-used-col equation_used`.

Exit codes: `0` success / `3` validation failure / `4` runtime
processing failure.

M20 writeback is deliberately opt-in:

- `--bq-stage-table` engages the writeback path and requires
  `--bq-output-project`, `--bq-output-dataset`, and
  `--bq-control-dataset`.
- stage-only is the safe default; it writes and validates the stage
  table and records `status = "stage_only_succeeded"`.
- `--bq-allow-final-merge` plus `--bq-final-table` is required for any
  final-table mutation.
- biomass has no watermark table; the control surface records
  `cf_biomass_runs`.

---

## Input / output table contract

- **Original rows preserved** — same row count and same row order in
  the output table; no row dropping, no destructive filtering.
- **Original columns preserved in original order** — the input
  table is copied verbatim to the output.
- **Exactly two appended columns** at the end of the output:
  - `biomass_estimate` — numeric, in the equation's response units
    (kg for the M16 `dina` direct-biomass equations).
  - `equation_used` — the matched equation's `source_record_id`
    (e.g. `dina_001`..`dina_004` for the four mangrove species),
    or **null** for ineligible / unmatched rows.
- `equation_used` means **the equation actually applied**, not
  "matched then rejected". When `biomass_estimate` is NaN (any
  reason), `equation_used` is null. M16's full per-row API can
  surface a rejected `source_record_id` for audit, but the M17
  column-shaped output deliberately hides that to keep the
  contract clean for downstream BigQuery / cloud consumers.

Current default input-field names follow
[`../08_pkg/docs/forest_data_schema.csv`](../08_pkg/docs/forest_data_schema.csv):
`species`, `dbh_cm`, `tree_height_m`, `life_stage`. These are
**defaults, not rigid hard-codes** — colleagues with different
table schemas can remap via the per-column CLI flags above.

Eligibility rules for the default `--dataset dina`:

- `dbh_cm` is **always required**. Missing →
  `match_status="dbh_missing"`, `biomass_estimate=NaN`,
  `equation_used=null`.
- `life_stage` must normalize to `"Adult"` for direct biomass.
  Missing or juvenile → `match_status="life_stage_not_adult"`,
  `biomass_estimate=NaN`, `equation_used=null`.
- `tree_height_m` is **optional** for direct-biomass dina rows
  (the wd-fixed expressions reference `diam` only).

For volume (`--dataset infys`), the equations reference both
`diam` and `alt`; height becomes required.

---

## Species normalization (M17A)

Inbound species values pass through two deterministic steps before
matching:

1. `_normalize_species` strips whitespace, lowercases, and collapses
   internal whitespace.
2. A small **explicit alias map** in
   `08_pkg/src/miaproc/biomass/equations.py` resolves the two
   known mangrove typos surfaced by the M17 fixture-refresh
   evidence:

   ```
   "rizophora mangle"  -> "rhizophora mangle"
   "rizophora manlge"  -> "rhizophora mangle"
   ```

This is **not** broad fuzzy matching:

- no Levenshtein / Jaro-Winkler / phonetic dependency,
- no probabilistic nearest-species logic,
- the alias map is exhaustive and lives in source code,
- adding more aliases requires a code edit + a recorded review
  pass (the `test_alias_map_contains_both_known_typos_only`
  test is the audit trigger).

Unknown species and null species still produce
`match_status="no_equation_found"` honestly. Source-side data
cleaning of inbound species values remains the cleanest long-term
fix — the alias map is a pragmatic recovery layer for the
**already-observed** bad spellings.

---

## Local and BigQuery mimics

The file-based command remains useful for local case-study evidence.
The realistic source for this is the case-study data at
[`../01_data/case_study/biomass/forest_structure_biomass_test.csv`](../01_data/case_study/biomass/forest_structure_biomass_test.csv)
(3,457 rows × 29 cols; the same forest-structure shape colleagues
will pass in from BigQuery).

### Host CLI (no Docker, no R needed for biomass)

```bash
mkdir -p .runs/m18_handoff

miaproc biomass enrich-table \
    --input-table 01_data/case_study/biomass/forest_structure_biomass_test.csv \
    --output-table .runs/m18_handoff/enriched.parquet \
    --output-run-json .runs/m18_handoff/run.json
```

### Docker (local mimic)

```bash
mkdir -p .runs/m18_handoff_docker

docker run --rm \
    -v "$PWD/01_data/case_study/biomass:/data:ro" \
    -v "$PWD/.runs/m18_handoff_docker:/out" \
    miaproc:cli-r45 \
    miaproc biomass enrich-table \
      --input-table /data/forest_structure_biomass_test.csv \
      --output-table /out/enriched.parquet \
      --output-run-json /out/run.json
```

On Windows + Git Bash / Docker Desktop, prefix the `docker run`
invocation with `MSYS_NO_PATHCONV=1` so `/data` and `/out` are not
path-translated.

Both invocations write the same shape: a 31-column enriched table
(29 input + 2 appended) and a run-metadata JSON with row counts,
match-status counts, dataset, equations source, and exit code.

### BigQuery-native command (current cloud-shaped path)

The current handoff command is `miaproc biomass run-bigquery`. It
reads the source table directly from BigQuery, writes local artifacts,
and can optionally stage or explicitly merge back into the staging
project. The table names below are placeholders pending operator
establishment of the real biomass source, stage, final, and merge-key
contract.

Stage-only safe default:

```bash
miaproc biomass run-bigquery \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-input-table forest_structure_biomass_SOURCE_PLACEHOLDER \
    --bq-billing-project manglaria-staging \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table biomass_stage_PLACEHOLDER \
    --bq-control-dataset _orch \
    --bq-merge-key primary_key \
    --output-table /tmp/enriched.parquet \
    --output-run-json /tmp/run.json
```

Explicit final-table MERGE:

```bash
miaproc biomass run-bigquery \
    --bq-input-project manglaria \
    --bq-input-dataset manglaria_lakehouse_ds \
    --bq-input-table forest_structure_biomass_SOURCE_PLACEHOLDER \
    --bq-billing-project manglaria-staging \
    --bq-output-project manglaria-staging \
    --bq-output-dataset manglaria_lakehouse_ds \
    --bq-stage-table biomass_stage_PLACEHOLDER \
    --bq-final-table forest_structure_biomass_FINAL_PLACEHOLDER \
    --bq-control-dataset _orch \
    --bq-merge-key primary_key \
    --bq-allow-final-merge \
    --output-table /tmp/enriched.parquet \
    --output-run-json /tmp/run.json
```

---

## Draft cloud-wrapper shape (non-authoritative)

This repo now includes draft biomass Cloud Run YAML examples under
[`cloudrun/`](cloudrun/) for the stage-only and explicit-merge job
shapes. They are staging-oriented handoff drafts, not proof that the
operator table names, service account, or IAM bindings have been
established.

Wrapper responsibilities owned by infra colleagues:

- establish the source, stage, final, and merge-key table names;
- bind a runtime service account with read access to the source table,
  job-user permissions on the billing project, and tightly scoped
  write permissions only in the staging output project;
- keep stage-only and merge jobs separate so routine execution cannot
  accidentally mutate the final table;
- capture non-zero exit codes as job failures and surface the run JSON
  / `cf_biomass_runs` record to operators.

Container responsibilities owned by this repo:

- read the source table through `miaproc biomass run-bigquery`;
- preserve rows and original columns and append exactly
  `biomass_estimate` + `equation_used` by default;
- stage-write only when `--bq-stage-table` is set;
- MERGE only when `--bq-allow-final-merge` is explicitly present;
- exit non-zero if argument validation, stage validation, or runtime
  writeback fails.

---

## Case-study reality note

Live evidence (3,457 rows × 29 cols) lives in
[`../03_experiments/run_summary.md`](../03_experiments/run_summary.md)
under the M17 fixture-refresh addendum and the M17A block:
3,457 in → 3,457 out, 1,799 estimated / 1,658 skipped post-M17A,
11 typo rows recovered (all → `dina_002`), 18 remaining
`no_equation_found` are all null-species, container smoke matches
host smoke. Cloud wrappers do not need to re-prove these; cite
the M17 / M17A records.

---

## Cross-references

- M17 + M17A implementation + evidence:
  [`../03_experiments/run_summary.md`](../03_experiments/run_summary.md),
  [`../05_governance/review_log.md`](../05_governance/review_log.md).
- Docker runtime + smoke recipes:
  [`../docker/README.md`](../docker/README.md).
- Package / CLI reference:
  [`../08_pkg/README.md`](../08_pkg/README.md) (Biomass section).
- Forest-structure incoming-table schema:
  [`../08_pkg/docs/forest_data_schema.csv`](../08_pkg/docs/forest_data_schema.csv).
- Equation parquet schema:
  [`../08_pkg/docs/equation_application_unified.zstd.json`](../08_pkg/docs/equation_application_unified.zstd.json).
- Eddy handoff draft (companion):
  [`eddy_colleague_handoff_drafts.md`](eddy_colleague_handoff_drafts.md).
- Decisions to preserve in any cloud wrapper:
  [`../05_governance/decision_log.md`](../05_governance/decision_log.md)
  (Decisions 009 / 010 / 011);
  [`../05_governance/risks.md`](../05_governance/risks.md).
