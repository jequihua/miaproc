# IAM grants required for `cf-s2-eddy-runner@manglaria-staging`

The Cloud Run Job manifests in this directory run as

```
cf-s2-eddy-runner@manglaria-staging.iam.gserviceaccount.com
```

This SA was created in M11 by an operator who can manage SAs in
`manglaria-staging` but does **not** hold Project-IAM-Admin on either
`manglaria` or `manglaria-staging`. Today that SA carries:

- `WRITER` on `manglaria-staging:_orch` (granted via dataset ACL by
  the `_orch` dataset owner).

That single grant is **not enough** to run the job. A privileged
operator (Project-IAM-Admin on `manglaria-staging` AND
Project-IAM-Admin or table-IAM-admin on `manglaria`) needs to apply
the four bindings below before the Cloud Run Job will execute
successfully.

The bindings are deliberately the smallest set consistent with
[`../deployment.md`](../deployment.md) and the
production-read-only invariant (table-level reader on the two named
source tables; **no dataset-wide or project-wide reader role on
`manglaria`**).

## Grant commands (run as Project-IAM-Admin)

```bash
SA=cf-s2-eddy-runner@manglaria-staging.iam.gserviceaccount.com

# 1. roles/bigquery.jobUser on project manglaria-staging
#    Lets the SA submit BigQuery query jobs (and bill them to staging).
gcloud projects add-iam-policy-binding manglaria-staging \
    --member="serviceAccount:${SA}" \
    --role=roles/bigquery.jobUser \
    --condition=None

# 2. roles/bigquery.dataEditor on the staging dataset
#    Lets the SA write the stage table, the final table (when MERGE
#    is authorized), and the orchestration control tables.
#    (Granted by the dataset OWNER, not the project IAM admin --
#    can be applied via `bq update --source <ACL JSON>` if running
#    a working bq CLI, or via the ``BigQuery Admin`` console UI.)
bq update --source <(echo '{
  "access": [
    {"role": "roles/bigquery.dataEditor",
     "userByEmail": "'"${SA}"'"}
  ]
}') manglaria-staging:manglaria_lakehouse_ds

# 3. roles/bigquery.dataViewer on the two production source tables
#    Table-level grant; do NOT grant dataset-wide or project-wide
#    reader on ``manglaria``. Production stays read-only at
#    table-grain.
for TBL in carbon_flux_eddycovariance carbon_flux_biomet; do
    bq add-iam-policy-binding \
        --project_id=manglaria \
        --table=true \
        --member="serviceAccount:${SA}" \
        --role=roles/bigquery.dataViewer \
        manglaria_lakehouse_ds.${TBL}
done

# 4. roles/artifactregistry.reader on the cloud-run-source-deploy repo
#    Lets the SA pull the pinned-by-digest miaproc image. Most
#    Cloud Run setups also grant this implicitly through the
#    project's default service-agent bindings; if your project
#    already does so, this command is a no-op.
gcloud artifacts repositories add-iam-policy-binding cloud-run-source-deploy \
    --project=manglaria-staging \
    --location=us-central1 \
    --member="serviceAccount:${SA}" \
    --role=roles/artifactregistry.reader
```

## Optional binding for the deploying operator

The operator who runs `gcloud run jobs replace` against these
manifests must be able to `actAs` the dedicated SA. The operator
who *created* the SA in M11 can already do that (creator is owner).
For a different deploying operator, grant
`roles/iam.serviceAccountUser` on the SA itself:

```bash
gcloud iam service-accounts add-iam-policy-binding "${SA}" \
    --project=manglaria-staging \
    --member="user:<deploying-operator-email>" \
    --role=roles/iam.serviceAccountUser
```

## Verifying the rollout landed

After the four bindings are applied, the next operator-driven
manual cloud run should succeed (stage-only first, then explicit
merge). The runbook for that path lives at
[`../../09_ops/runbooks.md`](../../09_ops/runbooks.md).

The smoke check that proves the four bindings are wired correctly:

```bash
# Stage-only smoke (safe; no _s2_filt_1 mutation).
gcloud run jobs execute miaproc-eddy-rbrl-stage \
    --project=manglaria-staging --region=us-central1 --wait
```

Expected on success:

- exit 0;
- new row in `manglaria-staging._orch.cf_s2_runs` with
  `status = "stage_only_succeeded"`;
- `manglaria-staging.manglaria_lakehouse_ds.cf_s2_stage_rbrl`
  populated;
- `_s2_filt_1` for `RBRL` unchanged (only the explicit-merge job
  mutates that);
- watermark unchanged (only the explicit-merge job advances it).

## What today's failure looks like (M12 evidence)

Without the four grants above, executing the deployed Cloud Run
Job under the dedicated SA fails with:

```
google.api_core.exceptions.Forbidden: 403 POST
  https://bigquery.googleapis.com/bigquery/v2/projects/manglaria-staging/jobs?prettyPrint=false:
  Access Denied: Project manglaria-staging:
  User does not have bigquery.jobs.create permission in project
  manglaria-staging.
```

Container exit code is `4` (runtime processing failure). The
in-cloud project-scoped preflight still runs and approves
`project-scoped (renv.lock in '/app'; R library under repo)`
before the read attempt; **Decision 010 / R11 is unaffected by
the IAM blocker**. Production project `manglaria` is not
written to (the failure is in front of any write, and the
writeback module's
`forbidden_write_projects=("manglaria",)` invariant prevents
production writes at the code layer regardless).

After the `bigquery.jobUser` binding lands but the production
`dataViewer` bindings have not, the failure shifts to:

```
google.api_core.exceptions.Forbidden: 403 Access Denied:
  Table manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance:
  User does not have permission to query table
  manglaria.manglaria_lakehouse_ds.carbon_flux_eddycovariance.
```

(The same 403 the M11 default-Compute-SA execution produced.)
This is expected and is the next IAM step.
