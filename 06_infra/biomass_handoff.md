# Biomass Cloud Engineer Handoff

Current as of M33 (carried unchanged in scope from M31; M32/M32A are
eddy-only). This is the only canonical biomass handoff in `06_infra/`.

## Goal

Run the `miaproc biomass` pipeline from the forward-facing repository and
deploy it using an image your cloud team builds and publishes. This handoff
intentionally uses placeholders for Google Cloud bindings and resource names
because this repository update does not push a Docker image or mutate cloud
infrastructure. M33 only updates eddy-side deployment artifacts; biomass
behavior is unchanged.

## Repository And Docker

Build from the forward-facing repository root:

```bash
docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
  -t miaproc:cli-r45 .
```

Optional local smoke checks:

```bash
docker run --rm miaproc:cli-r45 miaproc --help
docker run --rm miaproc:cli-r45 miaproc biomass run-bigquery --help
```

When your team is ready to publish, tag the image with your Artifact Registry target:

```bash
docker tag miaproc:cli-r45 \
  <REGION>-docker.pkg.dev/<PROJECT_ID>/<ARTIFACT_REPOSITORY>/miaproc:cli-r45

docker push \
  <REGION>-docker.pkg.dev/<PROJECT_ID>/<ARTIFACT_REPOSITORY>/miaproc:cli-r45
```

## Google Cloud Placeholders

Fill these before deployment:

```text
PROJECT_ID=<google-cloud-project>
BILLING_PROJECT=<billing-project>
REGION=<cloud-run-region>
ARTIFACT_REPOSITORY=<artifact-registry-repository>
IMAGE_URI=<REGION>-docker.pkg.dev/<PROJECT_ID>/<ARTIFACT_REPOSITORY>/miaproc:cli-r45
BIOMASS_SERVICE_ACCOUNT=<service-account-email>
INPUT_DATASET=<bigquery-input-dataset>
OUTPUT_DATASET=<bigquery-output-dataset>
CONTROL_DATASET=<orchestration-control-dataset>
STAGE_TABLE=<biomass-stage-table>
FINAL_TABLE=<biomass-final-table>
```

## IAM Bindings To Confirm

These are the Google Cloud **IAM bindings** (role grants binding a member,
commonly a service account, to a Google Cloud resource) your team should
confirm or create. Exact role scope is your cloud team's call; prefer the
narrowest dataset/repository/job scope that works.

```text
<BIOMASS_SERVICE_ACCOUNT> -> Artifact Registry reader on <ARTIFACT_REPOSITORY>
<BIOMASS_SERVICE_ACCOUNT> -> BigQuery jobUser on <BILLING_PROJECT or PROJECT_ID>
<BIOMASS_SERVICE_ACCOUNT> -> BigQuery dataViewer on input dataset(s)
<BIOMASS_SERVICE_ACCOUNT> -> BigQuery dataEditor or narrower table-writer role on output/stage/control dataset(s)
<DEPLOYER_PRINCIPAL> -> Cloud Run developer/admin role for the target job
<DEPLOYER_PRINCIPAL> -> iam.serviceAccountUser on <BIOMASS_SERVICE_ACCOUNT>
```

## BigQuery Write Safety

The package rejects writes to the production project name `manglaria` by default. Stage and final writes should target a non-production/write-authorized project such as staging unless your governance process explicitly changes that invariant.

Do not store service account JSON keys in the repository. Use Workload Identity, attached Cloud Run service accounts, or operator-local credentials outside git.

## Expected Biomass Flow

The biomass module reads a source table, computes biomass columns for eligible records using the packaged allometry/equation data, writes a stage table, and can merge to a final table when explicitly authorized by the command flags used by your deployment.

Before running a mutating job, verify the CLI help inside the exact image your team built:

```bash
docker run --rm <IMAGE_URI> miaproc biomass run-bigquery --help
```

## Deployment Notes

- This handoff does not pin a pushed image digest because this pass does not publish an image.
- Use the checked-in Dockerfile from the forward-facing repo, not the development workspace Dockerfile.
- Keep deployment-specific environment variables, service account names, and target table names in your cloud deployment system, not in source code.
- If a Cloud Run Job YAML is used, treat checked-in YAML as a template and replace placeholders with your environment-specific values before applying.
