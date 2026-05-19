#!/usr/bin/env bash
# Apply (create or update) one of the miaproc Cloud Run Job manifests
# under ``06_infra/cloudrun/`` and optionally execute it. Wraps the
# two ``gcloud`` invocations in the M11 runbook so an operator can
# replay them without retyping the project / region flags.
#
# Usage:
#   scripts/bash/deploy_cloud_run_job.sh <manifest> [--execute]
#
# Examples:
#   scripts/bash/deploy_cloud_run_job.sh \
#     06_infra/cloudrun/miaproc-eddy-rbrl-stage.yaml --execute
#   scripts/bash/deploy_cloud_run_job.sh \
#     06_infra/cloudrun/miaproc-eddy-rbrl-merge.yaml
#
# The merge manifest mutates the staging final table; deploy and
# execute are kept as separate operator decisions on purpose.
set -euo pipefail

PROJECT="${PROJECT:-manglaria-staging}"
REGION="${REGION:-us-central1}"

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <manifest.yaml> [--execute]" >&2
    exit 2
fi
MANIFEST="$1"
EXECUTE=0
if [[ "${2:-}" == "--execute" ]]; then
    EXECUTE=1
fi

if [[ ! -f "${MANIFEST}" ]]; then
    echo "manifest not found: ${MANIFEST}" >&2
    exit 2
fi

JOB_NAME="$(grep -E '^  name:' "${MANIFEST}" | head -1 | awk '{print $2}')"
if [[ -z "${JOB_NAME}" ]]; then
    echo "could not parse job name from ${MANIFEST}" >&2
    exit 2
fi

echo "Deploying Cloud Run Job '${JOB_NAME}' from ${MANIFEST} to ${PROJECT}/${REGION}"
gcloud run jobs replace "${MANIFEST}" \
    --project "${PROJECT}" \
    --region "${REGION}"

if [[ ${EXECUTE} -eq 1 ]]; then
    echo
    echo "Executing job '${JOB_NAME}' (blocking on completion):"
    gcloud run jobs execute "${JOB_NAME}" \
        --project "${PROJECT}" \
        --region "${REGION}" \
        --wait
fi
