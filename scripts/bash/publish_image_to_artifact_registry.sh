#!/usr/bin/env bash
# Tag the locally-built ``miaproc:cli-r45`` image and push it to the
# Artifact Registry repo the Cloud Run Job pulls from. Prints the
# resulting digest so the operator can pin
# ``06_infra/cloudrun/*.yaml`` to the immutable digest reference
# rather than the mutable ``:cli-r45`` tag.
#
# Prereqs:
#   - the image already built locally
#     (docker build -f docker/Dockerfile.miaproc-r45-reddyproc \
#        -t miaproc:cli-r45 .);
#   - ``gcloud auth configure-docker us-central1-docker.pkg.dev`` ran
#     once for this user;
#   - the Artifact Registry repo
#     ``us-central1-docker.pkg.dev/manglaria-staging/cloud-run-source-deploy``
#     exists.
#
# Usage:
#   scripts/bash/publish_image_to_artifact_registry.sh
set -euo pipefail

PROJECT="${PROJECT:-manglaria-staging}"
REGION="${REGION:-us-central1}"
REPO="${REPO:-cloud-run-source-deploy}"
LOCAL_TAG="${LOCAL_TAG:-miaproc:cli-r45}"
REMOTE_TAG="${REMOTE_TAG:-cli-r45}"

REMOTE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/miaproc:${REMOTE_TAG}"

echo "Tagging  : ${LOCAL_TAG} -> ${REMOTE}"
docker tag "${LOCAL_TAG}" "${REMOTE}"

echo "Pushing  : ${REMOTE}"
docker push "${REMOTE}"

echo
echo "Resolving immutable digest (pin this in 06_infra/cloudrun/*.yaml):"
docker inspect --format='{{index .RepoDigests 0}}' "${REMOTE}"
