# PowerShell parity of scripts/bash/publish_image_to_artifact_registry.sh.
# Tag the locally-built ``miaproc:cli-r45`` image and push it to the
# Artifact Registry repo the Cloud Run Job pulls from. Prints the
# resulting digest so the operator can pin
# ``06_infra/cloudrun/*.yaml`` to the immutable digest.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File `
#     scripts/powershell/publish_image_to_artifact_registry.ps1
[CmdletBinding()]
param(
    [string]$Project = "manglaria-staging",
    [string]$Region = "us-central1",
    [string]$Repo = "cloud-run-source-deploy",
    [string]$LocalTag = "miaproc:cli-r45",
    [string]$RemoteTag = "cli-r45"
)

$ErrorActionPreference = "Stop"

$Remote = "$Region-docker.pkg.dev/$Project/$Repo/miaproc:$RemoteTag"

Write-Output "Tagging  : $LocalTag -> $Remote"
docker tag $LocalTag $Remote
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Output "Pushing  : $Remote"
docker push $Remote
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Output ""
Write-Output "Resolving immutable digest (pin this in 06_infra/cloudrun/*.yaml):"
docker inspect --format='{{index .RepoDigests 0}}' $Remote
