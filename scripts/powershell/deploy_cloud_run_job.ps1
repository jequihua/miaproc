# PowerShell parity of scripts/bash/deploy_cloud_run_job.sh.
# Apply (create or update) one of the miaproc Cloud Run Job manifests
# under ``06_infra/cloudrun/`` and optionally execute it.
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File `
#     scripts/powershell/deploy_cloud_run_job.ps1 `
#     -Manifest 06_infra/cloudrun/miaproc-eddy-rbrl-stage.yaml -Execute
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Manifest,
    [string]$Project = "manglaria-staging",
    [string]$Region = "us-central1",
    [switch]$Execute
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $Manifest)) {
    Write-Error "manifest not found: $Manifest"
    exit 2
}

$JobName = (Select-String -Path $Manifest -Pattern '^  name:' | Select-Object -First 1).Line.Trim().Split()[1]
if (-not $JobName) {
    Write-Error "could not parse job name from $Manifest"
    exit 2
}

Write-Output "Deploying Cloud Run Job '$JobName' from $Manifest to $Project/$Region"
gcloud run jobs replace $Manifest `
    --project $Project `
    --region $Region
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($Execute) {
    Write-Output ""
    Write-Output "Executing job '$JobName' (blocking on completion):"
    gcloud run jobs execute $JobName `
        --project $Project `
        --region $Region `
        --wait
}
