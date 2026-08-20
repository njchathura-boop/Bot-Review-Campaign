[CmdletBinding()]
param(
    [string]$ImageTag = "latest",
    [string]$Registry = "ghcr.io/njchathura-boop",
    [switch]$Gpu,
    [switch]$SkipWait
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw "kubectl is not installed or is not on PATH."
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$manifestPath = if ($Gpu) {
    Join-Path $repositoryRoot "k8s/overlays/gpu"
} else {
    Join-Path $repositoryRoot "k8s"
}

$apiImage = "${Registry}/bot-review-campaign-api:${ImageTag}"
$jobsImage = "${Registry}/bot-review-campaign-jobs:${ImageTag}"

Write-Host "Cluster context: $(& kubectl config current-context)"
Write-Host "Applying manifests from $manifestPath"
& kubectl apply -k $manifestPath
if ($LASTEXITCODE -ne 0) { throw "Kubernetes manifest apply failed." }

# Pin the running workloads to one immutable release identity. The ETL CronJob
# inherits this image when a manual Job is created from it.
& kubectl -n bot-campaign set image deployment/detectra-api "api=$apiImage"
if ($LASTEXITCODE -ne 0) { throw "Could not set the API image." }
& kubectl -n bot-campaign set image deployment/detectra-ray "ray=$jobsImage"
if ($LASTEXITCODE -ne 0) { throw "Could not set the Ray image." }
& kubectl -n bot-campaign set image cronjob/detectra-etl "etl=$jobsImage"
if ($LASTEXITCODE -ne 0) { throw "Could not set the ETL image." }

if (-not $SkipWait) {
    & kubectl -n bot-campaign rollout status deployment/detectra-api --timeout=600s
    if ($LASTEXITCODE -ne 0) { throw "API rollout did not become ready." }
    & kubectl -n bot-campaign rollout status deployment/detectra-ray --timeout=600s
    if ($LASTEXITCODE -ne 0) { throw "Ray rollout did not become ready." }
    & kubectl -n bot-campaign rollout status deployment/detectra-mlflow --timeout=300s
    if ($LASTEXITCODE -ne 0) { throw "MLflow rollout did not become ready." }
    & kubectl -n bot-campaign rollout status deployment/detectra-prometheus --timeout=300s
    if ($LASTEXITCODE -ne 0) { throw "Prometheus rollout did not become ready." }
    & kubectl -n bot-campaign rollout status deployment/detectra-grafana --timeout=300s
    if ($LASTEXITCODE -ne 0) { throw "Grafana rollout did not become ready." }
}

Write-Host "Detectra workloads:"
& kubectl -n bot-campaign get pods,services,cronjobs
Write-Host "Use kubectl port-forward commands from k8s/README.md to open each UI."
