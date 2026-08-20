[CmdletBinding()]
param(
    [string]$ImageTag = "latest",
    [string]$Registry = "ghcr.io/njchathura-boop",
    [switch]$Gpu,
    [switch]$Observability,
    [switch]$Full,
    [switch]$SkipWait
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw "kubectl is not installed or is not on PATH."
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
if ($Observability -and $Full) {
    throw "Choose either -Observability or -Full, not both. Full already includes observability."
}

$manifestPath = if ($Full -and $Gpu) {
    Join-Path $repositoryRoot "k8s/overlays/full-gpu"
} elseif ($Full) {
    Join-Path $repositoryRoot "k8s/overlays/full"
} elseif ($Observability -and $Gpu) {
    throw "Use -Full -Gpu for the declarative GPU plus observability stack."
} elseif ($Observability) {
    Join-Path $repositoryRoot "k8s/overlays/observability"
} elseif ($Gpu) {
    Join-Path $repositoryRoot "k8s/overlays/gpu"
} else {
    Join-Path $repositoryRoot "k8s"
}

$apiImage = "${Registry}/bot-review-campaign-api:${ImageTag}"
$jobsImage = "${Registry}/bot-review-campaign-jobs:${ImageTag}"
$airflowImage = "${Registry}/bot-review-campaign-airflow:${ImageTag}"

Write-Host "Cluster context: $(& kubectl config current-context)"
Write-Host "Rendering manifests from $manifestPath"
$renderedLines = & kubectl kustomize $manifestPath
if ($LASTEXITCODE -ne 0) { throw "Kubernetes manifest render failed." }

# Replace only the three project-owned image references. This pins Deployments,
# CronJobs, and the immutable Airflow migration Job before Kubernetes creates them.
$renderedManifest = $renderedLines -join [Environment]::NewLine
$renderedManifest = $renderedManifest.Replace(
    "ghcr.io/njchathura-boop/bot-review-campaign-api:latest",
    $apiImage
)
$renderedManifest = $renderedManifest.Replace(
    "ghcr.io/njchathura-boop/bot-review-campaign-jobs:latest",
    $jobsImage
)
$renderedManifest = $renderedManifest.Replace(
    "ghcr.io/njchathura-boop/bot-review-campaign-airflow:latest",
    $airflowImage
)

# Setup/migration Jobs have immutable pod templates. Recreate only these idempotent
# administrative Jobs before applying a new release; persistent data is untouched.
if ($Observability -or $Full) {
    & kubectl -n bot-campaign delete job detectra-kibana-setup --ignore-not-found --wait=true
    if ($LASTEXITCODE -ne 0) { throw "Could not prepare the Kibana setup Job." }
}
if ($Full) {
    & kubectl -n bot-campaign delete job detectra-airflow-migrate --ignore-not-found --wait=true
    if ($LASTEXITCODE -ne 0) { throw "Could not prepare the Airflow migration Job." }
}

Write-Host "Applying immutable release $ImageTag"
$renderedManifest | & kubectl apply -f -
if ($LASTEXITCODE -ne 0) { throw "Kubernetes manifest apply failed." }

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
    if ($Observability -or $Full) {
        & kubectl -n bot-campaign rollout status statefulset/detectra-elasticsearch --timeout=600s
        if ($LASTEXITCODE -ne 0) { throw "Elasticsearch rollout did not become ready." }
        & kubectl -n bot-campaign rollout status deployment/detectra-kibana --timeout=600s
        if ($LASTEXITCODE -ne 0) { throw "Kibana rollout did not become ready." }
        & kubectl -n bot-campaign wait --for=condition=complete job/detectra-kibana-setup --timeout=300s
        if ($LASTEXITCODE -ne 0) { throw "Kibana data-view setup did not complete." }
    }
    if ($Full) {
        & kubectl -n bot-campaign wait --for=condition=complete job/detectra-airflow-migrate --timeout=600s
        if ($LASTEXITCODE -ne 0) { throw "Airflow database migration did not complete." }
        & kubectl -n bot-campaign rollout status deployment/detectra-airflow-api --timeout=600s
        & kubectl -n bot-campaign rollout status deployment/detectra-airflow-scheduler --timeout=600s
        & kubectl -n bot-campaign rollout status deployment/detectra-airflow-dag-processor --timeout=600s
        if ($LASTEXITCODE -ne 0) { throw "An Airflow rollout did not become ready." }
    }
}

Write-Host "Detectra workloads:"
& kubectl -n bot-campaign get pods,services,cronjobs
Write-Host "Use kubectl port-forward commands from k8s/README.md to open each UI."
