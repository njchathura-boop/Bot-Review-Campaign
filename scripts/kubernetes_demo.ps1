[CmdletBinding()]
param(
    [string]$ImageTag = "latest",
    [string]$Registry = "ghcr.io/njchathura-boop",
    [switch]$Gpu,
    [switch]$ResetNamespace,
    [switch]$ConfirmReset,
    [switch]$SkipWait
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw "kubectl is not installed or is not on PATH."
}
if (-not (Get-Command powershell.exe -ErrorAction SilentlyContinue)) {
    throw "PowerShell is required to call deploy_kubernetes.ps1."
}

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$namespace = "bot-campaign"
$airflowSecrets = Join-Path $repositoryRoot "k8s/secrets/airflow-secrets.local.yaml"
$coreSecrets = Join-Path $repositoryRoot "k8s/secrets/core-secrets.local.yaml"
$pullSecret = Join-Path $repositoryRoot "k8s/secrets/ghcr-pull-secret.local.yaml"
$deployScript = Join-Path $repositoryRoot "scripts/deploy_kubernetes.ps1"

if ($ResetNamespace) {
    if (-not $ConfirmReset) {
        throw "Reset requires both -ResetNamespace and -ConfirmReset. This deletes all Kubernetes PVC data, including the Kubernetes MLflow history."
    }
    Write-Host "Deleting namespace $namespace and its Kubernetes workloads/PVCs..."
    & kubectl delete namespace $namespace --wait=true
    if ($LASTEXITCODE -ne 0) { throw "Could not delete namespace $namespace." }
}

Write-Host "Kubernetes context: $(& kubectl config current-context)"
& kubectl get nodes
if ($LASTEXITCODE -ne 0) { throw "The Kubernetes API is not reachable or no node is ready." }

Write-Host "Creating namespace..."
& kubectl apply -f (Join-Path $repositoryRoot "k8s/base/namespace.yaml")
if ($LASTEXITCODE -ne 0) { throw "Could not create namespace." }

if (-not (Test-Path $airflowSecrets)) {
    throw "Missing $airflowSecrets. Copy airflow-secrets.example.yaml, fill the values, and create the local file before using the full demo."
}

foreach ($secretFile in @($coreSecrets, $airflowSecrets, $pullSecret)) {
    if (Test-Path $secretFile) {
        Write-Host "Applying secret $(Split-Path -Leaf $secretFile)..."
        & kubectl apply -f $secretFile
        if ($LASTEXITCODE -ne 0) { throw "Could not apply $secretFile." }
    }
}

$deployParameters = @{
    ImageTag = $ImageTag
    Registry = $Registry
    Full = $true
}
if ($Gpu) { $deployParameters.Gpu = $true }
if ($SkipWait) { $deployParameters.SkipWait = $true }

Write-Host "Deploying the full Detectra stack..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $deployScript @deployParameters
if ($LASTEXITCODE -ne 0) { throw "Kubernetes deployment failed." }

Write-Host "Demo bootstrap complete. The Airflow retraining DAG is deployed but is not triggered automatically."
Write-Host "See docs/KUBERNETES_DEMO_RUNBOOK.md for data checks, DAG trigger, monitoring, and port-forward commands."
