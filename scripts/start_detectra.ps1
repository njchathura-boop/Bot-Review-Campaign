[CmdletBinding()]
param(
    [switch]$Gpu,
    [switch]$Observability,
    [switch]$Build
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot "docker-compose.yml"
$gpuComposeFile = Join-Path $repoRoot "docker-compose.gpu.yml"
$indexFile = Join-Path $repoRoot "web\index.html"

if (-not (Test-Path -LiteralPath $composeFile)) {
    throw "docker-compose.yml was not found under $repoRoot"
}

# This marker distinguishes the current Detectra checkout from the older clone
# that can otherwise replace containers because both use project name bot-campaign.
$indexContent = Get-Content -LiteralPath $indexFile -Raw
if (-not $indexContent.Contains("Run Detectra on your machine")) {
    throw "This checkout does not contain the current Detectra UI. Run the script from: $repoRoot"
}

$composeArgs = @(
    "compose",
    "--project-directory", $repoRoot,
    "-f", $composeFile
)

if ($Gpu) {
    $composeArgs += @("-f", $gpuComposeFile)
}

$composeArgs += @("--profile", "stream")
if ($Observability) {
    $composeArgs += @("--profile", "observability")
}

$composeArgs += @("up", "-d")
if ($Build) {
    $composeArgs += "--build"
}

$services = @(
    "kafka", "kafka-init",
    "spark-master", "spark-worker", "spark-stream", "campaign-scorer",
    "mlflow", "api", "prometheus", "grafana",
    "ray-head", "ray-worker"
)
if ($Observability) {
    $services += @("elasticsearch", "kibana", "filebeat")
}
$composeArgs += $services

Write-Host "Starting Detectra from canonical checkout: $repoRoot"
& docker @composeArgs
if ($LASTEXITCODE -ne 0) {
    throw "Docker Compose failed with exit code $LASTEXITCODE"
}

Write-Host ""
Write-Host "Detectra:    http://localhost:8000"
Write-Host "Grafana:    http://localhost:3000/d/detectra-ray-cluster/detectra-ray-cluster"
Write-Host "Prometheus: http://localhost:9090/targets"
Write-Host "Ray:        http://localhost:8265/#/metrics"
Write-Host "MLflow:     http://localhost:5001"
Write-Host "Spark:      http://localhost:8082"
if ($Observability) {
    Write-Host "Kibana:     http://localhost:5601"
}
