[CmdletBinding()]
param(
    [switch]$Gpu
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot "orchestration\docker-compose.airflow.yml"

if (-not (Test-Path -LiteralPath $composeFile)) {
    throw "Airflow Compose file was not found: $composeFile"
}

if ($Gpu) {
    # This value is read by the DAG when it submits the Ray Jobs API request.
    # Ray itself must already be running with docker-compose.gpu.yml.
    $env:BOT_CAMPAIGN_GPUS_PER_TRIAL = "1"
    if (-not $env:BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS) {
        $env:BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS = "1"
    }
    Write-Host "Airflow GPU mode: each Ray Tune trial requests 1 GPU."
} else {
    $env:BOT_CAMPAIGN_GPUS_PER_TRIAL = "0"
    if (-not $env:BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS) {
        $env:BOT_CAMPAIGN_MAX_CONCURRENT_TRIALS = "1"
    }
    Write-Host "Airflow CPU mode: each Ray Tune trial requests 0 GPUs."
}

Push-Location $repoRoot
try {
    # The init container is idempotent: it migrates the DB and preserves admin/admin.
    & docker compose -f $composeFile up airflow-init
    if ($LASTEXITCODE -ne 0) { throw "Airflow initialization failed." }

    # Recreate these containers so the selected GPU/CPU request reaches the DAG.
    & docker compose -f $composeFile up -d --force-recreate `
        airflow-api-server airflow-scheduler airflow-dag-processor
    if ($LASTEXITCODE -ne 0) { throw "Airflow services failed to start." }
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "Airflow: http://localhost:8084"
Write-Host "Ray:     http://localhost:8265"
Write-Host "GPU request passed to DAG: $env:BOT_CAMPAIGN_GPUS_PER_TRIAL"
