# AKBAR pipeline runner (no extra infrastructure needed).
#
#   .\run_pipeline.ps1                 # full 11.2M-row build
#   .\run_pipeline.ps1 -RowLimit 20000 # fast dev run on a slice
#
# Suitable for Windows Task Scheduler: it logs to orchestration\logs and exits
# with dbt's exit code, so the scheduler fails when a model or test fails.

param(
    [int]$RowLimit = 0
)

$ErrorActionPreference = 'Stop'

$root   = Split-Path -Parent $PSScriptRoot
$dbtExe = Join-Path $root '.venv-dbt\Scripts\dbt.exe'
$projectDir = Join-Path $root 'dbt'
$logDir = Join-Path $PSScriptRoot 'logs'

if (-not (Test-Path $dbtExe)) {
    throw "dbt not found at $dbtExe - create the venv first (see README)."
}
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Path $logDir | Out-Null
}

$stamp  = Get-Date -Format 'yyyyMMdd_HHmmss'
$logFile = Join-Path $logDir "akbar_$stamp.log"

$dbtArgs = @('build', '--project-dir', $projectDir, '--profiles-dir', $projectDir)
if ($RowLimit -gt 0) {
    $dbtArgs += @('--vars', "{row_limit: $RowLimit}")
}

Write-Host "Running: dbt $($dbtArgs -join ' ')"
Write-Host "Log:     $logFile"

& $dbtExe @dbtArgs *>&1 | Tee-Object -FilePath $logFile
$exitCode = $LASTEXITCODE

if ($exitCode -eq 0) {
    Write-Host "Pipeline succeeded."
} else {
    Write-Warning "Pipeline failed with exit code $exitCode - see $logFile"
}
exit $exitCode
