param(
    [string] $ReferenceDate = ""
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$LogDir = Join-Path $RepoRoot "backend\data\generated\reports\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogPath = Join-Path $LogDir "$Timestamp-generate-report.log"

function Write-RunLog {
    param([string] $Message)
    $Line = "$(Get-Date -Format o) $Message"
    Add-Content -LiteralPath $LogPath -Value $Line
    Write-Output $Line
}

$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONPATH = (Resolve-Path (Join-Path $RepoRoot "backend\src")).Path

$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    throw "Missing Python virtual environment at $Python"
}

$Arguments = @("-m", "reports.generate_report")
if (-not [string]::IsNullOrWhiteSpace($ReferenceDate)) {
    $Arguments += @("--reference-date", $ReferenceDate)
}

Write-RunLog "Starting local fight report generation. reference_date=$ReferenceDate"

try {
    $Output = & $Python @Arguments 2>&1
    $ExitCode = $LASTEXITCODE
    foreach ($Line in $Output) {
        Write-RunLog $Line
    }

    if ($ExitCode -ne 0) {
        throw "Immediate fight report command failed with exit code $ExitCode."
    }

    Write-RunLog "Fight report command completed."
} catch {
    Write-RunLog "Failed: $($_.Exception.Message)"
    exit 1
}
