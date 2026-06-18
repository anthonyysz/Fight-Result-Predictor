param(
    [ValidateSet("early", "late")]
    [string] $RunType = "late",

    [string] $ReferenceDate = ""
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$LogDir = Join-Path $RepoRoot "backend\data\generated\automation\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogPath = Join-Path $LogDir "$Timestamp-send-now-$RunType.log"

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

$Arguments = @("-m", "automation.send_report_now", "--run-type", $RunType)
if (-not [string]::IsNullOrWhiteSpace($ReferenceDate)) {
    $Arguments += @("--reference-date", $ReferenceDate)
}

Write-RunLog "Starting immediate fight report send. run_type=$RunType reference_date=$ReferenceDate"

try {
    $Output = & $Python @Arguments 2>&1
    $ExitCode = $LASTEXITCODE
    foreach ($Line in $Output) {
        Write-RunLog $Line
    }

    if ($ExitCode -ne 0) {
        throw "Immediate fight report command failed with exit code $ExitCode."
    }

    Write-RunLog "Immediate fight report command completed."
} catch {
    Write-RunLog "Failed: $($_.Exception.Message)"
    exit 1
}
