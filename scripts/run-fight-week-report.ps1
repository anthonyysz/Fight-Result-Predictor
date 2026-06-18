param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("early", "late")]
    [string] $RunType,

    [string] $ApiBaseUrl = "http://127.0.0.1:8000"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$LogDir = Join-Path $RepoRoot "backend\data\generated\automation\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogPath = Join-Path $LogDir "$Timestamp-$RunType.log"

function Write-RunLog {
    param([string] $Message)
    $Line = "$(Get-Date -Format o) $Message"
    Add-Content -LiteralPath $LogPath -Value $Line
    Write-Output $Line
}

$TodayNoon = Get-Date -Hour 12 -Minute 0 -Second 0
$MissedCutoff = $TodayNoon.AddMinutes(5)

Write-RunLog "Starting fight week report automation. run_type=$RunType scheduled_for=$($TodayNoon.ToString("s"))"

if ((Get-Date) -gt $MissedCutoff) {
    Write-RunLog "Skipped: local task started after the noon send window."
    exit 0
}

try {
    Invoke-RestMethod -Method Get -Uri "$ApiBaseUrl/health" -TimeoutSec 10 | Out-Null
} catch {
    Write-RunLog "Skipped: local backend is not live at $ApiBaseUrl. $($_.Exception.Message)"
    exit 0
}

$Body = @{
    run_type = $RunType
    scheduled_for = $TodayNoon.ToString("s")
} | ConvertTo-Json

try {
    $Response = Invoke-RestMethod `
        -Method Post `
        -Uri "$ApiBaseUrl/admin/automation/fight-week-report" `
        -ContentType "application/json" `
        -Body $Body `
        -TimeoutSec 1800

    Write-RunLog "Completed: $($Response | ConvertTo-Json -Depth 8 -Compress)"
} catch {
    Write-RunLog "Failed: $($_.Exception.Message)"
    exit 1
}
