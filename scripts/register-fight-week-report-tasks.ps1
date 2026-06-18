param(
    [string] $TaskPrefix = "Fight Result Predictor"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$Runner = Join-Path $RepoRoot "scripts\run-fight-week-report.ps1"

if (-not (Test-Path -LiteralPath $Runner)) {
    throw "Missing runner script: $Runner"
}

$PowerShell = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"

function Register-ReportTask {
    param(
        [string] $Name,
        [string] $RunType,
        [string] $DayOfWeek
    )

    $Action = New-ScheduledTaskAction `
        -Execute $PowerShell `
        -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`" -RunType $RunType"

    $Trigger = New-ScheduledTaskTrigger `
        -Weekly `
        -DaysOfWeek $DayOfWeek `
        -At "11:45AM"

    $Settings = New-ScheduledTaskSettingsSet `
        -StartWhenAvailable:$false `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -MultipleInstances IgnoreNew

    Register-ScheduledTask `
        -TaskName "$TaskPrefix $Name" `
        -Action $Action `
        -Trigger $Trigger `
        -Settings $Settings `
        -Description "Runs the local Fight Result Predictor $RunType fight-week email report." `
        -Force | Out-Null
}

Register-ReportTask -Name "Monday Early Report" -RunType "early" -DayOfWeek "Monday"
Register-ReportTask -Name "Friday Late Report" -RunType "late" -DayOfWeek "Friday"

Write-Output "Registered fight week report tasks for Monday and Friday at 11:45 AM local time."
