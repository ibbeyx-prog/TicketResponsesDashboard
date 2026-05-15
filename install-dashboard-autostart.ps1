<#
.SYNOPSIS
    Register a Windows scheduled task so the dashboard watchdog starts at logon.

    The watchdog keeps http://localhost:8501 available (restarts Streamlit if it stops).

.EXAMPLE
    .\install-dashboard-autostart.ps1
    .\install-dashboard-autostart.ps1 -Remove
#>
[CmdletBinding()]
param([switch]$Remove)

$ErrorActionPreference = 'Stop'
$root = (Resolve-Path (Split-Path -Parent $MyInvocation.MyCommand.Path)).Path
$taskName = 'TELEBOT-FieldTicketDashboard'
$runScript = Join-Path $root 'run-dashboard.ps1'

if ($Remove) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed scheduled task: $taskName"
    exit 0
}

if (-not (Test-Path $runScript)) {
    Write-Host "[FAIL] Missing $runScript"
    exit 1
}

$action = New-ScheduledTaskAction `
    -Execute 'powershell.exe' `
    -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$runScript`" -Watch -Quiet"

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask `
    -TaskName $taskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Description 'Keeps TELEBOT Streamlit dashboard on http://localhost:8501' `
    -Force | Out-Null

Write-Host "[OK] Scheduled task registered: $taskName"
Write-Host "     At Windows sign-in, a hidden watchdog starts and maintains port 8501."
Write-Host "     Open the dashboard anytime:  .\open-dashboard.ps1  or double-click dashboard.bat"
Write-Host "     Remove autostart:  .\install-dashboard-autostart.ps1 -Remove"
