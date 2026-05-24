<#
.SYNOPSIS
    Start (and optionally keep alive) the Streamlit dashboard on http://localhost:8501

.PARAMETER Background
    Start detached in the background and exit once the port responds.

.PARAMETER Watch
    Stay running and restart the dashboard if it stops (use with install-dashboard-autostart.ps1).

.PARAMETER Open
    Open http://localhost:8501 in the default browser after the server is ready.

.PARAMETER Quiet
    Less console output (for autostart / start.ps1).

.EXAMPLE
    .\run-dashboard.ps1
    Foreground — Ctrl+C stops the dashboard.

.EXAMPLE
    .\run-dashboard.ps1 -Background -Open
    Start in background and open the browser (bookmark this).

.EXAMPLE
    .\run-dashboard.ps1 -Watch
    Watchdog — restarts Streamlit if it crashes or port 8501 goes down.
#>
[CmdletBinding()]
param(
    [switch]$Background,
    [switch]$Watch,
    [switch]$Open,
    [switch]$Quiet
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

. (Join-Path $root 'scripts\dashboard.ps1')

function Write-Info([string]$msg) {
    if (-not $Quiet) { Write-Host $msg }
}

if ($Watch) {
    Start-DashboardWatchLoop -Quiet:$Quiet
    exit 0
}

function Get-ReactLoginUrl {
    $envFile = Join-Path $root '.env'
    if (-not (Test-Path $envFile)) { return $null }
    foreach ($line in Get-Content $envFile) {
        if ($line -match '^\s*DASHBOARD_REACT_LOGIN_URL\s*=\s*(.+)\s*$') {
            $u = $Matches[1].Trim().Trim('"').Trim("'")
            if ($u) { return $u }
        }
    }
    return $null
}

function Start-LoginWebDev {
    $loginDir = Join-Path $root 'login-web'
    if (-not (Test-Path (Join-Path $loginDir 'package.json'))) { return }
    $node = 'C:\Program Files\nodejs\npm.cmd'
    if (-not (Test-Path $node)) { return }
    foreach ($conn in Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue) {
        Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
    }
    Start-Sleep -Seconds 1
    Write-Info 'Starting React login on http://localhost:3000 ...'
    Start-Process -FilePath $node -ArgumentList 'run', 'dev' -WorkingDirectory $loginDir -WindowStyle Hidden | Out-Null
    Start-Sleep -Seconds 6
}

function Open-DashboardEntry {
    $react = Get-ReactLoginUrl
    if ($react) {
        Start-Process $react | Out-Null
    } else {
        Open-DashboardBrowser
    }
}

if (Test-DashboardListening) {
    Write-Info "Dashboard already running at $($script:DashboardUrl)"
    if ($Open) { Open-DashboardEntry }
    exit 0
}

if ($Background) {
    Write-Info "Starting dashboard in background on $($script:DashboardUrl) ..."
    Start-LoginWebDev
    Start-DashboardProcess
    if (-not (Wait-DashboardReady -TimeoutSec 60)) {
        Write-Host "[FAIL] Dashboard did not become ready on port $($script:DashboardPort)."
        Write-Host "       See logs\dashboard.err.log in the project folder."
        exit 1
    }
    Write-Info "[OK] Dashboard ready: $($script:DashboardUrl)"
    if ($Open) { Open-DashboardEntry }
    exit 0
}

# Foreground
Start-LoginWebDev
Write-Info "Starting dashboard at $($script:DashboardUrl) (Ctrl+C to stop) ..."
$py = Get-VenvPython
& $py -m streamlit run app.py `
    --server.port $script:DashboardPort `
    --server.headless true `
    --server.address localhost
