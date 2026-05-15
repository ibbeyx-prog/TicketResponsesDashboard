# Shared helpers for starting and monitoring the Streamlit dashboard on :8501.

$script:DashboardPort = 8501
$script:DashboardUrl = "http://localhost:$($script:DashboardPort)/"

function Get-ProjectRoot {
    $scriptsDir = Split-Path -Parent $PSCommandPath
    return (Resolve-Path (Join-Path $scriptsDir '..')).Path
}

function Get-VenvPython {
    $root = Get-ProjectRoot
    $py = Join-Path $root 'venv\Scripts\python.exe'
    if (-not (Test-Path $py)) {
        throw @"
Virtualenv not found at: $py

Create it once:
  cd $root
  py -3.11 -m venv venv
  .\venv\Scripts\python.exe -m pip install -r requirements.txt
"@
    }
    return $py
}

function Test-DashboardListening {
    return [bool](
        Get-NetTCPConnection -LocalPort $script:DashboardPort -ErrorAction SilentlyContinue |
            Where-Object { $_.State -eq 'Listen' }
    )
}

function Wait-DashboardReady {
    param(
        [int]$TimeoutSec = 45,
        [int]$IntervalMs = 500
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-DashboardListening) {
            try {
                $r = Invoke-WebRequest -Uri $script:DashboardUrl -UseBasicParsing -TimeoutSec 3
                if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {
                    return $true
                }
            } catch {
                # Port may be open before Streamlit is ready.
            }
        }
        Start-Sleep -Milliseconds $IntervalMs
    }
    return $false
}

function Start-DashboardProcess {
    $root = Get-ProjectRoot
    $py = Get-VenvPython
    $logDir = Join-Path $root 'logs'
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
    $stdout = Join-Path $logDir 'dashboard.log'
    $stderr = Join-Path $logDir 'dashboard.err.log'

    $args = @(
        '-m', 'streamlit', 'run', 'app.py',
        '--server.port', "$($script:DashboardPort)",
        '--server.headless', 'true',
        '--server.address', 'localhost'
    )

    Start-Process -FilePath $py `
        -ArgumentList $args `
        -WorkingDirectory $root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr | Out-Null
}

function Open-DashboardBrowser {
    Start-Process $script:DashboardUrl | Out-Null
}

function Start-DashboardWatchLoop {
    param([switch]$Quiet)
    if (-not $Quiet) {
        Write-Host "Dashboard watchdog on $($script:DashboardUrl) (checks every 15s). Ctrl+C to stop."
    }
    while ($true) {
        if (-not (Test-DashboardListening)) {
            $ts = Get-Date -Format "HH:mm:ss"
            if (-not $Quiet) {
                Write-Host "[$ts] Dashboard not running - starting..."
            }
            try {
                Start-DashboardProcess
                if (-not (Wait-DashboardReady -TimeoutSec 60)) {
                    Write-Host "[$ts] Dashboard failed to start. See logs\dashboard.err.log"
                } elseif (-not $Quiet) {
                    Write-Host "[$ts] Dashboard is up."
                }
            } catch {
                Write-Host "[$ts] $($_.Exception.Message)"
            }
        }
        Start-Sleep -Seconds 15
    }
}
