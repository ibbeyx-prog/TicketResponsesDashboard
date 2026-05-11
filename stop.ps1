<#
.SYNOPSIS
    Stop ngrok tunnel, FastAPI bot, and Streamlit dashboard started by start.ps1.

.DESCRIPTION
    Identifies processes by the ports they are listening on (4040 ngrok web UI,
    8000 bot, 8501 dashboard) and stops them. Safe to run when nothing is
    running -- it just reports nothing to kill.

.EXAMPLE
    .\stop.ps1
#>

[CmdletBinding()]
param()

$ports = @(
    @{ Name = "ngrok web UI";          Port = 4040 },
    @{ Name = "bot (FastAPI)";         Port = 8000 },
    @{ Name = "dashboard (Streamlit)"; Port = 8501 }
)

$killed = 0
foreach ($entry in $ports) {
    $port = $entry.Port
    $name = $entry.Name
    $conns = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue |
        Where-Object { $_.State -eq 'Listen' }
    if (-not $conns) {
        Write-Host "[..]  $name on :${port} not running"
        continue
    }
    foreach ($c in $conns) {
        $pid_ = $c.OwningProcess
        try {
            $proc = Get-Process -Id $pid_ -ErrorAction Stop
            Stop-Process -Id $pid_ -Force -ErrorAction Stop
            Write-Host "[OK ] killed $($proc.ProcessName) (PID ${pid_}) holding $name on :${port}"
            $killed++
        } catch {
            Write-Host "[FAIL] could not stop PID ${pid_} on :${port}: $_"
        }
    }
}

# Also clean up any lingering ngrok windows that may not be holding 4040
# (e.g. crashed before binding the API port).
Get-Process ngrok -ErrorAction SilentlyContinue | ForEach-Object {
    try {
        Stop-Process -Id $_.Id -Force -ErrorAction Stop
        Write-Host "[OK ] killed stray ngrok process (PID $($_.Id))"
        $killed++
    } catch {
        Write-Host "[FAIL] could not stop ngrok PID $($_.Id): $_"
    }
}

# Sweep zombie python.exe processes that target this project's bot or
# dashboard but no longer hold a listening port (crashed/half-shutdown).
$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$pattern = '(bot\.py|streamlit\s+run\s+(app|dashboard)\.py)'
Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" -ErrorAction SilentlyContinue |
    Where-Object {
        $_.CommandLine -and
        $_.CommandLine -match $pattern -and
        ($_.CommandLine -match [regex]::Escape($projectRoot) -or $_.ExecutablePath -match [regex]::Escape($projectRoot))
    } |
    ForEach-Object {
        try {
            Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop
            Write-Host "[OK ] killed zombie python (PID $($_.ProcessId)) -- $($_.CommandLine -replace '\s+', ' ')"
            $killed++
        } catch {
            Write-Host "[FAIL] could not stop python PID $($_.ProcessId): $_"
        }
    }

if ($killed -eq 0) {
    Write-Host ""
    Write-Host "Nothing was running."
} else {
    Write-Host ""
    Write-Host "Stopped $killed process(es)."
}
