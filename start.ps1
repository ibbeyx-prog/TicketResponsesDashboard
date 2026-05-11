<#
.SYNOPSIS
    Start ngrok tunnel, FastAPI bot, and Streamlit dashboard for this project.

.PARAMETER NoDashboard
    Skip launching the Streamlit dashboard.

.PARAMETER NoBot
    Skip launching the Telegram bot (ngrok will still start so the static
    domain is up if you want to start the bot manually later).

.EXAMPLE
    .\start.ps1                 # all three
    .\start.ps1 -NoDashboard    # tunnel + bot only
    .\start.ps1 -NoBot          # tunnel + dashboard only

.NOTES
    Each component is launched in its own minimized PowerShell window so the
    script returns immediately. Use stop.ps1 to shut everything down.
#>

[CmdletBinding()]
param(
    [switch]$NoDashboard,
    [switch]$NoBot
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$staticDomain = "grimace-predator-debtless.ngrok-free.dev"
$publicUrl    = "https://$staticDomain"
$venvPython   = Join-Path $root "venv\Scripts\python.exe"

function Test-Listen([int]$port) {
    return [bool](Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue |
        Where-Object { $_.State -eq 'Listen' })
}

function Start-InNewWindow($title, $command, $argList) {
    $argLine = ($argList | ForEach-Object {
        if ($_ -match '\s') { "`"$_`"" } else { $_ }
    }) -join ' '
    $cmdLine = "$command $argLine"
    Start-Process -FilePath "powershell.exe" `
        -ArgumentList @("-NoExit", "-Command", "`$Host.UI.RawUI.WindowTitle = '$title'; Set-Location '$root'; $cmdLine") `
        -WindowStyle Minimized | Out-Null
}

# Pre-flight: venv must exist if we are starting bot/dashboard
if (-not $NoBot -or -not $NoDashboard) {
    if (-not (Test-Path $venvPython)) {
        Write-Host "[FAIL] venv missing at $venvPython"
        Write-Host "       Run: py -3.11 -m venv venv ; .\venv\Scripts\python.exe -m ensurepip --upgrade ; .\venv\Scripts\python.exe -m pip install -r requirements.txt"
        exit 1
    }
}

# 1) ngrok tunnel
if (Test-Listen 4040) {
    Write-Host "[OK ] ngrok already running (api on :4040)"
} else {
    Write-Host "[..] starting ngrok bound to $staticDomain"
    Start-InNewWindow "ngrok" "ngrok" @("http", "--domain=$staticDomain", "8000")
    Start-Sleep -Seconds 3
}

# 2) bot.py on :8000
if ($NoBot) {
    Write-Host "[--] bot start skipped (-NoBot)"
} elseif (Test-Listen 8000) {
    Write-Host "[OK ] bot already running on :8000"
} else {
    Write-Host "[..] starting bot.py"
    Start-InNewWindow "ticket-bot" $venvPython @("bot.py")
    Start-Sleep -Seconds 3
}

# 3) Streamlit on :8501
if ($NoDashboard) {
    Write-Host "[--] dashboard start skipped (-NoDashboard)"
} elseif (Test-Listen 8501) {
    Write-Host "[OK ] dashboard already running on :8501"
} else {
    Write-Host "[..] starting streamlit dashboard"
    Start-InNewWindow "ticket-dashboard" $venvPython @(
        "-m", "streamlit", "run", "app.py",
        "--server.port", "8501",
        "--server.headless", "true"
    )
    Start-Sleep -Seconds 2
}

Start-Sleep -Seconds 1

# Status summary
Write-Host ""
Write-Host "Status:"
$checks = [ordered]@{
    "ngrok web UI"          = 4040
    "bot (FastAPI)"         = 8000
    "dashboard (Streamlit)" = 8501
}
foreach ($name in $checks.Keys) {
    $port = $checks[$name]
    if (Test-Listen $port) {
        Write-Host "  [OK ] $name on :${port}"
    } else {
        Write-Host "  [..]  $name on :${port} (not listening)"
    }
}

# Public-tunnel health check
try {
    $h = Invoke-RestMethod -Uri "$publicUrl/health" `
        -Headers @{ "ngrok-skip-browser-warning" = "1" } -TimeoutSec 8
    Write-Host "  [OK ] tunnel health: $($h.status)"
} catch {
    Write-Host "  [..]  tunnel health check failed: $($_.Exception.Message)"
}

# Telegram webhook info (optional)
try {
    $token = (Get-Content (Join-Path $root ".env") -Encoding UTF8 |
        Where-Object { $_ -match '^TELEGRAM_TOKEN=' }) -replace '^TELEGRAM_TOKEN=', ''
    if ($token) {
        $w = Invoke-RestMethod -Uri "https://api.telegram.org/bot$token/getWebhookInfo" -TimeoutSec 8
        Write-Host "  [OK ] Telegram webhook: $($w.result.url) (pending=$($w.result.pending_update_count))"
    }
} catch {
    # Non-fatal; webhook info is informational only.
}

Write-Host ""
Write-Host "Public bot:  $publicUrl"
Write-Host "Dashboard:   http://localhost:8501"
Write-Host ""
Write-Host "Stop everything with:  .\stop.ps1"
