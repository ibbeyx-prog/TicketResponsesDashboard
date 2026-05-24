# Start login-web on port 3000 (stops stale listeners on 3000 first).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

foreach ($conn in Get-NetTCPConnection -LocalPort 3000 -State Listen -ErrorAction SilentlyContinue) {
    Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Seconds 1

$npm = "C:\Program Files\nodejs\npm.cmd"
if (-not (Test-Path $npm)) {
    throw "Node.js not found. Install from https://nodejs.org/"
}
& $npm run dev
