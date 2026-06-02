<#
.SYNOPSIS
    Checklist and helpers to sync your local TELEBOT folder with this repo / Cursor Cloud.

.DESCRIPTION
    Default local path (adjust if yours differs):
      C:\Users\ibrahim_ali\Desktop\CSM\TELEBOT

    Git-tracked code should sync via git push/pull. This script focuses on
    local-only files: .env, Streamlit secrets, Telethon sessions, logs.

.EXAMPLE
    cd C:\Users\ibrahim_ali\Desktop\CSM\TELEBOT
    .\scripts\sync-local-telebot.ps1 -CheckOnly

.EXAMPLE
    .\scripts\sync-local-telebot.ps1 -ExportEnvTemplateForCursor
#>

[CmdletBinding()]
param(
    [string]$TelebotRoot = "",
    [switch]$CheckOnly,
    [switch]$ExportEnvTemplateForCursor
)

$ErrorActionPreference = 'Stop'

if (-not $TelebotRoot) {
    $TelebotRoot = "C:\Users\ibrahim_ali\Desktop\CSM\TELEBOT"
}

if (-not (Test-Path $TelebotRoot)) {
    Write-Error "TELEBOT folder not found: $TelebotRoot"
}

Set-Location $TelebotRoot

$syncItems = [ordered]@{
    ".env" = "Required for bot + dashboard (Supabase, Telegram, webhooks). Never commit."
    ".streamlit\secrets.toml" = "Optional Streamlit Cloud / local secrets (same keys as .env)."
    "telethon_sidecar_session.session" = "Optional; regenerate on cloud if missing."
    "telethon_bot_session.session" = "Optional legacy session file."
    "logs\" = "Optional local logs only."
}

Write-Host "TELEBOT root: $TelebotRoot"
Write-Host ""
Write-Host "=== Files to keep in sync (local <-> cloud, not in git) ==="
foreach ($rel in $syncItems.Keys) {
    $full = Join-Path $TelebotRoot $rel
    $exists = Test-Path $full
    $flag = if ($exists) { "[OK ]" } else { "[--]" }
    Write-Host "  $flag $rel"
    Write-Host "       $($syncItems[$rel])"
}

Write-Host ""
Write-Host "=== Git-tracked app (sync via git) ==="
Write-Host "  app.py, bot.py, requirements.txt, supabase/migrations/, etc."
Write-Host "  From this folder: git pull / git push to match GitHub."

if ($CheckOnly) {
    return
}

$envPath = Join-Path $TelebotRoot ".env"
if (-not (Test-Path $envPath)) {
    Write-Host ""
    Write-Host "[WARN] No .env — copy .env.example to .env and fill values first."
    return
}

# Keys the cloud agent needs (names only)
$requiredKeys = @(
    "SUPABASE_URL",
    "SUPABASE_KEY",
    "SUPABASE_ANON_KEY",
    "TELEGRAM_TOKEN",
    "TELEGRAM_GROUP_CHAT_ID",
    "TELEGRAM_WEBHOOK_SECRET",
    "WEBHOOK_BASE_URL",
    "WEBHOOK_FULL_URL",
    "RAILWAY_PUBLIC_DOMAIN",
    "TG_API_ID",
    "TG_API_HASH",
    "DASHBOARD_ADMIN_USERNAMES"
)

Write-Host ""
Write-Host "=== .env keys present (values hidden) ==="
$lines = Get-Content $envPath -Encoding UTF8
$found = @{}
foreach ($line in $lines) {
    if ($line -match '^\s*#' -or $line -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=') { continue }
    $name = $Matches[1]
    $found[$name] = $true
}
foreach ($k in $requiredKeys) {
    $flag = if ($found.ContainsKey($k)) { "[set]" } else { "[missing]" }
    Write-Host "  $flag $k"
}

if ($ExportEnvTemplateForCursor) {
    $out = Join-Path $TelebotRoot "cursor-secrets-checklist.txt"
    @"
# Paste each VALUE into Cursor → Cloud Agent → Secrets (name must match).
# Do not commit this file if you fill in values.

SUPABASE_URL=
SUPABASE_KEY=
TELEGRAM_TOKEN=
TELEGRAM_GROUP_CHAT_ID=
TELEGRAM_WEBHOOK_SECRET=
TG_API_ID=
TG_API_HASH=
WEBHOOK_BASE_URL=
"@ | Set-Content -Path $out -Encoding UTF8
    Write-Host ""
    Write-Host "[OK ] Wrote template: $out"
    Write-Host "     Fill from your .env, add as Cursor Secrets, then re-run the cloud setup agent."
}

Write-Host ""
Write-Host "=== Cloud VM (/workspace) ==="
Write-Host "  1. Add the secrets in Cursor (or upload .env via Desktop sync)."
Write-Host "  2. On the agent VM run: bash scripts/write_dotenv_from_environment.sh"
Write-Host "  3. Verify: .venv\Scripts\python.exe scripts\check_supabase_connection.py  (local)"
Write-Host "     or: .venv/bin/python scripts/check_supabase_connection.py  (cloud)"
