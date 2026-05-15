<#
.SYNOPSIS
    Ensure the dashboard is running on :8501, then open it in your browser.

.EXAMPLE
    .\open-dashboard.ps1
#>
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
& (Join-Path $root 'run-dashboard.ps1') -Background -Open
exit $LASTEXITCODE
