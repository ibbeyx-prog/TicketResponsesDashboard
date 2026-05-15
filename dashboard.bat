@echo off
REM Double-click to start the dashboard and open http://localhost:8501
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0open-dashboard.ps1"
if errorlevel 1 pause
