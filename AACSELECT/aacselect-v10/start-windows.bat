@echo off
cd /d "%~dp0"
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed. Opening the download page - install the LTS version, then double-click this file again.
  start https://nodejs.org
  pause
  exit /b
)
set AAC_OPEN=1
node server.js
pause
