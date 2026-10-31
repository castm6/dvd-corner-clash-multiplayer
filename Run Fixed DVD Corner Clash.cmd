@echo off
cd /d "%~dp0"
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js was not found. Install Node.js 20 or newer, then run this file again.
  pause
  exit /b 1
)
start "DVD Corner Clash Fixed Server" cmd /k "set PORT=3002 && node server.js"
timeout /t 2 /nobreak >nul
start "" "http://127.0.0.1:3002"
