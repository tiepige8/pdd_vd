@echo off
setlocal
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo Python not found. Please install Python 3.10+ and enable "Add Python to PATH".
  pause
  exit /b 1
)

start "" /b python server.py
timeout /t 2 >nul
start "" http://127.0.0.1:3000

echo.
echo App is running. Keep this window open.
echo Close this window to stop the app.
pause >nul
