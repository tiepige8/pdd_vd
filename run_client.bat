@echo off
setlocal
cd /d "%~dp0"

set "UNPACKED_EXE=dist\\win-unpacked\\PDD Video Helper.exe"

if exist "%UNPACKED_EXE%" (
  start "" "%UNPACKED_EXE%"
  exit /b 0
)

if exist "electron\\node_modules\\.bin\\electron.cmd" (
  pushd electron
  call npm run dev
  popd
  exit /b 0
)

echo Client not found.
echo 1) If you already built the Electron client, run: %UNPACKED_EXE%
echo 2) Or install Electron deps: cd electron ^&^& npm install
pause
