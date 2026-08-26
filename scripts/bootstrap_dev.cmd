@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."

if not exist "%PROJECT_ROOT%\.venv\Scripts\python.exe" (
  call "%~dp0run_python.cmd" -m venv "%PROJECT_ROOT%\.venv"
  if errorlevel 1 exit /b %ERRORLEVEL%
)

set "PIP_CACHE_DIR=%PROJECT_ROOT%\.cache\pip"
if not exist "%PIP_CACHE_DIR%" mkdir "%PIP_CACHE_DIR%"
"%PROJECT_ROOT%\.venv\Scripts\python.exe" -m pip install --require-hashes -r "%PROJECT_ROOT%\requirements.lock"
if errorlevel 1 exit /b %ERRORLEVEL%
"%PROJECT_ROOT%\.venv\Scripts\python.exe" -m pip check
exit /b %ERRORLEVEL%
