@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."

if defined DBAGENT_PYTHON (
  if not exist "%DBAGENT_PYTHON%" (
    echo FAIL: DBAGENT_PYTHON does not exist: %DBAGENT_PYTHON% 1>&2
    exit /b 1
  )
  "%DBAGENT_PYTHON%" -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 12) else 1)" >nul 2>nul
  if errorlevel 1 (
    echo FAIL: DBAGENT_PYTHON must be CPython 3.12. 1>&2
    exit /b 1
  )
  "%DBAGENT_PYTHON%" %*
  exit /b %ERRORLEVEL%
)

if exist "%PROJECT_ROOT%\runtime\python\python.exe" (
  "%PROJECT_ROOT%\runtime\python\python.exe" %*
  exit /b %ERRORLEVEL%
)

if exist "%PROJECT_ROOT%\.venv\Scripts\python.exe" (
  "%PROJECT_ROOT%\.venv\Scripts\python.exe" %*
  exit /b %ERRORLEVEL%
)

where py >nul 2>nul
if not errorlevel 1 (
  py -3.12 -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 12) else 1)" >nul 2>nul
  if not errorlevel 1 (
    py -3.12 %*
    exit /b %ERRORLEVEL%
  )
)

where python >nul 2>nul
if not errorlevel 1 (
  python -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 12) else 1)" >nul 2>nul
  if not errorlevel 1 (
    python %*
    exit /b %ERRORLEVEL%
  )
)

echo FAIL: CPython 3.12 was not found. Run scripts\bootstrap_dev.cmd after installing Python 3.12. 1>&2
exit /b 1
