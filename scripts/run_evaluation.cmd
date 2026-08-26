@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."
call "%~dp0run_python.cmd" "%PROJECT_ROOT%\runtime\app\frontends\nl2db_evaluation.py" %*
exit /b %ERRORLEVEL%
