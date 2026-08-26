@echo off
setlocal
set "ROOT=%~dp0.."
call "%~dp0run_python.cmd" "%ROOT%\scripts\run_bird_benchmark.py" %*
exit /b %ERRORLEVEL%
