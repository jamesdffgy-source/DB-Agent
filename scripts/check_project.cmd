@echo off
setlocal
call "%~dp0run_python.cmd" "%~dp0project_gate.py" %*
exit /b %ERRORLEVEL%
