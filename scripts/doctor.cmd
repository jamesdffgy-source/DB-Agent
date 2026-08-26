@echo off
setlocal
call "%~dp0run_python.cmd" "%~dp0doctor.py"
exit /b %ERRORLEVEL%
