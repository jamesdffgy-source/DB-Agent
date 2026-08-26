@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."
call "%~dp0run_python.cmd" "%PROJECT_ROOT%\dbagent_launcher.pyw" %*
exit /b %ERRORLEVEL%
