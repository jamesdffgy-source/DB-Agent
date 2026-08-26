@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."
call "%~dp0run_python.cmd" "%PROJECT_ROOT%\scripts\run_spider_benchmark.py" %*
exit /b %errorlevel%
