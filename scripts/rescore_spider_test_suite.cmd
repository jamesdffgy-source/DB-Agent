@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."
call "%~dp0run_python.cmd" "%PROJECT_ROOT%\scripts\rescore_spider_test_suite.py" %*
exit /b %ERRORLEVEL%
