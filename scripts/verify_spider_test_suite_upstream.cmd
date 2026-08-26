@echo off
setlocal
set "PROJECT_ROOT=%~dp0.."
call "%~dp0run_python.cmd" "%PROJECT_ROOT%\scripts\verify_spider_test_suite_upstream.py" %*
exit /b %ERRORLEVEL%
