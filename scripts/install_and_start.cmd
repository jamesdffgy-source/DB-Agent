@echo off
setlocal

echo [1/3] Creating the isolated Python environment...
call "%~dp0bootstrap_dev.cmd"
if errorlevel 1 exit /b %ERRORLEVEL%

echo [2/3] Checking this device...
call "%~dp0doctor.cmd"
if errorlevel 1 exit /b %ERRORLEVEL%

echo [3/3] Starting DBQuill...
call "%~dp0start_dbquill.cmd"
exit /b %ERRORLEVEL%
