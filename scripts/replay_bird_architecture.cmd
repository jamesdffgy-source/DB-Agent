@echo off
setlocal
call "%~dp0run_python.cmd" "%~dp0replay_bird_architecture.py" %*
exit /b %errorlevel%
