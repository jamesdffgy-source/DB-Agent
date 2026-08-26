@echo off
setlocal
call "%~dp0run_evaluation.cmd" --with-model --record-model-baseline %*
exit /b %ERRORLEVEL%
