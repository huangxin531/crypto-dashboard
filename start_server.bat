@echo off
title Crypto Intelligence Server
cd /d "C:\crypto-dashboard"

REM 强制 UTF-8，避免 emoji / 中文导致 UnicodeEncodeError
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

echo.
echo  Crypto Intelligence Server
echo  ============================
echo  Starting...
echo.

if exist "C:\Users\asus\miniconda3\python.exe" (
    "C:\Users\asus\miniconda3\python.exe" server.py
) else (
    python server.py
)

pause
