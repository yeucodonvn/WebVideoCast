@echo off
chcp 65001 >nul
title WebVideoCast PC Studio (Port 30001)
cd /d "%~dp0"
start "" "http://localhost:30001"
python server.py
pause
