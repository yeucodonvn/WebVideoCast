@echo off
chcp 65001 >nul
title WebVideoCast Studio v3.0 (Port 30001)
cd /d "%~dp0"

echo ======================================================================
echo   DANG KHOI DONG WEBVIDEOCAST STUDIO v3.0...
echo   - Giao dien May Tinh (PC) : http://localhost:30001
echo   - Giao dien Dien Thoai    : Xem dia chi IP tren man hinh web
echo   (KHONG tat cua so den nay trong khi dang xem phim tren Tivi)
echo ======================================================================
echo.

:: Kiem tra nhanh neu chua cai thu vien thi tu cai luon
python -c "import fastapi, uvicorn, httpx, yt_dlp" >nul 2>&1
if %errorlevel% neq 0 (
    echo [!] Phat hien thieu thu vien Python. Dang tu dong cai dat...
    python -m pip install -r requirements.txt
    python -m playwright install chromium
)

start "" "http://localhost:30001"
set PYTHONIOENCODING=utf-8
python server.py
pause
