@echo off
chcp 65001 >nul
title Cai Dat Tu Dong - WebVideoCast Studio v3.0
cd /d "%~dp0"

echo ======================================================================
echo   CAI DAT TU DONG - WEBVIDEOCAST STUDIO v3.0 (PC, MOBILE ^& TV)
echo ======================================================================
echo.

:: 1. Kiem tra Python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [!] May tinh chua cai Python. Dang tu dong tai va cai Python 3.12 qua Winget...
    winget install -e --id Python.Python.3.12 --accept-package-agreements --accept-source-agreements
    if %errorlevel% neq 0 (
        echo [X] Khong the tu cai Python. Vui long tai Python tai https://www.python.org/downloads/
        echo     (Nho tich chon "Add Python to PATH" khi cai dat!)
        pause
        exit /b 1
    )
    echo [OK] Da cai Python! Vui long tat cua so nay va chay lai file 1_CAI_DAT_LAN_DAU.bat.
    pause
    exit /b 0
)

echo [1/3] Dang cai dat cac thu vien Python can thiet (FastAPI, yt-dlp, Playwright, curl_cffi)...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo.
echo [2/3] Dang tai trinh duyet Chromium ngam cho Playwright (de boc tach phim web ^& chan quang cao)...
python -m playwright install chromium

echo.
echo [3/3] Dang mo cong ket noi mang LAN (Port 30001) de Tivi va Dien Thoai ket noi...
netsh advfirewall firewall add rule name="WebVideoCast Studio (Port 30001)" dir=in action=allow protocol=TCP localport=30001 >nul 2>&1

echo.
echo ======================================================================
echo   [HOAN TAT 100%%] CAI DAT THANH CONG!
echo   Bay gio ban chi can bam dup vao file: 2_CHAY_WEBVIDEOCAST_STUDIO.bat
echo ======================================================================
pause
