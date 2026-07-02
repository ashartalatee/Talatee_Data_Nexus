@echo off
REM ============================================================
REM  BUILD SCRIPT - PDF Extractor Engine
REM  Jalankan file ini di Windows (double-click atau lewat cmd)
REM  untuk membuat file .exe yang bisa dipakai orang non-IT.
REM ============================================================

echo.
echo === PDF Extractor Engine - Build ke .exe ===
echo.

echo [1/3] Install semua library yang dibutuhkan...
pip install -r requirements.txt
if %errorlevel% neq 0 (
    echo.
    echo [GAGAL] Install library gagal. Pastikan Python sudah terinstall
    echo         dan bisa diakses lewat perintah "pip".
    pause
    exit /b 1
)

echo.
echo [2/3] Membungkus aplikasi jadi satu file .exe (PyInstaller)...
pyinstaller --noconfirm --onefile --windowed ^
    --name "PDF_Extractor_Engine" ^
    --icon "assets\icon.ico" ^
    app.py

if %errorlevel% neq 0 (
    echo.
    echo [GAGAL] Build .exe gagal. Lihat pesan error di atas.
    pause
    exit /b 1
)

echo.
echo [3/3] Selesai!
echo File .exe ada di folder: dist\PDF_Extractor_Engine.exe
echo Tinggal copy file itu ke manapun, tinggal double-click, langsung jalan.
echo.
pause
