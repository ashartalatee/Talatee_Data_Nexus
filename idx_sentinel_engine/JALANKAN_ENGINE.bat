@echo off
:: Mengubah judul terminal window
title IDX Capital Sentinel Engine - Runner v1.0
cls

echo ==========================================================
echo           IDX CAPITAL SENTINEL AUTOMATION ENGINE          
echo ==========================================================
echo [SYSTEM] Memulai inisialisasi lingkungan sistem...

:: 1. Validasi Instalasi Python
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo [ERROR] Python tidak ditemukan di komputer ini!
    echo [SOLUSI] Silakan unduh dan instal Python 3.10 ke atas,
    echo          dan pastikan centang "Add Python to PATH" saat instalasi.
    echo.
    pause
    exit /b
)

:: 2. Pembuatan Virtual Environment (.venv) untuk Isolasi Dependency
if not exist ".venv" (
    echo [SYSTEM] Membuat virtual environment terisolasi (.venv)...
    python -m venv .venv
    if %errorlevel% neq 0 (
        echo [ERROR] Gagal membuat virtual environment.
        pause
        exit /b
    )
)

:: 3. Aktivasi Lingkungan Virtual
call .venv\Scripts\activate

:: 4. Cek dan Instalasi Dependency Otomatis jika Belum Ada
if exist "requirements.txt" (
    echo [SYSTEM] Memeriksa dan menginstal library pendukung (Pandas, Openpyxl, dll)...
    pip install -r requirements.txt --quiet
) else (
    echo [WARNING] File requirements.txt tidak ditemukan. 
    echo           Mencoba menginstal library utama secara manual...
    pip install pandas openpyxl pdfplumber playwright python-telegram-bot --quiet
)

:: 5. Pastikan Browser Automation Playwright Sudah Terinstal
echo [SYSTEM] Memvalidasi browser web automation driver...
playwright install chromium --quiet

:: 6. Mengeksekusi Core Engine Orchestrator
echo [SYSTEM] Menjalankan automasi data...
echo ----------------------------------------------------------
python core_system/main.py
echo ----------------------------------------------------------

:: 7. Penanganan Kondisi Akhir Setelah Python Selesai Berjalan
if %errorlevel% neq 0 (
    echo.
    echo [OPS] Terjadi kesalahan saat mengeksekusi engine.
    echo       Silakan periksa folder 'core_system/logs/' untuk detail error.
    echo.
    pause
) else (
    echo.
    echo [SUKSES] Seluruh proses pemrosesan data IDX selesai!
    echo          Silakan ambil laporan Anda di folder '2_OUTPUT_REPORT'.
    echo.
    timeout /t 5
)