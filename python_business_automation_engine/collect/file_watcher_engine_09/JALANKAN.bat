@echo off
title File Watcher Engine - Launcher
color 0A

echo.
echo  ============================================
echo   Pyton Business Automation Engine
echo   Collect Engine #9 - File Watcher
echo  ============================================
echo.

:: Cek apakah Python sudah terinstall
python --version >nul 2>&1
if %errorlevel% neq 0 (
    color 0C
    echo  [ERROR] Python tidak ditemukan di komputer ini!
    echo.
    echo  Cara install Python:
    echo  1. Buka browser, pergi ke: https://python.org/downloads
    echo  2. Klik Download Python
    echo  3. Centang "Add Python to PATH" saat install
    echo  4. Setelah selesai, coba jalankan file ini lagi
    echo.
    pause
    exit
)

echo  Membuka File Watcher Engine...
echo  Tunggu sebentar...
echo.

:: Jalankan GUI
python "%~dp0src\gui.py"

:: Kalau error
if %errorlevel% neq 0 (
    color 0C
    echo.
    echo  [ERROR] Gagal membuka aplikasi.
    echo  Pastikan folder src\gui.py ada di lokasi yang benar.
    echo.
    pause
)