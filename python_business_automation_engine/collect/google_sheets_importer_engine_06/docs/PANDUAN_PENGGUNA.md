# 📘 Panduan Pengguna (Non-IT)

Panduan ini untuk kamu yang **tidak punya background coding** tapi ingin pakai engine ini untuk mengimpor data dari Google Sheets.

## Yang Kamu Butuhkan
- Komputer/laptop (Windows/Mac/Linux)
- Koneksi internet
- Link Google Sheets yang datanya ingin diimpor

## Langkah 1 — Install Python (sekali saja)
1. Buka https://www.python.org/downloads/
2. Download dan install Python versi terbaru
3. **Penting (Windows)**: saat instalasi, centang kotak "Add Python to PATH"

## Langkah 2 — Download Project Ini
1. Download project ini dari GitHub (tombol hijau **Code → Download ZIP**)
2. Extract ZIP-nya ke folder pilihanmu

## Langkah 3 — Install Kebutuhan Engine
1. Buka folder project di Command Prompt / Terminal
   - Windows: ketik `cmd` di address bar folder lalu Enter
   - Mac: klik kanan folder → "New Terminal at Folder"
2. Ketik perintah ini lalu Enter:
   ```
   pip install -r requirements.txt
   ```
   Tunggu sampai selesai (sekali saja, untuk pemakaian berikutnya tidak perlu ulang).

## Langkah 4 — Jalankan Engine
Ketik perintah ini di Terminal/Command Prompt yang sama:
```
streamlit run app.py
```
Browser akan terbuka otomatis menampilkan halaman **Google Sheets Importer Engine**.

## Langkah 5 — Siapkan Google Sheets-mu
1. Buka Google Sheets yang ingin diimpor
2. Klik tombol **Share** (kanan atas)
3. Di bagian "General access", pilih **"Anyone with the link"** dengan role **Viewer**
4. Klik **Copy link**

> 🔒 Jika sheet-mu harus tetap privat (data sensitif perusahaan), gunakan **Mode Privat** — lihat [SETUP_GOOGLE_API.md](SETUP_GOOGLE_API.md).

## Langkah 6 — Import Data
1. Di halaman web yang terbuka, pilih mode **"🌐 Sheet Publik"**
2. Paste link Google Sheets-mu ke kotak yang tersedia
3. Klik tombol **🚀 Import Data**
4. Data akan muncul sebagai tabel preview

## Langkah 7 — Download Hasil
Klik salah satu tombol:
- **Unduh CSV** — untuk dibuka di Excel/Sheets lagi atau dipakai sistem lain
- **Unduh Excel** — format .xlsx siap pakai
- **Unduh JSON** — untuk kebutuhan developer/integrasi sistem

Selesai! File akan tersimpan di folder Downloads komputermu.

---

## ❓ Troubleshooting

| Masalah | Solusi |
|---|---|
| "Gagal mengambil data" | Pastikan sheet sudah di-set "Anyone with the link can view" |
| Browser tidak terbuka otomatis | Buka manual: `http://localhost:8501` |
| `streamlit: command not found` | Jalankan ulang `pip install -r requirements.txt` |
| Data kosong/salah tab | Pastikan link mengarah ke tab yang benar (cek bagian `gid=` di URL) |
