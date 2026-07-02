# 📄 PDF Extractor Engine

**Ambil teks, tabel, gambar, dan metadata dari file PDF — otomatis, dalam hitungan detik, tanpa perlu bisa coding.**

Bagian dari proyek **Python Business Automation Engine** (Engine #01 — COLLECT ENGINE, tools #5: PDF Extractor), dibangun sebagai portofolio *real problem solver* untuk otomasi data & bisnis.

![status](https://img.shields.io/badge/status-active-success)
![python](https://img.shields.io/badge/python-3.10%2B-blue)
![license](https://img.shields.io/badge/license-MIT-green)
![platform](https://img.shields.io/badge/platform-Windows%20desktop-lightgrey)

---

## 🎯 Masalah yang Diselesaikan

Banyak bisnis punya data penting terkubur di dalam file PDF: laporan penjualan, invoice, katalog produk, laporan keuangan, hasil scan dokumen. Untuk mengambil isinya, orang biasanya:

- Copy-paste manual satu-satu dari PDF ke Excel (buang waktu, gampang typo)
- Bayar jasa data entry per dokumen
- Atau — kalau non-IT — sama sekali tidak tahu harus mulai dari mana

**PDF Extractor Engine** menyelesaikan ini: drop satu file PDF, dapatkan teks, tabel siap pakai di Excel, semua gambar di dalamnya, dan metadata dokumen — otomatis, lewat aplikasi desktop yang tinggal di-double click.

## ✨ Fitur

| Fitur | Output | Library |
|---|---|---|
| 📝 Ekstraksi Teks | `.txt` per halaman | `pdfplumber` |
| 📊 Ekstraksi Tabel | `.xlsx` multi-sheet, siap analisis | `pdfplumber` + `pandas` |
| 🖼️ Ekstraksi Gambar | `.png` per gambar embedded | `PyMuPDF` |
| ℹ️ Metadata Dokumen | `.json` (judul, penulis, jumlah halaman, dst.) | `pypdf` |
| 📋 Ringkasan Proses | `.json` laporan hasil ekstraksi | — |

Semua bisa dijalankan sekaligus atau dipilih sebagian, lewat GUI desktop yang ramah untuk pengguna non-IT.

## 🖥️ Untuk Pengguna Non-IT (Tidak Perlu Coding)

1. Download `PDF_Extractor_Engine.exe` dari folder [releases](#-cara-build-jadi-exe) atau build sendiri (lihat panduan di bawah).
2. Double-click file `.exe`.
3. Klik **"Pilih PDF..."** → pilih file yang mau diambil isinya.
4. Centang jenis data yang mau diambil (default: semua).
5. Klik **"Pilih Folder..."** → tentukan tempat menyimpan hasil.
6. Klik **🚀 PROSES SEKARANG**.
7. Setelah selesai, klik **📂 Buka Folder Hasil** — semua file hasil ekstraksi sudah ada di sana.

Tidak perlu install Python, tidak perlu buka terminal, tidak perlu tahu coding sama sekali.

## 🛠️ Untuk Developer

### Jalankan dari source code

```bash
git clone https://github.com/USERNAME/pdf-extractor-engine.git
cd pdf-extractor-engine
pip install -r requirements.txt
python app.py
```

### Pakai sebagai library / lewat CLI

```bash
python extractor.py path/ke/file.pdf folder_output/
```

```python
from extractor import PDFExtractorEngine

engine = PDFExtractorEngine("laporan.pdf", log=print)
result = engine.extract_all("hasil/")
print(result.to_dict())
```

### Jalankan unit test

```bash
python -m unittest tests/test_extractor.py -v
```

## 📦 Cara Build Jadi `.exe`

Build dilakukan di **Windows** (PyInstaller membungkus sesuai OS tempat build dijalankan):

```bash
pip install -r requirements.txt
build_exe.bat
```

Hasilnya ada di `dist/PDF_Extractor_Engine.exe` — satu file, tinggal dibagikan ke siapa pun, tidak perlu install Python di komputer mereka.

## 🗂️ Struktur Proyek

```
pdf-extractor-engine/
├── app.py                  # GUI desktop (Tkinter) — entry point untuk end-user
├── extractor.py             # Engine inti: logic ekstraksi PDF
├── requirements.txt
├── build_exe.bat            # Script build ke .exe (Windows + PyInstaller)
├── tests/
│   └── test_extractor.py    # Unit test
├── assets/
│   └── icon.ico              # Icon aplikasi
├── CONTENT_TIKTOK.md         # Ide konten personal branding (8 sudut pandang)
└── README.md
```

## 🚧 Tantangan & Solusi Teknis

- **Tabel PDF susah dibaca konsisten** → dipakai `pdfplumber` yang layout-aware, bukan sekadar `pdftotext` biasa, supaya struktur baris/kolom lebih akurat.
- **Gambar vector vs raster** → `PyMuPDF` dipakai khusus untuk gambar raster embedded; gambar vector (chart hasil Excel/matplotlib) memang di luar cakupan tool ini secara desain.
- **User non-IT jangan sampai bingung** → proses berat (baca file, ekstrak) dijalankan di thread terpisah supaya aplikasi tidak "not responding" saat file besar.
- **Distribusi tanpa install Python** → dibungkus PyInstaller `--onefile --windowed` supaya hasil akhir tinggal 1 file `.exe`.

## 🗺️ Roadmap Selanjutnya

- [ ] OCR untuk PDF hasil scan (belum ada layer teks)
- [ ] Mode batch: proses banyak PDF sekaligus dalam satu folder
- [ ] Versi web (Streamlit) untuk yang tidak pakai Windows
- [ ] Export ringkasan otomatis (AI summary) dari hasil teks yang diekstrak

## 📄 Lisensi

MIT License — bebas dipakai, dimodifikasi, dan dikembangkan lagi. Lihat [LICENSE](LICENSE).

---

**Dibangun oleh [Nama Kamu]** sebagai bagian dari roadmap *Python Business Automation Engine — 100+ Engine Portfolio*.
📺 Ikuti proses pembuatan engine lainnya di TikTok: [@username-kamu](#)
