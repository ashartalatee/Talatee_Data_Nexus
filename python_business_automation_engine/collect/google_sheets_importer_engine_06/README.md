# 📥 Google Sheets Importer Engine

> **Collect Engine #6** dari koleksi *Python Business Automation Engine* — 100+ engine untuk mengotomatisasi alur kerja data & bisnis end-to-end.

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/UI-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Non-IT Friendly](https://img.shields.io/badge/Non--IT-Friendly-success)](#untuk-pengguna-non-it)

Engine Python yang mengimpor data dari Google Sheets secara otomatis — **tanpa perlu setup API** untuk sheet publik, dan dengan **Service Account** untuk sheet privat perusahaan. Dilengkapi antarmuka visual (Streamlit) sehingga **siapa pun, termasuk yang tidak bisa coding, dapat menggunakannya**.

---

## 🎯 Masalah yang Diselesaikan

Tim non-teknis (sales, finance, ops) sering menyimpan data operasional di Google Sheets, tapi data itu **terjebak** di sana — sulit digabungkan dengan sistem lain, sulit dianalisis otomatis, dan harus di-download manual setiap kali dibutuhkan.

**Google Sheets Importer Engine** menjembatani itu: satu klik, data sheet langsung jadi CSV/Excel/JSON yang siap dipakai untuk analisis, dashboard, atau pipeline automation lainnya.

---

## ✨ Fitur

- 🌐 **Mode Publik** — import langsung dari link share, tanpa setup Google Cloud sama sekali
- 🔒 **Mode Privat** — akses sheet internal perusahaan via Service Account
- 🖥️ **Antarmuka visual (Streamlit)** — paste link, klik, download. Cocok untuk pengguna non-IT
- ⌨️ **CLI** — untuk dijalankan otomatis/terjadwal (cron, Task Scheduler, Airflow, dll)
- 📦 **Export 3 format**: CSV, Excel, JSON
- ✅ **Teruji** — unit test untuk fungsi inti (`pytest`)

---

## 🖼️ Demo

| Mode Publik | Mode Privat |
|---|---|
| Paste link → klik *Import Data* → preview → download | Upload `credentials.json` → pilih worksheet → import |

*(tambahkan screenshot/GIF hasil `streamlit run app.py` di sini sebelum publish ke GitHub)*

---

## 🚀 Untuk Pengguna Non-IT

Tidak perlu install Python manual atau ngerti coding. Ikuti langkah di **[docs/PANDUAN_PENGGUNA.md](docs/PANDUAN_PENGGUNA.md)** — lengkap dengan langkah klik demi klik.

Ringkasnya:
```bash
pip install -r requirements.txt
streamlit run app.py
python -m streamlit run app.py
```
Browser akan terbuka otomatis. Tinggal paste link Google Sheets kamu dan klik **Import Data**.

---

## 👨‍💻 Untuk Pengguna Teknis

### Instalasi
```bash
git clone https://github.com/USERNAME/google-sheets-importer-engine.git
cd google-sheets-importer-engine
pip install -r requirements.txt
```

### Pemakaian sebagai library
```python
from src.sheets_importer.importer import GoogleSheetsImporter

importer = GoogleSheetsImporter()
df = importer.from_public_link("https://docs.google.com/spreadsheets/d/xxxx/edit")
importer.export(df, "output/data.csv", file_format="csv")
```

### Pemakaian via CLI
```bash
python cli.py --url "https://docs.google.com/spreadsheets/d/xxxx/edit" --out data.csv
```

### Sheet privat (Service Account)
Lihat panduan lengkap di **[docs/SETUP_GOOGLE_API.md](docs/SETUP_GOOGLE_API.md)**.
```bash
python cli.py --url "<link>" --out data.csv --credentials credentials.json --worksheet "Sheet2"
```

### Menjalankan test
```bash
pytest tests/
```

---

## 📁 Struktur Proyek

```
google-sheets-importer-engine/
├── app.py                      # Antarmuka Streamlit (non-IT)
├── cli.py                      # Command-line interface (teknis)
├── src/sheets_importer/
│   └── importer.py             # Logika inti engine
├── examples/example_usage.py   # Contoh pemakaian sebagai library
├── tests/test_importer.py      # Unit test
├── docs/
│   ├── PANDUAN_PENGGUNA.md     # Panduan non-IT (langkah klik demi klik)
│   └── SETUP_GOOGLE_API.md     # Setup Service Account untuk sheet privat
└── requirements.txt
```

---

## 🛠️ Tech Stack

`Python` · `pandas` · `Streamlit` · `gspread` · `Google Sheets API` · `requests`

---

## 🗺️ Bagian dari Roadmap Automation Engine

Engine ini adalah bagian dari **Automation OS**: `Collect → Clean → Transform → Analyze → Report → Act → Monitor`. Lihat repo lain di portofolio untuk engine pelengkap seperti Data Cleaner, Excel Report Generator, dan Auto Notifier.

---

## 📄 Lisensi

MIT License — bebas digunakan, dimodifikasi, dan didistribusikan.

---

*Dibuat sebagai bagian dari portofolio profesional Python Business Automation. Feedback dan kontribusi terbuka lewat Issues/PR.*
