# 🚀 CRM Data Collector Engine
### Python Business Automation Engine #08 | Collect Engine Series

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Portfolio](https://img.shields.io/badge/Portfolio-Professional-orange.svg)](#)
[![Non-IT Friendly](https://img.shields.io/badge/Non--IT-Friendly-brightgreen.svg)](#cara-pakai-non-it)

> **"Bangun sistem yang menyelesaikan masalah nyata. Dokumentasikan. Bagikan. Bantu lebih banyak bisnis."**

---

## 🎯 Apa Itu Engine Ini?

**CRM Data Collector Engine** adalah tools otomatis yang mengumpulkan data pelanggan dari berbagai sumber — CSV, Excel, hingga input manual — lalu menyimpannya ke satu database CRM terpusat yang rapi dan terstruktur.

Dibuat khusus agar **bisa dipakai siapapun, termasuk non-IT**, cukup dengan klik tombol.

---

## 🔥 Masalah Nyata Yang Diselesaikan

| Masalah Umum | Solusi Engine Ini |
|---|---|
| Data pelanggan tersebar di banyak file Excel | ✅ Import & gabung otomatis |
| Tim sales lupa follow-up customer | ✅ Semua tersimpan di satu database |
| Laporan manual setiap bulan memakan waktu | ✅ Export 1-klik ke Excel/CSV/JSON |
| Staff non-IT kesulitan pakai software CRM | ✅ GUI simpel, tinggal klik |
| Data duplikat & format tidak konsisten | ✅ Auto cleaning & normalisasi |

---

## ✨ Fitur Utama

- 📂 **Import Multi-Sumber** — CSV, Excel (.xlsx/.xls), input form manual
- 🧹 **Auto Data Cleaning** — normalisasi telepon, validasi email, standarisasi kolom
- 🔍 **Search Real-time** — cari customer langsung dari tabel
- 📊 **Dashboard Mini** — statistik total, prospect, customer, inactive
- 💾 **Export Fleksibel** — Excel, CSV, atau JSON
- 🗄️ **Database Lokal** — SQLite, tidak butuh internet, data aman di komputer sendiri
- 🖥️ **GUI Non-IT Friendly** — tampilan gelap modern, semua fitur bisa diakses lewat tombol

---

## 🖼️ Preview

```
┌─────────────────────────────────────────────────────────────────┐
│  🚀 CRM DATA COLLECTOR ENGINE v1.0                              │
├──────────────┬──────────────────────────────────────────────────┤
│ 👥 Total: 8  │  🎯 Prospect: 4  │  ✅ Customer: 3  │  💤 0     │
├──────────────┼──────────────────────────────────────────────────┤
│              │  🔍 Cari customer...                             │
│ 📥 IMPORT    │                                                  │
│ [CSV]        │  ID │ Nama          │ Email          │ Status    │
│ [Excel]      │  1  │ Budi Santoso  │ budi@ex...     │ Active    │
│              │  2  │ Siti Rahayu   │ siti@gm...     │ Active    │
│ ✏️ MANUAL    │  3  │ Ahmad Fauzi   │ ahmad@co...    │ Active    │
│ Nama: ____   │  ...                                             │
│ Email: ____  │                                                  │
│ Telp: ____   │                                                  │
│ [Simpan]     │                                                  │
│              │                                                  │
│ 📤 EXPORT    │                                                  │
│ [Excel ▼]    │                                                  │
│ [Export]     │                                                  │
└──────────────┴──────────────────────────────────────────────────┘
```

---

## 📁 Struktur Project

```
crm-data-collector/
│
├── engine/
│   ├── crm_collector.py      # 🔧 Engine utama (run ini!)
│   └── sample_data.csv       # 📋 Data contoh untuk dicoba
│
├── output/                   # 📂 Hasil export otomatis tersimpan di sini
│   └── crm_database.db       # 🗄️ Database SQLite
│
├── requirements.txt          # 📦 Dependencies
└── README.md                 # 📖 Dokumentasi ini
```

---

## ⚡ Cara Install & Jalankan

### Prasyarat
- Python 3.8+ ([Download di sini](https://python.org/downloads))
- Tidak butuh server, tidak butuh internet

### Langkah Install

```bash
# 1. Clone repository ini
git clone https://github.com/username/crm-data-collector.git
cd crm-data-collector

# 2. Install dependencies
pip install -r requirements.txt

# 3. Jalankan engine
python engine/crm_collector.py
```

---

## 🖱️ Cara Pakai (Non-IT)

Tidak perlu paham coding! Ikuti langkah ini:

**Import dari CSV atau Excel:**
1. Buka engine → klik **"Import dari CSV"** atau **"Import dari Excel"**
2. Pilih file Anda → otomatis diproses dan tersimpan
3. Lihat hasilnya langsung di tabel ✅

**Input Data Manual:**
1. Isi form di panel kiri (Nama, Email, Telepon, dll.)
2. Klik **"Simpan Data"**
3. Data langsung masuk ke database ✅

**Export Data:**
1. Pilih format: Excel / CSV / JSON
2. Klik **"Export Sekarang"**
3. File tersimpan otomatis di folder `/output` ✅

### Format CSV yang Didukung
Engine ini otomatis mengenali berbagai nama kolom:

| Yang Anda Punya | Dibaca Sebagai |
|---|---|
| name / full name / nama pelanggan | → Nama |
| email / e-mail / email address | → Email |
| phone / no hp / whatsapp | → Telepon |
| company / perusahaan / instansi | → Perusahaan |

---

## 🛠️ Tech Stack

| Komponen | Library |
|---|---|
| GUI | `tkinter` (built-in Python) |
| Data Processing | `pandas` |
| Database | `sqlite3` (built-in Python) |
| Excel Support | `openpyxl`, `xlrd` |

---

## 🔄 Siklus Engine (Automation OS Framework)

```
COLLECT → CLEAN → TRANSFORM → ANALYZE → REPORT → ACT → MONITOR
  ☑️         ☑️        ☑️           ⬜         ⬜       ⬜       ⬜
```

Engine ini berada di layer **COLLECT + CLEAN** — fondasi dari seluruh sistem otomasi bisnis.

---

## 🗺️ Roadmap Pengembangan

- [x] Import CSV & Excel
- [x] Input manual via GUI
- [x] Auto data cleaning
- [x] Export multi-format
- [x] Search & filter
- [ ] Import dari Google Sheets (v1.1)
- [ ] Duplikat detection otomatis (v1.1)
- [ ] Email notifikasi follow-up (v1.2)
- [ ] Dashboard analytics (v2.0)
- [ ] Web version (v2.0)

---

## 📊 Hasil Nyata

> Engine ini bisa menghemat **2-4 jam/minggu** untuk tim sales yang sebelumnya input data manual ke Excel.

**Kasus Penggunaan:**
- UMKM dengan 100-5000 data pelanggan
- Freelancer yang kelola 10-100 klien
- Tim sales yang proses leads dari berbagai channel

---

## 📜 Lisensi

MIT License — bebas digunakan, dimodifikasi, dan dikomersilkan.

---

## 👤 Tentang Developer

Dibuat sebagai bagian dari **Python Business Automation Engine Portfolio** — 100 engine untuk menyelesaikan masalah bisnis nyata dengan Python.

> *"Bukan sekadar kode. Ini solusi untuk bisnis yang lebih efisien."*

**Connect:**
- 🎵 TikTok: [@username](#) — konten Python Automation setiap hari
- 💼 LinkedIn: [Your Name](#)
- 🐙 GitHub: [github.com/username](#)

---

⭐ **Star repo ini jika bermanfaat!** Setiap star memotivasi pembuatan engine berikutnya.