# 🗄️ Database Connector Engine

> **Engine #07** dari Python Business Automation Engine — The Complete Portfolio Roadmap

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen.svg)]()

---

## 🎯 Masalah yang Diselesaikan

> *"Data bisnis kamu tersebar di mana-mana — Excel, database, CSV — dan butuh waktu lama untuk kumpulkan dan olah semuanya."*

Engine ini menghubungkan Python ke **berbagai sumber data** sekaligus, secara otomatis.

| Masalah | Solusi Engine Ini |
|---------|-------------------|
| Data di Excel tapi perlu masuk database | ✅ Baca Excel → simpan ke SQLite |
| Database beda-beda di setiap divisi | ✅ Satu engine, banyak koneksi |
| Tim non-IT tidak bisa akses database | ✅ Output otomatis ke Excel/CSV |
| Laporan manual tiap hari | ✅ Ambil data → simpan hasil → selesai |

---

## ⚡ Quickstart (3 Menit)

```bash
# 1. Clone repo ini
git clone https://github.com/[username]/database-connector-engine.git
cd database-connector-engine

# 2. Install dependencies
pip install -r requirements.txt

# 3. Jalankan demo langsung
python src/database_connector.py
```

Selesai. Tidak perlu konfigurasi tambahan untuk mode SQLite.

---

## 🔌 Database yang Didukung

| Database | Setup | Use Case |
|----------|-------|----------|
| **SQLite** | Langsung jalan (default) | Proyek personal, testing |
| **MySQL** | `pip install mysql-connector-python` | Bisnis menengah, web app |
| **PostgreSQL** | `pip install psycopg2-binary` | Enterprise, cloud |
| **Excel (.xlsx)** | `pip install pandas openpyxl` | Laporan, data dari tim lain |
| **CSV** | Tidak perlu install | Data export dari sistem lain |

---

## 📖 Cara Penggunaan

### Mode 1 — SQLite (paling mudah, tidak perlu setup)

```python
from src.database_connector import DatabaseConnectorEngine

# Inisialisasi
engine = DatabaseConnectorEngine()
engine.connect()

# Buat tabel
engine.buat_tabel("penjualan", {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "produk": "TEXT",
    "jumlah": "INTEGER",
    "harga": "REAL"
})

# Masukkan data
engine.tulis_data("penjualan", {
    "produk": "Laptop",
    "jumlah": 3,
    "harga": 15000000
})

# Ambil data
data = engine.baca_data("SELECT * FROM penjualan")
engine.tampilkan(data)

# Simpan ke Excel
engine.simpan_hasil(data, "laporan_penjualan")
engine.tutup()
```

### Mode 2 — MySQL

Ubah konfigurasi di file `config/settings.py`:

```python
CONFIG = {
    "db_type": "mysql",
    "mysql": {
        "host": "localhost",
        "database": "nama_database_kamu",
        "username": "root",
        "password": "password_kamu"
    }
}
```

### Mode 3 — Baca dari Excel

```python
CONFIG = {
    "db_type": "excel",
    "excel": {
        "file_path": "data/laporan_bulanan.xlsx",
        "sheet_name": "Januari"
    }
}

engine = DatabaseConnectorEngine(CONFIG)
engine.connect()
data = engine.baca_data("")  # baca semua isi sheet
engine.tampilkan(data)
```

---

## 📁 Struktur Project

```
database-connector-engine/
│
├── 📄 README.md                  # Dokumentasi ini
├── 📄 requirements.txt           # Daftar library yang dibutuhkan
│
├── 📂 src/
│   └── database_connector.py    # Engine utama
│
├── 📂 config/
│   └── settings.py              # Konfigurasi database
│
├── 📂 examples/
│   ├── contoh_sqlite.py         # Contoh penggunaan SQLite
│   ├── contoh_mysql.py          # Contoh penggunaan MySQL
│   └── contoh_excel.py          # Contoh penggunaan Excel
│
├── 📂 data/                     # Folder data (otomatis dibuat)
├── 📂 results/                  # Folder output (otomatis dibuat)
└── 📂 tests/
    └── test_engine.py           # Unit tests
```

---

## 🛠️ Referensi Fungsi

| Fungsi | Kegunaan | Contoh |
|--------|----------|--------|
| `connect()` | Hubungkan ke database | `engine.connect()` |
| `baca_data(query)` | Ambil data | `engine.baca_data("SELECT * FROM tabel")` |
| `tulis_data(tabel, data)` | Simpan data baru | `engine.tulis_data("users", {"nama": "Budi"})` |
| `buat_tabel(nama, kolom)` | Buat tabel baru | `engine.buat_tabel("produk", {...})` |
| `simpan_hasil(data, nama)` | Export ke file | `engine.simpan_hasil(data, "laporan")` |
| `tampilkan(data)` | Lihat data di terminal | `engine.tampilkan(data)` |
| `info_database()` | Info koneksi aktif | `engine.info_database()` |
| `tutup()` | Tutup koneksi | `engine.tutup()` |

---

## 🔄 Siklus Kerja Engine

```
[Sumber Data]          [Engine]              [Output]
  SQLite    ──┐                         ┌──  Excel (.xlsx)
  MySQL     ──┤  DatabaseConnector  ──  ├──  CSV (.csv)
  PostgreSQL──┤      Engine             ├──  JSON (.json)
  Excel     ──┤                         └──  Terminal
  CSV       ──┘
```

---

## 💼 Contoh Kasus Bisnis Nyata

### Kasus 1: Laporan Penjualan Harian Otomatis
```python
# Ambil data penjualan hari ini dari database toko
engine.connect()
data = engine.baca_data("""
    SELECT produk, SUM(jumlah) as total_terjual, SUM(harga*jumlah) as total_omzet
    FROM penjualan 
    WHERE tanggal = DATE('now')
    GROUP BY produk
    ORDER BY total_omzet DESC
""")
engine.simpan_hasil(data, f"laporan_{datetime.now().strftime('%Y%m%d')}")
```

### Kasus 2: Import Data dari Excel ke Database
```python
# Baca dari Excel
config_excel = {**CONFIG, "db_type": "excel", "excel": {"file_path": "data/stok.xlsx"}}
engine_excel = DatabaseConnectorEngine(config_excel)
data = engine_excel.baca_data("")

# Simpan ke SQLite
engine_db = DatabaseConnectorEngine()
engine_db.connect()
for row in data["data"]:
    record = dict(zip(data["kolom"], row))
    engine_db.tulis_data("stok_produk", record)
```

---

## 📦 Requirements

```
# Core (wajib)
# Tidak ada — Python standard library sudah cukup untuk SQLite + CSV

# Excel support
pandas>=1.5.0
openpyxl>=3.0.0

# MySQL support  
mysql-connector-python>=8.0.0

# PostgreSQL support
psycopg2-binary>=2.9.0
```

Install semua sekaligus:
```bash
pip install -r requirements.txt
```

---

## 🧪 Menjalankan Tests

```bash
python tests/test_engine.py
```

---

## 🗺️ Roadmap

- [x] SQLite connector
- [x] MySQL connector  
- [x] PostgreSQL connector
- [x] Excel reader
- [x] CSV reader
- [x] Export ke Excel/CSV/JSON
- [ ] MongoDB connector (coming soon)
- [ ] Google Sheets connector (coming soon)
- [ ] Auto-backup scheduler (coming soon)

---

## 👤 Tentang Project Ini

Project ini adalah bagian dari **Python Business Automation Engine — 100 Engine Portfolio**.

Tujuan: Membangun sistem otomatisasi bisnis yang bisa digunakan oleh siapa saja, termasuk non-IT.

**Connect dengan saya:**
- 🐙 GitHub: [github.com/username]
- 📱 TikTok: [@username] — *Python automation untuk bisnis*
- 💼 LinkedIn: [linkedin.com/in/username]

---

## 📄 License

MIT License — bebas digunakan, dimodifikasi, dan dibagikan.

---

*"Bangun sistem yang menyelesaikan masalah nyata. Dokumentasikan. Bagikan. Bantu lebih banyak bisnis."*