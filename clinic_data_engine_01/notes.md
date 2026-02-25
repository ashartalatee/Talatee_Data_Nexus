# CLINIC DATA ENGINE — DEVELOPMENT NOTES
Version: Level 1 Complete (Day 1–5)
Author: Talatee Data Nexus

---

## OVERVIEW

Clinic Data Engine adalah sistem modular untuk melakukan:
- Data extraction
- Structured parsing
- Versioned raw storage
- Versioned processed storage

Tujuan proyek ini bukan membuat script scraping sederhana,
tetapi membangun DATA ENGINE yang scalable dan reusable.

---

# DEVELOPMENT PROGRESS

---

# 🔵 LEVEL 1 — DATA ACQUISITION (DAY 1–5)

Fokus: Mengambil data mentah dengan sistem yang stabil dan modular.

---

## DAY 1 — PROJECT SETUP

Objective:
Membangun struktur sistem profesional sebelum menulis logic.

Yang Dibangun:
- Struktur folder modular (extractor, transformer, analyzer, dll)
- Entry point (`main.py`)
- Config terpisah (`settings.py`)
- Folder data & logs

Kenapa penting:
Struktur menentukan skalabilitas.
Tanpa arsitektur → sistem cepat berantakan.

Status:
✅ Structure modular siap dikembangkan
✅ Entry point berjalan tanpa error

---

## DAY 2 — HTTP REQUEST

Objective:
Mengambil 1 halaman HTML secara stabil.

Yang Dibangun:
- `fetcher.py`
- HTTP request dengan:
  - headers
  - timeout
  - error handling dasar
- Penyimpanan raw HTML

Kenapa penting:
Semua data system dimulai dari raw data.
Jika fetch tidak stabil → sistem gagal total.

Status:
✅ Bisa request 1 halaman
✅ HTML tersimpan
✅ Tidak crash

---

## DAY 3 — PARSING STRUCTURE

Objective:
Mengubah HTML menjadi structured data (list of dict).

Yang Dibangun:
- `parser.py`
- HTML → structured JSON
- Defensive parsing (hindari None crash)

Kenapa penting:
Data mentah tidak berguna.
Struktur adalah inti data engineering.

Status:
✅ Bisa parsing multi item
✅ JSON structured tersimpan
✅ Tidak crash jika field hilang

---

## DAY 4 — PAGINATION LOOP

Objective:
Mengambil banyak halaman secara otomatis.

Yang Dibangun:
- Dynamic URL formatting
- Loop multi-page
- Aggregation data
- Skip page jika gagal

Kenapa penting:
Single page = script.
Multi-page + tolerance = engine.

Status:
✅ Bisa ambil 5 halaman
✅ Data tergabung
✅ Tidak crash jika 1 page gagal

---

## DAY 5 — VERSIONED STORAGE SYSTEM

Objective:
Membuat sistem penyimpanan profesional.

Yang Dibangun:
- Timestamp-based run_id
- Folder per run
- Raw data versioning
- Processed data versioning
- Metadata file per run

Struktur hasil:

data/
 ├── raw/
 │    └── <run_id>/
 │         ├── raw_page_1.html
 │         └── ...
 └── processed/
      └── <run_id>/
           ├── all_quotes.json
           └── metadata.json

Kenapa penting:
- Tidak overwrite data lama
- Bisa audit kapan data diambil
- Bisa reproduce run
- Siap automation

Status:
✅ Versioned data storage
✅ Metadata logging
✅ Reproducible runs

---

# CURRENT SYSTEM CAPABILITY (END OF LEVEL 1)

Engine saat ini mampu:

- Multi-page extraction
- Structured parsing
- Versioned storage
- Error tolerance
- Aggregated dataset output
- Metadata tracking

Ini sudah masuk kategori:
Mini Data Extraction Engine

---

# NEXT DEVELOPMENT PHASE

🟢 LEVEL 2 — DATA CLEANING
Fokus:
- Deduplication
- Normalization
- Validation
- Data integrity

Target akhir:
Data siap dipakai analisis bisnis.

---

# DESIGN PRINCIPLE

Proyek ini dibangun dengan prinsip:

1. Modular architecture
2. Separation of concern
3. No hardcoding
4. Versioned storage
5. Error tolerance
6. Reproducibility

---

# MINDSET TRANSFORMATION

Proyek ini bukan latihan scraping.

Tujuan akhirnya:
Berubah dari:
"orang yang bisa scraping"

Menjadi:
"builder data system profesional"

---

END OF LEVEL 1