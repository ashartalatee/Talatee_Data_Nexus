# Clinic Data Engine

Clinic Data Engine adalah sistem modular untuk melakukan pengambilan, pemrosesan, dan penyimpanan data klinik secara terstruktur dan scalable.

Proyek ini dibangun sebagai DATA ENGINE, bukan sekadar script scraping sederhana.

---

## Tujuan Proyek

Membangun sistem yang mampu:

- Mengambil data multi-halaman (multi-page extraction)
- Mengubah HTML menjadi data terstruktur
- Menyimpan raw data secara versioned
- Menyimpan hasil parsing secara terpisah
- Mendukung audit dan reproducibility
- Siap dikembangkan menjadi sistem otomatis penuh

---

## Konsep Arsitektur

Proyek ini menggunakan pendekatan modular:

extract → parse → process → save

Struktur utama:

clinic_data_engine/
│
├── extractor/ # Pengambilan & parsing data
├── transformer/ # Cleaning & normalisasi (next phase)
├── analyzer/ # Analisis & insight (next phase)
├── loader/ # Penyimpanan data
├── services/ # Orchestrator pipeline
├── config/ # Konfigurasi sistem
├── data/ # Penyimpanan raw & processed
├── logs/ # Logging sistem
└── main.py # Entry point


Pendekatan ini memastikan:
- Modular
- Mudah dikembangkan
- Tidak hardcode
- Mudah di-maintain

---

## Fitur yang Sudah Dibangun (Level 1)

✅ HTTP Request dengan header & timeout  
✅ Multi-page pagination  
✅ Defensive parsing (tidak mudah crash)  
✅ Penyimpanan raw HTML per halaman  
✅ Versioned run dengan timestamp  
✅ Penyimpanan hasil parsing terstruktur (JSON)  
✅ Metadata per run  

Contoh struktur penyimpanan:
data/
├── raw/
│ └── 20260225_154210/
│ ├── raw_page_1.html
│ └── raw_page_2.html
│
└── processed/
└── 20260225_154210/
├── all_quotes.json
└── metadata.json

---

## 🛠 Cara Menjalankan

1. python main.py

3. Hasil akan tersimpan di folder `data/`

---

## Roadmap Pengembangan

Level 1 — Data Acquisition ✅  
Level 2 — Data Cleaning (Dedup, Normalisasi, Validasi)  
Level 3 — Analisis & Insight  
Level 4 — Automation & Scheduler  
Level 5 — Database & Full Pipeline Engine  

Target akhir:
Sistem data klinik yang siap dipakai untuk analisis pasar dan kebutuhan bisnis.

---

## Prinsip Pengembangan

Proyek ini dibangun dengan prinsip:

- Separation of Concern
- Modular Design
- Data Integrity
- Versioned Storage
- Error Tolerance
- Reproducible Run

---

## Author

Talatee Data Nexus  
System Builder | Data Engine Development

---

Clinic Data Engine bukan sekadar scraping.
Ini adalah fondasi untuk membangun sistem data profesional.