# Order Intelligence Engine — v1

##  Overview

**Order Intelligence Engine v1** adalah sistem **data pipeline modular** untuk bisnis online (marketplace, reseller, dropship) yang dirancang untuk:

- Menggabungkan data dari berbagai sumber (Shopee, Tokopedia, TikTok Shop, WhatsApp, dll)  
- Membersihkan dan menstandarkan data dunia nyata (messy data)  
- Menghasilkan output bisnis berupa laporan sederhana dan file CSV/Excel yang siap pakai  

> Tujuan utama:  
> **Menyediakan fondasi data bersih yang dapat langsung dipakai untuk analisis dan insight bisnis.**  

---

##  Tujuan Dibuat

- Menyederhanakan proses pengumpulan dan pembersihan data multi-source  
- Menyediakan pipeline **reusable dan modular** untuk UMKM atau tim kecil  
- Memberikan **output nyata** (CSV/Excel + laporan ringkas) tanpa harus menulis kode ulang  
- Menjadi **portfolio atau MVP** yang bisa diperluas ke versi v2–v5  

---

##  Fitur Utama v1

| Fitur | Deskripsi |
|-------|-----------|
| **Load Data Multi-Source** | Menggabungkan CSV / Excel dari berbagai marketplace |
| **Data Cleaning** | Handle missing values, duplikasi, typo, dan normalisasi kolom |
| **Transformasi Dasar** | Hitung revenue, metrics dasar, transformasi numerik |
| **Business Report** | Cetak insight bisnis sederhana: total revenue, total orders, top products |
| **Export File** | CSV & Excel report siap pakai |
| **Logging** | Pipeline mencatat aktivitas ke file `logs/pipeline.log` |

---

##  Struktur Project

```text
order_intelligence_engine/
│
├── src/
│   ├── ingestion/          # Load data dari multi-source
│   ├── cleaning/           # Cleaning & standardisasi
│   ├── transform/          # Feature & metrics
│   ├── analytics/          # Business insights
│   ├── output/             # Export CSV/Excel
│   └── utils/              # Logger, helper
│
├── data/raw/               # Folder input CSV/Excel
├── output/                 # File output: clean_data.csv, report.xlsx
├── logs/                   # Log pipeline
└── main.py                 # Entry point pipeline

##  Cara Pakai

1. **Siapkan data input** di `data/raw/` (CSV minimal 3 kolom: `product_name`, `quantity`, `price`)
2. **Jalankan pipeline**:

```bash
python src/main.py
```

3. **Hasil output**:

* `output/clean_data.csv` → data bersih lengkap
* `output/report.xlsx` → file Excel dengan sheet: Clean Data & Top Products
* `logs/pipeline.log` → log aktivitas pipeline

4. **Lihat laporan di terminal** untuk insight bisnis singkat

---

##  Pandangan Upgrade Masa Depan

| Versi                            | Fokus Upgrade                                                                                              |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **v2 — Automation & Scheduler**  | Auto pipeline, file watcher, logging lebih canggih, scheduler (cron/Task Scheduler), siap multi-client     |
| **v3 — Insight Engine**          | Insight otomatis berbasis bisnis: tren penjualan, kenaikan produk, analisis bulk order, prediksi sederhana |
| **v4 — Decision Engine**         | Rule-based recommendation system: rekomendasi aksi bisnis (promo, stok, harga)                             |
| **v5 — Autonomous Agent System** | Multi-agent, workflow otomatis, minim intervensi manusia, integrasi API, dashboard, alert system           |

> Tujuan upgrade:
> Dari **v1 = data bersih & insight manual**, → **v5 = sistem otomatis end-to-end yang bisa membantu pengambilan keputusan bisnis secara real-time**

---

##  Tips untuk Memahami di Masa Depan

1. Fokus ke **struktur modular**, bukan file tunggal
2. Lihat `main.py` sebagai **entry point** → semua step pipeline dijalankan di sini
3. Folder `src/` → modular: mudah diupgrade ke v2–v5
4. `output/` dan `logs/` → **artifact nyata**, tunjukkan engine berhasil jalan
5. Saat upgrade, jangan hapus folder lama → jadikan fondasi pipeline

---

##  Catatan

* Engine ini **MVP & production-ready untuk level kecil**
* Bisa langsung dipakai untuk UMKM atau portfolio
* Memahami flow ini akan sangat membantu saat kamu naik ke **v2–v5**

---

**By Ashar Talatee — Talatee Data Nexus**
**Version:** v1 — Order Intelligence Engine
**Date:** 2026-03-31

```