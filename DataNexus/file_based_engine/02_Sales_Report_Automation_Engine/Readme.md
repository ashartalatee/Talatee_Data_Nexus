# Sales Report Automation Engine

## Overview

**Sales Report Automation Engine** adalah sistem automation berbasis Python yang dirancang untuk:

* Mengolah data penjualan dari file (CSV / Excel)
* Membersihkan dan memvalidasi data
* Menggabungkan data produk & transaksi
* Menghitung metrik bisnis
* Menghasilkan laporan penjualan otomatis
* Menjalankan pipeline end-to-end secara otomatis

Engine ini merepresentasikan **workflow nyata di industri**, khususnya pada:

* E-commerce reporting
* Retail sales reporting
* Small business analytics

---

## Objectives

Tujuan utama dari project ini:

* Membangun **end-to-end data pipeline**
* Melatih mindset **Automation Engineer**
* Menghasilkan sistem yang:

  * reusable
  * scalable
  * configurable
  * production-ready (basic level)

---

## Architecture Overview

```
INPUT DATA
(sales.csv, products.csv)
        │
        ▼
Data Ingestion (loader.py)
        │
        ▼
Data Validation (validator.py)
        │
        ▼
Data Cleaning (cleaner.py)
        │
        ▼
Data Transformation (transformer.py)
        │
        ▼
Data Merge (merger.py)
        │
        ▼
Aggregation (merger.py)
        │
        ▼
Report Generation (reporter.py)
        │
        ▼
Automation Execution (run_engine.py)
        │
        ▼
OUTPUT (sales_report.xlsx)

Logs → logs/engine.log
Errors → logs/error_rows.csv
```

---

## Project Structure

```
sales-report-engine/
│
├── engine/
│   ├── loader.py
│   ├── validator.py
│   ├── cleaner.py
│   ├── transformer.py
│   ├── merger.py
│   ├── reporter.py
│   ├── config_loader.py
│
├── scripts/
│   └── run_engine.py
│
├── input/
│   ├── sales_data.csv
│   └── products.csv
│
├── output/
│   └── sales_report.xlsx
│
├── logs/
│   ├── engine.log
│   └── error_rows.csv
│
├── config/
│   └── settings.yaml
│
└── README.md
```

---

## Features

* ✅ Automated data pipeline
* ✅ Data validation system
* ✅ Error logging (invalid rows tracking)
* ✅ Data cleaning & transformation
* ✅ Data merging & aggregation
* ✅ Excel report generation
* ✅ Config-driven system (YAML)
* ✅ CLI execution
* ✅ Logging system

---

## How to Run

### 1. Install dependencies

```
pip install pandas openpyxl pyyaml
```

### 2. Run the engine

```
python scripts/run_engine.py --run
```

### 3. Output

```
✅ PIPELINE SUCCESS
📊 Report generated at: output/sales_report.xlsx
```

---

## Input Example

### sales_data.csv

```
order_id,date,product_id,quantity,price
1001,2026-03-01,P01,2,50
1002,2026-03-01,P02,1,100
```

### products.csv

```
product_id,product_name,category
P01,Keyboard,Electronics
P02,Mouse,Electronics
```

---

## Output Example

| Date       | Category    | Total Sales |
| ---------- | ----------- | ----------- |
| 2026-03-01 | Electronics | 200         |

---

## Configuration System

Semua path diatur melalui:

```
config/settings.yaml
```

Contoh:

```
paths:
  sales_data: input/sales_data.csv
  products_data: input/products.csv
  output_report: output/sales_report.xlsx
  error_log: logs/error_rows.csv
  log_file: logs/engine.log
```

 Tidak perlu mengubah code untuk mengganti input/output.

---

## Engineering Concepts Applied

Project ini melatih konsep penting:

* Data Pipeline Design
* Separation of Concerns
* Logging & Monitoring
* Error Handling
* Data Quality Control
* Config-driven Architecture
* CLI Tooling

---

## Limitations (Current Version)

Versi ini adalah **Basic Production Level**, dengan keterbatasan:

* Belum ada scheduler (manual run)
* Belum ada retry system
* Belum ada alert (email/notification)
* Belum ada database integration
* Belum ada API ingestion

---

## Future Upgrade (INDUSTRY LEVEL ROADMAP)

Project ini DIDESAIN untuk bisa di-upgrade ke level industri.

### Phase 2 — Industry Level Upgrade

#### 1. Modular Pipeline System

* Pisahkan pipeline menjadi independent modules
* Gunakan pattern:

  ```
  pipeline/
    ingestion/
    validation/
    transformation/
  ```

#### 2. Scheduler (Automation)

* Gunakan:

  * cron (Linux)
  * Airflow (advanced)
* Tujuan:

  * auto run harian

#### 3. Retry System

* Jika gagal:

  * auto retry
* Tools:

  * tenacity / custom retry logic

#### 4. Data Quality Alert

* Kirim alert jika:

  * data kosong
  * error tinggi
* Tools:

  * email / Telegram bot

#### 5. Database Integration

* Input:

  * MySQL / PostgreSQL
* Output:

  * data warehouse

#### 6. Structured Logging

* Gunakan format JSON log
* Siap untuk monitoring tools

#### 7. CLI Advanced

Contoh:

```
python run_engine.py --run --env=prod --date=2026-03-01
```

#### 8. Packaging

* Jadikan project sebagai:

  ```
  pip install sales-engine
  ```

---

## Real-World Value

Engine ini bisa digunakan untuk:

* Automasi laporan bisnis
* Mengurangi kerja manual
* Membantu pengambilan keputusan
* Meningkatkan efisiensi operasional

---

## Author Mindset

Project ini dibangun dengan tujuan:

> “Bukan sekadar belajar Python, tapi membangun sistem yang bernilai di dunia nyata.”

Jika kamu membaca ini di masa depan:

* Upgrade engine ini
* Tambahkan kompleksitas
* Jadikan ini foundation kariermu

---

## 🏁 Final Note

Jika kamu sudah sampai tahap ini:

✅ Kamu sudah memahami end-to-end pipeline
✅ Kamu sudah membangun sistem nyata
✅ Kamu sudah selangkah lebih dekat ke level profesional

---

## Next Step

Bangun 2 engine tambahan:

1. Finance Reconciliation Engine
2. Inventory Monitoring Engine

 Setelah itu, kamu bukan lagi pemula.

---

**Keep building. Keep shipping.**
