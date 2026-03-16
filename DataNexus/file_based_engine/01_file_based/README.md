
---

# Talatee Data Nexus – File Data Automation Engine

Talatee Data Nexus adalah **automation data pipeline engine** yang dirancang untuk memproses data berbasis file secara otomatis mulai dari ingestion, validation, cleaning hingga storage.

Engine ini dibangun sebagai **mini data platform** yang mensimulasikan workflow yang biasa digunakan dalam sistem **Data Engineering dan Automation Systems modern**.

Tujuan utama project ini adalah membangun **automation pipeline end-to-end** yang dapat membaca banyak file, memprosesnya secara otomatis, dan menghasilkan dataset yang siap digunakan untuk analitik atau sistem downstream.

Project ini merupakan bagian dari pengembangan **Talatee Data Nexus**, sebuah modular automation data platform.

---

# System Architecture Overview

Pipeline bekerja dalam beberapa tahap utama berikut:

```
File Scanner
      ↓
File Classifier
      ↓
Parallel Data Loader
      ↓
Data Merge
      ↓
Data Validation
      ↓
Data Cleaning
      ↓
Dataset Storage
      ↓
Pipeline Reporting
```

Selain pipeline utama, engine ini juga memiliki beberapa sistem pendukung seperti:

* Logging system
* Incremental file processing
* Dataset versioning
* Pipeline run reporting
* Parallel file loading
* Scheduler automation

Struktur ini mensimulasikan **workflow ETL pipeline yang umum digunakan dalam sistem data modern**.

---

# Key Features

## 1. File Ingestion

Engine dapat membaca berbagai sumber file seperti:

* CSV
* Excel
* PDF (untuk ekstraksi pada tahap selanjutnya)

Folder input akan discan secara otomatis untuk menemukan file baru yang masuk ke sistem.

---

## 2. Incremental Processing

Engine hanya memproses **file baru yang belum pernah diproses sebelumnya**.

Metadata file disimpan pada:

```
metadata/processed_files.json
```

Tujuan incremental processing:

* menghindari duplicate processing
* meningkatkan efisiensi pipeline
* mensimulasikan incremental pipeline pada sistem produksi.

---

## 3. Parallel File Processing

CSV dan Excel loader menggunakan **multi-threading** sehingga beberapa file dapat diproses secara bersamaan.

Hal ini membuat pipeline lebih scalable ketika menangani banyak file.

Teknologi yang digunakan:

```
concurrent.futures
ThreadPoolExecutor
```

---

## 4. Data Validation

Dataset yang telah digabung akan diperiksa kualitasnya.

Beberapa metrik yang diperiksa:

* total rows
* missing values
* duplicate rows

Laporan kualitas data akan ditampilkan melalui console log.

---

## 5. Data Cleaning

Pipeline melakukan proses pembersihan dataset seperti:

* menghapus duplicate rows
* menangani missing values
* memastikan dataset siap digunakan untuk analisis.

---

## 6. Dataset Versioning

Dataset hasil pipeline tidak akan menimpa dataset sebelumnya.

Dataset disimpan dengan format berikut:

```
clean_dataset_YYYY_MM_DD_v1.csv
clean_dataset_YYYY_MM_DD_v2.csv
```

Tujuan dataset versioning:

* menjaga histori dataset
* memungkinkan audit data pipeline
* mensimulasikan data versioning pada data lake architecture.

---

## 7. Pipeline Scheduler

Pipeline dapat dijalankan secara otomatis menggunakan scheduler.

Contoh konfigurasi:

```
Pipeline run every 5 minutes
```

Hal ini mensimulasikan workflow automation seperti pada sistem orkestrasi pipeline.

---

## 8. Pipeline Run Report

Setiap pipeline run menghasilkan laporan yang disimpan pada:

```
reports/pipeline_run_report.json
```

Contoh laporan:

```
{
  "timestamp": "2026-03-16 14:55:10",
  "files_processed": 2,
  "rows_processed": 1240,
  "duplicates_removed": 30,
  "runtime_seconds": 1.87,
  "status": "SUCCESS"
}
```

Tujuan laporan ini adalah menyediakan **observability untuk pipeline automation**.

---

# Project Structure

Struktur project Talatee Data Nexus:

```
Talatee_Data_Nexus
│
├── config
│   settings.py
│
├── engine
│   file_scanner.py
│   file_classifier.py
│
│   csv_loader.py
│   excel_loader.py
│
│   data_validator.py
│   data_cleaner.py
│   data_storage.py
│
│   logger.py
│   scheduler.py
│
│   file_tracker.py
│   pipeline_reporter.py
│
├── metadata
│   processed_files.json
│
├── reports
│   pipeline_run_report.json
│
├── output
│   clean_dataset_YYYY_MM_DD_v1.csv
│
└── scripts
    run_engine.py
```

Struktur ini dibuat modular agar pipeline mudah dikembangkan dan diintegrasikan dengan engine lain di masa depan.

---

# How to Run the Engine

Jalankan pipeline dengan perintah berikut:

```
python scripts/run_engine.py
```

Engine akan melakukan langkah berikut secara otomatis:

1. Scan folder input
2. Mendeteksi file baru
3. Memproses dataset
4. Membersihkan data
5. Menyimpan dataset hasil
6. Membuat laporan pipeline run.

---

# Technologies Used

Project ini menggunakan beberapa teknologi utama:

* Python
* Pandas
* Concurrent Futures
* JSON metadata tracking
* Python logging system

Teknologi ini digunakan untuk mensimulasikan **workflow automation pipeline pada sistem data engineering modern**.

---

# Talatee Data Nexus – Engine Ecosystem

Talatee Data Nexus dirancang sebagai **modular automation data platform**.

File Data Automation Engine adalah **engine pertama** dalam sistem ini.

Arsitektur platform ke depannya akan terlihat seperti berikut:

```
                Talatee Data Nexus
                       │
        ┌──────────────┼──────────────┐
        │                              │
 File Data Automation Engine     Web Data Extraction Engine
        │                              │
        └──────────────┬──────────────┘
                       │
                Data Pipeline Engine
                       │
                Data Storage Layer
                       │
                 Data Service API
                       │
                Data Product Layer
```

Tujuan arsitektur ini adalah membangun **automation platform yang mampu mengumpulkan, memproses, dan menyediakan data secara otomatis**.

---

# Planned Engine Modules

Talatee Data Nexus akan berkembang dengan beberapa engine tambahan:

### Web Data Extraction Engine

Engine untuk mengumpulkan data dari:

* website scraping
* REST API
* automated crawling

Pipeline:

```
Web Source
     ↓
Data Scraper
     ↓
Data Parser
     ↓
Raw Data Storage
```

---

### API Data Pipeline Engine

Pipeline yang menangani data dari API eksternal.

```
External API
     ↓
Data Fetcher
     ↓
Data Normalizer
     ↓
Database Storage
```

---

### Database Data Pipeline Engine

Engine untuk mengelola pipeline berbasis database.

Database yang direncanakan:

* PostgreSQL
* MySQL
* SQLite

Pipeline:

```
Raw Data
    ↓
Transform Engine
    ↓
Database Storage
```

---

### Data Service API Engine

Engine ini akan menyediakan dataset melalui REST API.

Contoh alur:

```
Dataset
   ↓
API Layer
   ↓
Client Application
```

---

# Planned System Upgrades

Beberapa upgrade teknis akan dikembangkan untuk meningkatkan kemampuan pipeline.

---

## Upgrade 1 — Advanced Parallel Processing

Pipeline akan ditingkatkan menggunakan:

```
ThreadPoolExecutor
ProcessPoolExecutor
```

Tujuan:

* meningkatkan kecepatan pemrosesan file
* menangani dataset besar
* mensimulasikan high-volume data ingestion.

---

## Upgrade 2 — Schema Alignment Engine

Engine ini akan menyelaraskan schema dataset dari berbagai sumber.

Contoh kasus:

```
file1.csv → name
file2.csv → customer_name
file3.csv → full_name
```

Pipeline akan melakukan mapping otomatis:

```
name
customer_name
full_name
     ↓
customer_name
```

Tujuan:

memungkinkan pipeline menangani **dataset dengan struktur berbeda**.

---

## Upgrade 3 — Data Profiling System

Engine akan menghasilkan laporan profiling data secara otomatis.

Contoh informasi:

* column statistics
* missing values ratio
* duplicate percentage
* column distribution

Report akan dihasilkan dalam bentuk:

```
HTML Data Quality Dashboard
```

---

## Upgrade 4 — Data Lake Architecture

Talatee Data Nexus akan mengadopsi struktur data lake:

```
RAW DATA (Bronze)
       ↓
CLEAN DATA (Silver)
       ↓
ANALYTICS DATA (Gold)
```

Struktur ini memungkinkan pipeline menjadi lebih scalable dan audit-friendly.

---

## Upgrade 5 — Pipeline Monitoring Dashboard

Dashboard monitoring pipeline akan dibangun untuk menampilkan:

* pipeline runs
* data volume
* processing time
* error logs

Dashboard ini akan memberikan **observability penuh terhadap automation pipeline**.

---

# Long-Term Vision

Talatee Data Nexus bertujuan menjadi **automation data platform modular** yang mampu menangani berbagai jenis pipeline seperti:

* file data ingestion
* web data scraping
* API data pipelines
* database pipelines
* automation workflows

Dengan arsitektur modular ini, Talatee Data Nexus dapat berkembang menjadi **automation system untuk pengumpulan, pemrosesan, dan penyediaan data secara end-to-end**.

---

# Author

Ashar Talatee
Automation & Data Systems Builder

Project ini merupakan bagian dari perjalanan membangun **automation systems dan data pipelines dari nol**, dengan fokus pada praktik engineering yang digunakan dalam sistem data modern.

---