# Engine #05: High-Integrity Excel Cleaner Engine

## 📌 1. Executive Summary

**Excel Cleaner Engine (Engine #05)** adalah komponen subsystem enterprise dari arsitektur **Talatee Data Nexus** yang dirancang khusus untuk menangani proses data ingestion, sanitasi, penyeragaman skema, dan audit trail pada berkas laporan marketplace (*E-commerce*) berskala besar.

Mayoritas skrip otomatisasi konvensional di industri rentan terhadap kegagalan fatal seperti kebocoran memori (*memory leaks*) saat membaca berkas di atas 50MB, data corruption, dan penghentian sistem sepihak (*hard crash*) akibat inkonsistensi tipe data sel. Engine ini menyelesaikan seluruh problem tersebut melalui pendekatan **Deterministic Chunking Pipeline** dan **Stateful Isolation Rule**.

### Key Architectural Pillars:
* **Memory-Bound Streaming (Zero OOM):** Memanfaatkan C-based Excel tokenization via `calamine` engine yang dikombinasikan dengan Python generator. RAM terkunci stabil di bawah 150 MB meskipun memproses jutaan baris data (*Gigabyte-scale assignment*).
* **Error Quarantine Circuit Breaker:** Mengisolasi record data yang korup atau menyalahi tipe aturan skema ke dalam log audit terpisah tanpa menginterupsi jalannya antrean pipeline (*Fault-Tolerant Engine*).
* **Decoupled Governance (Strict Separation of Concerns):** Logika bisnis, pemetaan matriks, aturan drop, dan ambang batas toleransi diisolasi penuh di dalam abstraksi file konfigurasi YAML, mengeliminasi kebutuhan modifikasi source code masa depan (*Open/Closed Principle*).

---

## 🏗️ 2. System & Repository Architecture

Sistem ini menerapkan hierarki struktur folder berbasis Clean Architecture untuk memisahkan dengan tegas antara zona data fisik, infrastruktur I/O, komponen inti, dan jejak audit (*audit trail*):

```text
excel-cleaner-engine/
├── config/                  # Governance Zone
│   ├── settings.yaml        # Data cleaning matrix rules & threshold definitions
│   └── logging.yaml         # Logging rotation & retention profiles
├── data/                    # State Isolation Zone (Strictly under .gitignore)
│   ├── 01_raw/              # Read-Only source files from marketplaces (Shopee/Tokopedia/TikTok)
│   ├── 02_processing/       # Shared-memory buffers/temporary swap files
│   ├── 03_output/           # High-integrity normalized files ready for analytics ingestion
│   └── 04_archive/          # Time-stamped historical historical record storage
├── logs/                    # Audit Trail Zone
│   ├── cleaner_execution.log# System execution traces, throughput statistics & resource monitors
│   └── cleaner_errors.log   # Error quarantine repository (Row-level schema fault dumps)
├── src/                     # Logic Source Code Zone
│   ├── core/                # Jantung fungsional: CleanerCore, SchemaValidator
│   ├── infra_io/            # High-performance IO: StreamReader, StreamWriter
│   │   ├── __init__.py
│   │   ├── reader.py
│   │   └── writer.py
│   └── main.py              # Central Pipeline Orchestrator & Context Manager
├── tests/                   # Automated Assurance Zone (Unit tests for core & IO pipelines)
├── .env.example             # Local deployment environment schema configuration
├── .gitignore               # Strict leakage protection rules
└── README.md                # System Architecture Blueprint Documentation