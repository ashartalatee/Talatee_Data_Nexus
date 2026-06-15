# 🚀 High-Integrity Excel Merger Engine (v1.0.0)

Engine otomasi backend produksi yang dirancang khusus untuk menangani konsolidasi data multi-skema secara deterministik, aman memori, dan bebas *crash*. Mengeliminasi kegagalan integrasi akibat manipulasi struktur file Excel manual, pergeseran kolom data, serta kendala *file locking* pada sistem operasi.

---

## ⚡ Masalah Industri & Solusi Engine

* **Masalah Umum (Mengapa Skrip Biasa Sering Crash?):** 
  Mayoritas otomasi berbasis skrip tunggal (*spaghetti code*) langsung membaca data ke memori tanpa validasi skema dasar. Hal ini memicu `KeyError` atau kegagalan kalkulasi matematika jika ada file kiriman klien yang kolomnya bergeser, tipenya korup, atau file sedang dibuka di latar belakang (*file lock*).
* **Solusi Talatee Engine v1.0.0:** 
  Mengimplementasikan **Clean Architecture** dengan pemisahan mutlak antara manipulasi I/O fisik (*Drivers*), logika pemeriksaan skema (*Pure Services*), dan kendala alur kerja (*Orchestrator*). Proses dilengkapi dengan *Isolasi Vault* (Folder Staging) untuk menjamin integritas data primer.

---

## 🏗️ Filosofi Arsitektur & Folder Blueprint

Proyek ini dibangun di atas prinsip pemisahan tanggung jawab yang ketat (*Separation of Concerns*) agar sistem mudah diuji secara otomatis dan siap diskalakan ke lingkungan cloud native:

```text
excel_merger_engine/
│
├── config/         # Layer Skema Global & Konfigurasi Structured JSON Logging
├── core/           # Pure Orkestrator & Runtime Context Tracker (Run ID Global)
├── drivers/        # Technical Wrapper I/O (Safe File System & Memory-Efficient Reader)
├── services/       # Pure Business Logic (Aturan Validasi Struktur & Algoritma Merger)
├── storage/        # Sandbox Fisik Lokal (Input Area, Processing Vault, Output Area)
└── tests/          # Automated Testing Environment (Unit Testing & Robustness Test)