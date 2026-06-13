Kumpulan engine otomasi data tingkat produksi (*production-grade*) yang dirancang khusus untuk menangani interupsi data mentah (*raw data*) sebelum masuk ke dalam pipeline ETL/ELT. Sistem ini menyelesaikan masalah inkonsistensi data, karakter ilegal, serta kekacauan tata letak dokumen hasil ekspor dari berbagai marketplace (Shopee, Tokopedia, TikTok Shop) dan sistem internal perusahaan.

---

## 📌 Mengapa Ekosistem Ini Dibuat? (Latar Belakang Bisnis)

Di dalam industri data engineering, **kegagalan pembacaan skrip otomasi data paling sering disebabkan oleh hal-hal sepele pada level file-system**, seperti:
1. **Inkonsistensi Nama Berkas**: Adanya spasi acak dan karakter simbol berbahaya (`%`, `&`, `$`, `@`) pada berkas mentah hasil ekspor marketplace yang memicu *crash* saat dibaca oleh pustaka pemroses data atau saat diunggah ke *Cloud Storage* (AWS S3 / Google Cloud Storage).
2. **Kekacauan File Staging**: File transaksi, dokumen kerja, dan aset promosi bercampur di dalam satu folder yang sama, menyulitkan skrip *ingestion* melakukan pemindaian secara berkala (*automated cron scanning*).
3. **Risiko Kehilangan Data (Data Destruction)**: Skrip otomasi biasa sering kali menimpa file yang sudah ada jika terjadi kesamaan nama target (*file collision*), atau menyisakan folder dalam kondisi setengah rusak jika proses terhenti di tengah jalan akibat kendala hak akses OS.

**Data Nexus Suite** hadir untuk menyelesaikan seluruh masalah tersebut menggunakan pendekatan **Safety-First, Deterministic Routing, dan Transaksional (Fail-Safe Rollback)**.

---

## 🛠️ Modul Utama & Arsitektur

Ekosistem ini terbagi menjadi dua mesin independen yang saling melengkapi namun berjalan di bawah aturan arsitektur yang ketat:

### 1. Level 2 Engine: Batch Rename Engine
Berfokus pada sanitasi nama berkas menggunakan pendekatan *Strict Whitelist Regex*.
* **Pre-Flight Integrity Auditor**: Memindai seluruh direktori dan membuat rencana perubahan target (*manifest*) secara virtual untuk memastikan tidak ada rute nama baru yang bertabrakan (*collision protection*) sebelum satu pun file diubah di disk fisik.
* **Fail-Safe Automatic Rollback**: Jika sistem operasi menolak akses tulis di tengah-tengah proses *batch processing*, engine otomatis membatalkan operasi pada file yang terlanjur diubah dan mengembalikannya ke nama semula untuk mencegah kondisi *corrupted partial state*.

### 2. Level 3 Engine: File Organizer Engine
Berfokus pada tata kelola dan distribusi berkas otomatis berbasis *Deterministic Routing Matrix*.
* **Dynamic Capacity Check**: Secara proaktif memeriksa ketersediaan ruang penyimpanan (*disk usage analysis*) pada drive tujuan sebelum memindahkan file berukuran besar.
* **Isolation Routing**: Berkas yang memiliki ekstensi tidak dikenal atau tidak terdaftar di dalam konfigurasi tidak akan memicu *crash*, melainkan dialirkan secara aman ke zona isolasi folder `unclassified` untuk diaudit secara manual.

---

## 📁 Struktur Direktori Standar Industri

Struktur proyek dirancang menggunakan prinsip pemisahan logika yang ketat (*Separation of Concerns*) sehingga sangat mudah diuji (*highly testable*) dan bebas dari ketergantungan melingkar (*circular imports*):

```text
business_automation_lab/
├── engines/
│   ├── batch_rename_engine/
│   │   ├── config/              # Manajemen skema aturan penamaan (.yaml)
│   │   ├── src/
│   │   │   ├── core/            # Logika mutasi (engine.py, rules.py, validator.py)
│   │   │   ├── exceptions/      # Custom Exception Domain (Pencegah generic crash)
│   │   │   ├── utils/           # Enterprise contextual logger
│   │   │   └── main.py          # Entry point demonstrasi laborat
│   │   └── logs/                # Berkas log audit penamaan
│   │
│   ├── file_organizer_engine/
│   │   ├── config/              # Matriks rute distribusi data (.yaml)
│   │   ├── src/
│   │   │   ├── core/            # Logika perutean (engine.py, router.py, validator.py)
│   │   │   ├── exceptions/      # Custom Exception Domain lintas I/O Storage
│   │   │   ├── utils/           # Enterprise contextual logger
│   │   │   └── main.py          # Entry point demonstrasi laborat
│   │   └── logs/                # Berkas log audit pergerakan berkas