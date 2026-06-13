### Level 2 Engine: Automated Staging File Cleanser & Batch Renamer

Engine ini dirancang untuk mengatasi masalah inkonsistensi penamaan data mentah (raw data exports) dari berbagai marketplace sebelum didorong masuk ke dalam Data Warehouse pipeline.

#### Masalah Bisnis yang Diselesaikan:
- Mengeliminasi kegagalan pembacaan script ETL akibat spasi acak dan karakter ilegal (`%`, `&`, `$`, dll) pada file staging.
- Mencegah insiden hilangnya data asli akibat tabrakan nama baru (File Collision) melalui modul Pre-Flight Integrity Auditor.
- Memastikan atomisitas eksekusi lewat Fail-Safe Automatic Rollback jika terjadi interupsi I/O atau penolakan akses oleh OS di tengah proses batch.

#### Karakteristik Teknis:
- **Clean Architecture Principles**: Pemisahan ketat antara mutasi status I/O (engine) dengan pure functional transformation logic (rules).
- **Whitelist Sanitization**: Menggunakan regular expression berbasis whitelist guna menjamin keluaran nama file yang seragam dan rapi.
- **Transaksional**: Folder staging dibersihkan dan diisolasi per sesi guna memastikan idempotensi sistem.