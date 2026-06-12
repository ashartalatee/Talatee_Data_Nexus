
---

```markdown
# 📊 PDF Table to CSV Automation Engine (v1.0.0)

**PDF Table to CSV Engine** adalah sistem otomasi berbasis Python yang dirancang untuk mendeteksi, mengekstrak, dan menggabungkan seluruh data tabel dari dokumen PDF (bahkan dokumen tebal ratusan halaman) menjadi satu file CSV bersih yang siap pakai secara otomatis dalam hitungan detik.

---

## 💡 Mengapa Engine Ini Dibuat? (The Problem)
Di dunia bisnis dan operasional *marketplace*, laporan keuangan, mutasi rekening, atau rekapitulasi data seringkali terkunci di dalam format **PDF**. 

Melakukan *copy-paste* tabel dari PDF ke Excel secara manual memiliki 3 masalah besar:
1. **Membuang Waktu:** Proses seleksi data per halaman memakan waktu berjam-jam.
2. **Rawan Salah Input:** Karakter angka sering kali bergeser, spasi berantakan, atau baris data terlewat (*human error*).
3. **Format Rusak:** Jika struktur kolom di halaman 1 dan halaman berikutnya sedikit bergeser, struktur Excel Anda akan hancur berantakan.

**Solusinya:** Engine ini mengotomatisasi seluruh proses tersebut dengan standar *data engineering* profesional—menjaga akurasi data 100% dan memotong waktu kerja menjadi hitungan detik.

---

## 🛠️ Kegunaan & Fitur Utama Engine
* **Otomasi Folder Otomatis:** Anda cukup menaruh file PDF di folder `input/`, jalankan sistem, dan hasil CSV otomatis muncul di folder `output/`.
* **Mendukung Dokumen Ratusan Halaman:** Dilengkapi algoritma manajemen memori berbasis halaman (*page-by-page streaming*), mencegah komputer Anda *hang* atau *crash* saat memproses PDF raksasa.
* **Auto Schema Alignment:** Jika tabel di halaman terpisah memiliki perbedaan jumlah atau nama kolom, *engine* akan otomatis menyelaraskan struktur kolomnya tanpa memutus jalannya program.
* **Isolasi Error (Robust):** Jika terdapat satu file PDF yang rusak (*corrupt*), sistem akan mencatatnya di log dan tetap melanjutkan pemrosesan untuk file PDF lainnya.
* **Audit Trail (Log System):** Setiap proses mencetak laporan performa yang mendetail (Nama file, jumlah halaman, total tabel ditemukan, dan total baris data yang diekstrak).

---

## 📁 Struktur Project Standar Produksi

```text
pdf_table_to_csv_engine/
│
├── input/                  # Tempat menaruh file PDF mentah Anda
├── output/                 # Hasil konversi berupa CSV bersih (Auto-generated)
├── logs/                   # Log operasional untuk audit & pelacakan error
├── src/
│   ├── extractor.py        # Modul pembaca & pengekstraksi tabel PDF
│   ├── processor.py        # Modul pengolah & penyelarasan dataframe (Pandas)
│   └── main.py             # Orchestrator / Entry Point utama aplikasi
│
├── generate_mock_pdf.py    # Script utilitas pembuat data tiruan (untuk demo)
└── requirements.txt        # Dependensi pustaka Python

```

---

## 🚀 Cara Menjalankan Engine

### 1. Kebutuhan Sistem

Pastikan komputer Anda sudah terinstall **Python 3.8** atau versi di atasnya.

### 2. Install Dependensi

Buka terminal / Command Prompt di direktori project ini, lalu instal pustaka yang dibutuhkan dengan perintah:

```bash
pip install -r requirements.txt

```

### 3. Jalankan Pengujian Data Tiruan (Optional / Untuk Demo)

Jika Anda belum punya file PDF tebal dan ingin mencoba performa engine, jalankan script pembentuk data tiruan 150 baris ini:

```bash
python generate_mock_pdf.py

```

*File `REKAP_TRANSAKSI_MARKETPLACE_LARGE.pdf` otomatis akan muncul di folder `input/`.*

### 4. Jalankan Otomasi Utama

Letakkan file PDF asli Anda di dalam folder `input/`, lalu eksekusi *orchestrator* utama menggunakan perintah:

```bash
python src/main.py

```

---

## 📋 Contoh Hasil Audit Log Terminal

Ketika sistem sukses mengeksekusi data, terminal dan file `.log` Anda akan menampilkan laporan validasi seperti ini:

```text
2026-06-12 11:45:02 [INFO] PDFEngine.Main - === PDF Table to CSV Automation Engine Dimulai ===
2026-06-12 11:45:02 [INFO] PDFEngine.Main - Menemukan 1 file PDF siap diproses.
2026-06-12 11:45:02 [INFO] PDFEngine.Main - Starting pipeline untuk file: REKAP_TRANSAKSI_MARKETPLACE_LARGE.pdf
2026-06-12 11:45:03 [INFO] PDFEngine.Extractor - Memproses REKAP_TRANSAKSI_MARKETPLACE_LARGE.pdf | Total: 5 halaman.
2026-06-12 11:45:05 [INFO] PDFEngine.Main - 

[REPORT SUCCESS] File Sukses Diproses:
   - Nama File           : REKAP_TRANSAKSI_MARKETPLACE_LARGE.pdf
   - Jumlah Halaman      : 5
   - Jumlah Tabel        : 5
   - Total Baris Ekstrak : 150
   - Lokasi Output       : D:\pdf_table_to_csv_engine\output\REKAP_TRANSAKSI_MARKETPLACE_LARGE_exported.csv

2026-06-12 11:45:05 [INFO] PDFEngine.Main - === Seluruh Pipeline Selesai Dieksekusi ===

```

---

## ⚖️ Lisensi & Kontribusi

Dibuat untuk kebutuhan laboratorium otomasi bisnis dan data independen. Silakan gunakan, modifikasi, dan bagikan kode ini untuk meningkatkan efisiensi operasional bisnis Anda.

*“Garbage In, Garbage Out. Bersihkan datamu sebelum menggunakannya untuk keputusan bisnis.”*

```

```