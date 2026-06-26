# 🚀 IDX Capital Sentinel Engine (v1.0.0)

**IDX Capital Sentinel Engine** adalah sistem otomatisasi data (*data decision system*) berbasis Python yang dirancang untuk mengekstrak, menganalisis, dan melacak perubahan data kepemilikan saham harian di atas 5% langsung dari dokumen keterbukaan informasi PT Bursa Efek Indonesia (IDX).

Sistem ini dibangun menggunakan **Clean Architecture** untuk memisahkan logika pengambilan data (*data ingestion*), komputasi matematika (*intelligence layer*), dan tampilan laporan (*formatting layer*). Engine ini dilengkapi dengan *wrapper script* khusus sehingga **dapat dioperasikan 100% oleh pengguna non-IT tanpa perlu menyentuh baris kode.**

---

## 🎯 Masalah Masalah yang Diselesaikan (Problem Validation)
Bagi analis finansial, trader, atau fund manager, melacak pergerakan investor besar (*Whale tracking*) dari dokumen PDF resmi IDX setiap hari adalah pekerjaan yang melelahkan:
* **Gagal Scraping:** Penarikan otomatis menggunakan Google Script seringkali diblokir atau gagal karena struktur web dinamis.
* **Excel Macro Lambat:** Konversi PDF ke CSV menggunakan VBA/Macro Excel konvensional memakan waktu lama, sering *crash*, dan hasilnya berantakan.
* **Analisis Manual:** Membandingkan data hari ini dan kemarin untuk mencari tahu siapa *whale* yang menambah atau mengurangi porsi saham masih dilakukan secara manual menggunakan rumus baris demi baris.

**Solusi Engine Ini:** Melakukan ekstraksi berbasis komponen, melakukan kalkulasi delta matematis di latar belakang, dan langsung menyajikan laporan Excel yang interaktif, bersih, serta menggunakan pewarnaan otomatis (*conditional formatting*) dalam hitungan detik.

---

## 📂 Cetak Biru Arsitektur Folder (Directory Blueprint)

Projek ini menerapkan pemisahan ruang kerja yang ketat antara area interaksi pengguna awam (*User Space*) dan komponen internal kode (*Core System*):

```text
idx_sentinel_engine/
│
├── 🚀 JALANKAN_ENGINE.bat          <-- Pintu utama pengguna (Cukup klik-ganda)
├── 📄 requirements.txt             <-- Daftar library pendukung sistem
│
├── 📂 1_INPUT_PDF/                 <-- USER SPACE: Tempat menyimpan file PDF mentah dari IDX
│   └── 💡 arsip_diproses/          <-- Auto-Generated: PDF yang sukses diproses otomatis pindah ke sini
│
├── 📂 2_OUTPUT_REPORT/             <-- USER SPACE: Tempat mengambil hasil analisis akhir
│   ├── 📊 rekap_saham_master.xlsx  <-- File Excel utama (Interaktif & Berwarna)
│   └── 🕒 histori_harian/          <-- Data simpanan internal (.csv) untuk basis data kemarin
│
├── 📂 automation_gateway/          <-- EXTENSION LAYER: Gerbang otomatisasi hulu
│   ├── 🌐 idx_scraper.py           <-- Otomatisasi download PDF dari IDX via Playwright
│   └── 🤖 telegram_bot.py          <-- Penghubung interaksi data nirkabel via Telegram Bot
│
└── 📂 core_system/                 <-- ENGINE LAYER: Otak utama sistem (Hidden/Jangan diubah)
    ├── 📄 config.json              <-- Single Source of Truth (Konfigurasi global & path)
    ├── 📄 main.py                  <-- Orchestrator utama pengatur alur data
    ├── 📄 pdf_processor.py         <-- Ingestion: Ekstraktor tabel PDF menjadi DataFrame bersih
    ├── 📄 analyst_intelligence.py  <-- Intelligence: Kalkulator kalkulasi akumulasi & distribusi
    └── 📄 excel_formatter.py       <-- UI/UX: Modul kosmetik pewarnaan otomatis pada Excel



## Cara menjalankan di vscode : python core_system/main.py