# Engine 06: Marketplace Data Collector (Shopee, Tokopedia, TikTok Shop)

Mesin otomatisasi pengumpulan data (*Data Ingestion Engine*) berskala produksi yang dirancang khusus untuk menarik metrik produk, harga, stok, dan performa penjualan dari berbagai marketplace terbesar di Indonesia secara *end-to-end*.

## 🚀 Fitur Utama & Desain Industri (Anti-Crash)
* **Clean Architecture Decoupling:** Pemisahan mutlak antara proses penarikan data (*Ingestion Layer*) dan pembersihan data (*Transformation Layer*). Perubahan API di salah satu marketplace tidak akan merusak sirkuit utama sistem.
* **Immutable Raw Data (Audit Trail):** Menyimpan muatan asli (*raw payload JSON*) dari marketplace ke penyimpanan lokal sebelum diproses. Jika terjadi kegagalan skema di kemudian hari, pemrosesan ulang dapat dilakukan langsung tanpa *scraping* ulang.
* **Deterministic Circuit Protection:** Menggunakan pustaka **Pydantic** untuk validasi skema data yang ketat, mencegah data korup atau kosong masuk ke gudang data (*Data Warehouse*).
* **Exponential Backoff Resiliency:** Penanganan otomatis terhadap limitasi jaringan, *rate limiting* (HTTP 429), dan gangguan server *upstream* menggunakan strategi percobaan ulang (*retry mechanism*).

## 🛠️ Struktur Repositori
```text
marketplace_data_collector/
├── config/           # Konfigurasi global & template rahasia
├── data/             # I/O lokal terisolasi (raw, processed, logs)
├── src/              # Kode sumber utama (core, extractors, transformers, storage)
└── tests/            # Automated unit testing suite