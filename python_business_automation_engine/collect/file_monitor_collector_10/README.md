# 📁 File Monitor Collector

> **Python Business Automation Engine #10** — Level 1: Data Collection Engine

Monitor folder secara real-time dan jalankan aksi otomatis saat ada file masuk, berubah, atau terhapus. Cocok untuk bisnis yang masih manual dalam mengelola file laporan, invoice, CSV, dan dokumen lainnya.

---

## 🎯 Problem Bisnis yang Diselesaikan

| Sebelum (Manual) | Sesudah (Otomatis) |
|---|---|
| Buka folder → cek file baru → sortir manual | Engine deteksi otomatis, sortir langsung |
| Lupa backup file penting | Auto-backup setiap file masuk |
| Tidak tahu kapan file diterima | Log lengkap: waktu, nama, ukuran |
| Butuh orang untuk monitor folder 24/7 | Engine jalan terus di background |

---

## ⚙️ Fitur

- ✅ **Real-time monitoring** — deteksi file Created, Modified, Deleted, Moved
- 📁 **Auto-sort** — file langsung pindah ke subfolder sesuai ekstensi (.pdf, .xlsx, .jpg, dll)
- 💾 **Auto-backup** — salinan otomatis setiap file masuk
- 🔔 **Notifikasi** — alert di console (bisa dikembangkan ke WhatsApp/Email/Slack)
- 📊 **Activity Log** — tersimpan sebagai `.log` dan `.csv` (bisa dibuka di Excel)
- 🔧 **Konfigurasi YAML** — ubah behavior tanpa menyentuh kode
- 🧪 **Unit tested** — coverage untuk logger dan action dispatcher

---

## 🏗️ Struktur Proyek

```
file-monitor-collector/
├── engine/
│   ├── __init__.py
│   ├── monitor.py       # Core: watchdog observer & event handler
│   ├── actions.py       # Auto-sort, backup, rename, notify
│   └── logger.py        # Dual logger: .log + .csv
├── config/
│   └── settings.yaml    # Konfigurasi folder, aksi, pola ignored
├── tests/
│   └── test_engine.py   # Unit tests (pytest)
├── logs/                # Generated: activity.log & activity.csv
├── inbox/               # Default: folder yang dimonitor
├── output/              # Default: hasil sortir
├── backup/              # Default: file backup
├── main.py              # Entry point
└── requirements.txt
```

---

## 🚀 Cara Pakai

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Konfigurasi folder

Edit `config/settings.yaml`:

```yaml
watch_folders:
  - "inbox"           # Ganti dengan path folder kamu
  # - "D:/Downloads"  # Bisa monitor lebih dari 1 folder

output_folder: "output"
```

### 3. Jalankan engine

```bash
# Jalankan dengan config default
python main.py

# Jalankan dengan config custom
python main.py --config config/my_settings.yaml

# Monitor folder tertentu langsung dari CLI
python main.py --watch ./laporan ./invoice

# Lihat ringkasan log aktivitas
python main.py --summary
```

### 4. Contoh output

```
╔══════════════════════════════════════════════════╗
║       FILE MONITOR COLLECTOR  v1.0.0             ║
║       Python Business Automation Engine #10      ║
╚══════════════════════════════════════════════════╝

🚀 File Monitor Engine AKTIF
   Folder: inbox
   Tekan Ctrl+C untuk berhenti.

2024-01-15 09:32:11 | INFO | ✅ [CREATED] inbox/laporan_jan.pdf
🔔 NOTIFIKASI: Ada aktivitas file baru!
   File : laporan_jan.pdf
   Size : 245.3 KB
   Waktu: 09:32:11

2024-01-15 09:32:11 | INFO | 📁 AUTO-SORT: laporan_jan.pdf → documents/pdf/
2024-01-15 09:32:11 | INFO | 💾 BACKUP: laporan_jan.pdf → backup/
```

---

## 🔧 Konfigurasi Aksi

```yaml
actions:

  # Sortir file otomatis berdasarkan ekstensi
  auto_sort:
    triggers: [on_created]
    extension_map:
      ".pdf":  "documents/pdf"
      ".xlsx": "documents/excel"
      ".csv":  "documents/data"
      ".jpg":  "images"

  # Backup otomatis saat file masuk
  backup:
    triggers: [on_created]
    backup_folder: "backup"

  # Notifikasi saat ada event
  notify:
    triggers: [on_created, on_deleted]
    message: "Ada aktivitas file baru!"
```

---

## 🧪 Testing

```bash
pip install pytest
pytest tests/ -v
```

```
tests/test_engine.py::TestActivityLogger::test_log_creates_log_file       PASSED
tests/test_engine.py::TestActivityLogger::test_log_creates_csv_file       PASSED
tests/test_engine.py::TestActivityLogger::test_get_summary_returns_correct_counts PASSED
tests/test_engine.py::TestActionDispatcher::test_auto_sort_moves_file_to_correct_folder PASSED
tests/test_engine.py::TestActionDispatcher::test_backup_copies_file       PASSED
tests/test_engine.py::TestActionDispatcher::test_notify_does_not_raise    PASSED
```

---

## 🔌 Use Case Nyata

| Industri | Penggunaan |
|---|---|
| **HR/Rekrutmen** | Auto-sort CV (.pdf) yang masuk ke folder per posisi |
| **Keuangan** | Deteksi invoice baru, backup otomatis, log waktu terima |
| **Operasional** | Monitor folder laporan harian dari berbagai cabang |
| **E-commerce** | Pantau folder export data order dari marketplace |
| **Manufaktur** | Deteksi file laporan produksi masuk dari mesin/sistem |

---

## 🗺️ Pengembangan Selanjutnya

- [ ] Notifikasi via **WhatsApp** (Twilio/WA Business API)
- [ ] Notifikasi via **Email** (SMTP)
- [ ] Notifikasi via **Telegram Bot**
- [ ] Upload otomatis ke **Google Drive**
- [ ] Filter berdasarkan **ukuran file** dan **nama file** (regex)
- [ ] **Dashboard web** real-time (Flask/FastAPI)
- [ ] Jalankan sebagai **background service** (systemd / Windows Service)

---

## 👤 About

Dibuat sebagai bagian dari **Python Business Automation Roadmap** — 100 engine untuk membantu bisnis bekerja lebih cepat, efisien, dan otomatis.

📌 **Engine #10 dari 100** | Level 1: Data Collection Engine

> *"Jangan cari ide setiap hari. Bangun sistem, ikuti roadmap, dan biarkan konsistensi yang bekerja untukmu."*

---

## 📄 License

MIT License — bebas digunakan dan dimodifikasi.