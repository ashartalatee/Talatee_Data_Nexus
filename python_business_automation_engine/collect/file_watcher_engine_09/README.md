# 📂 File Watcher Engine
### Collect Engine #9 · Python Business Automation Portfolio

> **Pantau folder otomatis. Tidak perlu coding. Langsung jalan.**

[![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Portfolio](https://img.shields.io/badge/Portfolio-100%2B%20Engines-orange)](https://github.com/[username])
[![Non-IT Friendly](https://img.shields.io/badge/Non--IT-Friendly%20GUI-purple)](docs/gui-guide.md)

---

## 🎯 Masalah yang Dipecahkan

| Situasi | Solusi |
|---|---|
| File laporan datang manual dari email | Deteksi otomatis → copy ke folder laporan |
| Tim sering lupa kirim file ke folder server | Monitor folder + notifikasi real-time |
| Butuh tahu kapan file berubah/dihapus | Event log lengkap dengan timestamp |
| Staf non-IT tidak bisa pakai script | GUI sederhana, tinggal klik |

---

## ✨ Fitur Utama

- 🔍 **Real-time monitoring** — deteksi file baru, berubah, atau dihapus
- ⚡ **3 aksi otomatis** — Copy, Move, atau Log Only
- 🎛️ **Filter ekstensi** — pantau hanya `.pdf`, `.xlsx`, atau semua file
- 🖥️ **GUI untuk non-IT** — tidak perlu terminal, tinggal Browse & Start
- 📋 **Activity log** — log harian otomatis tersimpan di folder `logs/`
- 🔁 **Background thread** — tidak memblokir komputer Anda
- 📦 **Zero dependency** — hanya pakai Python bawaan (no pip install)

---

## 🚀 Quick Start

### Option 1: GUI (untuk non-IT / business user)
```bash
python src/gui.py
```
1. Klik **Browse** → pilih folder yang ingin dipantau
2. Pilih folder **Output** tujuan file
3. Pilih **Aksi** (Copy / Move / Log Only)
4. Klik **▶ Start Watching**

### Option 2: Terminal / Script
```bash
python src/engine.py --watch "C:/Downloads" --output "C:/Laporan" --action copy --ext pdf,xlsx
```

### Option 3: Import di project Anda
```python
from src.engine import FileWatcherEngine, WatcherConfig

config = WatcherConfig(
    watch_folder  = "C:/Downloads",
    output_folder = "C:/Laporan/Otomatis",
    action        = "copy",                # copy | move | log_only
    file_extensions = ["pdf", "xlsx"],     # ["*"] untuk semua file
    check_interval_seconds = 5,
)

engine = FileWatcherEngine(config)
engine.start()
```

---

## 📸 Tampilan GUI

```
┌─────────────────────────────────────────────────────────────┐
│ 📂 File Watcher Engine   Collect Engine #9          ● Idle  │
├─────────────────────────────────────────────────────────────┤
│ 📁 Folder to Watch                                          │
│ [ C:/Users/Anda/Downloads              ] [ Browse… ]        │
│                                                             │
│ 📤 Output / Destination Folder                              │
│ [ C:/Laporan/Otomatis                  ] [ Browse… ]        │
│                                                             │
│ ⚡ Aksi [copy ▼]  🔍 Ekstensi [pdf,xlsx]  ⏱ Interval [5]   │
│                                                             │
│ [ ▶ Start Watching ]  [ ■ Stop ]  [ 🗑 Clear Log ]          │
├─────────────────────────────────────────────────────────────┤
│ 📋 Activity Log                                             │
│ Time              Event       File Name          Size (KB)  │
│ 2025-01-15 09:31  ✅ CREATED  laporan_jan.pdf    245.3      │
│ 2025-01-15 09:28  🔄 MODIFIED data_sales.xlsx    88.1       │
│ 2025-01-15 09:22  🗑 DELETED  draft_lama.docx    12.0       │
└─────────────────────────────────────────────────────────────┘
```

---

## 📁 Struktur Project

```
file-watcher-engine/
├── src/
│   ├── engine.py       # Core engine (bisa dipakai tanpa GUI)
│   └── gui.py          # Aplikasi GUI untuk non-IT user
├── logs/               # Log harian otomatis (dibuat saat jalan)
├── output/             # Default output folder
├── tests/
│   └── test_engine.py  # Unit tests
├── docs/
│   └── gui-guide.md    # Panduan GUI (dengan screenshot)
├── requirements.txt
└── README.md
```

---

## ⚙️ Konfigurasi Lengkap

```python
WatcherConfig(
    watch_folder            = "path/to/folder",  # WAJIB
    output_folder           = "output",           # default: "output"
    file_extensions         = ["*"],              # ["pdf","xlsx"] atau ["*"]
    action                  = "copy",             # copy | move | log_only | custom
    recursive               = False,              # pantau subfolder juga?
    check_interval_seconds  = 5,                  # cek setiap N detik
    auto_create_output      = True,               # buat folder output otomatis
)
```

---

## 📊 Real Business Case

**Kasus: Tim Keuangan menerima laporan PDF setiap hari**
```
Input  : C:/Email Attachments/
Output : C:/Laporan/2025/Januari/
Action : copy
Filter : pdf
```
Hasil: PDF masuk ke Email Attachments → otomatis tersalin ke folder laporan + tercatat di log.

---

## 🧪 Testing

```bash
python -m pytest tests/
```

---

## 🗺️ Roadmap

- [x] Core file detection (CREATED, MODIFIED, DELETED)
- [x] GUI untuk non-IT users
- [x] Daily activity log
- [ ] Desktop notifikasi (Windows/Mac)
- [ ] Export log ke Excel
- [ ] Email alert saat file terdeteksi
- [ ] Scheduled auto-start saat komputer nyala

---

## 🤝 Kontribusi

Pull request welcome! Untuk perubahan besar, buka Issue dulu.

---

## 📜 License

MIT — bebas dipakai, dimodifikasi, dan dikomersilkan.

---

## 👤 Author

**[Nama Anda]**
- 🐙 GitHub: [@username](https://github.com/username)
- 💼 LinkedIn: [linkedin.com/in/username](https://linkedin.com/in/username)
- 🎵 TikTok: [@username](https://tiktok.com/@username)

> *"Bangun sistem yang menyelesaikan masalah nyata. Dokumentasikan. Bagikan. Bantu lebih banyak bisnis."*
> — Python Business Automation Engine

---

⭐ **Jika engine ini membantu, beri star di GitHub!**
