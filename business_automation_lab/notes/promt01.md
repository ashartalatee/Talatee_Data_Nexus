# BUSINESS AUTOMATION LAB ENGINE GENERATOR

Bertindaklah sebagai Senior Python Automation Engineer.

Tugasmu adalah MEMBANGUN ENGINE secara nyata untuk repository Business Automation Lab.

==================================================
INPUT
=====

ENGINE_NUMBER = [ISI]
ENGINE_NAME = [ISI]

==================================================
TUJUAN
======

Buat 1 engine profesional yang siap masuk ke repository:

business-automation-lab/

Engine harus menjadi solusi bisnis nyata.

Fokus pada implementasi.

Jangan membuat artikel panjang.

Jangan menjelaskan teori umum.

==================================================
OUTPUT WAJIB
============

1. Tujuan Engine

Jelaskan singkat:

* Masalah yang diselesaikan
* Input
* Output
* Benefit bisnis

==================================================
2. STRUKTUR FOLDER FINAL
========================

Gunakan format WAJIB berikut:

ENGINE_NAME/

├── README.md
│
├── input/
│
├── output/
│
├── logs/
│
├── config/
│   └── config.yaml
│
├── src/
│   ├── main.py
│   ├── [module_1].py
│   ├── [module_2].py
│   ├── [module_3].py
│   ├── [module_4].py
│   └── report.py
│
├── tests/
│
└── examples/

CATATAN:

Module harus relevan dengan engine.

Jangan menggunakan nama file generik jika tidak relevan.

Contoh:

Duplicate Remover:

scanner.py
hash_engine.py
duplicate_detector.py
remover.py

File Organizer:

scanner.py
classifier.py
mover.py
folder_builder.py

Invoice Engine:

invoice_builder.py
pdf_generator.py
email_sender.py
invoice_tracker.py

==================================================
3. INPUT FLOW
=============

Tampilkan contoh isi folder input.

Contoh:

input/

├── sales_januari.xlsx
├── sales_februari.xlsx
└── sales_maret.xlsx

==================================================
4. OUTPUT FLOW
==============

Tampilkan contoh hasil output.

Contoh:

output/

├── merged_sales.xlsx
├── summary_report.xlsx
└── report.pdf

==================================================
5. PENJELASAN SETIAP FILE
=========================

Jelaskan fungsi setiap file dalam src.

Contoh:

main.py
→ orchestrator utama

scanner.py
→ membaca file input

validator.py
→ validasi data

processor.py
→ proses bisnis utama

report.py
→ membuat laporan

==================================================
6. CONFIG.YAML
==============

Buat config.yaml yang realistis sesuai engine.

Contoh:

input_folder: input

output_folder: output

generate_report: true

remove_duplicates: true

==================================================
7. ROADMAP PENGEMBANGAN
=======================

Buat roadmap:

V1 = MVP

V2 = Intermediate

V3 = Production Ready

==================================================
ATURAN WAJIB
============

* Fokus implementasi
* Semua folder harus digunakan
* Struktur harus konsisten dengan Business Automation Lab
* Engine harus reusable
* Engine harus portfolio-ready
* Engine harus bisa dijadikan konten TikTok
* Engine harus terlihat layak digunakan perusahaan
* Jangan membuat kode terlebih dahulu
* Mulai dari desain arsitektur engine
* Pastikan nama module relevan dengan fungsi engine

Mulai bangun engine berdasarkan ENGINE_NUMBER dan ENGINE_NAME yang diberikan.
