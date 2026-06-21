# NOTE.md

# Form Data Collector Engine

## Tujuan File Ini

File ini dibuat agar ketika saya membuka kembali proyek ini di masa depan, saya tidak bingung harus mulai dari mana.

Fokus utama proyek ini bukan membuat script.

Fokus utama proyek ini adalah membangun sebuah engine yang mampu mengumpulkan data dari berbagai sumber form secara otomatis dan mengubahnya menjadi data yang siap digunakan bisnis.

---

# Sebelum Menulis Kode

Jawab pertanyaan ini terlebih dahulu:

1. Data berasal dari mana?
2. Format data seperti apa?
3. Siapa yang menggunakan data tersebut?
4. Keputusan bisnis apa yang akan dibuat dari data tersebut?
5. Kemana data harus dikirim setelah dikumpulkan?

Jika belum bisa menjawab pertanyaan ini, jangan mulai coding.

---

# Cara Berpikir

Jangan berpikir:

"Bagaimana cara membaca CSV?"

Jangan berpikir:

"Bagaimana cara menggunakan pandas?"

Berpikirlah:

"Bagaimana cara memastikan data bisnis dapat masuk ke sistem secara konsisten dan otomatis?"

Library hanyalah alat.

Tujuan utamanya adalah menyelesaikan masalah bisnis.

---

# Tahapan Pengerjaan Engine

## PHASE 1 — Memahami Input

Target:

Mengetahui semua sumber data yang akan didukung.

Contoh:

* Google Form
* CSV
* Excel
* Website Form
* API
* Webhook

Output:

```text
Saya tahu dari mana data berasal.
```

Checklist:

* [ ] Identifikasi sumber data
* [ ] Identifikasi format data
* [ ] Buat sample data
* [ ] Dokumentasikan struktur data

---

## PHASE 2 — Membuat Collector

Target:

Mengambil data dari sumber.

Contoh:

```text
Google Form
↓
Collector
↓
Raw Data
```

Library:

* pathlib
* pandas
* requests

Checklist:

* [ ] Collector CSV
* [ ] Collector Excel
* [ ] Collector API
* [ ] Logging collector

Output:

```text
Data berhasil diambil.
```

---

## PHASE 3 — Data Validation

Target:

Memastikan data layak digunakan.

Contoh validasi:

* Email valid
* Nomor HP valid
* Field wajib terisi
* Format tanggal benar

Checklist:

* [ ] Email validator
* [ ] Phone validator
* [ ] Required field validator
* [ ] Date validator

Output:

```text
Data valid.
```

---

## PHASE 4 — Duplicate Detection

Target:

Menghilangkan data ganda.

Contoh:

```text
Andi
Andi
Andi
```

Menjadi:

```text
Andi
```

Checklist:

* [ ] Duplicate email
* [ ] Duplicate phone
* [ ] Duplicate ID

Output:

```text
Data unik.
```

---

## PHASE 5 — Data Standardization

Target:

Membuat format data konsisten.

Sebelum:

```text
jakarta
Jakarta
JAKARTA
```

Sesudah:

```text
Jakarta
```

Checklist:

* [ ] Standardisasi nama
* [ ] Standardisasi tanggal
* [ ] Standardisasi nomor HP

Output:

```text
Data konsisten.
```

---

## PHASE 6 — Storage

Target:

Menyimpan data.

Versi awal:

* CSV
* Excel

Versi lanjutan:

* SQLite
* PostgreSQL

Checklist:

* [ ] CSV Export
* [ ] Excel Export
* [ ] Database Storage

Output:

```text
Data tersimpan.
```

---

## PHASE 7 — Reporting

Target:

Mengetahui apa yang terjadi.

Contoh:

* Total data masuk
* Data valid
* Data invalid
* Data duplikat

Checklist:

* [ ] Summary report
* [ ] Error report
* [ ] Activity log

Output:

```text
Insight dasar tersedia.
```

---

# Urutan Implementasi

Jangan langsung membuat versi sempurna.

Bangun secara bertahap.

V1

CSV → CSV

```text
CSV
↓
Collector
↓
CSV
```

V2

CSV → Excel

```text
CSV
↓
Validation
↓
Excel
```

V3

API → Database

```text
API
↓
Collector
↓
PostgreSQL
```

V4

API → Database → Dashboard

```text
API
↓
Collector
↓
Database
↓
Dashboard
```

---

# Kapan Engine Dianggap Selesai?

Bukan ketika semua fitur selesai.

Engine dianggap berhasil ketika:

* Data dapat dikumpulkan otomatis
* Data dapat divalidasi
* Data dapat disimpan
* Error dapat dilacak
* Dokumentasi lengkap
* Orang lain dapat menggunakan engine

---

# Yang Tidak Boleh Dilakukan

❌ Menambah fitur tanpa alasan bisnis

❌ Gonta-ganti library terus menerus

❌ Fokus pada teknologi daripada masalah

❌ Mengejar kompleksitas

❌ Membuat sistem yang tidak pernah digunakan

---

# Filosofi Engine

Collect Engine adalah gerbang pertama seluruh sistem.

Jika Collect Engine buruk:

* Clean Engine akan bermasalah
* Analyze Engine akan salah
* Report Engine akan menyesatkan
* Automation akan gagal

Karena itu fokus utama:

```text
Reliable
Simple
Maintainable
Reusable
```

Bukan:

```text
Canggih
Rumit
Banyak fitur
```

---

# Target Akhir

Membangun engine yang mampu:

```text
Form
↓
Collect
↓
Validate
↓
Store
↓
Report
```

Sehingga bisnis tidak lagi menghabiskan waktu memindahkan data secara manual dan dapat fokus mengambil keputusan yang lebih baik.
