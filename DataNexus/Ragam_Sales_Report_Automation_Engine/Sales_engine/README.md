# 🚀 MSIDE — Marketplace Sales Intelligence & Decision Engine

## 🧠 Overview

MSIDE adalah **end-to-end data automation engine** yang dirancang untuk:

* Mengolah data marketplace (Shopee, Tokopedia, TikTok)
* Mengubah data mentah menjadi **insight bisnis**
* Memberikan **dashboard interaktif + alert otomatis**
* Membantu pengambilan keputusan berbasis data

> Engine ini mensimulasikan sistem yang digunakan di industri data modern:
> **Data Pipeline → Analytics → Dashboard → Alert → Decision**

---

## 🎯 Tujuan Dibuatnya Engine Ini

Engine ini dibuat bukan sekadar project coding.

Tapi untuk:

* Melatih **real-world data engineering workflow**
* Membangun **produk digital yang bisa dijual**
* Menjadi fondasi untuk:

  * Freelance Data Automation
  * BI Dashboard Service
  * Mini SaaS Analytics Tool

---

## ⚙️ Arsitektur Sistem

```
RAW DATA (CSV Marketplace)
        ↓
[LOADER]
        ↓
[VALIDATOR]
        ↓
[CLEANER]
        ↓
[TRANSFORMER]
        ↓
[STANDARDIZATION]
        ↓
[MERGER] → master_data.csv
        ↓
=========================
📊 ANALYTICS ENGINE
=========================
        ↓
daily / product / source / growth
        ↓
=========================
📊 DASHBOARD (STREAMLIT)
=========================
        ↓
=========================
🚨 ALERT ENGINE
=========================
        ↓
NOTIFICATION + LOG
```

---

## 📁 Struktur Project

```
src/
├── ingestion/
│   └── loader.py
│
├── processing/
│   ├── validator.py
│   ├── cleaner.py
│   ├── transformer.py
│   ├── standardization.py
│   └── merger.py
│
├── analytics/
│   └── analytics.py
│
├── dashboard/
│   └── app.py
│
├── alert/
│   └── alert.py
│
├── utils/
│   └── config.py

data/
├── raw/
├── output/

run_pipeline.py
requirements.txt
Dockerfile
```

---

## 🔄 Alur Kerja Engine

### 1. Ingestion

* Load data dari berbagai marketplace
* Format bisa berbeda-beda

### 2. Validation

* Cek struktur data
* Pastikan kolom penting ada

### 3. Cleaning

* Handle missing values
* Normalize text
* Drop data rusak
* Logging perubahan data

### 4. Transformation

* Standarisasi field:

  * price
  * quantity
  * product
  * date
* Hitung:

  * revenue

### 5. Standardization

* Samakan format antar platform

### 6. Merger

* Gabungkan semua data
* Remove duplicate
* Validasi akhir
* Output: `master_data.csv`

### 7. Analytics

Generate:

* Daily revenue
* Top products
* Source performance
* Growth analysis

### 8. Dashboard

* Visualisasi data
* KPI bisnis
* Filter interaktif

### 9. Alert System

Deteksi:

* Revenue drop
* Revenue spike
* Perubahan produk terlaris

---

## ▶️ Cara Menjalankan

### 1. Install dependencies

```
pip install -r requirements.txt
```

### 2. Jalankan pipeline

```
python run_pipeline.py
```

### 3. Jalankan dashboard

```
streamlit run src/dashboard/app.py
```

---

## 📊 Output Engine

```
data/output/
├── master_data.csv
├── analytics/
│   ├── daily_revenue.csv
│   ├── top_products.csv
│   ├── source_performance.csv
│   └── growth.csv
│
├── alerts/
│   └── alerts_log.csv
```

---

## 🚨 Kenapa Engine Ini Penting

Engine ini menyelesaikan masalah nyata:

❌ Data berantakan
❌ Tidak tahu produk mana yang perform
❌ Tidak sadar revenue turun
❌ Manual Excel tiap hari

👉 Diubah jadi:

✅ Data rapi & otomatis
✅ Insight langsung tersedia
✅ Alert otomatis
✅ Dashboard real-time

---

## 💰 Potensi Monetisasi

Engine ini bisa dijadikan:

1. **Dashboard UMKM**
2. **Marketplace Analytics Service**
3. **Monthly Reporting System**
4. **Mini SaaS BI Tool**

---

## 🔥 Cara Upgrade di Masa Depan

Ini bagian paling penting.

Kalau kamu buka repo ini 6 bulan lagi,
ini roadmap upgrade-nya:

---

### 🚀 LEVEL 1 — DATA SOURCE UPGRADE

* Tambah API (bukan CSV)
* Integrasi database (PostgreSQL)

---

### 🚀 LEVEL 2 — ANALYTICS UPGRADE

Tambah:

* Customer segmentation
* Repeat order rate
* Profit (bukan revenue)

---

### 🚀 LEVEL 3 — ALERT UPGRADE

* Kirim ke Telegram / WhatsApp
* Threshold dinamis (AI-based)

---

### 🚀 LEVEL 4 — DASHBOARD UPGRADE

* Multi-client login
* Role-based dashboard
* Real-time update

---

### 🚀 LEVEL 5 — SYSTEM UPGRADE

* Deploy ke cloud (AWS/GCP)
* Gunakan Airflow (scheduler)
* Gunakan Docker + CI/CD

---

## 🧠 Filosofi Engine Ini

Engine ini dibangun dengan prinsip:

> “Data bukan untuk dilihat… tapi untuk diambil keputusan”

---

## ⚡ Catatan untuk Diri Sendiri (Future You)

Kalau kamu baca ini di masa depan:

* Engine ini adalah **fondasi**
* Jangan rewrite dari nol
* Upgrade pelan-pelan
* Fokus ke:

  * value
  * user
  * problem nyata

---

## 🚀 Next Step

Kalau engine ini sudah jalan:

* Buat demo video
* Tawarkan ke 5 orang
* Dapatkan 1 client pertama

---

## 🏁 Penutup

Ini bukan project biasa.

Ini adalah:
**produk pertama kamu di dunia data & automation**

Kalau kamu lanjut → jadi income
Kalau kamu berhenti → jadi arsip

Pilihan selalu di kamu.
