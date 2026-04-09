```md
# SENTINEL ANALYTICS ENGINE — USAGE GUIDE (NOTE.MD)

## 1. TUJUAN ENGINE
Engine ini digunakan untuk:
- Mengolah data multi-marketplace (Shopee, Tokopedia, TikTokShop, WhatsApp)
- Menyatukan format data (schema standard)
- Menghasilkan laporan siap keputusan (decision-ready)
- Mendukung multi-client (agency workflow)

---

## 2. FLOW SISTEM (END-TO-END)

Pipeline utama:
```

INGESTION → VALIDATION → CLEANING → TRANSFORM → ANALYSIS → OUTPUT

```

Penjelasan:
1. **Ingestion**
   - Ambil data dari CSV / API / manual export marketplace

2. **Validation**
   - Cek struktur data sesuai schema
   - Reject / flag jika tidak sesuai

3. **Cleaning**
   - Handle missing values
   - Remove duplicate
   - Standardisasi text

4. **Transform**
   - Mapping kolom ke schema standard
   - Normalisasi tanggal
   - Feature engineering

5. **Analysis**
   - Hitung metrics bisnis
   - Generate summary
   - Generate insight

6. **Output**
   - Export ke Excel / CSV / JSON
   - Siap dikirim ke client

---

## 3. CARA MENJALANKAN ENGINE

### STEP 1 — SIAPKAN DATA
Masukkan data ke folder:
```

/data/raw/{client_name}/

```

Contoh:
```

data/raw/client_a/shopee.csv
data/raw/client_a/tokopedia.csv
data/raw/client_a/tiktokshop.csv

```

---

### STEP 2 — SET CONFIG CLIENT
Edit:
```

config/clients/client_a.json

````

---

### STEP 3 — RUN ENGINE

#### Run full pipeline:
```bash
python main.py --client client_a
````

#### Run partial pipeline:

```bash
python main.py --client client_a --steps ingestion,cleaning,analysis
```

---

### STEP 4 — CEK OUTPUT

Hasil ada di:

```
/data/exports/{client_name}/
```

---

## 4. JENIS DATA YANG BISA DITANGANI

### 1. DATA TRANSAKSI (WAJIB)

* order_id
* product_name
* quantity
* price
* total_price
* date
* marketplace (shopee/tokopedia/tiktokshop/whatsapp)

---

### 2. DATA PRODUK

* product_id
* product_name
* category
* cost
* price

---

### 3. DATA CUSTOMER (OPSIONAL)

* customer_id
* name
* location
* repeat_flag

---

### 4. DATA MARKETING (OPSIONAL)

* campaign_name
* ads_spend
* clicks
* impressions

---

### 5. DATA CHAT / WHATSAPP (OPSIONAL)

* message
* timestamp
* conversion_flag

---

## 5. OUTPUT YANG DIDAPATKAN CLIENT

### 1. METRICS UTAMA

* Total Revenue
* Total Orders
* Average Order Value (AOV)
* Conversion Rate
* Repeat Customer Rate

---

### 2. ANALISA PRODUK

* Top Selling Product
* Low Performing Product
* Profit per Product

---

### 3. ANALISA MARKETPLACE

* Revenue per Channel
* Channel Contribution (%)
* Channel Performance Ranking

---

### 4. ANALISA WAKTU

* Sales per Day
* Peak Sales Time
* Trend Penjualan

---

### 5. INSIGHT OTOMATIS

Contoh:

* "70% revenue datang dari 3 produk utama"
* "Shopee outperform Tokopedia 2x"
* "Penjualan drop di weekday"

---

### 6. FORMAT OUTPUT

* Excel (report utama client)
* CSV (data olahan)
* JSON (integrasi sistem lain)

---

## 6. STRUKTUR OUTPUT

```
data/exports/client_a/

├── metrics.xlsx
├── summary.csv
├── insights.json
└── cleaned_data.csv
```

---

## 7. CARA TAMBAH CLIENT BARU

### STEP 1 — DUPLIKAT CONFIG

Copy:

```
config/clients/client_template.json
```

Rename:

```
client_b.json
```

---

### STEP 2 — EDIT CONFIG

Isi minimal:

```json
{
  "client_name": "client_b",
  "data_sources": {
    "shopee": true,
    "tokopedia": true,
    "tiktokshop": false,
    "whatsapp": false
  },
  "file_paths": {
    "shopee": "data/raw/client_b/shopee.csv",
    "tokopedia": "data/raw/client_b/tokopedia.csv"
  },
  "schema": {
    "required_columns": [
      "order_id",
      "product_name",
      "quantity",
      "price",
      "date"
    ]
  },
  "output": {
    "export_excel": true,
    "export_csv": true,
    "export_json": true
  }
}
```

---

### STEP 3 — MASUKKAN DATA

```
data/raw/client_b/
```

---

### STEP 4 — RUN

```bash
python main.py --client client_b
```

---

## 8. BEST PRACTICE BIAR ENGINE GAK ERROR

### 1. PASTIKAN KOLOM MINIMAL ADA

* order_id
* product_name
* quantity
* price
* date

---

### 2. FORMAT TANGGAL KONSISTEN

Gunakan:

```
YYYY-MM-DD
```

---

### 3. HINDARI DUPLIKASI ORDER_ID

---

### 4. PASTIKAN TIDAK ADA NILAI NULL KRITIS

* price
* quantity

---

### 5. GUNAKAN NAMING FILE KONSISTEN

---

## 9. SCALING KE AGENCY

Engine ini sudah siap untuk:

* Multi-client paralel
* Integrasi scheduler (cron/job)
* Automasi laporan mingguan/bulanan
* White-label reporting ke client

---

## 10. NEXT UPGRADE (RECOMMENDED)

* Dashboard (Streamlit / Web)
* Auto anomaly detection
* AI insight generator
* API ingestion (real-time)
* Ads performance integration

---

## 11. SUMMARY INTI

Engine ini:

* Mengubah data mentah → insight bisnis
* Menghemat waktu analisa manual
* Membuat output yang langsung bisa dijual ke client

---

## 12. POSISI LU (IMPORTANT)

Dengan engine ini:
Lu bukan tukang olah data.

Lu:
→ Decision System Builder
→ Problem Solver
→ Revenue Optimizer

Kalau dipakai konsisten:
Ini bisa jadi core asset agency lu.

```
```
