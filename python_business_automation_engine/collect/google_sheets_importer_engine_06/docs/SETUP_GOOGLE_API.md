# 🔒 Setup Google Service Account (untuk Sheet Privat)

Mode ini hanya diperlukan jika sheet kamu **tidak bisa/tidak boleh** di-set sebagai publik (misal data finansial atau internal perusahaan).

## Langkah 1 — Buat Project di Google Cloud
1. Buka https://console.cloud.google.com/
2. Klik **New Project**, beri nama bebas (mis. `sheets-importer-engine`)

## Langkah 2 — Aktifkan API
1. Di sidebar, buka **APIs & Services → Library**
2. Cari dan aktifkan:
   - **Google Sheets API**
   - **Google Drive API**

## Langkah 3 — Buat Service Account
1. Buka **APIs & Services → Credentials**
2. Klik **Create Credentials → Service Account**
3. Isi nama, klik **Create and Continue**, lalu **Done**
4. Klik service account yang baru dibuat → tab **Keys**
5. Klik **Add Key → Create new key → JSON**
6. File `credentials.json` akan otomatis terdownload — **simpan baik-baik, jangan upload ke GitHub publik**

## Langkah 4 — Share Sheet ke Service Account
1. Buka file `credentials.json`, cari field `client_email` (formatnya seperti `xxx@xxx.iam.gserviceaccount.com`)
2. Buka Google Sheets yang ingin diakses → klik **Share**
3. Tempel email tadi, beri akses **Viewer** (atau Editor jika perlu tulis data juga)

## Langkah 5 — Gunakan di Engine
**Lewat antarmuka web:**
1. Pilih mode **"🔒 Sheet Privat"** di sidebar
2. Upload `credentials.json`
3. Paste link sheet, isi nama worksheet (opsional)
4. Klik **Import Data**

**Lewat CLI:**
```bash
python cli.py --url "<link sheet>" --out data.csv --credentials credentials.json --worksheet "Sheet2"
```

## ⚠️ Keamanan
- **Jangan pernah** commit `credentials.json` ke GitHub. File ini sudah otomatis di-ignore lewat `.gitignore`
- Service Account hanya bisa mengakses sheet yang secara eksplisit di-share kepadanya
