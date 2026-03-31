# src/config/settings.py

DATA_SOURCES = [
    # single file
    {
        "name": "shope",
        "type": "csv",
        "path": "data/raw/hari01_shopee.csv"
    },

        # single file
    {
        "name": "tokopedia",
        "type": "csv",
        "path": "data/raw/hari01_tokopedia.csv"
    },
        # single file
    {
        "name": "tiktok",
        "type": "csv",
        "path": "data/raw/hari01_tiktok.csv"
    },
    # multiple files
    {
        "name": "tokopedia",
        "type": "csv_folder",  # auto load semua CSV di folder
        "path": "data/raw/tokopedia/"
    },
    # Excel file example
    {
        "name": "tiktok",
        "type": "excel",
        "path": "data/raw/tiktok.xlsx"
    }
]

REQUIRED_COLUMNS = ["product_name", "quantity", "price"]

OUTPUT_CONFIG = {
    "csv_path": "output/clean_data.csv",
    "excel_path": "output/report.xlsx"
}

# ============================================================
# HOW TO ADD OR REPLACE DATA FILES
# ============================================================
# 1. SINGLE FILE (CSV atau Excel)
#    - Gunakan "type": "csv" atau "type": "excel"
#    - Update "path" ke file terbaru
#
#    Contoh:
#    {
#        "name": "shopee",
#        "type": "csv",
#        "path": "data/raw/shopee_april.csv"
#    }
#
#    → Engine otomatis load file ini
#    → Kolom "source" akan tetap bernama "shopee"
#
# 2. MULTIPLE FILES (CSV di satu folder)
#    - Gunakan "type": "csv_folder"
#    - "path" → folder tempat semua CSV berada
#
#    Contoh:
#    {
#        "name": "tokopedia",
#        "type": "csv_folder",
#        "path": "data/raw/tokopedia/"
#    }
#
#    → Engine akan scan semua CSV di folder
#    → Kolom tambahan "source_file" otomatis berisi nama file asli
#
# 3. REPLACING FILES
#    - Jika ingin ganti data:
#      a) Ganti "path" ke file baru (misal bulan baru)
#      b) Jangan ubah "name" kecuali ingin source berbeda
#
#    Contoh:
#    # sebelumnya
#    {
#        "name": "shopee",
#        "type": "csv",
#        "path": "data/raw/shopee.csv"
#    }
#
#    # update April
#    {
#        "name": "shopee",
#        "type": "csv",
#        "path": "data/raw/shopee_april.csv"
#    }
#
# 4. MULTI SOURCE
#    - Bisa tambahkan sebanyak mungkin dictionary di DATA_SOURCES
#    - Engine akan otomatis load semua source dan gabungkan
#
#    Contoh:
#    DATA_SOURCES = [
#        {"name": "shopee", "type": "csv", "path": "data/raw/shopee.csv"},
#        {"name": "tokopedia", "type": "csv_folder", "path": "data/raw/tokopedia/"},
#        {"name": "tiktok", "type": "excel", "path": "data/raw/tiktok.xlsx"}
#    ]
#
# 5. NOTES
#    - Kolom wajib: "product_name", "quantity", "price"
#    - Untuk CSV folder, pastikan semua file memiliki struktur kolom yang sama
#    - Jika menambahkan file baru, tidak perlu ubah code utama, cukup update config
# ============================================================