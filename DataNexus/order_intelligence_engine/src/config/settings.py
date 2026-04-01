# =========================
# 🔧 GLOBAL SETTINGS
# =========================

APP_NAME = "Order Intelligence Engine"
VERSION = "1.0.0"
DEBUG = True

# =========================
# 📥 DATA SOURCES
# =========================

DATA_SOURCES = [
    {
        "name": "shopee",
        "type": "csv",
        "path": "data/raw/hari01_shopee.csv",
        "active": False,
        "options": {
            "delimiter": ",",
            "encoding": "utf-8"
        }
    },
    {
        "name": "tokopedia_single",
        "type": "csv",
        "path": "data/raw/hari01_tokopedia.csv",
        "active": False
    },
    {
        "name": "tiktok_bulk",
        "type": "csv_folder",
        "path": "data/raw/tiktok/",
        "active": True
    },
    {
        "name": "tiktok_csv",
        "type": "csv",
        "path": "data/raw/tiktok.csv",
        "active": False
    },
    {
        "name": "tiktok_excel",
        "type": "excel",
        "path": "data/raw/tiktok.xlsx",
        "active": False,
        "options": {
            "sheet_name": 0
        }
    },
]

# =========================
# 📊 STANDARD SCHEMA
# =========================

STANDARD_SCHEMA = [
    "product_name",
    "quantity",
    "price",
    "source"
]

# =========================
# ⚠️ REQUIRED COLUMNS
# =========================

REQUIRED_COLUMNS = [
    "product_name",
    "quantity",
    "price"
]

# =========================
# 🔄 COLUMN MAPPING
# =========================

COLUMN_MAPPING = {
    "product": "product_name",
    "nama_produk": "product_name",
    "item": "product_name",

    "qty": "quantity",
    "jumlah": "quantity",
    "pcs": "quantity",
    "qty_order": "quantity",      # ✅ TAMBAHAN

    "harga": "price",
    "total": "price",
    "price_total": "price",
    "price_value": "price"        # ✅ TAMBAHAN
}

# =========================
# 🧹 CLEANING CONFIG
# =========================

CLEANING_CONFIG = {
    "text_replacements": {
        "rp": "",
        "idr": "",
        "pcs": "",
        ",": "",
        ".": ""
    },
    "number_map": {
        "satu": 1,
        "dua": 2,
        "tiga": 3
    },
    "lowercase": True,
    "strip_whitespace": True,
    "dropna": True,
    "product_normalization_map": {
        "kaos hitam ": "kaos hitam",
        "kaos  hitam": "kaos hitam",
        "sepatu sport ": "sepatu sport",
        "kaos item": "kaos hitam",
        "kaos hitam polos": "kaos hitam"
    }
}

# =========================
# 🧠 DEDUPLICATION CONFIG
# =========================

DUPLICATE_CONFIG = {
    "strategy": "both",  # exact / subset / both
    "subset_columns": ["product_name", "quantity", "price"]
}

# =========================
# 🚫 MISSING VALUE CONFIG
# =========================

MISSING_CONFIG = {
    "critical_columns": ["product_name", "quantity", "price"],
    "fill_defaults": {"source": "unknown"},
    "fill_strategy": {}  # bisa diisi {"price": "mean"} misal
}

# =========================
# 📊 ANALYTICS CONFIG
# =========================

ANALYTICS_CONFIG = {
    "top_n": 10,
    "metrics": ["revenue", "orders", "bulk_order_ratio"],
    "include_charts": False
}

# =========================
# 📤 OUTPUT CONFIG
# =========================

OUTPUT_CONFIG = {
    "save_csv": True,
    "csv_path": "output/clean_data.csv",
    "save_excel": True,
    "excel_path": "output/report.xlsx",
    "include_timestamp": True,
    "debug_mode": True
}

# =========================
# 🧪 VALIDATION CONFIG
# =========================

VALIDATION_CONFIG = {
    "enforce_schema": True,
    "strict_mode": False,
    "log_missing_columns": True
}

# =========================
# 📝 LOGGING CONFIG
# =========================

LOGGING_CONFIG = {
    "log_level": "INFO",
    "log_to_file": True,
    "log_file_path": "logs/engine.log"
}

# =========================
# 📐 STANDARDIZATION / SCHEMA CONFIG
# =========================

SCHEMA_CONFIG = {
    "column_groups": {
        "product_name": ["product_name", "product", "nama_produk", "item"],
        "quantity": ["quantity", "qty", "jumlah", "pcs"],
        "price": ["price", "harga", "total", "price_total"],
        "source": ["source"]
    },
    "required_columns": REQUIRED_COLUMNS,
    "final_columns": STANDARD_SCHEMA
}