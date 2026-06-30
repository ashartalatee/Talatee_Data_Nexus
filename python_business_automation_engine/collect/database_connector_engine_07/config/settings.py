"""
config/settings.py
------------------
Ubah konfigurasi di sini sesuai kebutuhan bisnis kamu.
Tidak perlu edit file lain selain file ini.
"""

CONFIG = {
    # ── PILIH TIPE DATABASE ──────────────────────────────────────────────────
    # Opsi: "sqlite", "mysql", "postgresql", "excel", "csv"
    "db_type": "sqlite",

    # ── KONFIGURASI SQLITE ───────────────────────────────────────────────────
    # Tidak perlu install apapun. File database dibuat otomatis.
    "sqlite": {
        "database_file": "data/bisnis_saya.db"
    },

    # ── KONFIGURASI MYSQL ────────────────────────────────────────────────────
    # Perlu install: pip install mysql-connector-python
    "mysql": {
        "host": "localhost",          # Alamat server database (biasanya localhost)
        "port": 3306,                 # Port default MySQL
        "database": "nama_database",  # Ganti dengan nama database kamu
        "username": "root",           # Username MySQL kamu
        "password": "password_kamu"   # Password MySQL kamu
    },

    # ── KONFIGURASI POSTGRESQL ───────────────────────────────────────────────
    # Perlu install: pip install psycopg2-binary
    "postgresql": {
        "host": "localhost",
        "port": 5432,
        "database": "nama_database",
        "username": "postgres",
        "password": "password_kamu"
    },

    # ── KONFIGURASI EXCEL ────────────────────────────────────────────────────
    # Perlu install: pip install pandas openpyxl
    "excel": {
        "file_path": "data/data_bisnis.xlsx",  # Lokasi file Excel kamu
        "sheet_name": "Sheet1"                  # Nama sheet yang mau dibaca
    },

    # ── KONFIGURASI CSV ──────────────────────────────────────────────────────
    # Tidak perlu install apapun.
    "csv": {
        "file_path": "data/data_bisnis.csv",
        "delimiter": ","    # Pemisah kolom: "," untuk CSV standar, ";" untuk Excel export
    },

    # ── PENGATURAN OUTPUT ────────────────────────────────────────────────────
    "output": {
        "save_results": True,
        "output_folder": "results",
        "format": "excel"   # Pilih: "excel", "csv", atau "json"
    }
}