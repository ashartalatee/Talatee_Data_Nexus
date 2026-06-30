"""
================================================================================
  DATABASE CONNECTOR ENGINE
  Engine #07 dari Python Business Automation Engine
  
  Oleh: [Nama Kamu]
  GitHub: github.com/[username]/database-connector-engine
  
  Deskripsi:
    Engine ini menghubungkan Python ke berbagai database (SQLite, MySQL, 
    PostgreSQL, Excel, CSV) secara otomatis. Dirancang untuk non-IT: 
    cukup isi konfigurasi, engine bekerja sendiri.
================================================================================
"""

import sqlite3
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

try:
    import openpyxl
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False


# ============================================================
#  KONFIGURASI - BAGIAN YANG BISA DIUBAH OLEH NON-IT
# ============================================================

CONFIG = {
    # Pilih tipe database: "sqlite", "mysql", "postgresql", "excel", "csv"
    "db_type": "sqlite",

    # SQLite (tidak perlu install apapun, langsung jalan)
    "sqlite": {
        "database_file": "data/bisnis_saya.db"
    },

    # MySQL (perlu install: pip install mysql-connector-python)
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "database": "nama_database",
        "username": "root",
        "password": "password_kamu"
    },

    # PostgreSQL (perlu install: pip install psycopg2-binary)
    "postgresql": {
        "host": "localhost",
        "port": 5432,
        "database": "nama_database",
        "username": "postgres",
        "password": "password_kamu"
    },

    # Excel (perlu install: pip install openpyxl pandas)
    "excel": {
        "file_path": "data/data_bisnis.xlsx",
        "sheet_name": "Sheet1"
    },

    # CSV (tidak perlu install apapun)
    "csv": {
        "file_path": "data/data_bisnis.csv",
        "delimiter": ","
    },

    # Pengaturan output
    "output": {
        "save_results": True,
        "output_folder": "results",
        "format": "excel"  # "excel", "csv", atau "json"
    }
}


# ============================================================
#  KELAS UTAMA - DATABASE CONNECTOR ENGINE
# ============================================================

class DatabaseConnectorEngine:
    """
    Engine untuk menghubungkan dan mengambil data dari berbagai sumber database.
    Dirancang agar mudah digunakan tanpa latar belakang IT.
    """

    def __init__(self, config=None):
        self.config = config or CONFIG
        self.connection = None
        self.db_type = self.config.get("db_type", "sqlite")
        self._log("🚀 Database Connector Engine siap digunakan")
        self._log(f"   Tipe database: {self.db_type.upper()}")

    # ----------------------------------------------------------
    #  KONEKSI DATABASE
    # ----------------------------------------------------------

    def connect(self):
        """Hubungkan ke database berdasarkan konfigurasi."""
        try:
            if self.db_type == "sqlite":
                return self._connect_sqlite()
            elif self.db_type == "mysql":
                return self._connect_mysql()
            elif self.db_type == "postgresql":
                return self._connect_postgresql()
            elif self.db_type in ["excel", "csv"]:
                self._log(f"✅ Mode file ({self.db_type.upper()}) - tidak perlu koneksi")
                return True
            else:
                self._log(f"❌ Tipe database '{self.db_type}' tidak dikenal", "ERROR")
                return False
        except Exception as e:
            self._log(f"❌ Gagal terhubung: {e}", "ERROR")
            return False

    def _connect_sqlite(self):
        cfg = self.config["sqlite"]
        db_file = cfg["database_file"]

        # Buat folder jika belum ada
        Path(db_file).parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(db_file)
        self.connection.row_factory = sqlite3.Row
        self._log(f"✅ Terhubung ke SQLite: {db_file}")
        return True

    def _connect_mysql(self):
        if not MYSQL_AVAILABLE:
            self._log("❌ Library MySQL belum terinstall", "ERROR")
            self._log("   Jalankan: pip install mysql-connector-python", "INFO")
            return False

        cfg = self.config["mysql"]
        self.connection = mysql.connector.connect(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["username"],
            password=cfg["password"]
        )
        self._log(f"✅ Terhubung ke MySQL: {cfg['host']}/{cfg['database']}")
        return True

    def _connect_postgresql(self):
        if not POSTGRES_AVAILABLE:
            self._log("❌ Library PostgreSQL belum terinstall", "ERROR")
            self._log("   Jalankan: pip install psycopg2-binary", "INFO")
            return False

        cfg = self.config["postgresql"]
        self.connection = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            dbname=cfg["database"],
            user=cfg["username"],
            password=cfg["password"]
        )
        self._log(f"✅ Terhubung ke PostgreSQL: {cfg['host']}/{cfg['database']}")
        return True

    # ----------------------------------------------------------
    #  BACA DATA
    # ----------------------------------------------------------

    def baca_data(self, query_atau_tabel, parameter=None):
        """
        Baca data dari database.
        
        Untuk SQL database: masukkan query SQL
            contoh: engine.baca_data("SELECT * FROM penjualan")
        
        Untuk Excel/CSV: masukkan nama sheet atau langsung panggil
            contoh: engine.baca_data("Sheet1")
        """
        try:
            if self.db_type == "sqlite":
                return self._baca_sql(query_atau_tabel, parameter)
            elif self.db_type == "mysql":
                return self._baca_sql(query_atau_tabel, parameter)
            elif self.db_type == "postgresql":
                return self._baca_sql(query_atau_tabel, parameter)
            elif self.db_type == "excel":
                return self._baca_excel()
            elif self.db_type == "csv":
                return self._baca_csv()
        except Exception as e:
            self._log(f"❌ Gagal membaca data: {e}", "ERROR")
            return None

    def _baca_sql(self, query, parameter=None):
        if not self.connection:
            self._log("❌ Belum terhubung ke database. Panggil .connect() dulu", "ERROR")
            return None

        cursor = self.connection.cursor()
        if parameter:
            cursor.execute(query, parameter)
        else:
            cursor.execute(query)

        hasil = cursor.fetchall()
        kolom = [desc[0] for desc in cursor.description]

        self._log(f"✅ Data berhasil diambil: {len(hasil)} baris, {len(kolom)} kolom")
        return {"kolom": kolom, "data": [list(row) for row in hasil], "jumlah_baris": len(hasil)}

    def _baca_excel(self):
        cfg = self.config["excel"]
        file_path = cfg["file_path"]
        sheet = cfg.get("sheet_name", 0)

        if not os.path.exists(file_path):
            self._log(f"❌ File Excel tidak ditemukan: {file_path}", "ERROR")
            return None

        if PANDAS_AVAILABLE:
            df = pd.read_excel(file_path, sheet_name=sheet)
            self._log(f"✅ File Excel berhasil dibaca: {len(df)} baris")
            return {"kolom": list(df.columns), "data": df.values.tolist(), "jumlah_baris": len(df)}
        else:
            self._log("❌ Library pandas belum terinstall", "ERROR")
            self._log("   Jalankan: pip install pandas openpyxl", "INFO")
            return None

    def _baca_csv(self):
        cfg = self.config["csv"]
        file_path = cfg["file_path"]
        delimiter = cfg.get("delimiter", ",")

        if not os.path.exists(file_path):
            self._log(f"❌ File CSV tidak ditemukan: {file_path}", "ERROR")
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            data = list(reader)

        kolom = list(data[0].keys()) if data else []
        rows = [list(row.values()) for row in data]

        self._log(f"✅ File CSV berhasil dibaca: {len(rows)} baris")
        return {"kolom": kolom, "data": rows, "jumlah_baris": len(rows)}

    # ----------------------------------------------------------
    #  TULIS DATA
    # ----------------------------------------------------------

    def tulis_data(self, nama_tabel, data_baru):
        """
        Tulis data baru ke database.
        
        Contoh:
            data_baru = {"nama": "Budi", "penjualan": 150000, "tanggal": "2024-01-15"}
            engine.tulis_data("penjualan", data_baru)
        """
        if self.db_type not in ["sqlite", "mysql", "postgresql"]:
            self._log("⚠️  Tulis data hanya tersedia untuk SQLite, MySQL, PostgreSQL", "WARNING")
            return False

        if not self.connection:
            self._log("❌ Belum terhubung ke database", "ERROR")
            return False

        try:
            kolom = list(data_baru.keys())
            nilai = list(data_baru.values())

            if self.db_type in ["sqlite", "mysql"]:
                placeholder = ", ".join(["?" if self.db_type == "sqlite" else "%s"] * len(kolom))
            else:
                placeholder = ", ".join([f"%s"] * len(kolom))

            query = f"INSERT INTO {nama_tabel} ({', '.join(kolom)}) VALUES ({placeholder})"

            cursor = self.connection.cursor()
            cursor.execute(query, nilai)
            self.connection.commit()

            self._log(f"✅ Data berhasil disimpan ke tabel '{nama_tabel}'")
            return True
        except Exception as e:
            self._log(f"❌ Gagal menyimpan data: {e}", "ERROR")
            return False

    # ----------------------------------------------------------
    #  BUAT TABEL (SQLite)
    # ----------------------------------------------------------

    def buat_tabel(self, nama_tabel, kolom_definisi):
        """
        Buat tabel baru di database SQLite.
        
        Contoh:
            kolom = {
                "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
                "nama": "TEXT NOT NULL",
                "penjualan": "REAL",
                "tanggal": "TEXT"
            }
            engine.buat_tabel("penjualan", kolom)
        """
        if self.db_type != "sqlite":
            self._log("⚠️  Buat tabel saat ini hanya untuk SQLite", "WARNING")
            return False

        if not self.connection:
            self._log("❌ Belum terhubung ke database", "ERROR")
            return False

        try:
            definisi = ", ".join([f"{k} {v}" for k, v in kolom_definisi.items()])
            query = f"CREATE TABLE IF NOT EXISTS {nama_tabel} ({definisi})"

            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()

            self._log(f"✅ Tabel '{nama_tabel}' berhasil dibuat")
            return True
        except Exception as e:
            self._log(f"❌ Gagal membuat tabel: {e}", "ERROR")
            return False

    # ----------------------------------------------------------
    #  SIMPAN HASIL KE FILE
    # ----------------------------------------------------------

    def simpan_hasil(self, data, nama_file=None):
        """
        Simpan hasil query ke file (Excel, CSV, atau JSON).
        Data harus berupa hasil dari baca_data().
        """
        if not data:
            self._log("⚠️  Tidak ada data untuk disimpan", "WARNING")
            return None

        output_cfg = self.config.get("output", {})
        folder = output_cfg.get("output_folder", "results")
        format_output = output_cfg.get("format", "excel")

        Path(folder).mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not nama_file:
            nama_file = f"hasil_{timestamp}"

        try:
            if format_output == "excel" and PANDAS_AVAILABLE:
                file_path = f"{folder}/{nama_file}.xlsx"
                df = pd.DataFrame(data["data"], columns=data["kolom"])
                df.to_excel(file_path, index=False)
                self._log(f"✅ Hasil disimpan ke Excel: {file_path}")

            elif format_output == "csv":
                file_path = f"{folder}/{nama_file}.csv"
                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(data["kolom"])
                    writer.writerows(data["data"])
                self._log(f"✅ Hasil disimpan ke CSV: {file_path}")

            elif format_output == "json":
                file_path = f"{folder}/{nama_file}.json"
                hasil_json = [dict(zip(data["kolom"], row)) for row in data["data"]]
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(hasil_json, f, ensure_ascii=False, indent=2)
                self._log(f"✅ Hasil disimpan ke JSON: {file_path}")

            else:
                file_path = f"{folder}/{nama_file}.csv"
                with open(file_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(data["kolom"])
                    writer.writerows(data["data"])
                self._log(f"✅ Hasil disimpan ke CSV (fallback): {file_path}")

            return file_path

        except Exception as e:
            self._log(f"❌ Gagal menyimpan hasil: {e}", "ERROR")
            return None

    # ----------------------------------------------------------
    #  TAMPILKAN DATA DI TERMINAL
    # ----------------------------------------------------------

    def tampilkan(self, data, maks_baris=10):
        """Tampilkan data di terminal dengan format tabel yang rapi."""
        if not data:
            print("  (tidak ada data)")
            return

        kolom = data["kolom"]
        rows = data["data"][:maks_baris]

        # Hitung lebar kolom
        lebar = []
        for i, k in enumerate(kolom):
            max_len = len(str(k))
            for row in rows:
                if i < len(row):
                    max_len = max(max_len, len(str(row[i])))
            lebar.append(min(max_len, 25))

        # Cetak header
        separator = "+" + "+".join(["-" * (w + 2) for w in lebar]) + "+"
        header = "|" + "|".join([f" {str(k)[:25]:<{lebar[i]}} " for i, k in enumerate(kolom)]) + "|"

        print(separator)
        print(header)
        print(separator)

        for row in rows:
            baris = "|" + "|".join([f" {str(row[i] if i < len(row) else '')[:25]:<{lebar[i]}} " for i in range(len(kolom))]) + "|"
            print(baris)

        print(separator)

        if data["jumlah_baris"] > maks_baris:
            print(f"  ... dan {data['jumlah_baris'] - maks_baris} baris lainnya")
        print(f"  Total: {data['jumlah_baris']} baris | {len(kolom)} kolom\n")

    # ----------------------------------------------------------
    #  INFO DATABASE
    # ----------------------------------------------------------

    def info_database(self):
        """Tampilkan informasi database yang terhubung."""
        print("\n" + "=" * 50)
        print("  INFO DATABASE CONNECTOR ENGINE")
        print("=" * 50)
        print(f"  Tipe Database  : {self.db_type.upper()}")

        if self.db_type == "sqlite":
            cfg = self.config["sqlite"]
            print(f"  File Database  : {cfg['database_file']}")
            if self.connection:
                cursor = self.connection.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tabel = [row[0] for row in cursor.fetchall()]
                print(f"  Daftar Tabel   : {', '.join(tabel) if tabel else '(kosong)'}")

        elif self.db_type == "mysql":
            cfg = self.config["mysql"]
            print(f"  Host           : {cfg['host']}:{cfg['port']}")
            print(f"  Database       : {cfg['database']}")

        elif self.db_type == "postgresql":
            cfg = self.config["postgresql"]
            print(f"  Host           : {cfg['host']}:{cfg['port']}")
            print(f"  Database       : {cfg['database']}")

        elif self.db_type == "excel":
            cfg = self.config["excel"]
            print(f"  File Excel     : {cfg['file_path']}")
            print(f"  Sheet          : {cfg.get('sheet_name', 'Sheet1')}")

        elif self.db_type == "csv":
            cfg = self.config["csv"]
            print(f"  File CSV       : {cfg['file_path']}")

        print("=" * 50 + "\n")

    # ----------------------------------------------------------
    #  TUTUP KONEKSI
    # ----------------------------------------------------------

    def tutup(self):
        """Tutup koneksi database."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self._log("🔒 Koneksi database ditutup")

    # ----------------------------------------------------------
    #  LOGGING
    # ----------------------------------------------------------

    def _log(self, pesan, level="INFO"):
        waktu = datetime.now().strftime("%H:%M:%S")
        print(f"[{waktu}] {pesan}")


# ============================================================
#  CONTOH PENGGUNAAN (jalankan langsung file ini untuk demo)
# ============================================================

def demo_sqlite():
    """Demo lengkap dengan SQLite - tidak perlu install apapun."""
    print("\n" + "=" * 60)
    print("  DEMO DATABASE CONNECTOR ENGINE - SQLite")
    print("=" * 60)

    # 1. Inisialisasi engine
    engine = DatabaseConnectorEngine()

    # 2. Hubungkan ke database
    if not engine.connect():
        print("Gagal terhubung!")
        return

    # 3. Tampilkan info database
    engine.info_database()

    # 4. Buat tabel contoh
    engine.buat_tabel("penjualan", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "nama_produk": "TEXT NOT NULL",
        "jumlah": "INTEGER",
        "harga": "REAL",
        "tanggal": "TEXT"
    })

    # 5. Masukkan data contoh
    data_contoh = [
        {"nama_produk": "Laptop Gaming", "jumlah": 5, "harga": 15000000, "tanggal": "2024-01-10"},
        {"nama_produk": "Mouse Wireless", "jumlah": 20, "harga": 250000, "tanggal": "2024-01-11"},
        {"nama_produk": "Keyboard Mechanical", "jumlah": 15, "harga": 850000, "tanggal": "2024-01-12"},
        {"nama_produk": "Monitor 4K", "jumlah": 8, "harga": 6500000, "tanggal": "2024-01-13"},
        {"nama_produk": "Headset Gaming", "jumlah": 12, "harga": 1200000, "tanggal": "2024-01-14"},
    ]

    print("\n📝 Memasukkan data contoh...")
    for row in data_contoh:
        engine.tulis_data("penjualan", row)

    # 6. Baca semua data
    print("\n📊 Semua data penjualan:")
    semua_data = engine.baca_data("SELECT * FROM penjualan")
    engine.tampilkan(semua_data)

    # 7. Query spesifik
    print("📊 Produk dengan harga di atas 1 juta:")
    mahal = engine.baca_data("SELECT nama_produk, harga FROM penjualan WHERE harga > ?", (1000000,))
    engine.tampilkan(mahal)

    # 8. Simpan hasil
    print("💾 Menyimpan hasil...")
    engine.simpan_hasil(semua_data, "laporan_penjualan")

    # 9. Tutup koneksi
    engine.tutup()

    print("\n✅ Demo selesai! Cek folder 'results' untuk melihat file output.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    demo_sqlite()