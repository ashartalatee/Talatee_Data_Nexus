"""
examples/contoh_bisnis_nyata.py
--------------------------------
Contoh penggunaan Database Connector Engine untuk skenario bisnis nyata.
Cocok untuk pemula dan non-IT.

Cara menjalankan:
    python examples/contoh_bisnis_nyata.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database_connector import DatabaseConnectorEngine


# ============================================================
#  SKENARIO 1: TOKO ONLINE - Catat dan laporan penjualan
# ============================================================

def skenario_toko_online():
    print("\n" + "🛒 " * 20)
    print("  SKENARIO: TOKO ONLINE")
    print("  Catat transaksi dan buat laporan otomatis")
    print("🛒 " * 20)

    engine = DatabaseConnectorEngine()
    engine.connect()

    # Buat struktur database toko
    engine.buat_tabel("produk", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "nama": "TEXT NOT NULL",
        "stok": "INTEGER DEFAULT 0",
        "harga_jual": "REAL"
    })

    engine.buat_tabel("transaksi", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "nama_pembeli": "TEXT",
        "produk": "TEXT",
        "jumlah": "INTEGER",
        "total": "REAL",
        "tanggal": "TEXT"
    })

    # Tambah produk
    produk_list = [
        {"nama": "Kaos Polos", "stok": 100, "harga_jual": 85000},
        {"nama": "Celana Jogger", "stok": 50, "harga_jual": 150000},
        {"nama": "Jaket Hoodie", "stok": 30, "harga_jual": 250000},
    ]
    for p in produk_list:
        engine.tulis_data("produk", p)

    # Catat transaksi
    transaksi_list = [
        {"nama_pembeli": "Budi", "produk": "Kaos Polos", "jumlah": 3, "total": 255000, "tanggal": "2024-01-15"},
        {"nama_pembeli": "Sari", "produk": "Jaket Hoodie", "jumlah": 1, "total": 250000, "tanggal": "2024-01-15"},
        {"nama_pembeli": "Andi", "produk": "Celana Jogger", "jumlah": 2, "total": 300000, "tanggal": "2024-01-16"},
        {"nama_pembeli": "Dewi", "produk": "Kaos Polos", "jumlah": 5, "total": 425000, "tanggal": "2024-01-16"},
    ]
    for t in transaksi_list:
        engine.tulis_data("transaksi", t)

    # Laporan: total penjualan per produk
    print("\n📊 LAPORAN PENJUALAN PER PRODUK:")
    laporan = engine.baca_data("""
        SELECT produk, 
               SUM(jumlah) as total_terjual, 
               SUM(total) as total_omzet
        FROM transaksi
        GROUP BY produk
        ORDER BY total_omzet DESC
    """)
    engine.tampilkan(laporan)

    # Simpan laporan ke Excel
    engine.simpan_hasil(laporan, "laporan_toko_online")
    engine.tutup()


# ============================================================
#  SKENARIO 2: MANAJEMEN STOK - Monitor stok gudang
# ============================================================

def skenario_stok_gudang():
    print("\n" + "📦 " * 20)
    print("  SKENARIO: MANAJEMEN STOK GUDANG")
    print("  Monitor stok dan deteksi stok menipis")
    print("📦 " * 20)

    engine = DatabaseConnectorEngine()
    engine.connect()

    engine.buat_tabel("stok_barang", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "kode": "TEXT UNIQUE",
        "nama_barang": "TEXT",
        "stok_saat_ini": "INTEGER",
        "stok_minimum": "INTEGER",
        "satuan": "TEXT"
    })

    barang_list = [
        {"kode": "BRG001", "nama_barang": "Kertas A4", "stok_saat_ini": 500, "stok_minimum": 100, "satuan": "rim"},
        {"kode": "BRG002", "nama_barang": "Tinta Printer", "stok_saat_ini": 20, "stok_minimum": 50, "satuan": "botol"},
        {"kode": "BRG003", "nama_barang": "Amplop", "stok_saat_ini": 5, "stok_minimum": 20, "satuan": "box"},
        {"kode": "BRG004", "nama_barang": "Ballpoint", "stok_saat_ini": 80, "stok_minimum": 30, "satuan": "lusin"},
    ]
    for b in barang_list:
        engine.tulis_data("stok_barang", b)

    # Deteksi barang yang stoknya menipis
    print("\n⚠️  PERINGATAN: STOK MENIPIS!")
    stok_menipis = engine.baca_data("""
        SELECT kode, nama_barang, stok_saat_ini, stok_minimum, satuan
        FROM stok_barang
        WHERE stok_saat_ini <= stok_minimum
        ORDER BY stok_saat_ini ASC
    """)
    engine.tampilkan(stok_menipis)

    if stok_menipis and stok_menipis["jumlah_baris"] > 0:
        print(f"  ⚠️  Ada {stok_menipis['jumlah_baris']} barang yang perlu segera diorder!\n")

    engine.simpan_hasil(stok_menipis, "alert_stok_menipis")
    engine.tutup()


# ============================================================
#  JALANKAN SEMUA SKENARIO
# ============================================================

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  DATABASE CONNECTOR ENGINE - Contoh Bisnis Nyata")
    print("=" * 60)

    skenario_toko_online()
    skenario_stok_gudang()

    print("\n✅ Semua skenario selesai!")
    print("📁 Cek folder 'results' untuk melihat laporan Excel.")
    print("=" * 60 + "\n")