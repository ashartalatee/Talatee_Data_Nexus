"""
tests/test_engine.py
---------------------
Test otomatis untuk Database Connector Engine.
Jalankan: python tests/test_engine.py
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from src.database_connector import DatabaseConnectorEngine, CONFIG

TEST_CONFIG = {
    **CONFIG,
    "db_type": "sqlite",
    "sqlite": {"database_file": "data/test_database.db"},
    "output": {"save_results": True, "output_folder": "results/tests", "format": "csv"}
}


class TestDatabaseConnectorEngine(unittest.TestCase):

    def setUp(self):
        self.engine = DatabaseConnectorEngine(TEST_CONFIG)
        self.engine.connect()
        # Buat tabel test yang bersih
        cursor = self.engine.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_produk")
        self.engine.connection.commit()

    def tearDown(self):
        self.engine.tutup()

    def test_01_koneksi_berhasil(self):
        """Test koneksi ke SQLite berhasil."""
        self.assertIsNotNone(self.engine.connection)
        print("  ✅ Test 1: Koneksi SQLite berhasil")

    def test_02_buat_tabel(self):
        """Test membuat tabel baru."""
        hasil = self.engine.buat_tabel("test_produk", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "nama": "TEXT NOT NULL",
            "harga": "REAL"
        })
        self.assertTrue(hasil)
        print("  ✅ Test 2: Buat tabel berhasil")

    def test_03_tulis_data(self):
        """Test menulis data ke tabel."""
        self.engine.buat_tabel("test_produk", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "nama": "TEXT NOT NULL",
            "harga": "REAL"
        })
        hasil = self.engine.tulis_data("test_produk", {"nama": "Produk Test", "harga": 99000})
        self.assertTrue(hasil)
        print("  ✅ Test 3: Tulis data berhasil")

    def test_04_baca_data(self):
        """Test membaca data dari tabel."""
        self.engine.buat_tabel("test_produk", {
            "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
            "nama": "TEXT NOT NULL",
            "harga": "REAL"
        })
        self.engine.tulis_data("test_produk", {"nama": "Produk A", "harga": 50000})
        self.engine.tulis_data("test_produk", {"nama": "Produk B", "harga": 75000})

        data = self.engine.baca_data("SELECT * FROM test_produk")
        self.assertIsNotNone(data)
        self.assertEqual(data["jumlah_baris"], 2)
        self.assertIn("nama", data["kolom"])
        print("  ✅ Test 4: Baca data berhasil")

    def test_05_simpan_hasil(self):
        """Test menyimpan hasil ke file."""
        data = {"kolom": ["nama", "nilai"], "data": [["Test", 100]], "jumlah_baris": 1}
        path = self.engine.simpan_hasil(data, "test_output")
        self.assertIsNotNone(path)
        self.assertTrue(os.path.exists(path))
        print("  ✅ Test 5: Simpan hasil berhasil")


def jalankan_tests():
    print("\n" + "=" * 50)
    print("  MENJALANKAN UNIT TESTS")
    print("=" * 50)

    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestDatabaseConnectorEngine)
    runner = unittest.TextTestRunner(verbosity=0)
    result = runner.run(suite)

    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print(f"  ✅ SEMUA TEST BERHASIL ({result.testsRun} test)")
    else:
        print(f"  ❌ ADA TEST YANG GAGAL")
        print(f"     Berhasil: {result.testsRun - len(result.failures) - len(result.errors)}")
        print(f"     Gagal   : {len(result.failures) + len(result.errors)}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    jalankan_tests()