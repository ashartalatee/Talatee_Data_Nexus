import unittest
from src.transformers.market_cleaner import MarketplaceTransformer
from src.transformers.schemas import MarketplaceUnifiedModel

class TestMarketplaceTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = MarketplaceTransformer()
        self.run_id = "test-run-123"

    def test_transform_shopee_success(self):
        """Memastikan data mentah Shopee berhasil dikonversi dan harga dinormalisasi"""
        raw_shopee = {
            "item": {
                "itemid": "12345",
                "name": "  Kemeja Flanel Premium  ",
                "price": 15000000000,  # Format API Shopee asli (150.000 * 100.000)
                "stock": 50,
                "historical_sold": 300
            }
        }
        
        result = self.transformer.standardize(raw_shopee, platform="shopee", run_id=self.run_id)
        
        # Validasi Integritas Hasil
        self.setIsinstance(result, MarketplaceUnifiedModel)
        self.assertEqual(result.product_name, "Kemeja Flanel Premium")  # Teks harus sudah bersih (di-strip)
        self.assertEqual(result.price, 150000.0)  # Harga harus terkonversi dengan benar
        self.assertEqual(result.platform, "shopee")

    def test_transform_missing_key_raises_error(self):
        """Sistem harus melempar KeyError secara prediktif jika struktur JSON marketplace berubah"""
        raw_broken_data = {
            "produk_ngawur": {  # Struktur kunci berubah mendadak
                "id": "123"
            }
        }
        
        with self.assertRaises(KeyError):
            self.transformer.standardize(raw_broken_data, platform="shopee", run_id=self.run_id)

if __name__ == "__main__":
    unittest.main()