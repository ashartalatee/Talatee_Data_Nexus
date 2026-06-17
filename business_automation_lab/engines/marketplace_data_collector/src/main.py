import sys
import logging
from src.core.orchestrator import DataCollectionOrchestrator
from src.storage.local_storage import LocalJsonStorage
from src.transformers.market_cleaner import MarketplaceTransformer

# Konfigurasi Logging Dasar untuk Keperluan Konsol Proyek Portofolio
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(run_id)s) %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Dummy Driver Extractor untuk keperluan demonstrasi lokal
class MockShopeeExtractor:
    def fetch_product_details(self, target_id: str) -> dict:
        # Simulasi response sukses dari server Shopee API
        return {
            "item": {
                "itemid": target_id,
                "name": " Kaos Polos Katun Premium V4 ",
                "price": 7500000000, # Format harga API mentah (dikali 100rb)
                "stock": 142,
                "historical_sold": 1250
            }
        }

if __name__ == "__main__":
    # Inisialisasi komponen-komponen utama (Dependency Injection)
    storage_engine = LocalJsonStorage()
    transformer_engine = MarketplaceTransformer()
    mock_extractor = MockShopeeExtractor()

    # Satukan komponen ke dalam orkestrator utama
    orchestrator = DataCollectionOrchestrator(
        extractor=mock_extractor,
        transformer=transformer_engine,
        storage=storage_engine
    )

    print("=== MENGALIRKAN DATA PIPELINE AUTOMATION ENGINE ===")
    
    # Eksekusi pipeline pengumpulan data produk Shopee dengan ID "8849102"
    result = orchestrator.execute_pipeline(target_url="8849102", platform="shopee")
    
    print(f"Hasil Akhir Eksekusi Status: {result['status']}")