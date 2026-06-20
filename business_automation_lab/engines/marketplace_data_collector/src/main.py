import logging
from src.core.logging_config import setup_global_logging
from src.core.config_loader import ConfigLoader
from src.core.orchestrator import DataCollectionOrchestrator
from src.transformers.market_cleaner import MarketplaceTransformer
from src.storage.local_storage import LocalJsonStorage
from src.storage.analytics_exporter import AnalyticsExporter

setup_global_logging()
logger = logging.getLogger("TalateeEngine")

# Mock Extractor untuk simulasi pipeline produk sukses
class MockMarketplaceClient:
    def fetch_product_details(self, target_id: str) -> dict:
        return {
            "item": {
                "itemid": target_id,
                "name": "Sepatu Sneakers Running Pro X",
                "price": 12000000000, # Rp 120.000 setelah konversi
                "stock": 85,
                "historical_sold": 950 # Omset > 100jt -> Grade A
            }
        }

if __name__ == "__main__":
    logger.info("Menjalankan Siklus Final Ekosistem Engine 06...")

    try:
        # 1. Inisialisasi komponen utama
        storage = LocalJsonStorage()
        transformer = MarketplaceTransformer()
        extractor = MockMarketplaceClient()
        analytics = AnalyticsExporter()

        orchestrator = DataCollectionOrchestrator(extractor, transformer, storage)

        # 2. Jalankan pengumpulan data beberapa produk dummy
        orchestrator.execute_pipeline(target_url="1002021", platform="shopee")
        
        # 3. Eksekusi Pelaporan Analitik Bisnis di Akhir Sesi
        report_file = analytics.generate_daily_executive_summary()
        logger.info(f"Proses automasi selesai penuh. Berkas laporan siap dikirim: {report_file}")

    except Exception as e:
        logger.critical(f"Kegagalan sistem di fase akhir eksekusi: {str(e)}")