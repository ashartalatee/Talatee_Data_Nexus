# src/core/orchestrator.py
import logging
import uuid
from typing import Dict, Any

logger = logging.getLogger("TalateeEngine")

class DataCollectionOrchestrator:
    def __init__(self, extractor, transformer, storage):
        self.extractor = extractor
        self.transformer = transformer
        self.storage = storage

    def execute_pipeline(self, target_url: str, platform: str) -> Dict[str, Any]:
        """
        Menjalankan satu siklus penuh ekstraksi data secara aman (Sirkuit Terisolasi).
        """
        run_id = str(uuid.uuid4())
        logger.info(f"Memulai pipeline eksekusi untuk platform: {platform}", extra={"run_id": run_id})

        # --- FASE 1: AMBIL DATA MENTAH (RAW EXTRACTION) ---
        try:
            # Extractor dijamin mengimplementasikan retry mechanism secara internal
            raw_payload = self.extractor.fetch_product_details(target_url)
            
            # Amankan payload mentah sebagai single-source-of-truth historis
            self.storage.save_raw(raw_payload, platform, run_id)
            
        except Exception as err:
            logger.critical(f"Kegagalan fatal pada fase ekstraksi data: {str(err)}", extra={"run_id": run_id})
            return {"status": "FAILED_INGESTION", "run_id": run_id, "error": str(err)}

        # --- FASE 2: NORMALISASI DAN VALIDASI DATA ---
        try:
            # Mengubah struktur payload mentah platform spesifik menjadi objek data terpadu
            clean_records = self.transformer.standardize(raw_payload, platform, run_id)
            
            # Menyimpan hasil data bersih siap pakai ke folder processed
            self.storage.save_processed(clean_records, run_id)
            
            logger.info("Pipeline berhasil dieksekusi dengan integritas data 100%.", extra={"run_id": run_id})
            return {"status": "SUCCESS", "run_id": run_id}
            
        except Exception as err:
            # Jika skema pecah, data mentah di Fase 1 tetap terselamatkan dengan aman untuk di-debug
            logger.error(f"Gagal memvalidasi/mentransformasi data mentah: {str(err)}", extra={"run_id": run_id})
            return {"status": "FAILED_TRANSFORMATION", "run_id": run_id, "error": str(err)}