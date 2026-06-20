import logging
import uuid
from src.transformers.intelligence_analyst import IntelligenceAnalyst

logger = logging.getLogger("TalateeEngine")

class DataCollectionOrchestrator:
    def __init__(self, extractor, transformer, storage):
        self.extractor = extractor
        self.transformer = transformer
        self.storage = storage
        # Mengintegrasikan analisis intelijen data langsung ke dalam pipeline
        self.analyst = IntelligenceAnalyst()

    def execute_pipeline(self, target_url: str, platform: str) -> dict:
        run_id = str(uuid.uuid4())
        logger.info(f"Memulai orkestrasi otomatis Engine no. 6", extra={"run_id": run_id})

        # Fase 1: Ingestion Data Mentah secara Aman
        try:
            raw_payload = self.extractor.fetch_product_details(target_url)
            self.storage.save_raw(raw_payload, platform, run_id)
        except Exception as e:
            logger.critical(f"Gagal fatal pada fase pengambilan data: {str(e)}", extra={"run_id": run_id})
            return {"status": "FAILED_INGESTION", "reason": str(e)}

        # Fase 2: Transformasi Data Standardisasi Awal
        try:
            # Mengubah JSON mentah menjadi objek Pydantic awal
            unified_data = self.transformer.standardize(raw_payload, platform, run_id)
            
            # --- FASE INTEGRASI BARU: INTELLIGENCE LAYER ---
            # Jalankan analisis grading tanpa mencampuradukkan logika di dalam extractor maupun transformer
            grade_result = self.analyst.classify_product_grade(
                price=unified_data.price, 
                total_sold=unified_data.total_sold, 
                run_id=run_id
            )
            
            # Perbarui nilai model secara deterministik sebelum ditulis ke penyimpanan fisik
            unified_data.product_grade = grade_result
            
            # Fase 3: Persistensi Data Bersih Akhir oleh Single Writer
            self.storage.save_processed(unified_data, run_id)
            
            logger.info("Seluruh alur pipeline dan analisis data sukses diselesaikan.", extra={"run_id": run_id})
            return {"status": "SUCCESS", "run_id": run_id, "grade": grade_result}

        except Exception as e:
            logger.error(f"Gagal memproses analisis data. Berkas mentah tetap aman terarsip: {str(e)}", extra={"run_id": run_id})
            return {"status": "FAILED_PROCESSING", "run_id": run_id}