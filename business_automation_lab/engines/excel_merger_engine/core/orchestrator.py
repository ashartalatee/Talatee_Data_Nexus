import logging
from typing import List, Dict
import pandas as pd

from core.context import EngineContext
from services.validator import ExcelSchemaValidator
from core.exceptions import SchemaMismatchError  # Asumsi custom exception di exceptions.py

logger = logging.getLogger("Talatee.ExcelMerger")

class ExcelMergerOrchestrator:
    def __init__(self, validator: ExcelSchemaValidator):
        self.validator = validator

    def execute_merge(self, context: EngineContext, file_paths: List[str]) -> pd.DataFrame:
        """
        Mengorkestrasikan ingestion, validasi defensif, hingga penyatuan data dataframe.
        """
        logger.info(f"Starting Excel Merger Engine Execution. Run ID: {context.run_id}")
        valid_dataframes: List[pd.DataFrame] = []

        for path in file_paths:
            try:
                # Simulasi pemanggilan Driver Reader (Di tingkat industri menggunakan openpyxl/calamine)
                df = pd.read_excel(path)
                
                # Jalankan Validasi Ketat
                is_valid, error_message = self.validator.validate_structure(df, path)
                
                if not is_valid:
                    logger.error(f"[RUN_ID: {context.run_id}] Kegagalan Validasi Skema: {error_message}")
                    context.increment_metric("errors_encountered")
                    # Di industri, kita bisa memilih: Skip file ini ATAU hentikan seluruh proses.
                    # Kita pilih fail-fast jika ini pipeline data finansial/omset krusial:
                    raise SchemaMismatchError(error_message)

                # Jika lolos validasi, masukkan ke antrean merge
                valid_dataframes.append(df)
                context.increment_metric("files_processed")
                context.increment_metric("rows_merged", len(df))
                logger.info(f"File {path} berhasil divalidasi dan masuk antrean.")

            except Exception as e:
                logger.critical(f"Fatal error saat memproses file {path}: {str(e)}")
                raise e

        # Proses Penggabungan Akhir (Merger Service)
        if not valid_dataframes:
            logger.warning("Tidak ada dataframe valid yang bisa digabungkan.")
            return pd.DataFrame()

        logger.info(f"Menggabungkan {len(valid_dataframes)} file Excel secara vertikal...")
        merged_result = pd.concat(valid_dataframes, ignore_index=True)
        
        logger.info(f"Eksekusi Selesai Sempurna. Durasi: {context.get_duration():.2f}s. Total Baris: {len(merged_result)}")
        return merged_result