import pandas as pd
import numpy as np
import logging

error_logger = logging.getLogger("error_quarantine")
exec_logger = logging.getLogger("cleaner_engine")

class ExcelCleanerCore:
    """Core logic for sanitizing, formatting, and handling edge cases in data columns."""
    
    def __init__(self, config: dict):
        self.config = config
        self.schema = config.get("columns_schema", {})

    def clean_chunk(self, chunk: pd.DataFrame, chunk_index: int) -> pd.DataFrame:
        """Cleans a single dataframe chunk without mutating the original state globally."""
        exec_logger.info(f"Processing cleaning matrix for chunk #{chunk_index}")
        
        # 1. Drop baris yang benar-benar kosong total jika dikonfigurasi
        if self.config.get("pipeline", {}).get("drop_empty_rows", True):
            chunk.dropna(how="all", inplace=True)

        cleaned_rows = []
        
        # 2. Row-by-row validation wrapper (Strategi Karantina Error)
        for idx, row in chunk.iterrows():
            try:
                processed_row = self._sanitize_row(row.to_dict())
                cleaned_rows.append(processed_row)
            except Exception as row_error:
                # Jika baris ini rusak (misal string aneh di kolom angka), catat ke log error
                # Engine tetap hidup dan lanjut memproses baris berikutnya!
                error_logger.error(
                    f"[Chunk {chunk_index}][Row ID {idx}] Quarantined due to validation error: {str(row_error)}"
                )
                continue
                
        return pd.DataFrame(cleaned_rows)

    def _sanitize_row(self, row_data: dict) -> dict:
        """Sanitizes individual cells according to the strict industrial schema definitions."""
        sanitized = {}
        
        for col_name, rules in self.schema.items():
            val = row_data.get(col_name, np.nan)
            
            # Cek jika kolom wajib (required) ternyata kosong
            if rules.get("required") and (pd.isna(val) or val == ""):
                raise ValueError(f"Missing required field in column '{col_name}'")
            
            # Jalankan aksi sanitasi berdasarkan tipe data aturan config
            if pd.isna(val):
                val = rules.get("default_value", None)
            
            if rules.get("type") == "string" and val is not None:
                val = str(val).strip() if rules.get("action") == "strip" else str(val)
                
            elif rules.get("numeric") and val is not None:
                try:
                    val = pd.to_numeric(val)
                except:
                    raise TypeError(f"Column '{col_name}' expected numeric, got '{val}'")
                    
            sanitized[col_name] = val
            
        return sanitized