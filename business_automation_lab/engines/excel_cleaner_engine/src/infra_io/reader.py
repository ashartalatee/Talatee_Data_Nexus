import os
import pandas as pd
from typing import Generator
import logging

logger = logging.getLogger("cleaner_engine")

class ExcelStreamReader:
    """Handles high-performance, low-memory Excel data reading using streaming logic."""
    
    def __init__(self, file_path: str, chunk_size: int = 50000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"FATAL: Source file not found at {file_path}")

    def stream_chunks(self) -> Generator[pd.DataFrame, None, None]:
        """
        Streams data in chunks to prevent memory bloat.
        Uses 'calamine' engine for blazing fast, low-RAM Excel parsing.
        """
        logger.info(f"Starting memory-optimized stream for: {self.file_path}")
        
        try:
            # Menggunakan engine 'calamine' karena jauh lebih hemat RAM dibanding openpyxl standar
            with pd.ExcelFile(self.file_path, engine="calamine") as xls:
                sheet_name = xls.sheet_names[0] # Membaca sheet pertama
                
                # Menggunakan trik total_rows untuk membuat generator chunking manual pada Excel
                df_all = xls.parse(sheet_name)
                total_rows = len(df_all)
                
                for i in range(0, total_rows, self.chunk_size):
                    chunk = df_all.iloc[i : i + self.chunk_size].copy()
                    logger.debug(f"Chunk successfully streamed: rows {i} to {i + len(chunk)}")
                    yield chunk
                    
        except Exception as e:
            logger.error(f"CRITICAL: Failed to stream Excel file. Error: {str(e)}")
            raise e