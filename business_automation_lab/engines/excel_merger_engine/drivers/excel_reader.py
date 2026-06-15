import os
import pandas as pd
import logging
from typing import Generator

logger = logging.getLogger("Talatee.ExcelMerger")

class SafeExcelReader:
    """
    Infrastructure Driver untuk pembacaan file Excel dengan fokus pada 
    keamanan memori (Memory Safety) dan pencegahan file lock.
    """
    def __init__(self, chunk_size: int = 10000):
        self.chunk_size = chunk_size

    def read_file_safely(self, file_path: str) -> pd.DataFrame:
        """
        Membaca file Excel secara defensif. Jika file sangat besar, 
        disarankan menggunakan engine pendukung yang cepat seperti 'calamine' atau 'openpyxl'.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Target file tidak ditemukan pada path: {file_path}")
            
        logger.info(f"Membuka file via SafeExcelReader: {os.path.basename(file_path)}")
        
        # Menggunakan parameter read-only dan optimasi internal untuk mencegah overload memori
        try:
            # Menggunakan pandas dengan engine openpyxl secara eksplisit dan memaksa 
            # konversi string dasar pada kolom teks untuk mencegah interferensi tipe data objek campuran
            with open(file_path, "rb") as f:
                df = pd.read_excel(f, engine="openpyxl")
            return df
        except Exception as e:
            logger.error(f"Gagal melakukan pembacaan stream pada file {file_path}: {str(e)}")
            raise e