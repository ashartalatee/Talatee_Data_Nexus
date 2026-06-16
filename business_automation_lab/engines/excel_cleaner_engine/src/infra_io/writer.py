import os
import pandas as pd
import logging

logger = logging.getLogger("cleaner_engine")

class ExcelStreamWriter:
    """Handles high-performance, low-memory data writing to the output directory."""
    
    def __init__(self, output_path: str):
        self.output_path = output_path
        self._ensure_directory_exists()

    def _ensure_directory_exists(self):
        """Creates the output directory if it doesn't exist yet."""
        directory = os.path.dirname(self.output_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    def write_chunk(self, chunk_df: pd.DataFrame, chunk_index: int):
        """
        Appends the cleaned chunk to the output file.
        If it's the first chunk, it writes the header; otherwise, it appends seamlessly.
        """
        if chunk_df.empty:
            logger.warning(f"Chunk #{chunk_index} is empty. Skipping write operation.")
            return

        # Menentukan apakah kita perlu menulis header baru atau tidak
        # Jika file belum ada, write_header = True
        write_header = not os.path.exists(self.output_path)
        
        try:
            # Menggunakan format CSV untuk kestabilan IO skala besar (bisa diconvert ke Excel di akhir jika perlu)
            chunk_df.to_csv(
                self.output_path,
                mode='a',          # 'a' berarti APPEND, data ditambahkan ke bawahnya, bukan menimpa
                index=False,       # Tidak menulis baris index dataframe agar rapi
                header=write_header
            )
            logger.info(f"Chunk #{chunk_index} successfully committed to disk.")
        except Exception as e:
            logger.error(f"FATAL: Failed to write chunk #{chunk_index} to {self.output_path}. Error: {str(e)}")
            raise e