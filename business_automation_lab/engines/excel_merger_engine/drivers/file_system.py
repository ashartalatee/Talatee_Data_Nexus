import os
import shutil
import logging
from typing import List

logger = logging.getLogger("Talatee.ExcelMerger")

class LocalFileSystemDriver:
    """
    Mengelola siklus hidup berkas fisik (I/O) pada local storage secara atomik 
    dan mengisolasi area kerja guna mencegah korupsi data.
    """
    def __init__(self, base_storage_path: str = "storage"):
        self.input_dir = os.path.join(base_storage_path, "input")
        self.processing_dir = os.path.join(base_storage_path, "processing")
        self.output_dir = os.path.join(base_storage_path, "output")
        self._ensure_directories()

    def _ensure_directories(self):
        """Memastikan semua folder sandbox tersedia saat engine diinisialisasi."""
        for directory in [self.input_dir, self.processing_dir, self.output_dir]:
            os.makedirs(directory, exist_ok=True)

    def scan_input_files(self) -> List[str]:
        """Memindai hanya file Excel valid (.xlsx) dari drop zone input."""
        files = [
            os.path.join(self.input_dir, f) 
            for f in os.listdir(self.input_dir) 
            if f.endswith(".xlsx") and not f.startswith("~$")  # Abaikan temporary owner file bawaan MS Excel
        ]
        return files

    def move_to_processing(self, file_path: str) -> str:
        """Memindahkan berkas ke vault pemrosesan untuk mengisolasi status file."""
        file_name = os.path.basename(file_path)
        destination = os.path.join(self.processing_dir, file_name)
        
        # Menggunakan shutil.move untuk memindahkan file secara atomik
        shutil.move(file_path, destination)
        logger.debug(f"File diisolasi ke sandbox pemrosesan: {file_name}")
        return destination

    def clean_processing_file(self, file_path: str):
        """Menghapus file dari folder pemrosesan setelah sukses digabungkan."""
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.debug(f"File pada sandbox berhasil dibersihkan: {os.path.basename(file_path)}")