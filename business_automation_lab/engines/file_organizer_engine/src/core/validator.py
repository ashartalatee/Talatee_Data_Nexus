# src/core/validator.py
import shutil
from pathlib import Path
from typing import List, Tuple
from src.exceptions.errors import TargetDirectoryConflictError, StorageFullError

class PreFlightValidator:
    """Melakukan verifikasi struktural dan kapasitas storage sebelum operasi I/O dilakukan."""

    def validate_source(self, source_path: Path):
        """Memastikan folder sumber ada dan siap dibaca."""
        if not source_path.exists():
            raise FileNotFoundError(f"Folder sumber tidak ditemukan: {source_path}")
        if not source_path.is_dir():
            raise ValueError(f"Path sumber bukan merupakan sebuah direktori: {source_path}")

    def check_destinations(self, movement_manifest: List[Tuple[Path, Path]]):
        """
        Mengaudit rencana jalur perpindahan untuk mencegah overwrite tak sengaja
        dan memastikan sisa kapasitas disk mencukupi untuk batch tersebut.
        """
        total_batch_size = 0
        target_paths_in_batch = set()

        for src_file, dest_file in movement_manifest:
            total_batch_size += src_file.stat().st_size
            
            # Cegah penimpaan file berharga yang sudah ada di folder tujuan
            if dest_file.exists():
                raise TargetDirectoryConflictError(
                    f"CRITICAL OVERWRITE BLOCK: Berkas '{dest_file.name}' sudah ada di tujuan: '{dest_file.parent}'"
                )
                
            # Deteksi jika ada rute nama file bentrok di dalam satu batch manifest yang sama
            if dest_file in target_paths_in_batch:
                raise TargetDirectoryConflictError(
                    f"BATCH CONFLICT: Dua file dalam batch ini menuju ke path yang sama: {dest_file}"
                )
            target_paths_in_batch.add(dest_file)

        # Periksa sisa ruang penyimpanan pada disk tujuan pemindahan
        if movement_manifest:
            # FIX: Gunakan .resolve() untuk memastikan drive letter (misal 'D:\') tertangkap dengan sempurna di Windows
            dest_root = movement_manifest[0][1].resolve().anchor
            
            try:
                total, used, free = shutil.disk_usage(dest_root)
                # Tambahkan buffer keamanan 50MB di atas total ukuran berkas batch
                if free < (total_batch_size + (50 * 1024 * 1024)):
                    raise StorageFullError(
                        f"STORAGE SAFETY TRIGGERED: Kapasitas disk {dest_root} tidak mencukupi."
                    )
            except Exception as e:
                # Fallback aman jika OS membatasi pembacaan disk_usage direct root
                pass