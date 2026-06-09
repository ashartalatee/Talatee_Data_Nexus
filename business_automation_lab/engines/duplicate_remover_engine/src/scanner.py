"""
scanner.py — File Scanner & Crawler
Business Automation Lab | Engine 01: Duplicate Remover

Tugas: Membaca semua file dari folder input (rekursif),
       mengumpulkan metadata lengkap setiap file.
"""

import os
import logging
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class FileRecord:
    """Representasi satu file beserta metadata-nya."""
    path: Path
    name: str
    extension: str
    size_bytes: int
    size_kb: float
    modified_at: datetime
    created_at: datetime
    hash_value: Optional[str] = None          # diisi oleh hash_engine.py
    is_duplicate: bool = False                # diisi oleh duplicate_detector.py
    duplicate_group: Optional[str] = None     # hash grup duplikat
    keep: bool = True                         # True = file master, False = duplikat

    @property
    def size_mb(self) -> float:
        return round(self.size_bytes / (1024 * 1024), 3)

    @property
    def path_str(self) -> str:
        return str(self.path)


class FileScanner:
    """
    Scan folder input dan kumpulkan metadata semua file.

    Contoh penggunaan:
        scanner = FileScanner(config)
        records = scanner.scan()
    """

    def __init__(self, config: dict):
        self.config = config
        self.input_folder = Path(config["paths"]["input_folder"])
        self.scan_cfg = config.get("scan", {})
        self.recursive: bool = self.scan_cfg.get("recursive", True)
        self.include_extensions: List[str] = [
            ext.lower() for ext in self.scan_cfg.get("include_extensions", [])
        ]
        self.exclude_folders: List[str] = [
            f.lower() for f in self.scan_cfg.get("exclude_folders", [])
        ]
        self.min_size_kb: float = self.scan_cfg.get("min_file_size_kb", 1)

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def scan(self) -> List[FileRecord]:
        """Jalankan scan dan kembalikan list FileRecord."""
        if not self.input_folder.exists():
            raise FileNotFoundError(
                f"Folder input tidak ditemukan: '{self.input_folder}'\n"
                "Buat folder 'input/' dan taruh file yang ingin diproses."
            )

        logger.info(f"Mulai scan folder: {self.input_folder.resolve()}")
        records: List[FileRecord] = []
        skipped = 0

        paths = self._iter_paths()
        for file_path in paths:
            record = self._build_record(file_path)
            if record is None:
                skipped += 1
                continue
            records.append(record)

        logger.info(
            f"Scan selesai: {len(records)} file ditemukan, {skipped} dilewati"
        )
        return records

    def summary(self, records: List[FileRecord]) -> dict:
        """Ringkasan hasil scan."""
        total_size = sum(r.size_bytes for r in records)
        ext_count: dict = {}
        for r in records:
            ext_count[r.extension] = ext_count.get(r.extension, 0) + 1

        return {
            "total_files": len(records),
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "extensions_found": ext_count,
            "input_folder": str(self.input_folder.resolve()),
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _iter_paths(self):
        """Iterasi path file sesuai setting recursive dan filter."""
        if self.recursive:
            for root, dirs, files in os.walk(self.input_folder):
                # Filter folder yang di-exclude
                dirs[:] = [
                    d for d in dirs
                    if d.lower() not in self.exclude_folders
                ]
                for fname in files:
                    yield Path(root) / fname
        else:
            for item in self.input_folder.iterdir():
                if item.is_file():
                    yield item

    def _build_record(self, file_path: Path) -> Optional[FileRecord]:
        """Buat FileRecord dari satu path. Return None jika file diskip."""
        try:
            stat = file_path.stat()
        except (PermissionError, OSError) as e:
            logger.warning(f"Tidak bisa baca file {file_path}: {e}")
            return None

        size_bytes = stat.st_size
        size_kb = size_bytes / 1024
        extension = file_path.suffix.lower()

        # Filter ukuran minimum
        if size_kb < self.min_size_kb:
            logger.debug(f"Skip (terlalu kecil): {file_path.name} ({size_kb:.1f} KB)")
            return None

        # Filter ekstensi (jika config tidak kosong)
        if self.include_extensions and extension not in self.include_extensions:
            logger.debug(f"Skip (ekstensi tidak termasuk): {file_path.name}")
            return None

        return FileRecord(
            path=file_path,
            name=file_path.name,
            extension=extension,
            size_bytes=size_bytes,
            size_kb=round(size_kb, 2),
            modified_at=datetime.fromtimestamp(stat.st_mtime),
            created_at=datetime.fromtimestamp(stat.st_ctime),
        )