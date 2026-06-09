"""
hash_engine.py — Hash Calculator dengan Caching
Business Automation Lab | Engine 01: Duplicate Remover

Tugas: Menghitung hash konten setiap file (MD5/SHA256).
       Hash identik = konten identik = duplikat 100% akurat.
       Cache hasil hash agar tidak rehash file yang sama.
"""

import hashlib
import json
import logging
from pathlib import Path
from typing import List, Optional

from scanner import FileRecord

logger = logging.getLogger(__name__)

# Baca file per 64KB chunk — aman untuk file besar (video, zip, dll)
CHUNK_SIZE = 65536
CACHE_FILE = ".hash_cache.json"


class HashEngine:
    """
    Hitung hash konten setiap FileRecord.

    Contoh penggunaan:
        engine = HashEngine(config)
        records = engine.compute_all(records)
    """

    def __init__(self, config: dict):
        self.algorithm: str = (
            config.get("detection", {}).get("hash_algorithm", "sha256").lower()
        )
        if self.algorithm not in ("md5", "sha256"):
            raise ValueError(
                f"Hash algorithm tidak valid: '{self.algorithm}'. "
                "Gunakan 'md5' atau 'sha256'."
            )
        self._cache: dict = {}
        self._cache_loaded = False

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def compute_all(self, records: List[FileRecord]) -> List[FileRecord]:
        """
        Hitung hash untuk semua FileRecord.
        Kembalikan records yang sudah terisi hash_value-nya.
        """
        self._load_cache()
        total = len(records)
        failed = 0

        logger.info(
            f"Menghitung hash {total} file menggunakan {self.algorithm.upper()}..."
        )

        for i, record in enumerate(records, 1):
            if i % 100 == 0 or i == total:
                logger.info(f"  Progress: {i}/{total} file")

            hash_val = self._get_hash(record)
            if hash_val:
                record.hash_value = hash_val
            else:
                failed += 1
                logger.warning(f"Gagal hash: {record.name}")

        self._save_cache()
        logger.info(
            f"Hash selesai: {total - failed} berhasil, {failed} gagal"
        )
        return records

    def compute_single(self, file_path: Path) -> Optional[str]:
        """Hitung hash untuk satu file path."""
        return self._hash_file(file_path)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_hash(self, record: FileRecord) -> Optional[str]:
        """Ambil hash dari cache atau hitung baru."""
        cache_key = self._cache_key(record)

        if cache_key in self._cache:
            logger.debug(f"Cache hit: {record.name}")
            return self._cache[cache_key]

        hash_val = self._hash_file(record.path)
        if hash_val:
            self._cache[cache_key] = hash_val
        return hash_val

    def _hash_file(self, file_path: Path) -> Optional[str]:
        """Baca file per chunk dan hitung hash."""
        try:
            hasher = hashlib.new(self.algorithm)
            with open(file_path, "rb") as f:
                while chunk := f.read(CHUNK_SIZE):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except (PermissionError, OSError) as e:
            logger.error(f"Error membaca {file_path.name}: {e}")
            return None

    def _cache_key(self, record: FileRecord) -> str:
        """
        Key cache = path + ukuran + waktu modifikasi.
        Jika file berubah, cache otomatis invalid.
        """
        mtime = record.modified_at.timestamp()
        return f"{record.path_str}|{record.size_bytes}|{mtime}"

    def _load_cache(self):
        """Muat cache dari disk jika ada."""
        if self._cache_loaded:
            return
        cache_path = Path(CACHE_FILE)
        if cache_path.exists():
            try:
                with open(cache_path, "r") as f:
                    self._cache = json.load(f)
                logger.info(f"Cache dimuat: {len(self._cache)} entri")
            except (json.JSONDecodeError, OSError):
                logger.warning("Cache rusak, akan dihitung ulang dari awal.")
                self._cache = {}
        self._cache_loaded = True

    def _save_cache(self):
        """Simpan cache ke disk untuk sesi berikutnya."""
        try:
            with open(CACHE_FILE, "w") as f:
                json.dump(self._cache, f)
            logger.debug(f"Cache disimpan: {len(self._cache)} entri")
        except OSError as e:
            logger.warning(f"Gagal simpan cache: {e}")