"""
duplicate_detector.py — Deteksi & Klasifikasi Duplikat
Business Automation Lab | Engine 01: Duplicate Remover

Tugas: Mengelompokkan file berdasarkan hash yang sama.
       Menentukan file "master" (yang dipertahankan) dan
       file "duplikat" (yang akan dihapus/diarsipkan).
"""

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict

from scanner import FileRecord

logger = logging.getLogger(__name__)


@dataclass
class DuplicateGroup:
    """Satu grup file dengan konten identik."""
    group_id: str              # hash SHA256/MD5 sebagai ID grup
    master: FileRecord         # file yang dipertahankan
    duplicates: List[FileRecord]  # file yang akan dihapus

    @property
    def total_files(self) -> int:
        return 1 + len(self.duplicates)

    @property
    def wasted_bytes(self) -> int:
        """Ukuran storage yang bisa dihemat."""
        return sum(d.size_bytes for d in self.duplicates)

    @property
    def wasted_mb(self) -> float:
        return round(self.wasted_bytes / (1024 * 1024), 3)


class DuplicateDetector:
    """
    Mendeteksi dan mengklasifikasikan file duplikat.

    Contoh penggunaan:
        detector = DuplicateDetector(config)
        groups, records = detector.detect(records)
    """

    STRATEGIES = ("oldest", "newest", "shortest_path")

    def __init__(self, config: dict):
        det_cfg = config.get("detection", {})
        self.keep_strategy: str = det_cfg.get("keep_strategy", "oldest")
        self.compare_by: List[str] = det_cfg.get(
            "compare_by", ["hash"]
        )

        if self.keep_strategy not in self.STRATEGIES:
            raise ValueError(
                f"keep_strategy tidak valid: '{self.keep_strategy}'. "
                f"Pilihan: {self.STRATEGIES}"
            )

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def detect(
        self, records: List[FileRecord]
    ) -> tuple[List[DuplicateGroup], List[FileRecord]]:
        """
        Jalankan deteksi duplikat pada list FileRecord.

        Returns:
            groups  — list DuplicateGroup yang terdeteksi
            records — FileRecord yang sudah ter-update is_duplicate & keep
        """
        logger.info(
            f"Mendeteksi duplikat ({', '.join(self.compare_by)}) "
            f"— strategi keep: {self.keep_strategy}"
        )

        groups: List[DuplicateGroup] = []

        if "hash" in self.compare_by:
            hash_groups = self._group_by_hash(records)
            groups.extend(hash_groups)

        if "name_and_size" in self.compare_by:
            # Deteksi tambahan: nama + ukuran sama tapi mungkin beda hash
            name_size_groups = self._group_by_name_and_size(records)
            # Hanya tambahkan grup yang belum terdeteksi via hash
            detected_paths = {
                d.path_str
                for g in groups
                for d in g.duplicates
            }
            for g in name_size_groups:
                if g.master.path_str not in detected_paths:
                    groups.append(g)

        # Update flag di setiap FileRecord
        self._mark_records(groups)

        total_dupes = sum(len(g.duplicates) for g in groups)
        total_wasted_mb = sum(g.wasted_mb for g in groups)
        logger.info(
            f"Ditemukan {len(groups)} grup duplikat, "
            f"{total_dupes} file duplikat, "
            f"potensi hemat {total_wasted_mb:.2f} MB"
        )

        return groups, records

    def summary(self, groups: List[DuplicateGroup]) -> dict:
        """Ringkasan hasil deteksi."""
        total_dupes = sum(len(g.duplicates) for g in groups)
        total_wasted = sum(g.wasted_bytes for g in groups)
        return {
            "duplicate_groups": len(groups),
            "total_duplicate_files": total_dupes,
            "space_wasted_mb": round(total_wasted / (1024 * 1024), 2),
            "keep_strategy": self.keep_strategy,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _group_by_hash(self, records: List[FileRecord]) -> List[DuplicateGroup]:
        """Kelompokkan file berdasarkan hash konten identik."""
        hash_map: Dict[str, List[FileRecord]] = defaultdict(list)

        for record in records:
            if record.hash_value:
                hash_map[record.hash_value].append(record)

        groups = []
        for hash_val, file_list in hash_map.items():
            if len(file_list) < 2:
                continue  # bukan duplikat
            master = self._pick_master(file_list)
            dupes = [f for f in file_list if f.path_str != master.path_str]
            groups.append(
                DuplicateGroup(
                    group_id=hash_val,
                    master=master,
                    duplicates=dupes,
                )
            )

        return groups

    def _group_by_name_and_size(
        self, records: List[FileRecord]
    ) -> List[DuplicateGroup]:
        """Kelompokkan file berdasarkan nama + ukuran yang sama."""
        key_map: Dict[str, List[FileRecord]] = defaultdict(list)

        for record in records:
            key = f"{record.name.lower()}|{record.size_bytes}"
            key_map[key].append(record)

        groups = []
        for key, file_list in key_map.items():
            if len(file_list) < 2:
                continue
            master = self._pick_master(file_list)
            dupes = [f for f in file_list if f.path_str != master.path_str]
            groups.append(
                DuplicateGroup(
                    group_id=f"name_size|{key}",
                    master=master,
                    duplicates=dupes,
                )
            )

        return groups

    def _pick_master(self, file_list: List[FileRecord]) -> FileRecord:
        """
        Pilih file master berdasarkan keep_strategy.

        oldest       → file dengan tanggal modifikasi terlama (dibuat paling awal)
        newest       → file dengan tanggal modifikasi terbaru
        shortest_path→ file dengan path paling pendek (biasanya di root folder)
        """
        if self.keep_strategy == "oldest":
            return min(file_list, key=lambda f: f.modified_at)
        elif self.keep_strategy == "newest":
            return max(file_list, key=lambda f: f.modified_at)
        elif self.keep_strategy == "shortest_path":
            return min(file_list, key=lambda f: len(f.path_str))
        else:
            return file_list[0]

    def _mark_records(self, groups: List[DuplicateGroup]):
        """Update flag is_duplicate, keep, dan duplicate_group di setiap FileRecord."""
        for group in groups:
            group.master.keep = True
            group.master.is_duplicate = False
            group.master.duplicate_group = group.group_id

            for dupe in group.duplicates:
                dupe.is_duplicate = True
                dupe.keep = False
                dupe.duplicate_group = group.group_id