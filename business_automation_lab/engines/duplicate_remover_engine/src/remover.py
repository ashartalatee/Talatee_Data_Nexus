"""
remover.py — Safe File Remover
Business Automation Lab | Engine 01: Duplicate Remover

Tugas: Hapus atau arsipkan file duplikat secara aman.
       Selalu ada dry-run mode — tidak pernah langsung delete
       tanpa preview dulu.
"""

import logging
import shutil
from pathlib import Path
from typing import List

from duplicate_detector import DuplicateGroup
from scanner import FileRecord

logger = logging.getLogger(__name__)


class RemovalResult:
    """Hasil eksekusi remove per file."""
    def __init__(self):
        self.removed: List[FileRecord] = []
        self.failed: List[tuple[FileRecord, str]] = []  # (record, error_msg)
        self.skipped: List[FileRecord] = []

    @property
    def total_removed(self) -> int:
        return len(self.removed)

    @property
    def total_failed(self) -> int:
        return len(self.failed)

    @property
    def bytes_freed(self) -> int:
        return sum(r.size_bytes for r in self.removed)

    @property
    def mb_freed(self) -> float:
        return round(self.bytes_freed / (1024 * 1024), 2)


class Remover:
    """
    Menghapus atau mengarsipkan file duplikat.

    Mode:
        dry_run=True  → hanya print preview, tidak ubah apapun
        mode=archive  → pindah ke folder arsip (aman, bisa rollback)
        mode=delete   → hapus permanen (HATI-HATI!)

    Contoh penggunaan:
        remover = Remover(config)
        result = remover.execute(groups)
    """

    def __init__(self, config: dict):
        action_cfg = config.get("action", {})
        paths_cfg = config.get("paths", {})

        self.dry_run: bool = action_cfg.get("dry_run", True)
        self.mode: str = action_cfg.get("mode", "archive")
        self.confirm: bool = action_cfg.get("confirm_before_run", True)
        self.archive_folder = Path(paths_cfg.get("archive_folder", "output/duplicates_archive"))

        if self.mode not in ("archive", "delete"):
            raise ValueError(
                f"Mode tidak valid: '{self.mode}'. Pilihan: archive | delete"
            )

    # ------------------------------------------------------------------
    # Public
    # ------------------------------------------------------------------

    def preview(self, groups: List[DuplicateGroup]):
        """Tampilkan preview file yang akan dihapus tanpa eksekusi."""
        total_dupes = sum(len(g.duplicates) for g in groups)
        total_mb = sum(g.wasted_mb for g in groups)

        print("\n" + "=" * 60)
        print("PREVIEW — FILE YANG AKAN DIHAPUS/DIARSIPKAN")
        print("=" * 60)
        print(f"Total grup duplikat : {len(groups)}")
        print(f"Total file duplikat : {total_dupes}")
        print(f"Potensi storage freed: {total_mb:.2f} MB")
        print("-" * 60)

        for i, group in enumerate(groups, 1):
            print(f"\nGrup #{i} — {group.total_files} file identik")
            print(f"  ✅ KEEP   : {group.master.path_str}")
            print(f"     Ukuran : {group.master.size_kb:.1f} KB | "
                  f"Dimodifikasi: {group.master.modified_at:%Y-%m-%d %H:%M}")
            for dupe in group.duplicates:
                action = "ARCHIVE" if self.mode == "archive" else "DELETE"
                print(f"  ❌ {action} : {dupe.path_str}")

        print("\n" + "=" * 60)
        if self.dry_run:
            print("⚠️  DRY RUN AKTIF — tidak ada file yang diubah")
            print("   Set dry_run: false di config.yaml untuk eksekusi nyata")
        print("=" * 60 + "\n")

    def execute(self, groups: List[DuplicateGroup]) -> RemovalResult:
        """
        Eksekusi penghapusan/pengarsipan duplikat.

        Returns RemovalResult berisi statistik hasil eksekusi.
        """
        result = RemovalResult()

        if self.dry_run:
            logger.info("DRY RUN aktif — tidak ada file yang diubah")
            for group in groups:
                for dupe in group.duplicates:
                    result.skipped.append(dupe)
            return result

        if self.confirm:
            total = sum(len(g.duplicates) for g in groups)
            confirm = input(
                f"\n⚠️  Akan {'mengarsipkan' if self.mode == 'archive' else 'MENGHAPUS PERMANEN'} "
                f"{total} file duplikat.\nKetik 'YA' untuk lanjut: "
            )
            if confirm.strip().upper() != "YA":
                logger.info("Dibatalkan oleh user.")
                for group in groups:
                    for dupe in group.duplicates:
                        result.skipped.append(dupe)
                return result

        if self.mode == "archive":
            self.archive_folder.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Eksekusi mode '{self.mode}' pada "
            f"{sum(len(g.duplicates) for g in groups)} file..."
        )

        for group in groups:
            for dupe in group.duplicates:
                success, error = self._process_file(dupe)
                if success:
                    result.removed.append(dupe)
                else:
                    result.failed.append((dupe, error))

        logger.info(
            f"Selesai: {result.total_removed} file diproses, "
            f"{result.total_failed} gagal, "
            f"{result.mb_freed:.2f} MB dibebaskan"
        )
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _process_file(self, record: FileRecord) -> tuple[bool, str]:
        """Proses satu file: archive atau delete."""
        try:
            if self.mode == "archive":
                return self._archive_file(record)
            else:
                return self._delete_file(record)
        except Exception as e:
            return False, str(e)

    def _archive_file(self, record: FileRecord) -> tuple[bool, str]:
        """Pindahkan file ke folder arsip, jaga nama unik jika bentrok."""
        dest = self.archive_folder / record.name

        # Hindari overwrite jika nama sudah ada di archive
        if dest.exists():
            stem = record.path.stem
            suffix = record.path.suffix
            counter = 1
            while dest.exists():
                dest = self.archive_folder / f"{stem}_{counter}{suffix}"
                counter += 1

        shutil.move(str(record.path), str(dest))
        logger.debug(f"Diarsipkan: {record.name} → {dest}")
        return True, ""

    def _delete_file(self, record: FileRecord) -> tuple[bool, str]:
        """Hapus file permanen."""
        record.path.unlink()
        logger.debug(f"Dihapus: {record.path_str}")
        return True, ""