"""
Action Dispatcher - Auto-actions triggered by file events
==========================================================
Actions yang tersedia:
- auto_sort    : Pindahkan file ke folder berdasarkan ekstensi
- auto_rename  : Rename file dengan timestamp
- notify       : Print/log notifikasi (bisa dikembangkan ke email/WA)
- backup       : Salin file ke folder backup
"""

import shutil
import logging
from pathlib import Path
from datetime import datetime


class ActionDispatcher:
    """Dispatches configured actions when file events occur."""

    # Peta ekstensi → subfolder tujuan
    EXTENSION_MAP = {
        ".pdf":  "documents/pdf",
        ".docx": "documents/word",
        ".xlsx": "documents/excel",
        ".csv":  "documents/csv",
        ".txt":  "documents/text",
        ".png":  "images",
        ".jpg":  "images",
        ".jpeg": "images",
        ".gif":  "images",
        ".mp4":  "videos",
        ".mov":  "videos",
        ".mp3":  "audio",
        ".wav":  "audio",
        ".zip":  "archives",
        ".rar":  "archives",
        ".py":   "code",
        ".js":   "code",
        ".html": "code",
    }

    def __init__(self, config: dict):
        self.config = config
        self.actions = config.get("actions", {})
        self.output_folder = Path(config.get("output_folder", "output"))

    def dispatch(self, event_type: str, file_path: str, file_info: dict):
        """
        Jalankan semua aksi yang dikonfigurasi untuk event ini.

        Args:
            event_type: 'on_created' | 'on_modified' | 'on_deleted' | 'on_moved'
            file_path: path lengkap file
            file_info: metadata file
        """
        for action_name, action_config in self.actions.items():
            # Cek apakah aksi ini aktif untuk event ini
            triggers = action_config.get("triggers", ["on_created"])
            if event_type not in triggers:
                continue

            try:
                if action_name == "auto_sort":
                    self._auto_sort(file_path, file_info, action_config)
                elif action_name == "auto_rename":
                    self._auto_rename(file_path, file_info, action_config)
                elif action_name == "backup":
                    self._backup(file_path, file_info, action_config)
                elif action_name == "notify":
                    self._notify(file_path, file_info, action_config)
            except Exception as e:
                logging.error(f"Action '{action_name}' gagal untuk {file_path}: {e}")

    def _auto_sort(self, file_path: str, file_info: dict, config: dict):
        """Pindahkan file ke subfolder sesuai ekstensinya."""
        ext = file_info.get("extension", "")
        subfolder = config.get("extension_map", self.EXTENSION_MAP).get(ext, "others")
        target_dir = self.output_folder / subfolder
        target_dir.mkdir(parents=True, exist_ok=True)

        src = Path(file_path)
        dest = target_dir / src.name

        # Hindari overwrite: tambahkan timestamp jika file sudah ada
        if dest.exists():
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            dest = target_dir / f"{src.stem}_{ts}{src.suffix}"

        shutil.move(str(src), str(dest))
        logging.info(f"📁 AUTO-SORT: {src.name} → {subfolder}/")

    def _auto_rename(self, file_path: str, file_info: dict, config: dict):
        """Rename file dengan prefix timestamp."""
        src = Path(file_path)
        prefix = config.get("prefix", "")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{prefix}{ts}_{src.name}"
        dest = src.parent / new_name
        src.rename(dest)
        logging.info(f"✏️  AUTO-RENAME: {src.name} → {new_name}")

    def _backup(self, file_path: str, file_info: dict, config: dict):
        """Buat salinan file ke folder backup."""
        backup_dir = Path(config.get("backup_folder", "backup"))
        backup_dir.mkdir(parents=True, exist_ok=True)

        src = Path(file_path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = backup_dir / f"{src.stem}_{ts}{src.suffix}"
        shutil.copy2(str(src), str(dest))
        logging.info(f"💾 BACKUP: {src.name} → backup/")

    def _notify(self, file_path: str, file_info: dict, config: dict):
        """Kirim notifikasi (console sekarang, bisa dikembangkan ke email/WA/Slack)."""
        msg = config.get("message", "File event terdeteksi!")
        name = file_info.get("name", Path(file_path).name)
        size = file_info.get("size_kb", "?")
        print(f"\n🔔 NOTIFIKASI: {msg}")
        print(f"   File : {name}")
        print(f"   Size : {size} KB")
        print(f"   Waktu: {datetime.now().strftime('%H:%M:%S')}\n")