"""
File Monitor Engine - Core Module
==================================
Real-time file system monitoring using watchdog.
Detects: file created, modified, deleted, moved.
"""

import time
import logging
from pathlib import Path
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from engine.logger import ActivityLogger
from engine.actions import ActionDispatcher


class FileMonitorHandler(FileSystemEventHandler):
    """Handles file system events and dispatches actions."""

    def __init__(self, config: dict, logger: ActivityLogger, dispatcher: ActionDispatcher):
        super().__init__()
        self.config = config
        self.logger = logger
        self.dispatcher = dispatcher
        self.ignored_patterns = config.get("ignored_patterns", [".tmp", ".DS_Store", "~$"])

    def _is_ignored(self, path: str) -> bool:
        """Check if file matches ignored patterns."""
        return any(pattern in path for pattern in self.ignored_patterns)

    def on_created(self, event):
        if event.is_directory or self._is_ignored(event.src_path):
            return
        file_info = self._get_file_info(event.src_path)
        self.logger.log("CREATED", event.src_path, file_info)
        self.dispatcher.dispatch("on_created", event.src_path, file_info)

    def on_modified(self, event):
        if event.is_directory or self._is_ignored(event.src_path):
            return
        file_info = self._get_file_info(event.src_path)
        self.logger.log("MODIFIED", event.src_path, file_info)
        self.dispatcher.dispatch("on_modified", event.src_path, file_info)

    def on_deleted(self, event):
        if event.is_directory or self._is_ignored(event.src_path):
            return
        self.logger.log("DELETED", event.src_path, {})
        self.dispatcher.dispatch("on_deleted", event.src_path, {})

    def on_moved(self, event):
        if event.is_directory:
            return
        file_info = self._get_file_info(event.dest_path)
        self.logger.log("MOVED", f"{event.src_path} → {event.dest_path}", file_info)
        self.dispatcher.dispatch("on_moved", event.dest_path, file_info)

    def _get_file_info(self, path: str) -> dict:
        """Extract metadata from file."""
        try:
            p = Path(path)
            stat = p.stat()
            return {
                "name": p.name,
                "extension": p.suffix.lower(),
                "size_bytes": stat.st_size,
                "size_kb": round(stat.st_size / 1024, 2),
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        except Exception:
            return {}


class FileMonitorEngine:
    """
    Main engine class. Orchestrates monitoring of one or more folders.

    Usage:
        engine = FileMonitorEngine(config)
        engine.start()
    """

    def __init__(self, config: dict):
        self.config = config
        self.watch_folders = config.get("watch_folders", [])
        self.recursive = config.get("recursive", True)
        self.observer = Observer()

        self.logger = ActivityLogger(
            log_file=config.get("log_file", "logs/activity.log"),
            log_to_console=config.get("log_to_console", True),
        )
        self.dispatcher = ActionDispatcher(config)
        self.handler = FileMonitorHandler(config, self.logger, self.dispatcher)

    def start(self):
        """Start monitoring all configured folders."""
        if not self.watch_folders:
            raise ValueError("No watch_folders configured in settings.")

        for folder in self.watch_folders:
            path = Path(folder)
            if not path.exists():
                logging.warning(f"⚠️  Folder tidak ditemukan, dibuat otomatis: {folder}")
                path.mkdir(parents=True, exist_ok=True)

            self.observer.schedule(self.handler, str(path), recursive=self.recursive)
            self.logger.log("SYSTEM", f"Monitoring aktif: {folder}", {})

        self.observer.start()
        print("\n🚀 File Monitor Engine AKTIF")
        print(f"   Folder: {', '.join(self.watch_folders)}")
        print("   Tekan Ctrl+C untuk berhenti.\n")

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        """Gracefully stop the observer."""
        self.observer.stop()
        self.observer.join()
        self.logger.log("SYSTEM", "Engine dihentikan.", {})
        print("\n✅ File Monitor Engine dihentikan.")