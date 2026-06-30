"""
Activity Logger
===============
Catat semua aktivitas file ke console dan file log (.log & .csv).
CSV log memudahkan analisis dan bisa dibuka langsung di Excel.
"""

import csv
import logging
from pathlib import Path
from datetime import datetime


class ActivityLogger:
    """Dual logger: file log (human-readable) + CSV (untuk analisis)."""

    def __init__(self, log_file: str = "logs/activity.log", log_to_console: bool = True):
        self.log_file = Path(log_file)
        self.csv_file = self.log_file.with_suffix(".csv")
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

        # Setup logging ke file
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[
                logging.FileHandler(str(self.log_file), encoding="utf-8"),
                *(
                    [logging.StreamHandler()]
                    if log_to_console
                    else []
                ),
            ],
        )

        # Buat CSV header jika file belum ada
        if not self.csv_file.exists():
            self._write_csv_header()

    def log(self, event_type: str, path: str, file_info: dict):
        """
        Catat satu event ke log file dan CSV.

        Args:
            event_type: CREATED | MODIFIED | DELETED | MOVED | SYSTEM
            path: path file atau pesan sistem
            file_info: dict metadata file
        """
        now = datetime.now()
        ts = now.strftime("%Y-%m-%d %H:%M:%S")

        # Emoji per event type untuk readability
        icons = {
            "CREATED":  "✅",
            "MODIFIED": "✏️ ",
            "DELETED":  "🗑️ ",
            "MOVED":    "📦",
            "SYSTEM":   "⚙️ ",
        }
        icon = icons.get(event_type, "📌")

        logging.info(f"{icon} [{event_type}] {path}")

        # Tulis ke CSV
        self._write_csv_row(
            timestamp=ts,
            event=event_type,
            path=path,
            filename=file_info.get("name", ""),
            extension=file_info.get("extension", ""),
            size_kb=file_info.get("size_kb", ""),
        )

    def _write_csv_header(self):
        with open(self.csv_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "event", "path", "filename", "extension", "size_kb"])

    def _write_csv_row(self, **kwargs):
        with open(self.csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                kwargs.get("timestamp", ""),
                kwargs.get("event", ""),
                kwargs.get("path", ""),
                kwargs.get("filename", ""),
                kwargs.get("extension", ""),
                kwargs.get("size_kb", ""),
            ])

    def get_summary(self) -> dict:
        """Baca CSV dan kembalikan ringkasan statistik."""
        if not self.csv_file.exists():
            return {}

        events = {"CREATED": 0, "MODIFIED": 0, "DELETED": 0, "MOVED": 0}
        total = 0

        with open(self.csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                event = row.get("event", "")
                if event in events:
                    events[event] += 1
                    total += 1

        return {"total_events": total, **events}