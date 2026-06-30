"""
Tests - File Monitor Collector
================================
Jalankan: pytest tests/ -v
"""

import pytest
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from engine.logger import ActivityLogger
from engine.actions import ActionDispatcher


class TestActivityLogger:
    """Test suite untuk ActivityLogger."""

    def test_log_creates_log_file(self, tmp_path):
        log_file = tmp_path / "logs" / "test.log"
        logger = ActivityLogger(log_file=str(log_file), log_to_console=False)
        logger.log("CREATED", "/tmp/test.txt", {"name": "test.txt", "extension": ".txt", "size_kb": 1.5})

        assert log_file.exists()

    def test_log_creates_csv_file(self, tmp_path):
        log_file = tmp_path / "logs" / "test.log"
        logger = ActivityLogger(log_file=str(log_file), log_to_console=False)
        logger.log("CREATED", "/tmp/test.txt", {"name": "test.txt", "extension": ".txt", "size_kb": 1.5})

        csv_file = log_file.with_suffix(".csv")
        assert csv_file.exists()

    def test_get_summary_returns_correct_counts(self, tmp_path):
        log_file = tmp_path / "logs" / "test.log"
        logger = ActivityLogger(log_file=str(log_file), log_to_console=False)

        logger.log("CREATED", "/tmp/a.txt", {"name": "a.txt", "extension": ".txt", "size_kb": 1})
        logger.log("CREATED", "/tmp/b.txt", {"name": "b.txt", "extension": ".txt", "size_kb": 2})
        logger.log("DELETED", "/tmp/c.txt", {})

        summary = logger.get_summary()
        assert summary["CREATED"] == 2
        assert summary["DELETED"] == 1
        assert summary["total_events"] == 3


class TestActionDispatcher:
    """Test suite untuk ActionDispatcher."""

    def test_auto_sort_moves_file_to_correct_folder(self, tmp_path):
        # Setup
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        output = tmp_path / "output"
        test_file = inbox / "laporan.pdf"
        test_file.write_text("dummy content")

        config = {
            "output_folder": str(output),
            "actions": {
                "auto_sort": {
                    "triggers": ["on_created"],
                    "extension_map": {".pdf": "documents/pdf"},
                }
            },
        }

        dispatcher = ActionDispatcher(config)
        dispatcher.dispatch(
            "on_created",
            str(test_file),
            {"name": "laporan.pdf", "extension": ".pdf", "size_kb": 10},
        )

        expected = output / "documents/pdf" / "laporan.pdf"
        assert expected.exists(), f"File tidak ditemukan di {expected}"
        assert not test_file.exists(), "File asli seharusnya sudah dipindah"

    def test_backup_copies_file(self, tmp_path):
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        backup = tmp_path / "backup"
        test_file = inbox / "data.csv"
        test_file.write_text("col1,col2\n1,2")

        config = {
            "output_folder": str(tmp_path / "output"),
            "actions": {
                "backup": {
                    "triggers": ["on_created"],
                    "backup_folder": str(backup),
                }
            },
        }

        dispatcher = ActionDispatcher(config)
        dispatcher.dispatch(
            "on_created",
            str(test_file),
            {"name": "data.csv", "extension": ".csv", "size_kb": 0.1},
        )

        backup_files = list(backup.glob("data_*.csv"))
        assert len(backup_files) == 1, "Harus ada tepat 1 file backup"
        assert test_file.exists(), "File asli tidak boleh terhapus saat backup"

    def test_notify_does_not_raise(self, tmp_path, capsys):
        config = {
            "output_folder": str(tmp_path / "output"),
            "actions": {
                "notify": {
                    "triggers": ["on_created"],
                    "message": "Test notifikasi!",
                }
            },
        }

        dispatcher = ActionDispatcher(config)
        # Tidak boleh raise exception
        dispatcher.dispatch(
            "on_created",
            "/tmp/test.txt",
            {"name": "test.txt", "extension": ".txt", "size_kb": 1},
        )

        captured = capsys.readouterr()
        assert "Test notifikasi!" in captured.out