"""
test_scanner.py — Unit Test untuk FileScanner
"""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from scanner import FileScanner, FileRecord


@pytest.fixture
def sample_config():
    return {
        "paths": {"input_folder": "input"},
        "scan": {
            "recursive": True,
            "include_extensions": [".txt", ".xlsx"],
            "exclude_folders": ["output", ".git"],
            "min_file_size_kb": 0,
        },
    }


@pytest.fixture
def temp_input_dir(tmp_path):
    """Buat struktur folder sementara untuk testing."""
    (tmp_path / "file_a.txt").write_text("konten file A")
    (tmp_path / "file_b.xlsx").write_bytes(b"\x00" * 1024)
    (tmp_path / "file_c.pdf").write_text("ini pdf")  # ekstensi tidak termasuk
    sub = tmp_path / "subfolder"
    sub.mkdir()
    (sub / "file_d.txt").write_text("di subfolder")
    return tmp_path


class TestFileScanner:
    def test_scan_returns_records(self, sample_config, temp_input_dir):
        sample_config["paths"]["input_folder"] = str(temp_input_dir)
        scanner = FileScanner(sample_config)
        records = scanner.scan()
        assert len(records) > 0
        assert all(isinstance(r, FileRecord) for r in records)

    def test_filter_by_extension(self, sample_config, temp_input_dir):
        sample_config["paths"]["input_folder"] = str(temp_input_dir)
        scanner = FileScanner(sample_config)
        records = scanner.scan()
        extensions = {r.extension for r in records}
        assert ".pdf" not in extensions

    def test_recursive_scan(self, sample_config, temp_input_dir):
        sample_config["paths"]["input_folder"] = str(temp_input_dir)
        scanner = FileScanner(sample_config)
        records = scanner.scan()
        # file_d.txt di subfolder harus ditemukan
        names = [r.name for r in records]
        assert "file_d.txt" in names

    def test_non_recursive_scan(self, sample_config, temp_input_dir):
        sample_config["paths"]["input_folder"] = str(temp_input_dir)
        sample_config["scan"]["recursive"] = False
        scanner = FileScanner(sample_config)
        records = scanner.scan()
        names = [r.name for r in records]
        assert "file_d.txt" not in names

    def test_raises_if_folder_missing(self, sample_config):
        sample_config["paths"]["input_folder"] = "/path/tidak/ada"
        scanner = FileScanner(sample_config)
        with pytest.raises(FileNotFoundError):
            scanner.scan()

    def test_record_has_correct_metadata(self, sample_config, temp_input_dir):
        sample_config["paths"]["input_folder"] = str(temp_input_dir)
        scanner = FileScanner(sample_config)
        records = scanner.scan()
        r = next(r for r in records if r.name == "file_a.txt")
        assert r.extension == ".txt"
        assert r.size_bytes > 0
        assert isinstance(r.modified_at, datetime)

    def test_summary_format(self, sample_config, temp_input_dir):
        sample_config["paths"]["input_folder"] = str(temp_input_dir)
        scanner = FileScanner(sample_config)
        records = scanner.scan()
        summary = scanner.summary(records)
        assert "total_files" in summary
        assert "total_size_mb" in summary
        assert "extensions_found" in summary