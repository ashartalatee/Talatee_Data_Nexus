"""
test_duplicate_detector.py — Unit Test untuk HashEngine & DuplicateDetector
"""

import pytest
from pathlib import Path
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from scanner import FileRecord
from hash_engine import HashEngine
from duplicate_detector import DuplicateDetector, DuplicateGroup


def make_record(path_str, name, size=1024, days_ago=0, hash_val=None):
    """Helper buat FileRecord palsu untuk testing."""
    from datetime import timedelta
    rec = FileRecord(
        path=Path(path_str),
        name=name,
        extension=Path(name).suffix,
        size_bytes=size,
        size_kb=size / 1024,
        modified_at=datetime.now() - timedelta(days=days_ago),
        created_at=datetime.now() - timedelta(days=days_ago),
        hash_value=hash_val,
    )
    return rec


@pytest.fixture
def config_sha256():
    return {
        "detection": {
            "hash_algorithm": "sha256",
            "keep_strategy": "oldest",
            "compare_by": ["hash"],
        }
    }


class TestHashEngine:
    def test_hash_real_file(self, tmp_path, config_sha256):
        f = tmp_path / "test.txt"
        f.write_text("hello world")
        engine = HashEngine(config_sha256)
        h = engine.compute_single(f)
        assert h is not None
        assert len(h) == 64  # sha256 = 64 karakter hex

    def test_same_content_same_hash(self, tmp_path, config_sha256):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("konten yang sama persis")
        f2.write_text("konten yang sama persis")
        engine = HashEngine(config_sha256)
        assert engine.compute_single(f1) == engine.compute_single(f2)

    def test_different_content_different_hash(self, tmp_path, config_sha256):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("konten A")
        f2.write_text("konten B berbeda")
        engine = HashEngine(config_sha256)
        assert engine.compute_single(f1) != engine.compute_single(f2)

    def test_invalid_algorithm_raises(self):
        with pytest.raises(ValueError, match="Hash algorithm tidak valid"):
            HashEngine({"detection": {"hash_algorithm": "invalid"}})

    def test_compute_all_fills_hash_value(self, tmp_path, config_sha256):
        f = tmp_path / "file.txt"
        f.write_text("isi file")
        from scanner import FileScanner
        config = {
            "paths": {"input_folder": str(tmp_path)},
            "scan": {"recursive": False, "include_extensions": [], "exclude_folders": [], "min_file_size_kb": 0},
        }
        scanner = FileScanner(config)
        records = scanner.scan()
        engine = HashEngine(config_sha256)
        records = engine.compute_all(records)
        assert all(r.hash_value is not None for r in records)


class TestDuplicateDetector:
    def test_detects_hash_duplicates(self, config_sha256):
        hash_val = "abc123" * 10  # hash palsu
        r1 = make_record("input/a.txt", "a.txt", days_ago=5, hash_val=hash_val)
        r2 = make_record("input/b.txt", "b.txt", days_ago=2, hash_val=hash_val)
        r3 = make_record("input/c.txt", "c.txt", hash_val="beda456")

        detector = DuplicateDetector(config_sha256)
        groups, records = detector.detect([r1, r2, r3])

        assert len(groups) == 1
        assert groups[0].total_files == 2

    def test_keep_oldest_strategy(self, config_sha256):
        hash_val = "samehash" * 8
        older = make_record("input/old.txt", "old.txt", days_ago=10, hash_val=hash_val)
        newer = make_record("input/new.txt", "new.txt", days_ago=1, hash_val=hash_val)

        detector = DuplicateDetector(config_sha256)
        groups, _ = detector.detect([older, newer])

        assert groups[0].master.name == "old.txt"
        assert groups[0].duplicates[0].name == "new.txt"

    def test_keep_newest_strategy(self):
        config = {"detection": {"hash_algorithm": "sha256", "keep_strategy": "newest", "compare_by": ["hash"]}}
        hash_val = "samehash" * 8
        older = make_record("input/old.txt", "old.txt", days_ago=10, hash_val=hash_val)
        newer = make_record("input/new.txt", "new.txt", days_ago=1, hash_val=hash_val)

        detector = DuplicateDetector(config)
        groups, _ = detector.detect([older, newer])

        assert groups[0].master.name == "new.txt"

    def test_marks_records_correctly(self, config_sha256):
        hash_val = "hashsama" * 8
        r1 = make_record("input/a.txt", "a.txt", days_ago=5, hash_val=hash_val)
        r2 = make_record("input/b.txt", "b.txt", days_ago=1, hash_val=hash_val)

        detector = DuplicateDetector(config_sha256)
        _, records = detector.detect([r1, r2])

        masters = [r for r in records if r.keep]
        dupes = [r for r in records if r.is_duplicate]
        assert len(masters) == 1
        assert len(dupes) == 1

    def test_no_duplicates_returns_empty_groups(self, config_sha256):
        r1 = make_record("input/a.txt", "a.txt", hash_val="hash1" * 13)
        r2 = make_record("input/b.txt", "b.txt", hash_val="hash2" * 13)

        detector = DuplicateDetector(config_sha256)
        groups, _ = detector.detect([r1, r2])
        assert len(groups) == 0

    def test_invalid_strategy_raises(self):
        config = {"detection": {"hash_algorithm": "sha256", "keep_strategy": "invalid", "compare_by": ["hash"]}}
        with pytest.raises(ValueError, match="keep_strategy tidak valid"):
            DuplicateDetector(config)

    def test_wasted_bytes_calculation(self, config_sha256):
        hash_val = "samehash" * 8
        r1 = make_record("input/a.txt", "a.txt", size=5000, days_ago=5, hash_val=hash_val)
        r2 = make_record("input/b.txt", "b.txt", size=5000, days_ago=1, hash_val=hash_val)

        detector = DuplicateDetector(config_sha256)
        groups, _ = detector.detect([r1, r2])
        assert groups[0].wasted_bytes == 5000