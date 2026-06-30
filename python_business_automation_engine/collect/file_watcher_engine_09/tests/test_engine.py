"""
Tests for File Watcher Engine
"""

import os
import sys
import time
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from engine import FileWatcherEngine, WatcherConfig, FileEvent


class TestFileWatcherEngine(unittest.TestCase):

    def setUp(self):
        """Create temp folders for each test."""
        self.watch_dir  = tempfile.mkdtemp(prefix="fw_watch_")
        self.output_dir = tempfile.mkdtemp(prefix="fw_output_")
        self.events: list[FileEvent] = []

    def _make_config(self, action="copy", exts=None):
        return WatcherConfig(
            watch_folder           = self.watch_dir,
            output_folder          = self.output_dir,
            action                 = action,
            file_extensions        = exts or ["*"],
            check_interval_seconds = 1,
        )

    def _on_event(self, event: FileEvent):
        self.events.append(event)

    def test_detects_new_file(self):
        """Engine should detect a newly created file."""
        config = self._make_config(action="log_only")
        engine = FileWatcherEngine(config, on_event=self._on_event)
        engine.start()
        time.sleep(1.5)  # let snapshot settle

        # Create a file
        (Path(self.watch_dir) / "test.txt").write_text("hello")
        time.sleep(2)  # wait for detection

        engine.stop()
        self.assertTrue(any(e.event_type == "CREATED" for e in self.events),
                        "CREATED event not detected")

    def test_copy_action(self):
        """Engine should copy detected files to output folder."""
        config = self._make_config(action="copy")
        engine = FileWatcherEngine(config, on_event=self._on_event)
        engine.start()
        time.sleep(1.5)

        fname = "copy_test.txt"
        (Path(self.watch_dir) / fname).write_text("copy me")
        time.sleep(2)

        engine.stop()
        copied = Path(self.output_dir) / fname
        self.assertTrue(copied.exists(), f"File not copied to output: {copied}")

    def test_extension_filter(self):
        """Engine should ignore files with unwatched extensions."""
        config = self._make_config(action="log_only", exts=["pdf"])
        engine = FileWatcherEngine(config, on_event=self._on_event)
        engine.start()
        time.sleep(1.5)

        (Path(self.watch_dir) / "ignore_me.txt").write_text("should be ignored")
        (Path(self.watch_dir) / "watch_me.pdf").write_text("should be watched")
        time.sleep(2)

        engine.stop()
        detected_names = [e.file_name for e in self.events]
        self.assertNotIn("ignore_me.txt", detected_names)
        self.assertIn("watch_me.pdf", detected_names)

    def test_invalid_watch_folder_raises(self):
        """Engine should raise FileNotFoundError for missing folder."""
        config = WatcherConfig(watch_folder="/nonexistent/path/xyz")
        with self.assertRaises(FileNotFoundError):
            FileWatcherEngine(config)

    def test_stats_tracking(self):
        """Stats should increment correctly."""
        config = self._make_config(action="log_only")
        engine = FileWatcherEngine(config, on_event=self._on_event)
        engine.start()
        time.sleep(1.5)

        (Path(self.watch_dir) / "a.txt").write_text("a")
        (Path(self.watch_dir) / "b.txt").write_text("b")
        time.sleep(2)

        engine.stop()
        stats = engine.get_stats()
        self.assertGreaterEqual(stats["created"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
