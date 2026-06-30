"""
File Watcher Engine
====================
Collect Engine #9 - Python Business Automation Engine Portfolio
Author  : [Your Name]
GitHub  : https://github.com/[username]/file-watcher-engine
License : MIT

Description:
    Monitors folders in real-time. Detects new/changed/deleted files
    and automatically triggers actions (copy, notify, log, process).
    Designed for non-IT users via GUI interface.
"""

import os
import time
import shutil
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Callable, Optional

# ── Logging Setup ──────────────────────────────────────────────────────────────

def setup_logger(log_dir: str = "logs") -> logging.Logger:
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"watcher_{datetime.now().strftime('%Y%m%d')}.log"

    logger = logging.getLogger("FileWatcherEngine")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)

    return logger


logger = setup_logger()


# ── Data Classes ───────────────────────────────────────────────────────────────

@dataclass
class WatcherConfig:
    """Configuration for a single folder watch job."""
    watch_folder: str
    output_folder: str          = "output"
    file_extensions: list       = field(default_factory=lambda: ["*"])
    action: str                 = "copy"     # copy | move | log_only | custom
    recursive: bool             = False
    check_interval_seconds: int = 5
    auto_create_output: bool    = True


@dataclass
class FileEvent:
    """Represents a detected file change event."""
    event_type: str             # CREATED | MODIFIED | DELETED
    file_path: str
    file_name: str
    file_size_kb: float
    detected_at: str
    extension: str


# ── Core Engine ────────────────────────────────────────────────────────────────

class FileWatcherEngine:
    """
    Watches a folder and reacts to file changes automatically.

    Usage (non-IT friendly via GUI, or direct Python):
        config = WatcherConfig(watch_folder="C:/Downloads", action="copy")
        engine = FileWatcherEngine(config)
        engine.start()
    """

    def __init__(self, config: WatcherConfig,
                 on_event: Optional[Callable[[FileEvent], None]] = None):
        self.config     = config
        self.on_event   = on_event      # callback for GUI updates
        self._running   = False
        self._thread    = None
        self._known     = {}            # {filepath: (size, mtime)}
        self._stats     = {"created": 0, "modified": 0, "deleted": 0, "errors": 0}

        # Validate / create folders
        watch = Path(config.watch_folder)
        if not watch.exists():
            raise FileNotFoundError(f"Watch folder not found: {watch}")

        out = Path(config.output_folder)
        if config.auto_create_output:
            out.mkdir(parents=True, exist_ok=True)

        logger.info(f"Engine initialized | watch={watch} | action={config.action}")

    # ── Public API ─────────────────────────────────────────────────────────

    def start(self):
        """Start watching in a background thread."""
        if self._running:
            logger.warning("Engine already running.")
            return
        self._running = True
        self._thread  = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()
        logger.info("▶  File Watcher started.")

    def stop(self):
        """Stop the watcher gracefully."""
        self._running = False
        logger.info("■  File Watcher stopped.")
        logger.info(f"Session stats: {self._stats}")

    def get_stats(self) -> dict:
        return dict(self._stats)

    # ── Internal Loop ──────────────────────────────────────────────────────

    def _watch_loop(self):
        # Build initial snapshot so we don't re-process existing files
        self._known = self._snapshot()
        logger.info(f"Snapshot: {len(self._known)} existing files indexed.")

        while self._running:
            time.sleep(self.config.check_interval_seconds)
            try:
                self._check()
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"Watch loop error: {e}")

    def _check(self):
        current = self._snapshot()

        # New or modified
        for path, (size, mtime) in current.items():
            if path not in self._known:
                self._handle(path, "CREATED")
            elif self._known[path] != (size, mtime):
                self._handle(path, "MODIFIED")

        # Deleted
        for path in list(self._known):
            if path not in current:
                self._handle(path, "DELETED")

        self._known = current

    def _snapshot(self) -> dict:
        """Returns {filepath_str: (size_bytes, mtime)} for watched files."""
        result = {}
        watch  = Path(self.config.watch_folder)
        exts   = self.config.file_extensions
        glob   = "**/*" if self.config.recursive else "*"

        for p in watch.glob(glob):
            if not p.is_file():
                continue
            if exts != ["*"] and p.suffix.lower().lstrip(".") not in exts:
                continue
            try:
                st = p.stat()
                result[str(p)] = (st.st_size, st.st_mtime)
            except OSError:
                pass

        return result

    def _handle(self, path: str, event_type: str):
        p = Path(path)
        ext = p.suffix.lower().lstrip(".")

        try:
            size_kb = round(p.stat().st_size / 1024, 2) if p.exists() else 0.0
        except OSError:
            size_kb = 0.0

        event = FileEvent(
            event_type  = event_type,
            file_path   = path,
            file_name   = p.name,
            file_size_kb= size_kb,
            detected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            extension   = ext,
        )

        # Log
        emoji = {"CREATED": "✅", "MODIFIED": "🔄", "DELETED": "🗑️"}.get(event_type, "❓")
        logger.info(f"{emoji} {event_type} | {p.name} | {size_kb} KB")
        self._stats[event_type.lower()] += 1

        # Execute action
        if event_type != "DELETED":
            self._execute_action(event)

        # Notify GUI/caller
        if self.on_event:
            self.on_event(event)

    def _execute_action(self, event: FileEvent):
        """Execute the configured action on a file event."""
        src  = Path(event.file_path)
        dest = Path(self.config.output_folder) / event.file_name

        action = self.config.action

        if action == "copy":
            # Avoid overwriting: append timestamp if exists
            if dest.exists():
                ts   = datetime.now().strftime("%H%M%S")
                dest = dest.with_stem(f"{dest.stem}_{ts}")
            shutil.copy2(src, dest)
            logger.info(f"  → Copied to {dest.name}")

        elif action == "move":
            shutil.move(str(src), str(dest))
            logger.info(f"  → Moved to {dest.name}")

        elif action == "log_only":
            pass  # already logged above

        elif action == "custom":
            logger.info(f"  → Custom action triggered for {event.file_name}")
            # Extend here: call your own function


# ── Standalone Run (no GUI needed) ─────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="File Watcher Engine — monitors a folder automatically."
    )
    parser.add_argument("--watch",    required=True, help="Folder to watch")
    parser.add_argument("--output",   default="output", help="Output folder")
    parser.add_argument("--action",   default="copy",
                        choices=["copy", "move", "log_only"],
                        help="Action on new/changed file")
    parser.add_argument("--ext",      default="*",
                        help="Extensions to watch, comma-separated (e.g. pdf,xlsx)")
    parser.add_argument("--interval", type=int, default=5,
                        help="Check interval in seconds")
    args = parser.parse_args()

    exts = args.ext.split(",") if args.ext != "*" else ["*"]

    config = WatcherConfig(
        watch_folder            = args.watch,
        output_folder           = args.output,
        action                  = args.action,
        file_extensions         = exts,
        check_interval_seconds  = args.interval,
    )

    engine = FileWatcherEngine(config)
    engine.start()

    print("\n📂 File Watcher Engine running. Press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        engine.stop()
        print("\nStopped. Check the logs/ folder for full activity log.")
