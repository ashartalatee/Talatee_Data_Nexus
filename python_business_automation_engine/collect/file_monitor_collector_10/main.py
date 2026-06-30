#!/usr/bin/env python3
"""
File Monitor Collector - Entry Point
=====================================
Python Business Automation Engine #10
Level 1: Data Collection Engine

Author  : [Nama Kamu]
GitHub  : https://github.com/[username]/file-monitor-collector
LinkedIn: https://linkedin.com/in/[username]

Usage:
    python main.py                        # Gunakan config default
    python main.py --config custom.yaml   # Gunakan config custom
    python main.py --watch ./inbox        # Override folder dari CLI
    python main.py --summary              # Tampilkan ringkasan log
"""

import argparse
import sys
from pathlib import Path

import yaml

from engine.monitor import FileMonitorEngine
from engine.logger import ActivityLogger


def load_config(config_path: str) -> dict:
    """Load konfigurasi dari file YAML."""
    path = Path(config_path)
    if not path.exists():
        print(f"❌ Config tidak ditemukan: {config_path}")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def print_banner():
    banner = """
╔══════════════════════════════════════════════════╗
║       FILE MONITOR COLLECTOR  v1.0.0             ║
║       Python Business Automation Engine #10      ║
║       Level 1: Data Collection Engine            ║
╚══════════════════════════════════════════════════╝
"""
    print(banner)


def print_summary(config: dict):
    """Tampilkan ringkasan aktivitas dari CSV log."""
    logger = ActivityLogger(log_file=config.get("log_file", "logs/activity.log"), log_to_console=False)
    summary = logger.get_summary()

    if not summary:
        print("📭 Belum ada aktivitas yang tercatat.")
        return

    print("\n📊 RINGKASAN AKTIVITAS LOG")
    print("=" * 35)
    print(f"  Total Events : {summary.get('total_events', 0)}")
    print(f"  ✅ Created   : {summary.get('CREATED', 0)}")
    print(f"  ✏️  Modified  : {summary.get('MODIFIED', 0)}")
    print(f"  🗑️  Deleted   : {summary.get('DELETED', 0)}")
    print(f"  📦 Moved     : {summary.get('MOVED', 0)}")
    print("=" * 35)
    log_file = Path(config.get("log_file", "logs/activity.log"))
    csv_file = log_file.with_suffix(".csv")
    print(f"\n📂 Log tersimpan di: {log_file}")
    print(f"📊 CSV tersimpan di : {csv_file}\n")


def main():
    print_banner()

    parser = argparse.ArgumentParser(
        description="File Monitor Collector - Python Business Automation Engine"
    )
    parser.add_argument(
        "--config",
        default="config/settings.yaml",
        help="Path ke file konfigurasi YAML (default: config/settings.yaml)",
    )
    parser.add_argument(
        "--watch",
        nargs="+",
        help="Override folder yang dimonitor (pisahkan dengan spasi)",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Tampilkan ringkasan log tanpa menjalankan monitor",
    )

    args = parser.parse_args()
    config = load_config(args.config)

    # Override watch_folders dari CLI jika disediakan
    if args.watch:
        config["watch_folders"] = args.watch
        print(f"📌 Override folder: {args.watch}")

    if args.summary:
        print_summary(config)
        return

    # Jalankan engine
    engine = FileMonitorEngine(config)
    engine.start()


if __name__ == "__main__":
    main()