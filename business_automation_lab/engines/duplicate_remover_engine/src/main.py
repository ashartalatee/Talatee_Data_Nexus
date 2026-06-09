"""
main.py — Duplicate Remover Engine | Orchestrator Utama
Business Automation Lab | Engine 01

Cara pakai:
    python src/main.py                        # pakai config default
    python src/main.py --dry-run              # preview tanpa eksekusi
    python src/main.py --config path/to/cfg   # config custom
    python src/main.py --preview-only         # hanya tampilkan duplikat

Pipeline:
    1. Load config.yaml
    2. Scan folder input → kumpulkan FileRecord
    3. Hitung hash setiap file (dengan cache)
    4. Deteksi & kelompokkan duplikat
    5. Preview daftar duplikat
    6. Hapus / arsipkan duplikat (jika bukan dry-run)
    7. Generate laporan Excel + log
"""

import argparse
import logging
import sys
from pathlib import Path

import yaml

# Pastikan src/ ada di path
sys.path.insert(0, str(Path(__file__).parent))

from scanner import FileScanner
from hash_engine import HashEngine
from duplicate_detector import DuplicateDetector
from remover import Remover
from report import ReportGenerator


# ── Logging setup ──────────────────────────────────────────────────────────
def setup_logging(log_folder: str = "logs", verbose: bool = False):
    log_dir = Path(log_folder)
    log_dir.mkdir(parents=True, exist_ok=True)

    from datetime import datetime
    log_file = log_dir / f"engine_{datetime.now():%Y%m%d_%H%M%S}.log"

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger("main")


# ── Config loader ───────────────────────────────────────────────────────────
def load_config(config_path: str = "config/config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        print(f"❌ Config tidak ditemukan: {path}")
        print("   Pastikan file config/config.yaml ada dan sudah diisi.")
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


# ── Banner ──────────────────────────────────────────────────────────────────
def print_banner(config: dict):
    dry_run = config.get("action", {}).get("dry_run", True)
    mode = config.get("action", {}).get("mode", "archive")

    print("\n" + "=" * 60)
    print("  DUPLICATE REMOVER ENGINE")
    print("  Business Automation Lab | Engine 01")
    print("=" * 60)
    print(f"  Input    : {config['paths']['input_folder']}")
    print(f"  Output   : {config['paths']['output_folder']}")
    print(f"  Mode     : {'⚠️  DRY RUN (preview saja)' if dry_run else f'🔴 LIVE — {mode.upper()}'}")
    print(f"  Strategy : Keep {config['detection']['keep_strategy']} file")
    print("=" * 60 + "\n")


# ── Main pipeline ───────────────────────────────────────────────────────────
def run(config: dict, preview_only: bool = False):
    logger = logging.getLogger("main")

    # ── Step 1: Scan ────────────────────────────────────────────────────
    print("🔍 Step 1/5 — Scanning folder input...")
    scanner = FileScanner(config)
    records = scanner.scan()

    if not records:
        print("  ℹ️  Tidak ada file ditemukan di folder input.")
        print("  Taruh file yang ingin diproses di folder 'input/'.")
        return

    scan_sum = scanner.summary(records)
    print(f"  ✓ {scan_sum['total_files']} file ditemukan "
          f"({scan_sum['total_size_mb']} MB total)")

    # ── Step 2: Hash ────────────────────────────────────────────────────
    print("\n🔐 Step 2/5 — Menghitung hash file...")
    hash_engine = HashEngine(config)
    records = hash_engine.compute_all(records)

    # ── Step 3: Detect ──────────────────────────────────────────────────
    print("\n🔎 Step 3/5 — Mendeteksi duplikat...")
    detector = DuplicateDetector(config)
    groups, records = detector.detect(records)

    det_sum = detector.summary(groups)
    print(f"  ✓ {det_sum['duplicate_groups']} grup duplikat ditemukan "
          f"({det_sum['total_duplicate_files']} file, "
          f"{det_sum['space_wasted_mb']} MB bisa dihemat)")

    if not groups:
        print("\n✅ Tidak ada duplikat ditemukan! Folder sudah bersih.")
        return

    # ── Step 4: Preview ─────────────────────────────────────────────────
    print("\n👁️  Step 4/5 — Preview duplikat yang ditemukan...")
    remover = Remover(config)
    remover.preview(groups)

    if preview_only:
        print("  Mode --preview-only aktif. Selesai.")
        return

    # ── Step 5: Remove ──────────────────────────────────────────────────
    print("🗑️  Step 5/5 — Eksekusi penghapusan/pengarsipan...")
    result = remover.execute(groups)

    # ── Report ──────────────────────────────────────────────────────────
    print("\n📋 Membuat laporan...")
    reporter = ReportGenerator(config)
    reporter.generate(groups, result, records)

    print("✅ Engine selesai!\n")


# ── CLI entry point ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Duplicate Remover Engine — Business Automation Lab",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Contoh penggunaan:
  python src/main.py                     # jalankan dengan config default
  python src/main.py --dry-run           # override dry_run ke True
  python src/main.py --preview-only      # hanya tampilkan duplikat
  python src/main.py --config config/config.yaml  # config custom
  python src/main.py --verbose           # output detail debug
        """,
    )
    parser.add_argument(
        "--config", default="config/config.yaml",
        help="Path ke file config.yaml (default: config/config.yaml)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Override config: jalankan dalam dry-run mode"
    )
    parser.add_argument(
        "--preview-only", action="store_true",
        help="Hanya tampilkan preview duplikat, tidak eksekusi apapun"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Tampilkan log debug detail"
    )

    args = parser.parse_args()

    config = load_config(args.config)

    # CLI override
    if args.dry_run:
        config["action"]["dry_run"] = True
        print("⚠️  --dry-run flag aktif: mode dry run digunakan")

    log_folder = config.get("paths", {}).get("log_folder", "logs")
    setup_logging(log_folder=log_folder, verbose=args.verbose)

    print_banner(config)

    try:
        run(config, preview_only=args.preview_only)
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Dihentikan oleh user.")
        sys.exit(0)
    except Exception as e:
        logging.getLogger("main").exception("Error tidak terduga:")
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()