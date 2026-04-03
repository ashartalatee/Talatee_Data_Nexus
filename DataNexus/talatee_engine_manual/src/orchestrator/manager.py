from src.config.loader import load_all_configs
from src.orchestrator.scheduler import should_run
from src.engine.runner import run_engine
import traceback
from datetime import datetime


def run_all_clients(target_client=None):
    """
    Main orchestrator:
    - Load semua config
    - Optional: filter client tertentu
    - Filter berdasarkan schedule
    - Jalankan engine per client
    - Handle error per client (tidak menghentikan sistem)
    - Summary hasil run
    """

    print("\n[START] Running engine...\n")

    configs = load_all_configs()

    total = len(configs)
    executed = 0
    skipped = 0
    failed = 0
    found = False  # untuk validasi target client

    start_time = datetime.now()

    for config in configs:
        # ========================
        # FIX: gunakan client_name (bukan name)
        # ========================
        client_name = config.get("client_name", "unknown_client")
        schedule = config.get("schedule", {})

        # ========================
        # FILTER CLIENT (INI KUNCI)
        # ========================
        if target_client and client_name != target_client:
            print(f"[SKIP] {client_name} (not target)")
            continue

        found = True

        print(f"\n[INFO] Processing client: {client_name}")

        try:
            # ========================
            # CHECK SCHEDULE
            # ========================
            if not should_run(schedule):
                print(f"[SKIP] {client_name} (schedule not matched)")
                skipped += 1
                continue

            # ========================
            # RUN ENGINE
            # ========================
            run_engine(config)

            print(f"[SUCCESS] {client_name} completed")
            executed += 1

        except Exception as e:
            failed += 1

            print(f"[ERROR] {client_name} failed")
            print(f"[ERROR] {str(e)}")

            # detail error (penting buat debugging)
            traceback.print_exc()

            continue

    # ========================
    # VALIDASI TARGET CLIENT
    # ========================
    if target_client and not found:
        print(f"\n[ERROR] Client '{target_client}' tidak ditemukan!\n")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # ========================
    # FINAL SUMMARY
    # ========================
    print("\n" + "=" * 40)
    print("[FINAL SUMMARY]")
    print(f"Total configs : {total}")
    print(f"Executed      : {executed}")
    print(f"Skipped       : {skipped}")
    print(f"Failed        : {failed}")
    print(f"Duration      : {duration:.2f} seconds")
    print("=" * 40 + "\n")