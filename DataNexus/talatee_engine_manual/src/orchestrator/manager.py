from src.config.loader import load_all_configs
from src.orchestrator.scheduler import should_run
from src.engine.runner import run_engine
import traceback
from datetime import datetime


def run_all_clients():
    """
    Main orchestrator:
    - Load semua config
    - Filter berdasarkan schedule
    - Jalankan engine per client
    - Handle error per client (tidak menghentikan sistem)
    - Summary hasil run
    """

    print("\n[START] Running all clients...\n")

    configs = load_all_configs()

    total = len(configs)
    executed = 0
    skipped = 0
    failed = 0

    start_time = datetime.now()

    for config in configs:
        client_name = config.get("name", "unknown_client")
        schedule = config.get("schedule", "unknown")

        print(f"\n[INFO] Processing client: {client_name} ({schedule})")

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

            # lanjut ke client berikutnya (anti crash system)
            continue

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # ========================
    # FINAL SUMMARY (INI VALUE)
    # ========================
    print("\n" + "=" * 40)
    print("[FINAL SUMMARY]")
    print(f"Total clients : {total}")
    print(f"Executed      : {executed}")
    print(f"Skipped       : {skipped}")
    print(f"Failed        : {failed}")
    print(f"Duration      : {duration:.2f} seconds")
    print("=" * 40 + "\n")