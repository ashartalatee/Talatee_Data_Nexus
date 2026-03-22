import time
import logging
import sys
from pathlib import Path
from datetime import datetime

# Setup root path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

# Import engine & config
from scripts.run_engine import main
from config.settings import CONFIG

# Setup logging
LOG_DIR = CONFIG["paths"]["log_dir"]
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "scheduler.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_scheduler():
    interval = CONFIG["engine"]["scheduler_interval"]
    max_runs = CONFIG["engine"].get("max_runs", None)  # optional control

    print(" Auto Scheduler Started...\n")

    run_count = 0

    while True:
        run_count += 1

        print(f" Run #{run_count} | {datetime.now()}")
        logging.info(f"Run #{run_count} started")

        try:
            main()
            logging.info(f"Run #{run_count} success")

        except Exception as e:
            logging.error(f"Run #{run_count} failed: {e}")

        # Stop condition (biar aman)
        if max_runs and run_count >= max_runs:
            print(" Max runs reached. Stopping scheduler.")
            logging.info("Scheduler stopped by max_runs limit")
            break

        print(f" Waiting {interval} seconds...\n")
        time.sleep(interval)


if __name__ == "__main__":
    run_scheduler()