import time
import schedule
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from utils.logger import setup_logger
from utils.config_loader import load_global_config

class SentinelScheduler:
    """
    Automated Task Scheduler for Talatee Sentinel Engine.
    Manages periodic execution of the main orchestrator for multi-client processing.
    """
    def __init__(self):
        self.base_dir = Path(__file__).resolve().parent
        self.log_dir = self.base_dir / "logs"
        self.log_dir.mkdir(exist_ok=True)
        
        self.logger = setup_logger("sentinel_scheduler", self.log_dir / "scheduler.log")
        self.global_config = self.load_settings()
        
    def load_settings(self) -> Dict[str, Any]:
        """Loads scheduler interval and operational settings."""
        try:
            config = load_global_config(self.base_dir / "config" / "global_settings.json")
            return config.get("scheduler_settings", {"interval_minutes": 60, "start_time": "00:00"})
        except Exception as e:
            self.logger.error(f"Failed to load scheduler settings: {e}")
            return {"interval_minutes": 60}

    def execute_job(self):
        """Triggers the main.py orchestrator as a subprocess to ensure memory isolation."""
        self.logger.info(f"--- Job Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
        
        try:
            # Using subprocess to run main.py ensures a fresh interpreter state per run
            result = subprocess.run(
                ["python", str(self.base_dir / "main.py")],
                capture_output=True,
                text=True,
                check=True
            )
            self.logger.info("Main orchestrator execution successful.")
            if result.stdout:
                self.logger.debug(f"Orchestrator Output: {result.stdout}")
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Orchestrator failed with exit code {e.returncode}")
            self.logger.error(f"Error details: {e.stderr}")
        except Exception as e:
            self.logger.critical(f"Unexpected error during job execution: {e}")
        
        self.logger.info("--- Job Finished ---")

    def run(self):
        """Initializes the schedule loop."""
        interval = self.global_config.get("interval_minutes", 60)
        
        self.logger.info(f"Sentinel Scheduler started. Polling every {interval} minutes.")
        
        # Run once immediately on startup
        self.execute_job()

        # Schedule subsequent runs
        schedule.every(interval).minutes.do(self.execute_job)

        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.logger.warning("Scheduler shutdown requested by user.")
        except Exception as e:
            self.logger.critical(f"Scheduler crashed: {e}")

if __name__ == "__main__":
    scheduler = SentinelScheduler()
    scheduler.run()