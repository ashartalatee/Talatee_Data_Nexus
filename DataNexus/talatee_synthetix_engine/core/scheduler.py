import logging
import time
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("engine_scheduler")

class SentinelScheduler:
    """
    Production-grade scheduler to orchestrate pipeline execution 
    based on client-specific intervals (daily, weekly, monthly, custom).
    """

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.main_script = Path(__file__).parent / "main.py"
        logger.info(f"Scheduler initialized. Monitoring configs in: {self.config_dir.absolute()}")

    def get_all_client_configs(self) -> List[Path]:
        """Retrieves all JSON configuration files from the config directory."""
        try:
            configs = list(self.config_dir.glob("client_*.json"))
            logger.debug(f"Found {len(configs)} client configurations.")
            return configs
        except Exception as e:
            logger.error(f"Failed to scan config directory: {e}")
            return []

    def should_run_now(self, client_config: Dict[str, Any]) -> bool:
        """
        Logic to determine if a client pipeline should execute 
        based on their 'schedule' setting and current timestamp.
        """
        schedule_type = client_config.get("schedule", "daily").lower()
        now = datetime.now()

        # Placeholder for complex scheduling logic (e.g., cron-like or persistence-based check)
        # For production-grade, this typically checks against a last_run database or state file.
        if schedule_type == "daily":
            # Example: Run every day at a specific default window if not specified
            return True 
        elif schedule_type == "weekly":
            return now.weekday() == 0  # Run on Mondays
        elif schedule_type == "monthly":
            return now.day == 1
        
        return False

    def execute_client_pipeline(self, config_path: Path):
        """Spawns a subprocess to run the main engine for a specific client."""
        client_id = config_path.stem
        logger.info(f"Triggering execution for: {client_id}")
        
        try:
            # Using subprocess to ensure memory isolation between client runs
            result = subprocess.run(
                ["python", str(self.main_script), "--config", str(config_path)],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info(f"Successfully completed pipeline for {client_id}")
            logger.debug(f"Subprocess Output: {result.stdout}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Pipeline failed for {client_id}. Exit code: {e.returncode}")
            logger.error(f"Subprocess Error: {e.stderr}")
        except Exception as e:
            logger.critical(f"Unexpected error triggering {client_id}: {e}")

    def run_forever(self, interval_seconds: int = 3600):
        """Main loop to monitor and execute schedules."""
        logger.info(f"Scheduler loop started. Polling interval: {interval_seconds}s")
        
        try:
            while True:
                configs = self.get_all_client_configs()
                
                for config_path in configs:
                    try:
                        import json
                        with open(config_path, "r") as f:
                            client_data = json.load(f)
                        
                        if self.should_run_now(client_data):
                            self.execute_client_pipeline(config_path)
                            
                    except Exception as config_err:
                        logger.error(f"Error processing config {config_path}: {config_err}")

                logger.debug(f"Sleeping for {interval_seconds}s...")
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user.")
        except Exception as e:
            logger.critical(f"Scheduler loop crashed: {e}", exc_info=True)

if __name__ == "__main__":
    scheduler = SentinelScheduler()
    # In production, this would be managed by a systemd service or Docker entrypoint
    scheduler.run_forever()