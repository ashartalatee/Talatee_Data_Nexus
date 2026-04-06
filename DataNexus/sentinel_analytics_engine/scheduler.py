import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import json

from utils.logger import setup_custom_logger
from main import SentinelAnalyticsEngine

logger = setup_custom_logger("scheduler")


class Scheduler:
    """
    Simple scheduler for running multi-client pipelines based on config schedules.
    Supports: daily, weekly, monthly
    """

    def __init__(self, config_dir: Path, poll_interval: int = 60):
        self.config_dir = config_dir
        self.poll_interval = poll_interval
        self.last_run_map: Dict[str, str] = {}

    def _load_configs(self) -> List[Path]:
        if not self.config_dir.exists():
            logger.error(f"Config directory not found: {self.config_dir}")
            return []
        return list(self.config_dir.glob("*.json"))

    def _read_config(self, path: Path) -> Dict[str, Any]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to read config {path}: {e}")
            return {}

    def _should_run(self, client_id: str, schedule_cfg: Dict[str, Any]) -> bool:
        now = datetime.now()

        schedule_type = schedule_cfg.get("type", "daily")

        last_run = self.last_run_map.get(client_id)

        if schedule_type == "daily":
            today = now.strftime("%Y-%m-%d")
            return last_run != today

        elif schedule_type == "weekly":
            weekday = schedule_cfg.get("day", now.weekday())  # 0=Monday
            today = now.strftime("%Y-%W")
            return now.weekday() == weekday and last_run != today

        elif schedule_type == "monthly":
            day = schedule_cfg.get("day", now.day)
            today = now.strftime("%Y-%m")
            return now.day == day and last_run != today

        return False

    def _mark_run(self, client_id: str, schedule_cfg: Dict[str, Any]) -> None:
        now = datetime.now()
        schedule_type = schedule_cfg.get("type", "daily")

        if schedule_type == "daily":
            self.last_run_map[client_id] = now.strftime("%Y-%m-%d")
        elif schedule_type == "weekly":
            self.last_run_map[client_id] = now.strftime("%Y-%W")
        elif schedule_type == "monthly":
            self.last_run_map[client_id] = now.strftime("%Y-%m")

    def run(self) -> None:
        logger.info("Scheduler started")

        while True:
            try:
                configs = self._load_configs()

                for config_path in configs:
                    config = self._read_config(config_path)
                    if not config:
                        continue

                    client_id = config.get("client_id", config_path.stem)
                    schedule_cfg = config.get("schedule", {"type": "daily"})

                    try:
                        if self._should_run(client_id, schedule_cfg):
                            logger.info(f"Running scheduled job for {client_id}")
                            engine = SentinelAnalyticsEngine(config_path)
                            engine.run_pipeline()
                            self._mark_run(client_id, schedule_cfg)
                        else:
                            logger.debug(f"Skipping {client_id}, not scheduled now")
                    except Exception as e:
                        logger.error(
                            f"Error executing pipeline for {client_id}: {e}",
                            exc_info=True
                        )
                        continue

                time.sleep(self.poll_interval)

            except Exception as e:
                logger.critical(f"Scheduler failure: {e}", exc_info=True)
                time.sleep(self.poll_interval)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Scheduler for Sentinel Engine")
    parser.add_argument(
        "--config_dir",
        type=str,
        required=True,
        help="Path to client config directory"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Polling interval in seconds"
    )

    args = parser.parse_args()
    config_dir = Path(args.config_dir)

    scheduler = Scheduler(config_dir=config_dir, poll_interval=args.interval)
    scheduler.run()


if __name__ == "__main__":
    main()