import logging
from pathlib import Path
from typing import List

from utils.logger import setup_custom_logger
from main import SentinelAnalyticsEngine

logger = setup_custom_logger("runner")


class MultiClientRunner:
    """
    Runner to execute analytics engine for multiple clients.
    """

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self.config_paths: List[Path] = self._load_all_configs()

    def _load_all_configs(self) -> List[Path]:
        if not self.config_dir.exists() or not self.config_dir.is_dir():
            logger.error(f"Invalid config directory: {self.config_dir}")
            return []

        configs = list(self.config_dir.glob("*.json"))
        if not configs:
            logger.warning(f"No config files found in {self.config_dir}")

        logger.info(f"Found {len(configs)} client config(s)")
        return configs

    def run_all(self) -> None:
        if not self.config_paths:
            logger.error("No client configs to process.")
            return

        for config_path in self.config_paths:
            try:
                logger.info(f"Running pipeline for config: {config_path.name}")
                engine = SentinelAnalyticsEngine(config_path)
                engine.run_pipeline()
            except Exception as e:
                logger.error(
                    f"Failed to run pipeline for {config_path.name}: {e}",
                    exc_info=True
                )
                continue


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Multi-Client Runner")
    parser.add_argument(
        "--config_dir",
        type=str,
        required=True,
        help="Path to directory containing client JSON configs"
    )

    args = parser.parse_args()
    config_dir = Path(args.config_dir)

    runner = MultiClientRunner(config_dir)
    runner.run_all()


if __name__ == "__main__":
    main()