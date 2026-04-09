import sys
import logging
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

from utils.logger import setup_custom_logger
from utils.config_loader import load_global_config, load_client_config
from runner import PipelineRunner

def main() -> None:
    """
    Central Orchestrator for Talatee Sentinel Engine.
    Handles high-level initialization and iterates through client-specific pipelines.
    """
    # Initialize base paths
    BASE_DIR = Path(__file__).resolve().parent
    LOG_DIR = BASE_DIR / "logs"
    CONFIG_DIR = BASE_DIR / "config" / "clients"
    
    LOG_DIR.mkdir(exist_ok=True)

    # Setup core logger
    logger = setup_custom_logger("sentinel_core", LOG_DIR / "engine_execution.log")
    logger.info("Initializing Talatee Sentinel Engine Orchestrator")

    try:
        # 1. Load Global Settings
        global_settings = load_global_config(BASE_DIR / "config" / "global_settings.json")
        if not global_settings:
            logger.critical("Global settings missing. Shutting down engine.")
            sys.exit(1)

        # 2. Identify Active Clients
        client_configs = list(CONFIG_DIR.glob("*.json"))
        # Filter out templates
        active_clients = [c for c in client_configs if "template" not in c.name]

        if not active_clients:
            logger.warning("No active client configurations found in config/clients/")
            return

        logger.info(f"Detected {len(active_clients)} client(s) to process.")

        # 3. Pipeline Execution Loop
        for config_path in active_clients:
            client_id = config_path.stem
            logger.info(f"--- Starting Pipeline for Client: [{client_id}] ---")

            try:
                # Load specific client configuration
                config = load_client_config(config_path)
                if not config or not config.get("enabled", True):
                    logger.info(f"Skipping client [{client_id}]: Config disabled or invalid.")
                    continue

                # Initialize Runner
                runner = PipelineRunner(
                    client_id=client_id,
                    config=config,
                    global_settings=global_settings,
                    base_dir=BASE_DIR
                )

                # Execute full or partial pipeline
                success = runner.run_pipeline()
                
                if success:
                    logger.info(f"Successfully completed pipeline for [{client_id}]")
                else:
                    logger.error(f"Pipeline finished with errors for [{client_id}]")

            except Exception as e:
                logger.error(f"Critical failure during client [{client_id}] execution: {str(e)}", exc_info=True)
                # Continue to next client even if one fails
                continue

        logger.info("All client processes completed.")

    except KeyboardInterrupt:
        logger.warning("Engine shutdown triggered by user.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Uncaught engine exception: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()