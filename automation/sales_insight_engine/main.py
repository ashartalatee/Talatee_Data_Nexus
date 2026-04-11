import os
import json
import argparse
from core.logger import CustomLogger
from core.orchestrator import SalesOrchestrator

def load_client_configs(config_path="config/clients/"):
    """
    Memuat semua file konfigurasi JSON dari direktori klien.
    """
    configs = []
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config directory not found: {config_path}")

    for filename in os.listdir(config_path):
        if filename.endswith(".json"):
            with open(os.path.join(config_path, filename), 'r') as f:
                try:
                    config_data = json.load(f)
                    configs.append(config_data)
                except json.JSONDecodeError as e:
                    print(f"Error parsing {filename}: {e}")
    return configs

def main():
    # Setup CLI Arguments
    parser = argparse.ArgumentParser(description="Sales Data Intelligence Engine")
    parser.add_argument("--client", type=str, help="Specific Client ID to process (optional)")
    args = parser.parse_args()

    # Initialize Logger
    logger = CustomLogger(name="MainEngine").get_logger()
    logger.info("Starting Sales Data Intelligence Engine...")

    try:
        # 1. Load Configurations
        client_configs = load_client_configs()
        
        if not client_configs:
            logger.warning("No client configurations found. Exiting.")
            return

        # 2. Filter Client if specified in CLI
        if args.client:
            client_configs = [c for c in client_configs if c.get("client_id") == args.client]
            if not client_configs:
                logger.error(f"Client ID '{args.client}' not found in configs.")
                return

        # 3. Initialize Orchestrator
        engine = SalesOrchestrator()

        # 4. Execute Processing for each client
        for config in client_configs:
            client_name = config.get("client_name", "Unknown")
            client_id = config.get("client_id", "Unknown")
            
            logger.info(f"Processing Client: {client_name} [{client_id}]")
            
            try:
                engine.run_pipeline(config)
                logger.info(f"Successfully finished processing for {client_name}")
            except Exception as e:
                logger.error(f"Failed to process client {client_name}: {str(e)}", exc_info=True)

    except Exception as e:
        logger.critical(f"System failure: {str(e)}", exc_info=True)
    finally:
        logger.info("Engine execution finished.")

if __name__ == "__main__":
    main()