import argparse
import sys
import os

# --- PATH BOOTSTRAPPING ---
# Memastikan root directory project masuk ke sys.path.
# Ini krusial agar 'import src.xxx' berfungsi di lingkungan produksi/server.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from src.utils.logger import setup_logger
from src.utils.file_handler import FileHandler
from src.pipeline.runner import PipelineRunner

def main():
    """
    Auto Data Cleaner Engine - Entry Point.
    Mengorkestrasi pembersihan data berdasarkan konfigurasi klien yang spesifik.
    """
    # 1. Setup CLI Arguments
    parser = argparse.ArgumentParser(
        description="Auto Data Cleaner Engine v1.0 - Production Ready",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--client", 
        required=True, 
        help="Target folder klien di dalam directory configs/ (contoh: client_a)"
    )
    args = parser.parse_args()

    # 2. Initialize Logger
    # Logger akan mencatat aktivitas ke console dan file logs/engine_YYYYMMDD.log
    logger = setup_logger(name="AutoCleaner")
    
    logger.info("="*60)
    logger.info(f"ENGINE START: Initiating Process for Client [{args.client.upper()}]")
    logger.info("="*60)

    try:
        # 3. Path Configuration
        # Mengarahkan ke file JSON yang mengontrol logika cleaning tanpa hardcoding
        config_path = os.path.join(BASE_DIR, "configs", args.client, "config.json")
        schema_path = os.path.join(BASE_DIR, "configs", args.client, "schema.json")

        # 4. Validation & Loading Configurations
        if not os.path.exists(config_path) or not os.path.exists(schema_path):
            logger.error(f"FATAL: Configuration or Schema files missing for: {args.client}")
            logger.error(f"Check: {config_path}")
            sys.exit(1)

        logger.info(f"Loading environment configurations...")
        config_data = FileHandler.load_json(config_path)
        schema_data = FileHandler.load_json(schema_path)

        # 5. Initialize & Execute Pipeline
        # PipelineRunner mengelola alur: Ingestion -> Rename -> Validate -> Clean -> Output
        runner = PipelineRunner(
            config=config_data,
            schema=schema_data,
            logger=logger
        )

        logger.info("Pipeline initialized. Starting execution sequence...")
        runner.run()

        logger.info("="*60)
        logger.info(f"ENGINE SUCCESS: Client [{args.client.upper()}] has been processed successfully.")
        logger.info("="*60)

    except KeyboardInterrupt:
        logger.warning("Process terminated by user (KeyboardInterrupt).")
        sys.exit(0)
    except Exception as e:
        # Mencatat detail error beserta traceback untuk debugging profesional
        logger.error(f"CRITICAL ENGINE FAILURE: {str(e)}", exc_info=True)
        logger.info("Check logs for detailed traceback.")
        sys.exit(1)

if __name__ == "__main__":
    main()