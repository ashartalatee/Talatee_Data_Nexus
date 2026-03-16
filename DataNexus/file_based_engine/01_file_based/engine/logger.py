import logging
from pathlib import Path
from config.settings import LOG_DIR

def setup_logger():

    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

    log_file = Path(LOG_DIR) / "engine.log"

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    return logging.getLogger()