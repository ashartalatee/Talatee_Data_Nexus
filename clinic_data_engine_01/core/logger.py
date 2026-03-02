import logging
import os
from datetime import datetime


def setup_logger(run_id=None):
    os.makedirs("logs", exist_ok=True)

    if run_id is None:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    log_file = f"logs/run_{run_id}.log"

    logger = logging.getLogger("clinic_engine")
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers if re-run
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger