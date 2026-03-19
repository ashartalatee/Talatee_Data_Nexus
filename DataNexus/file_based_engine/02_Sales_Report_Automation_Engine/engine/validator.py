import logging
import pandas as pd
from engine.config_loader import load_config

config = load_config()

REQUIRED_COLUMNS = [
    "order_id",
    "date",
    "product_id",
    "quantity",
    "price"
]


def validate_sales(df):

    logging.info("Starting sales data validation")

    # =========================
    # CHECK REQUIRED COLUMNS
    # =========================
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing columns: {missing_columns}")

    logging.info("All required columns exist")

    # =========================
    # CHECK MISSING VALUES
    # =========================
    invalid_rows = df[df.isnull().any(axis=1)]

    if len(invalid_rows) > 0:

        logging.warning(f"{len(invalid_rows)} invalid rows detected")

        error_path = config["paths"]["error_log"]

        # simpan error rows ke config path
        invalid_rows.to_csv(error_path, index=False)

        logging.info(f"Invalid rows saved to {error_path}")

        # buang baris invalid
        df = df.dropna()

    else:
        logging.info("No invalid rows detected")

    logging.info("Validation finished")

    return df