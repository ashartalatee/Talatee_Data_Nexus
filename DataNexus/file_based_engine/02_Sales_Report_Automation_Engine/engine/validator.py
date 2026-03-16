import logging
import pandas as pd

REQUIRED_COLUMNS = [
    "order_id",
    "date",
    "product_id",
    "quantity",
    "price"
]


def validate_sales(df):

    logging.info("Starting sales data validation")

    # cek kolom wajib
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing columns: {missing_columns}")

    logging.info("All required columns exist")

    # cek missing values
    invalid_rows = df[df.isnull().any(axis=1)]

    if len(invalid_rows) > 0:

        logging.warning(f"{len(invalid_rows)} invalid rows detected")

        invalid_rows.to_csv(
            "logs/error_rows.csv",
            index=False
        )

        logging.info("Invalid rows saved to logs/error_rows.csv")

        # buang baris invalid dari dataset
        df = df.dropna()

    else:

        logging.info("No invalid rows detected")

    logging.info("Validation finished")

    return df