import pandas as pd
import logging


def load_sales():

    logging.info("Loading sales data...")

    try:
        df = pd.read_csv("input/sales_data.csv")

        logging.info(f"Sales data loaded successfully. Rows: {len(df)}")

        return df

    except Exception as e:

        logging.error(f"Failed to load sales data: {e}")

        raise


def load_products():

    logging.info("Loading products data...")

    try:
        df = pd.read_csv("input/products.csv")

        logging.info(f"Products data loaded successfully. Rows: {len(df)}")

        return df

    except Exception as e:

        logging.error(f"Failed to load products data: {e}")

        raise