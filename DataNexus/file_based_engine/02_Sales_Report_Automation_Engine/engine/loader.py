from engine.config_loader import load_config
import pandas as pd
import logging

config = load_config()


def load_sales():

    path = config["paths"]["sales_data"]

    logging.info(f"Loading sales data from {path}")

    return pd.read_csv(path)


def load_products():

    path = config["paths"]["products_data"]

    logging.info(f"Loading products data from {path}")

    return pd.read_csv(path)