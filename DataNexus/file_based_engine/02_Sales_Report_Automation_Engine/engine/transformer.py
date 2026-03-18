import logging
import pandas as pd


def transform_sales(df):

    logging.info("Starting data transformation")

    # pastikan numeric
    df["quantity"] = df["quantity"].astype(float)
    df["price"] = df["price"].astype(float)

    # total sales
    df["total_sales"] = df["quantity"] * df["price"]

    logging.info("Added column: total_sales")

    # =========================
    # DATE HANDLING
    # =========================

    # convert ke datetime
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    logging.info("Converted date to datetime")

    # extract fitur waktu
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day

    logging.info("Extracted year, month, day from date")

    logging.info("Data transformation completed")

    return df