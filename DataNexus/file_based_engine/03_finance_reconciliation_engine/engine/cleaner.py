import logging
import pandas as pd

logger = logging.getLogger(__name__)


def clean_bank_data(df):
    logger.info("Cleaning bank data")

    # remove whitespace
    df["description"] = df["description"].astype(str).str.strip().str.lower()

    # convert amount
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # convert date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # handle missing amount
    missing_amount = df["amount"].isnull().sum()
    if missing_amount > 0:
        logger.warning(f"Bank data has {missing_amount} missing amounts")

    return df


def clean_invoice_data(df):
    logger.info("Cleaning invoice data")

    # normalize customer name
    df["customer"] = df["customer"].astype(str).str.strip().str.title()

    # convert amount
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # convert date
    df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

    # handle missing customer
    missing_customer = df["customer"].isnull().sum()
    if missing_customer > 0:
        logger.warning(f"Invoice data has {missing_customer} missing customer")

    return df