import logging
import pandas as pd

logger = logging.getLogger(__name__)


REQUIRED_BANK_COLUMNS = ["date", "transaction_id", "amount", "description"]
REQUIRED_INVOICE_COLUMNS = ["invoice_id", "customer", "amount", "invoice_date"]


def validate_columns(df, required_columns, dataset_name):
    missing_cols = [col for col in required_columns if col not in df.columns]

    if missing_cols:
        logger.error(f"{dataset_name} missing columns: {missing_cols}")
        raise ValueError(f"{dataset_name} missing columns: {missing_cols}")

    logger.info(f"{dataset_name} columns validated")


def validate_missing_values(df, dataset_name):
    missing = df.isnull().sum()
    missing = missing[missing > 0]

    if not missing.empty:
        logger.warning(f"{dataset_name} has missing values:\n{missing}")

    else:
        logger.info(f"{dataset_name} has no missing values")


def validate_date_format(df, column_name, dataset_name):
    try:
        pd.to_datetime(df[column_name])
        logger.info(f"{dataset_name} {column_name} format valid")
    except Exception:
        logger.error(f"{dataset_name} {column_name} invalid date format")
        raise


def validate_bank_data(df):
    logger.info("Validating bank data")

    validate_columns(df, REQUIRED_BANK_COLUMNS, "Bank Data")
    validate_missing_values(df, "Bank Data")
    validate_date_format(df, "date", "Bank Data")


def validate_invoice_data(df):
    logger.info("Validating invoice data")

    validate_columns(df, REQUIRED_INVOICE_COLUMNS, "Invoice Data")
    validate_missing_values(df, "Invoice Data")
    validate_date_format(df, "invoice_date", "Invoice Data")