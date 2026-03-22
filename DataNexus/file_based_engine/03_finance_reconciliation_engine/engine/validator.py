import logging
import pandas as pd

from config.errors import DataValidationError

logger = logging.getLogger(__name__)


# ================= REQUIRED COLUMNS =================
REQUIRED_BANK_COLUMNS = ["date", "transaction_id", "amount", "description"]
REQUIRED_INVOICE_COLUMNS = ["invoice_id", "customer", "amount", "invoice_date"]


# ================= CORE VALIDATIONS =================

def validate_columns(df, required_columns, dataset_name):
    missing_cols = [col for col in required_columns if col not in df.columns]

    if missing_cols:
        msg = f"{dataset_name} missing columns: {missing_cols}"
        logger.error(msg)
        raise DataValidationError(msg)

    logger.info(f"{dataset_name} columns validated")


def validate_missing_values(df, dataset_name, critical_columns=None):
    missing = df.isnull().sum()
    missing = missing[missing > 0]

    if not missing.empty:
        logger.warning(f"{dataset_name} has missing values:\n{missing}")

        # jika kolom critical ada missing → error
        if critical_columns:
            for col in critical_columns:
                if col in missing:
                    msg = f"{dataset_name} critical column '{col}' has missing values"
                    logger.error(msg)
                    raise DataValidationError(msg)
    else:
        logger.info(f"{dataset_name} has no missing values")


def validate_date_format(df, column_name, dataset_name):
    try:
        pd.to_datetime(df[column_name], errors="raise")
        logger.info(f"{dataset_name} {column_name} format valid")
    except Exception:
        msg = f"{dataset_name} {column_name} invalid date format"
        logger.error(msg)
        raise DataValidationError(msg)


def validate_numeric(df, column_name, dataset_name):
    try:
        pd.to_numeric(df[column_name], errors="raise")
        logger.info(f"{dataset_name} {column_name} numeric valid")
    except Exception:
        msg = f"{dataset_name} {column_name} invalid numeric format"
        logger.error(msg)
        raise DataValidationError(msg)


# ================= DATASET LEVEL VALIDATION =================

def validate_bank_data(df):
    logger.info("===== VALIDATING BANK DATA =====")

    validate_columns(df, REQUIRED_BANK_COLUMNS, "Bank Data")

    validate_missing_values(
        df,
        "Bank Data",
        critical_columns=["date", "transaction_id", "amount"]
    )

    validate_date_format(df, "date", "Bank Data")
    validate_numeric(df, "amount", "Bank Data")

    logger.info(" Bank data validation passed")


def validate_invoice_data(df):
    logger.info("===== VALIDATING INVOICE DATA =====")

    validate_columns(df, REQUIRED_INVOICE_COLUMNS, "Invoice Data")

    validate_missing_values(
        df,
        "Invoice Data",
        critical_columns=["invoice_id", "amount"]
    )

    validate_date_format(df, "invoice_date", "Invoice Data")
    validate_numeric(df, "amount", "Invoice Data")

    logger.info(" Invoice data validation passed")