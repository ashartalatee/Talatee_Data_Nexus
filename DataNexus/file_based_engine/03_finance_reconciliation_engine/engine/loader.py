import pandas as pd
import logging
from pathlib import Path

from config.settings import CONFIG
from config.errors import DataValidationError

logger = logging.getLogger(__name__)


# ================= CORE LOADER =================

def load_csv(file_path: Path, dataset_name: str):
    try:
        logger.info(f"[INGESTION] Loading {dataset_name} from {file_path}")

        df = pd.read_csv(file_path)

        if df.empty:
            logger.warning(f"{dataset_name} dataset is empty")

        logger.info(
            f"[INGESTION] {dataset_name} loaded: {df.shape[0]} rows, {df.shape[1]} columns"
        )

        return df

    except FileNotFoundError:
        logger.error(f"{dataset_name} file not found: {file_path}")
        raise DataValidationError(f"{dataset_name} file is missing")

    except pd.errors.EmptyDataError:
        logger.error(f"{dataset_name} file is empty or corrupted")
        raise DataValidationError(f"{dataset_name} file is empty")

    except Exception as e:
        logger.error(f"Unexpected error loading {dataset_name}: {e}")
        raise


# ================= BATCH MODE =================

def load_multiple_files(folder_path: Path, dataset_name: str):
    try:
        logger.info(f"[INGESTION] Loading multiple {dataset_name} files from {folder_path}")

        all_files = list(folder_path.glob("*.csv"))

        if not all_files:
            logger.error(f"No CSV files found in {folder_path}")
            raise DataValidationError(f"No files found for {dataset_name}")

        df_list = []

        for file in all_files:
            logger.info(f"Reading file: {file}")
            df = pd.read_csv(file)

            if df.empty:
                logger.warning(f"{file.name} is empty")

            df_list.append(df)

        combined_df = pd.concat(df_list, ignore_index=True)

        logger.info(
            f"[INGESTION] Combined {dataset_name}: {combined_df.shape[0]} rows"
        )

        return combined_df

    except Exception as e:
        logger.error(f"Error loading multiple {dataset_name} files: {e}")
        raise


# ================= CONFIG-DRIVEN LOADER =================

def load_data():
    """
    Fully controlled by CONFIG:
    mode = single | batch
    """

    mode = CONFIG["engine"]["mode"]

    paths = CONFIG["paths"]

    # ================= SINGLE MODE =================
    if mode == "single":
        bank_path = paths["input_dir"] / "bank_statement.csv"
        invoice_path = paths["input_dir"] / "invoice.csv"

        bank_df = load_csv(bank_path, "Bank Data")
        invoice_df = load_csv(invoice_path, "Invoice Data")

    # ================= BATCH MODE =================
    elif mode == "batch":
        bank_df = load_multiple_files(paths["bank_dir"], "Bank Data")
        invoice_df = load_multiple_files(paths["invoice_dir"], "Invoice Data")

    else:
        raise ValueError(f"Invalid engine mode: {mode}")

    return bank_df, invoice_df