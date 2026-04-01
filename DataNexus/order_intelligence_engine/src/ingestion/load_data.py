# =========================
# 📥 DATA INGESTION MODULE (PRODUCTION READY)
# =========================

import pandas as pd
import os

# ✅ IMPORT CONFIG
from config.settings import DATA_SOURCES

# ✅ LOGGER
from utils.logger import setup_logger

logger = setup_logger("ingestion")


# =========================
# 🔄 LOAD SINGLE SOURCE
# =========================

def load_single_source(source: dict) -> pd.DataFrame:
    """
    Load data dari satu source
    Support:
    - csv
    - excel
    - csv_folder
    """

    try:
        source_type = source.get("type")
        path = source.get("path")
        name = source.get("name")
        options = source.get("options", {})

        if not os.path.exists(path):
            logger.warning(f"Path not found: {path}")
            return pd.DataFrame()

        # =========================
        # CSV FILE
        # =========================
        if source_type == "csv":
            df = pd.read_csv(
                path,
                delimiter=options.get("delimiter", ","),
                encoding=options.get("encoding", "utf-8")
            )

        # =========================
        # EXCEL FILE
        # =========================
        elif source_type == "excel":
            df = pd.read_excel(
                path,
                sheet_name=options.get("sheet_name", 0)
            )

        # =========================
        # CSV FOLDER
        # =========================
        elif source_type == "csv_folder":
            all_files = []

            for file in os.listdir(path):
                if file.endswith(".csv"):
                    full_path = os.path.join(path, file)

                    tmp = pd.read_csv(
                        full_path,
                        delimiter=options.get("delimiter", ","),
                        encoding=options.get("encoding", "utf-8")
                    )

                    tmp["source_file"] = file
                    all_files.append(tmp)

            if not all_files:
                logger.warning(f"No CSV found in folder: {path}")
                return pd.DataFrame()

            df = pd.concat(all_files, ignore_index=True)

        else:
            raise ValueError(f"Unsupported type: {source_type}")

        # =========================
        # ADD SOURCE COLUMN
        # =========================
        df["source"] = name

        logger.info(f"Loaded: {path} | Shape: {df.shape}")

        return df

    except Exception as e:
        logger.exception(f"Failed to load {source.get('path')}: {e}")
        return pd.DataFrame()


# =========================
# 🔗 LOAD ALL DATA
# =========================

def load_all_data(sources=DATA_SOURCES) -> pd.DataFrame:
    """
    Load semua data dari config
    """

    all_df = []

    for source in sources:
        df = load_single_source(source)

        if df is not None and not df.empty:
            all_df.append(df)

    if not all_df:
        logger.error("No data loaded from any source")
        return pd.DataFrame()

    combined_df = pd.concat(all_df, ignore_index=True)

    logger.info(f"Combined dataframe shape: {combined_df.shape}")

    return combined_df


# =========================
# 🧹 MISSING HANDLER (BASIC)
# =========================

def handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic missing handling
    """

    before = len(df)

    # drop rows with critical columns missing
    required_cols = ["product_name", "quantity", "price"]

    existing_cols = [col for col in required_cols if col in df.columns]

    if existing_cols:
        df = df.dropna(subset=existing_cols)

    after = len(df)

    logger.info(f"Dropped {before - after} rows due to missing values")

    # fill source
    if "source" in df.columns:
        df["source"] = df["source"].fillna("unknown")

    return df