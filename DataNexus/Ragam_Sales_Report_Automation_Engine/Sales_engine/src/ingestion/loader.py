import os
import pandas as pd
from src.utils.logger import get_logger


# ==============================
# LOAD SINGLE FILE
# ==============================
def load_single_file(path, source_name, logger):
    try:
        df = pd.read_csv(path)
        df["source"] = source_name

        logger.info(f"Loaded {source_name} ({len(df)} rows)")
        return df

    except Exception as e:
        logger.error(f"Error loading {source_name}: {str(e)}", exc_info=True)
        return pd.DataFrame()


# ==============================
# LOAD ALL DATA (FINAL VERSION)
# ==============================
def load_all_data(config):
    logger = get_logger()
    dataframes = []

    # 🔥 DETEKSI MODE
    single_file_path = config.get("single_file")

    # ==============================
    # MODE 1: SINGLE FILE
    # ==============================
    if single_file_path and os.path.exists(single_file_path):
        print("📂 MODE: SINGLE FILE DETECTED")

        df = load_single_file(single_file_path, "single_source", logger)

        if df.empty:
            logger.warning("Single file is empty or failed to load")

        return [df]

    # ==============================
    # MODE 2: MULTI SOURCE
    # ==============================
    print("📂 MODE: MULTI SOURCE")

    sources = config.get("data_sources", {})

    if not sources:
        logger.warning("No data_sources found in config")
        return dataframes

    logger.info("Loading data sources...")

    for source_name, path in sources.items():
        if os.path.exists(path):
            df = load_single_file(path, source_name, logger)

            if df.empty:
                logger.warning(f"{source_name} is empty or failed to load")

            dataframes.append(df)

        else:
            logger.warning(f"File not found: {path}")

    logger.info(f"Total sources loaded: {len(dataframes)}")

    return dataframes