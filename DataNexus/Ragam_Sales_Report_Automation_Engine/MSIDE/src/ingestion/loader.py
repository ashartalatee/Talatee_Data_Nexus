import pandas as pd
from src.utils.logger import get_logger


# ==============================
# LOAD SINGLE FILE
# ==============================
def load_single_file(path, source_name, logger):
    try:
        df = pd.read_csv(path)
        df["source"] = source_name

        # detail → masuk log file
        logger.info(f"Loaded {source_name} ({len(df)} rows)")

        return df

    except Exception as e:
        logger.error(f"Error loading {source_name}: {str(e)}", exc_info=True)
        return pd.DataFrame()


# ==============================
# LOAD ALL DATA
# ==============================
def load_all_data(config):
    logger = get_logger()

    dataframes = []
    sources = config.get("data_sources", {})

    if not sources:
        logger.warning("No data_sources found in config")
        return dataframes

    # ❗ HANYA INFO → tidak muncul di terminal
    logger.info("Loading data sources...")

    for source_name, path in sources.items():
        df = load_single_file(path, source_name, logger)

        if df.empty:
            logger.warning(f"{source_name} is empty or failed to load")

        dataframes.append(df)

    logger.info(f"Total sources loaded: {len(dataframes)}")

    return dataframes