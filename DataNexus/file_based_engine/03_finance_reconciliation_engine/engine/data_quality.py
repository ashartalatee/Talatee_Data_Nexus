import logging

logger = logging.getLogger(__name__)


# ==============================
# CORE CHECK FUNCTIONS
# ==============================

def check_empty(df, name):
    if df.empty:
        logger.error(f" ERROR: {name} is EMPTY!")
        raise ValueError(f"{name} has no data")
    return {"empty": False}


def check_missing_values(df, name):
    missing = df.isnull().sum().sum()

    if missing > 0:
        logger.warning(f" WARNING: {name} has {missing} missing values")
    else:
        logger.info(f" {name} has no missing values")

    return {"missing_values": int(missing)}


def check_duplicates(df, name):
    dup = df.duplicated().sum()

    if dup > 0:
        logger.warning(f" WARNING: {name} has {dup} duplicate rows")
    else:
        logger.info(f" {name} has no duplicates")

    return {"duplicates": int(dup)}


def check_negative_values(df, column, name):
    if column not in df.columns:
        logger.warning(f" WARNING: Column '{column}' not found in {name}")
        return {"negative_values": 0}

    negative_count = (df[column] < 0).sum()

    if negative_count > 0:
        logger.error(f" ERROR: {name} has {negative_count} negative values in '{column}'")
        raise ValueError(f"Negative values detected in {name}.{column}")

    logger.info(f" {name} has no negative values in '{column}'")
    return {"negative_values": int(negative_count)}


# ==============================
# MAIN RUNNER
# ==============================

def run_all_checks(df, name, critical_columns=None):
    """
    Run all data quality checks.

    Args:
        df (DataFrame)
        name (str): dataset name
        critical_columns (list): columns to validate (e.g. ["amount"])

    Returns:
        dict: summary of checks
    """

    logger.info(f"\n Running Data Quality Checks for: {name}")

    summary = {}

    # HARD STOP CHECK
    summary.update(check_empty(df, name))

    # WARNING CHECKS
    summary.update(check_missing_values(df, name))
    summary.update(check_duplicates(df, name))

    # CRITICAL BUSINESS RULE
    if critical_columns:
        for col in critical_columns:
            summary.update(check_negative_values(df, col, name))

    logger.info(f" Summary {name}: {summary}\n")

    return summary