import logging
from config.settings import CONFIG

logger = logging.getLogger(__name__)


def generate_report(df):
    logger.info("Generating reconciliation report")

    # Urutkan data biar rapi & konsisten
    df = df.sort_values(by=["status", "customer"])

    return df


def save_report(df):
    try:
        output_dir = CONFIG["paths"]["output_dir"]
        file_name = CONFIG["files"]["output_file"]

        # Pastikan folder ada
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / file_name

        df.to_csv(output_path, index=False)

        logger.info(f"Report saved to {output_path}")

    except Exception as e:
        logger.error(f"Failed to save report: {e}")
        raise


def generate_summary(df):
    try:
        summary = df["status"].value_counts()

        logger.info(f"\nReconciliation Summary:\n{summary}")

        print("\n=== RECONCILIATION SUMMARY ===")
        print(summary)

        return summary

    except Exception as e:
        logger.error(f"Failed to generate summary: {e}")
        raise