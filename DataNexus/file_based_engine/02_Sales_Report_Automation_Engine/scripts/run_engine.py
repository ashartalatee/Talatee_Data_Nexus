import logging
import argparse

from engine.loader import load_sales, load_products
from engine.validator import validate_sales
from engine.cleaner import clean_sales
from engine.transformer import transform_sales
from engine.merger import merge_data, aggregate_sales
from engine.reporter import generate_report
from engine.config_loader import load_config


def setup_logging(log_path):
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def run_pipeline():

    config = load_config()

    setup_logging(config["paths"]["log_file"])

    logging.info("=== STARTING SALES AUTOMATION ENGINE ===")

    try:
        # LOAD
        sales = load_sales()
        products = load_products()

        # VALIDATE
        sales = validate_sales(sales)

        # CLEAN
        sales = clean_sales(sales)

        # TRANSFORM
        sales = transform_sales(sales)

        # MERGE
        merged = merge_data(sales, products)

        # AGGREGATE
        summary = aggregate_sales(merged)

        # REPORT
        report_path = generate_report(summary)

        logging.info(f"Pipeline SUCCESS. Report: {report_path}")

        print("\n✅ PIPELINE SUCCESS")
        print(f"📊 Report generated at: {report_path}")

    except Exception as e:

        logging.error(f"Pipeline FAILED: {e}")

        print("\n❌ PIPELINE FAILED")
        print(f"Error: {e}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Sales Report Automation Engine"
    )

    parser.add_argument(
        "--run",
        action="store_true",
        help="Run the full automation pipeline"
    )

    args = parser.parse_args()

    if args.run:
        run_pipeline()
    else:
        print("Use --run to execute the pipeline")