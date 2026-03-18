from engine.loader import load_sales, load_products
from engine.validator import validate_sales
from engine.cleaner import clean_sales
import logging

logging.basicConfig(
    filename="logs/engine.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def run():

    logging.info("Starting Sales Automation Engine")

    sales = load_sales()
    products = load_products()

    sales = validate_sales(sales)
    sales = clean_sales(sales)

    print("Cleaned Sales Data:")
    print(sales)

    logging.info("Cleaning completed")


if __name__ == "__main__":
    run()