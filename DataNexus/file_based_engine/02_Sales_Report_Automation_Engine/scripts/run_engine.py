from engine.loader import load_sales, load_products
from engine.validator import validate_sales
from engine.cleaner import clean_sales
from engine.transformer import transform_sales
from engine.merger import merge_data
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
    sales = transform_sales(sales)

    merged = merge_data(sales, products)

    print("Merged Data:")
    print(merged)

    logging.info("Merge completed")


if __name__ == "__main__":
    run()