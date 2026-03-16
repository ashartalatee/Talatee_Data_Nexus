from engine.loader import load_sales, load_products
from engine.validator import validate_sales
import logging

logging.basicConfig(
    filename="logs/engine.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def run():

    logging.info("Starting Sales Automation Engine")

    # Load data
    sales = load_sales()
    products = load_products()

    # Validate sales data
    sales = validate_sales(sales)

    print("Validated Sales Data:")
    print(sales)

    print("\nProducts Data:")
    print(products)

    logging.info("Data validation completed successfully")


if __name__ == "__main__":
    run()