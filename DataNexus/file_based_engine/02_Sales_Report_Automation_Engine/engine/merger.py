import logging
import pandas as pd

def merge_data(sales_df, products_df):

    logging.info("Starting data merge")

    # merge berdasarkan product_id
    merged_df = pd.merge(
        sales_df,
        products_df,
        on="product_id",
        how="left"
    )

    logging.info("Sales data merged with product data")

    # Cek apakah ada product_id yang tidak match
    missing_products = merged_df["products_name"].isnull().sum()

    if missing_products > 0:
        logging.warning(f"{missing_products} products not found in product dataset")
    
    else:
        logging.info("All products matched successfully")

    logging.info("Data merge completed")

    return merged_df