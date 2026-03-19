import logging
import pandas as pd


def merge_data(sales_df, products_df):

    logging.info("Starting data merge")

    merged_df = pd.merge(
        sales_df,
        products_df,
        on="product_id",
        how="left"
    )

    logging.info("Sales data merged with product data")

    # FIX DI SINI
    missing_products = merged_df["product_name"].isnull().sum()

    if missing_products > 0:
        logging.warning(f"{missing_products} products not found in product dataset")
    else:
        logging.info("All products matched successfully")

    logging.info("Data merge completed")

    return merged_df

def aggregate_sales(df):

    import logging

    logging.info("Starting data aggregation")

    # group by date dan category
    summary = (
        df
        .groupby(["date", "category"])["total_sales"]
        .sum()
        .reset_index()
    )

    logging.info("Aggregation completed")

    return summary