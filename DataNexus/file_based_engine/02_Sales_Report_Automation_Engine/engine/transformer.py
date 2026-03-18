import logging


def transform_sales(df):

    logging.info("Starting data transformation")

    # pastikan tipe data numeric
    df["quantity"] = df["quantity"].astype(float)
    df["price"] = df["price"].astype(float)

    # buat kolom baru: total_sales
    df["total_sales"] = df["quantity"] * df["price"]

    logging.info("Added column: total_sales")

    logging.info("Data transformation completed")

    return df