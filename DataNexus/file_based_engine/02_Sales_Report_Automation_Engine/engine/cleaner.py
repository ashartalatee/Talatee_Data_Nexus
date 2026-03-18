import logging


def clean_sales(df):

    logging.info("Starting data cleaning")

    # 1. Hapus duplicate berdasarkan order_id
    before = len(df)
    df = df.drop_duplicates(subset="order_id")
    after = len(df)

    logging.info(f"Removed duplicates: {before - after}")

    # 2. Handle missing values (jika masih ada)
    missing_before = df.isnull().sum().sum()

    if missing_before > 0:
        logging.warning(f"Remaining missing values: {missing_before}")

        # contoh: isi quantity dengan 1 jika kosong
        df["quantity"] = df["quantity"].fillna(1)

        # isi price dengan median
        df["price"] = df["price"].fillna(df["price"].median())

        logging.info("Missing values handled")

    else:
        logging.info("No missing values remaining")

    logging.info("Data cleaning completed")

    return df