import pandas as pd
import os

def load_data():
    print("📥 Loading data...")

    base_path = "data/input"

    shopee_path = os.path.join(base_path, "shopee.csv")
    tokopedia_path = os.path.join(base_path, "tokopedia.csv")

    df_shopee = pd.read_csv(shopee_path)
    df_tokopedia = pd.read_csv(tokopedia_path)

    print("✅ Shopee loaded:", df_shopee.shape)
    print("✅ Tokopedia loaded:", df_tokopedia.shape)

    return df_shopee, df_tokopedia