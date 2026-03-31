# src/cleaning/text_cleaner.py

def clean_text(df):
    df["product_name"] = (
        df["product_name"]
        .astype(str)
        .str.lower()          # lowercase
        .str.strip()          # hapus spasi depan-belakang
        .str.replace(r"\s+", " ", regex=True)  # hapus spasi ganda
    )

    return df

def normalize_product_name(df):
    replacements = {
        "kaos hitam ": "kaos hitam",
        "kaos  hitam": "kaos hitam",
        "sepatu sport ": "sepatu sport"
    }

    df["product_name"] = df["product_name"].replace(replacements)

    return df