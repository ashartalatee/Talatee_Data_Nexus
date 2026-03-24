import pandas as pd


# ==============================
# NORMALIZE TEXT
# ==============================
def normalize(text):
    return str(text).lower().strip().replace(" ", "").replace("_", "")


# ==============================
# DETECT NUMERIC COLUMN
# ==============================
def is_numeric_series(series):
    try:
        pd.to_numeric(series.dropna().head(10))
        return True
    except:
        return False


# ==============================
# SCORE COLUMN
# ==============================
def score_column(col, keywords):
    col_norm = normalize(col)
    score = 0

    for k in keywords:
        if k in col_norm:
            score += len(k) * 2  # 🔥 lebih tegas

    return score


# ==============================
# FIND BEST MATCH (WITH FILTER)
# ==============================
def find_best_match(df, keyword_groups, field_type=None):
    best_col = None
    best_score = 0

    for col in df.columns:
        col_series = df[col]

        # ==============================
        # FILTER KHUSUS
        # ==============================
        if field_type == "date":
            # ❌ hindari kolom numeric jadi date
            if is_numeric_series(col_series):
                continue

        if field_type in ["price", "quantity", "revenue"]:
            # ❌ hindari kolom text jadi numeric
            if not is_numeric_series(col_series):
                continue

        # ==============================
        # SCORING
        # ==============================
        for group in keyword_groups:
            score = score_column(col, group)

            if score > best_score:
                best_score = score
                best_col = col

    return best_col if best_score > 0 else None


# ==============================
# SMART MAP (FINAL)
# ==============================
def smart_map_columns(df):
    mapping = {}

    # ==============================
    # DATE (STRICT MODE)
    # ==============================
    mapping["date"] = find_best_match(df, [
        ["date"],
        ["tanggal"],
        ["createdat"],
        ["datetime"],
        ["time"]
    ], field_type="date")

    # ==============================
    # PRODUCT
    # ==============================
    mapping["product"] = find_best_match(df, [
        ["product"],
        ["productname"],
        ["nama"],
        ["barang"],
        ["item"]
    ])

    # ==============================
    # PRICE
    # ==============================
    mapping["price"] = find_best_match(df, [
        ["price"],
        ["harga"],
        ["unitprice"]
    ], field_type="numeric")

    # ==============================
    # QUANTITY
    # ==============================
    mapping["quantity"] = find_best_match(df, [
        ["quantity"],
        ["qty"],
        ["jumlah"],
        ["terjual"]
    ], field_type="numeric")

    # ==============================
    # REVENUE (PRIORITAS TINGGI)
    # ==============================
    mapping["revenue"] = find_best_match(df, [
        ["revenue"],
        ["totalrevenue"],
        ["total"],
        ["amount"],
        ["gmv"],
        ["pembayaran"]
    ], field_type="numeric")

    return mapping