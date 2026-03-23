def normalize(text):
    return str(text).lower().strip().replace(" ", "").replace("_", "")


def score_column(col, keywords):
    col_norm = normalize(col)
    score = 0

    for k in keywords:
        if k in col_norm:
            score += len(k)  # keyword panjang = lebih penting

    return score


def find_best_match(df, keyword_groups):
    best_col = None
    best_score = 0

    for col in df.columns:
        for group in keyword_groups:
            score = score_column(col, group)

            if score > best_score:
                best_score = score
                best_col = col

    return best_col if best_score > 0 else None


def smart_map_columns(df):
    mapping = {}

    # DATE (lebih ketat)
    mapping["date"] = find_best_match(df, [
        ["date"],
        ["created", "time"],
        ["order", "time"],
        ["tanggal"],
        ["waktu", "pesanan"]
    ])

    # PRODUCT
    mapping["product"] = find_best_match(df, [
        ["product"],
        ["name"],
        ["nama"],
        ["barang"]
    ])

    # PRICE
    mapping["price"] = find_best_match(df, [
        ["price"],
        ["harga"],
        ["unitprice"]
    ])

    # QUANTITY
    mapping["quantity"] = find_best_match(df, [
        ["quantity"],
        ["qty"],
        ["jumlah"],
        ["terjual"]
    ])

    # REVENUE
    mapping["revenue"] = find_best_match(df, [
        ["total"],
        ["amount"],
        ["revenue"],
        ["pembayaran"]
    ])

    return mapping