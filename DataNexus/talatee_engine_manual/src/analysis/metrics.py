import pandas as pd


# =========================================
# CORE METRICS (WAJIB ADA - BIAR ENGINE JALAN)
# =========================================
def calculate_metrics(df):
    """
    Hitung metrics utama (dipakai oleh engine)
    Output: dict (bukan DataFrame)
    """

    if df is None or df.empty:
        return {
            "total_orders": 0,
            "valid_orders": 0,
            "total_quantity": 0.0,
            "total_revenue": 0.0,
        }

    # ========================
    # SAFE EXTRACTION
    # ========================
    total_orders = len(df)

    total_quantity = (
        pd.to_numeric(df.get("quantity", pd.Series()), errors="coerce")
        .fillna(0)
        .sum()
    )

    total_revenue = (
        pd.to_numeric(df.get("revenue", pd.Series()), errors="coerce")
        .fillna(0)
        .sum()
    )

    # ========================
    # VALID ORDERS
    # ========================
    if "is_valid_order" in df.columns:
        valid_orders = df[df["is_valid_order"] == True].shape[0]
    elif "status" in df.columns:
        valid_orders = df[df["status"] == "completed"].shape[0]
    else:
        valid_orders = total_orders

    return {
        "total_orders": int(total_orders),
        "valid_orders": int(valid_orders),
        "total_quantity": float(total_quantity),
        "total_revenue": float(total_revenue),
    }


# =========================================
# SUMMARY (OPTIONAL DISPLAY LAYER)
# =========================================
def generate_summary(df):
    """
    Generate summary dalam bentuk DataFrame (buat report / export)
    """

    metrics = calculate_metrics(df)

    total_orders = metrics["total_orders"]
    valid_orders = metrics["valid_orders"]
    revenue = metrics["total_revenue"]

    base_orders = valid_orders if valid_orders > 0 else total_orders
    avg_order_value = revenue / base_orders if base_orders > 0 else 0

    return pd.DataFrame([{
        **metrics,
        "avg_order_value": round(avg_order_value, 2)
    }])


# =========================================
# TOP PRODUCTS
# =========================================
def get_top_products(df, top_n=10):
    """
    Top produk berdasarkan quantity
    """

    if df is None or df.empty:
        return pd.DataFrame(columns=["product_name", "quantity", "revenue"])

    required_cols = ["product_name", "quantity", "revenue"]
    if not all(col in df.columns for col in required_cols):
        return pd.DataFrame(columns=required_cols)

    temp_df = df.copy()

    # ========================
    # SAFE NUMERIC CONVERSION
    # ========================
    temp_df["quantity"] = pd.to_numeric(temp_df["quantity"], errors="coerce").fillna(0)
    temp_df["revenue"] = pd.to_numeric(temp_df["revenue"], errors="coerce").fillna(0)

    result = (
        temp_df.groupby("product_name", as_index=False)
        .agg({"quantity": "sum", "revenue": "sum"})
        .sort_values(by="quantity", ascending=False)
        .head(top_n)
    )

    # Pastikan selalu return DataFrame walau kosong
    return result.reset_index(drop=True)


# =========================================
# LOW PERFORMING PRODUCTS
# =========================================
def get_low_performing_products(df, threshold=1):
    """
    Produk dengan performa rendah (buat insight / optimasi)
    """

    if df is None or df.empty:
        return pd.DataFrame(columns=["product_name", "quantity"])

    if "product_name" not in df.columns or "quantity" not in df.columns:
        return pd.DataFrame(columns=["product_name", "quantity"])

    temp_df = df.copy()
    temp_df["quantity"] = pd.to_numeric(temp_df["quantity"], errors="coerce").fillna(0)

    result = temp_df.groupby("product_name", as_index=False)["quantity"].sum()

    result = result[result["quantity"] <= threshold]

    return result.sort_values(by="quantity", ascending=True).reset_index(drop=True)