def build_summary(metrics):
    """
    Build business summary dari metrics (safe + production ready)
    
    Args:
        metrics (dict): output dari calculate_metrics()

    Returns:
        dict: summary dengan key:
            - total_orders
            - valid_orders
            - total_quantity
            - total_revenue
            - avg_order_value
    """

    # ========================
    # VALIDASI INPUT
    # ========================
    if not metrics or not isinstance(metrics, dict):
        return {
            "total_orders": 0,
            "valid_orders": 0,
            "total_quantity": 0.0,
            "total_revenue": 0.0,
            "avg_order_value": 0.0,
        }

    # ========================
    # SAFE GET
    # ========================
    total_orders = int(metrics.get("total_orders", 0))
    valid_orders = int(metrics.get("valid_orders", total_orders))
    total_quantity = float(metrics.get("total_quantity", 0))
    total_revenue = float(metrics.get("total_revenue", 0))

    # ========================
    # AVG ORDER VALUE (AOV)
    # ========================
    base_orders = valid_orders if valid_orders > 0 else total_orders
    avg_order_value = total_revenue / base_orders if base_orders > 0 else 0

    # ========================
    # ROUNDING (BIAR RAPiH)
    # ========================
    total_revenue = round(total_revenue, 2)
    avg_order_value = round(avg_order_value, 2)

    # ========================
    # FINAL OUTPUT
    # ========================
    return {
        "total_orders": total_orders,
        "valid_orders": valid_orders,
        "total_quantity": total_quantity,
        "total_revenue": total_revenue,
        "avg_order_value": avg_order_value,
    }