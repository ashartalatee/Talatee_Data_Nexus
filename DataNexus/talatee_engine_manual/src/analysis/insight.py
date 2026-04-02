from typing import Dict, Any, List, Optional

def generate_insight(
    summary: Dict[str, Any],
    top_products: Optional[List[Dict[str, Any]]] = None,
    low_products: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Generate business insights (ultimate version - scalable)
    
    Args:
        summary (dict): summary hasil build_summary()
        top_products (list of dict): top produk (optional)
        low_products (list of dict): produk kurang laku (optional)
    
    Returns:
        dict: {
            "summary": summary,
            "insights": list of insights,
            "score": float 0-100
        }
    """

    # ========================
    # VALIDASI INPUT
    # ========================
    if not isinstance(summary, dict):
        return {
            "summary": {},
            "insights": ["Data tidak valid"],
            "score": 0.0
        }

    top_products = top_products or []
    low_products = low_products or []

    insights = []

    total_orders = summary.get("total_orders", 0)
    revenue = summary.get("total_revenue", 0)
    aov = summary.get("avg_order_value", 0)

    # ========================
    # CORE BUSINESS INSIGHT
    # ========================
    if total_orders == 0:
        insights.append("Belum ada order -> fokus traffic & ads")
    elif total_orders > 0 and aov < 50000:
        insights.append("AOV rendah -> coba bundling / upsell")
    elif total_orders > 0 and aov > 200000:
        insights.append("AOV tinggi -> scale ads")
    if total_orders > 50 and revenue < 1000000:
        insights.append("Order banyak tapi revenue kecil -> margin tipis")

    # ========================
    # PRODUCT INSIGHT
    # ========================
    if top_products:
        best = top_products[0]
        prod_name = best.get("product_name", "unknown")
        insights.append(f"Produk terlaris: {prod_name}")

    if low_products:
        worst = low_products[0]
        prod_name = worst.get("product_name", "unknown")
        insights.append(
            f"Produk kurang laku: {prod_name} -> pertimbangkan stop / improve"
        )

    # ========================
    # FALLBACK INSIGHT
    # ========================
    if not insights:
        insights.append("Performa stabil -> lanjutkan optimasi")

    # ========================
    # SCORING
    # ========================
    score = 0
    if total_orders > 0:
        score += 30
    if revenue > 0:
        score += 30
    if aov > 100000:
        score += 20
    if total_orders > 50:
        score += 20

    return {
        "summary": summary,
        "insights": insights,
        "score": min(score, 100)
    }