import logging
from typing import Dict, Any

logger = logging.getLogger("TalateeEngine")

class IntelligenceAnalyst:
    def __init__(self):
        pass

    def classify_product_grade(self, price: float, total_sold: int, run_id: str) -> str:
        """
        Menganalisis dan mengklasifikasikan tingkatan produk (Product Grading)
        berdasarkan nilai estimasi perputaran omset (Revenue Velocity Matrix).
        """
        estimated_revenue = price * total_sold
        
        logger.info(
            f"Menghitung kalkulasi Intelligence Layer untuk produk dengan estimasi omset: IDR {estimated_revenue:,}", 
            extra={"run_id": run_id}
        )

        # Matriks Klasifikasi Kelas Industri
        if estimated_revenue >= 100_000_000:
            return "Grade A (High-Velocity / Main Hero)"
        elif 50_000_000 <= estimated_revenue < 100_000_000:
            return "Grade B (Stable Performer)"
        elif 0 < estimated_revenue < 50_000_000:
            return "Grade C (Low Demand / Tail Product)"
        else:
            return "Special Category (Unverified / New Product)"