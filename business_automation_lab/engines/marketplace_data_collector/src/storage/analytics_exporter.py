import os
import json
import csv
import logging
from datetime import datetime

logger = logging.getLogger("TalateeEngine")

class AnalyticsExporter:
    def __init__(self, base_dir: str = "data"):
        self.processed_dir = os.path.join(base_dir, "processed")
        self.report_dir = os.path.join(base_dir, "reports")
        os.makedirs(self.report_dir, exist_ok=True)

    def generate_daily_executive_summary(self) -> str:
        """
        Membaca seluruh data hasil filter hari ini, menghitung total metrik agregat,
        dan mengekspornya menjadi laporan ringkasan eksekutif untuk manajemen.
        """
        total_products_audited = 0
        total_estimated_revenue = 0.0
        grade_distribution = {}

        logger.info("Memulai kompilasi laporan analitik harian...")

        # Iterasi membaca semua file bersih di folder processed
        for file_name in os.listdir(self.processed_dir):
            if file_name.endswith("_clean.json"):
                file_path = os.path.join(self.processed_dir, file_name)
                with open(file_path, "r", encoding="utf-8") as f:
                    try:
                        data = json.load(f)
                        price = data.get("price", 0.0)
                        total_sold = data.get("total_sold", 0)
                        grade = data.get("product_grade", "Unclassified")

                        total_products_audited += 1
                        total_estimated_revenue += (price * total_sold)
                        grade_distribution[grade] = grade_distribution.get(grade, 0) + 1
                    except Exception as e:
                        logger.error(f"Gagal membaca file analitik {file_name}: {str(e)}")

        # Buat objek laporan ringkasan eksekutif
        summary_report = {
            "report_generated_at": datetime.now().isoformat(),
            "metrics": {
                "total_products_audited": total_products_audited,
                "total_estimated_market_revenue": total_estimated_revenue,
                "grade_distribution": grade_distribution
            }
        }

        # Simpan laporan sebagai single-source-of-truth executive report
        timestamp = datetime.now().strftime("%Y%m%d")
        report_path = os.path.join(self.report_dir, f"executive_summary_{timestamp}.json")
        with open(report_path, "w", encoding="utf-8") as rf:
            json.dump(summary_report, rf, indent=4)

        logger.info(f"Laporan analitik harian berhasil diekspor ke {report_path}")
        return report_path