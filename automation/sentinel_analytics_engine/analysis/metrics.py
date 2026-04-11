import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

class MetricsEngine:
    """
    Calculates key performance indicators (KPIs) and business metrics 
    for e-commerce marketplace performance evaluation.
    """
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.analysis_cfg = config.get("analysis", {})
        self.metrics_list = self.analysis_cfg.get("metrics", ["gmv", "total_orders"])

    def generate_insights(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        ENTRY POINT UTAMA: Dipanggil oleh runner.py.
        Menghasilkan tabel ringkasan metrik berdasarkan granulitas yang dikonfigurasi.
        """
        if df is None or df.empty:
            self.logger.warning("Metrics calculation skipped: DataFrame is empty.")
            return None

        try:
            self.logger.info(f"Initiating business insight generation: {self.metrics_list}")
            
            # --- PROTEKSI 0: Hapus Kolom Duplikat ---
            # Menghindari ValueError: cannot assemble with duplicate keys
            df = df.loc[:, ~df.columns.duplicated()].copy()
            
            granularity = self.analysis_cfg.get("granularity", "daily").lower()
            group_cols = []

            # 1. Determine Time Grouping
            if "transaction_date" in df.columns:
                # Pastikan format datetime untuk operasi .dt
                df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors='coerce')
                
                # Buang baris dengan tanggal NaT agar grouping tidak error
                df = df.dropna(subset=['transaction_date']).copy()
                
                if granularity == "daily":
                    df["_period"] = df["transaction_date"].dt.date
                elif granularity == "weekly":
                    df["_period"] = df["transaction_date"].dt.to_period('W').apply(lambda r: r.start_time)
                elif granularity == "monthly":
                    df["_period"] = df["transaction_date"].dt.to_period('M').apply(lambda r: r.start_time)
                
                group_cols.append("_period")

            # 2. Add Platform Grouping (Shopee, Tokopedia, etc.)
            if "platform" in df.columns:
                group_cols.append("platform")

            if not group_cols:
                self.logger.warning("No grouping columns available. Calculating global totals.")
                return self._compute_aggregates(df).to_frame().T

            # 3. Perform Aggregation
            # Menggunakan include_groups=False untuk kompatibilitas pandas versi terbaru
            metrics_df = df.groupby(group_cols, as_index=False).apply(
                self._compute_aggregates, include_groups=False
            )
            
            # Cleanup internal column
            if "_period" in metrics_df.columns:
                metrics_df = metrics_df.rename(columns={"_period": "report_date"})

            self.logger.info(f"Metrics calculation complete. Generated {len(metrics_df)} summary rows.")
            return metrics_df

        except Exception as e:
            self.logger.error(f"Critical error in MetricsEngine: {str(e)}", exc_info=True)
            return None

    def _compute_aggregates(self, group: pd.DataFrame) -> pd.Series:
        """
        Internal logic to compute specific business KPIs for a data group.
        """
        results = {}

        # 1. GMV (Gross Merchandise Value)
        if "gmv" in self.metrics_list and "total_price" in group.columns:
            results["gmv"] = group["total_price"].sum()

        # 2. Total Orders
        if any(m in self.metrics_list for m in ["total_orders", "order_volume"]):
            results["total_orders"] = group["order_id"].nunique() if "order_id" in group.columns else len(group)

        # 3. Average Order Value (AOV)
        if "average_order_value" in self.metrics_list and "gmv" in results:
            total_orders = results.get("total_orders", 0)
            results["aov"] = results["gmv"] / total_orders if total_orders > 0 else 0

        # 4. SKU Performance (Top Selling SKU)
        if "sku_performance" in self.metrics_list and "sku" in group.columns:
            top_sku_series = group["sku"].mode()
            results["top_sku"] = top_sku_series.iloc[0] if not top_sku_series.empty else "N/A"
            results["unique_skus"] = group["sku"].nunique()

        # 5. Quantity Sold
        if "quantity" in group.columns:
            results["units_sold"] = group["quantity"].sum()

        return pd.Series(results)

# --- ALIAS UNTUK RUNNER ---
AnalyticsEngine = MetricsEngine