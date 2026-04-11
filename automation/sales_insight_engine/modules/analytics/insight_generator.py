import pandas as pd
from core.logger import CustomLogger

class InsightGenerator:
    """
    The Intelligence Layer.
    Translates raw metrics into actionable business insights and narrative findings.
    """

    def __init__(self, metrics: dict, analysis_settings: dict):
        self.metrics = metrics
        self.settings = analysis_settings
        self.logger = CustomLogger(name="InsightGenerator").get_logger()
        self.insights = []

    def generate(self) -> list:
        """
        Main entry point for insight generation.
        Returns a list of strings containing actionable insights.
        """
        if not self.metrics:
            self.logger.warning("No metrics provided. Cannot generate insights.")
            return ["No data available to generate insights."]

        self.logger.info("Generating business insights...")

        self._analyze_performance_trends()
        self._analyze_channel_dominance()
        self._analyze_product_concentration()
        self._detect_anomalies()

        return self.insights

    def _analyze_performance_trends(self):
        """Analyzes Growth and Time-series performance."""
        monthly_trend = self.metrics.get("monthly_trend")
        if monthly_trend is not None and len(monthly_trend) >= 1:
            latest_growth = monthly_trend['mom_growth_pct'].iloc[-1]
            period = monthly_trend.index[-1]
            
            if latest_growth > 0:
                self.insights.append(f"Positive Growth: Revenue increased by {latest_growth:.2f}% in {period}.")
            elif latest_growth < 0:
                self.insights.append(f"Action Required: Revenue declined by {abs(latest_growth):.2f}% in {period}. Investigate marketing spend.")

    def _analyze_channel_dominance(self):
        """Identifies which channel is the primary driver of revenue."""
        by_channel = self.metrics.get("by_channel")
        if by_channel is not None and not by_channel.empty:
            top_channel = by_channel.index[0]
            share = by_channel['revenue_share_pct'].iloc[0]
            
            self.insights.append(
                f"Channel Dominance: {top_channel} is your strongest channel, contributing {share:.2f}% of total revenue."
            )

    def _analyze_product_concentration(self):
        """Analyzes if the business relies too heavily on top products (Pareto Principle)."""
        top_products = self.metrics.get("top_products")
        overall = self.metrics.get("overall")
        
        if top_products is not None and overall:
            top_1_rev = top_products['total_sales'].iloc[0]
            total_rev = overall['total_gmv']
            concentration = (top_1_rev / total_rev) * 100
            
            if concentration > 30:
                self.insights.append(
                    f"Risk Warning: High product concentration. Your top product '{top_products.index[0]}' "
                    f"contributes {concentration:.2f}% of total sales."
                )

    def _detect_anomalies(self):
        """Identifies anomalies based on dynamic thresholds from client config."""
        threshold = self.settings.get("anomalies_threshold", 2.0)
        overall = self.metrics.get("overall")
        
        if overall:
            aov = overall['average_order_value']
            # Simple logic: If AOV is 0, something might be wrong with price data
            if aov == 0:
                self.insights.append("Critical Data Anomaly: Average Order Value is 0. Check price normalization.")
            
            # Additional logic can be added here for Z-score or standard deviation checks