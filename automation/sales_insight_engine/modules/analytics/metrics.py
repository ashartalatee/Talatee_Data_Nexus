import pandas as pd
import numpy as np
from core.logger import CustomLogger
from core.schema import DataSchema

class SalesAnalytics:
    """
    Processor responsible for calculating business metrics from standardized data.
    Focuses on GMV, Volume, AOV, and performance by Category/Channel.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.logger = CustomLogger(name="SalesAnalytics").get_logger()
        self.schema = DataSchema()

    def calculate_all(self) -> dict:
        """
        Executes all metric calculations and returns a dictionary summary.
        """
        if self.df.empty:
            self.logger.warning("SalesAnalytics received empty DataFrame. Returning empty metrics.")
            return {}

        self.logger.info("Calculating comprehensive sales metrics...")
        
        # Ensure total_sales exists for calculations
        self.df['total_sales'] = self.df[self.schema.PRICE] * self.df[self.schema.QUANTITY]

        metrics = {
            "overall": self._get_overall_metrics(),
            "by_channel": self._get_grouped_metrics(self.schema.CHANNEL),
            "by_category": self._get_grouped_metrics(self.schema.CATEGORY),
            "monthly_trend": self._get_time_series_metrics(),
            "top_products": self._get_top_performers(self.schema.PRODUCT)
        }

        self.logger.info("Metrics calculation completed successfully.")
        return metrics

    def _get_overall_metrics(self) -> dict:
        """Calculates high-level KPIs."""
        total_gmv = self.df['total_sales'].sum()
        total_orders = self.df[self.schema.ORDER_ID].nunique()
        
        return {
            "total_gmv": float(total_gmv),
            "total_orders": int(total_orders),
            "total_items_sold": int(self.df[self.schema.QUANTITY].sum()),
            "average_order_value": float(total_gmv / total_orders) if total_orders > 0 else 0.0,
            "unique_customers": int(self.df[self.schema.CUSTOMER_ID].nunique())
        }

    def _get_grouped_metrics(self, group_col: str) -> pd.DataFrame:
        """Helper to group metrics by specific dimensions (Channel/Category)."""
        grouped = self.df.groupby(group_col).agg({
            self.schema.ORDER_ID: 'nunique',
            self.schema.QUANTITY: 'sum',
            'total_sales': 'sum'
        }).rename(columns={
            self.schema.ORDER_ID: 'order_count',
            self.schema.QUANTITY: 'items_sold',
            'total_sales': 'revenue'
        })
        
        # Calculate contribution percentage
        total_revenue = grouped['revenue'].sum()
        grouped['revenue_share_pct'] = (grouped['revenue'] / total_revenue) * 100 if total_revenue > 0 else 0
        
        return grouped.sort_values(by='revenue', ascending=False)

    def _get_time_series_metrics(self) -> pd.DataFrame:
        """Calculates sales trend over time (Monthly)."""
        temp_df = self.df.copy()
        temp_df['month'] = temp_df[self.schema.DATE].dt.to_period('M')
        
        monthly = temp_df.groupby('month').agg({
            'total_sales': 'sum',
            self.schema.ORDER_ID: 'nunique'
        }).rename(columns={
            'total_sales': 'monthly_revenue',
            self.schema.ORDER_ID: 'monthly_orders'
        })
        
        # Calculate Month-over-Month growth
        monthly['mom_growth_pct'] = monthly['monthly_revenue'].pct_change() * 100
        return monthly.fillna(0)

    def _get_top_performers(self, col: str, limit: int = 10) -> pd.DataFrame:
        """Identifies top N performing items."""
        return self.df.groupby(col).agg({
            'total_sales': 'sum',
            self.schema.QUANTITY: 'sum'
        }).sort_values(by='total_sales', ascending=False).head(limit)