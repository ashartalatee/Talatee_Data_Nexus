import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from analysis.metrics import MetricsEngine
from analysis.insight import InsightEngine
from analysis.summary import InsightGenerator
from utils.logger import setup_logger

class TestAnalysis:
    """
    Unit tests for the Analysis and Insight layer of the Talatee Sentinel Engine.
    Ensures KPIs and business intelligence logic remain accurate and actionable.
    """

    @pytest.fixture
    def logger(self):
        return setup_logger("test_analysis_run", Path("logs/tests"))

    @pytest.fixture
    def mock_config(self):
        return {
            "client_id": "test_client",
            "analysis": {
                "metrics": ["gmv", "total_orders", "average_order_value"],
                "granularity": "daily",
                "insights": {
                    "growth_threshold": 0.1
                }
            }
        }

    @pytest.fixture
    def processed_data(self):
        """Generates a standardized DataFrame for analysis testing."""
        return pd.DataFrame({
            "order_id": ["ORD1", "ORD2", "ORD3", "ORD4"],
            "transaction_date": pd.to_datetime([
                "2026-04-01 10:00:00", 
                "2026-04-01 11:00:00", 
                "2026-04-02 10:00:00",
                "2026-04-02 11:00:00"
            ]),
            "total_price": [100000.0, 150000.0, 200000.0, 300000.0],
            "platform": ["shopee", "shopee", "tokopedia", "tokopedia"],
            "sku": ["SKU-A", "SKU-B", "SKU-A", "SKU-A"],
            "quantity": [1, 1, 2, 3]
        })

    def test_metrics_aggregation_logic(self, mock_config, logger, processed_data):
        """Verifies daily GMV and Order Volume calculations."""
        engine = MetricsEngine(mock_config, logger)
        metrics_df = engine.calculate(processed_data)

        assert metrics_df is not None
        # Grouped by date and platform
        assert "gmv" in metrics_df.columns
        assert "total_orders" in metrics_df.columns
        
        # Check specific day total
        day1_gmv = metrics_df[metrics_df["report_date"] == pd.Timestamp("2026-04-01").date()]["gmv"].sum()
        assert day1_gmv == 250000.0

    def test_insight_growth_detection(self, mock_config, logger):
        """Verifies trend analysis detects growth correctly between periods."""
        metrics_df = pd.DataFrame({
            "report_date": [pd.Timestamp("2026-04-01"), pd.Timestamp("2026-04-02")],
            "gmv": [100000.0, 150000.0],
            "total_orders": [10, 15]
        })
        engine = InsightEngine(mock_config, logger)
        insights = engine.extract(pd.DataFrame(), metrics_df)

        growth_insight = next((i for i in insights if i["type"] == "trend_analysis"), None)
        assert growth_insight is not None
        assert growth_insight["value"] == 50.0  # (150-100)/100
        assert "growth" in growth_insight["label"]

    def test_platform_distribution_insight(self, mock_config, logger, processed_data):
        """Ensures platform dominance analysis identifies the correct market leader."""
        engine = MetricsEngine(mock_config, logger)
        metrics_df = engine.calculate(processed_data)
        
        gen = InsightGenerator(mock_config, logger)
        summary = gen.generate(processed_data, metrics_df)
        
        platform_mix = summary.get("platform_distribution", {})
        # Tokopedia has 500k vs Shopee 250k
        assert platform_mix.get("tokopedia") > platform_mix.get("shopee")
        assert platform_mix.get("tokopedia") == 66.67

    def test_analysis_with_empty_data(self, mock_config, logger):
        """Ensures analysis engines return None or empty structures for empty inputs."""
        empty_df = pd.DataFrame()
        metrics_engine = MetricsEngine(mock_config, logger)
        metrics_df = metrics_engine.calculate(empty_df)
        
        assert metrics_df is None

        insight_engine = InsightEngine(mock_config, logger)
        insights = insight_engine.extract(empty_df, empty_df)
        
        assert isinstance(insights, list)
        assert len(insights) == 0

    def test_pareto_product_insight(self, mock_config, logger, processed_data):
        """Tests the logic identifying SKU concentration (Pareto principle)."""
        engine = InsightEngine(mock_config, logger)
        metrics_df = MetricsEngine(mock_config, logger).calculate(processed_data)
        insights = engine.extract(processed_data, metrics_df)
        
        product_insight = next((i for i in insights if i["type"] == "inventory_insight"), None)
        assert product_insight is not None
        assert "SKU-A" in product_insight["top_sku"]