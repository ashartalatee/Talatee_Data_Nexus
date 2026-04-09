import pytest
import pandas as pd
from pathlib import Path
from transform.column_mapper import ColumnMapper
from transform.date_normalizer import DateNormalizer
from transform.feature_engineering import FeatureEngineer
from utils.logger import setup_logger

class TestTransform:
    """
    Unit tests for the Transformation layer of the Talatee Sentinel Engine.
    Ensures that data mapping, normalization, and feature engineering 
    produce consistent, business-ready outputs.
    """

    @pytest.fixture
    def logger(self):
        return setup_logger("test_transform_run", Path("logs/tests"))

    @pytest.fixture
    def mock_config(self):
        return {
            "client_id": "test_client",
            "transformation": {
                "column_mapping": {
                    "No. Pesanan": "order_id",
                    "Harga Total": "total_price",
                    "Waktu Pesanan": "transaction_date"
                },
                "timezone": "Asia/Jakarta",
                "feature_engineering": {
                    "calculate_margin": True,
                    "tax_rate": 0.11
                }
            }
        }

    def test_column_mapping_success(self, mock_config, logger):
        """Verifies that marketplace-specific columns are renamed to the unified schema."""
        df = pd.DataFrame({
            "No. Pesanan": ["ORD123"],
            "Harga Total": [50000],
            "Unmapped_Col": ["Random"]
        })
        mapper = ColumnMapper(mock_config, logger)
        result_df = mapper.apply(df)
        
        assert "order_id" in result_df.columns
        assert "total_price" in result_df.columns
        assert "No. Pesanan" not in result_df.columns
        assert result_df.loc[0, "order_id"] == "ORD123"

    def test_date_normalization(self, mock_config, logger):
        """Ensures string dates are converted to localized, naive datetimes."""
        df = pd.DataFrame({
            "transaction_date": ["2026-04-01 10:00:00"]
        })
        normalizer = DateNormalizer(mock_config, logger)
        result_df = normalizer.process(df)
        
        assert pd.api.types.is_datetime64_any_dtype(result_df["transaction_date"])
        # Check if localized to Jakarta (UTC+7) but returned as naive for report compatibility
        assert result_df.loc[0, "transaction_date"].hour == 10

    def test_feature_engineering_calculations(self, mock_config, logger):
        """Tests the generation of business metrics like tax and margins."""
        df = pd.DataFrame({
            "order_id": ["A1"],
            "total_price": [100000.0],
            "cogs": [70000.0],
            "transaction_date": [pd.Timestamp("2026-04-01")]
        })
        engineer = FeatureEngineer(mock_config, logger)
        result_df = engineer.transform(df)
        
        # Check tax calculation
        assert "tax_amount" in result_df.columns
        assert result_df.loc[0, "tax_amount"] == 11000.0
        
        # Check margin calculation
        assert "profit_margin_abs" in result_df.columns
        assert result_df.loc[0, "profit_margin_abs"] == 30000.0
        
        # Check temporal features
        assert "day_name" in result_df.columns
        assert result_df.loc[0, "day_name"] == "Wednesday"

    def test_transform_empty_dataframe(self, mock_config, logger):
        """Ensures transformation modules do not crash on empty inputs."""
        df = pd.DataFrame()
        engineer = FeatureEngineer(mock_config, logger)
        result_df = engineer.transform(df)
        
        assert result_df.empty

    def test_mapper_missing_columns(self, mock_config, logger):
        """Verifies mapper handles data that lacks expected source columns gracefully."""
        df = pd.DataFrame({"Unknown": [1]})
        mapper = ColumnMapper(mock_config, logger)
        result_df = mapper.apply(df)
        
        # Should return original DF without crashing
        assert "Unknown" in result_df.columns
        assert len(result_df.columns) == 1