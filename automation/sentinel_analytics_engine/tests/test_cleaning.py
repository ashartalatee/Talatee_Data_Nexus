import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from cleaning.standardize import DataCleaner
from cleaning.missing_handler import MissingHandler
from cleaning.duplicate_handler import DuplicateHandler
from cleaning.text_cleaner import TextCleaner
from utils.logger import setup_logger

class TestCleaning:
    """
    Unit tests for the Data Cleaning and Standardization layer.
    Ensures that Talatee Sentinel Engine maintains high data quality standards.
    """

    @pytest.fixture
    def logger(self):
        return setup_logger("test_cleaning_run", Path("logs/tests"))

    @pytest.fixture
    def base_config(self):
        return {
            "cleaning": {
                "remove_duplicates": True,
                "duplicate_keys": ["order_id"],
                "handle_missing": {
                    "strategy": "fill",
                    "columns": {"sku": "UNKNOWN_SKU", "quantity": 0}
                },
                "text_standardization": ["product_name", "sku"]
            }
        }

    def test_duplicate_removal(self, base_config, logger):
        """Verify that duplicate orders are removed based on unique keys."""
        df = pd.DataFrame({
            "order_id": ["A1", "A1", "B2"],
            "total_price": [100, 100, 200]
        })
        handler = DuplicateHandler(base_config["cleaning"], logger)
        cleaned_df = handler.handle(df)
        
        assert len(cleaned_df) == 2
        assert cleaned_df["order_id"].is_unique

    def test_missing_value_filling(self, base_config, logger):
        """Verify that missing values are correctly filled according to mapping."""
        df = pd.DataFrame({
            "order_id": ["A1", "B2"],
            "sku": [np.nan, "PROD-001"],
            "quantity": [1, np.nan]
        })
        handler = MissingHandler(base_config["cleaning"], logger)
        cleaned_df = handler.handle(df)
        
        assert cleaned_df.loc[0, "sku"] == "UNKNOWN_SKU"
        assert cleaned_df.loc[1, "quantity"] == 0

    def test_text_standardization(self, base_config, logger):
        """Verify that text cleaning handles whitespace and casing correctly."""
        df = pd.DataFrame({
            "sku": [" prod-001  ", "PROD_002"],
            "product_name": ["  sabun cuci ", "Minyak Goreng"]
        })
        cleaner = TextCleaner(base_config["cleaning"], logger)
        cleaned_df = cleaner.clean(df, base_config["cleaning"]["text_standardization"])
        
        # SKUs should be UPPERCASE and stripped
        assert cleaned_df.loc[0, "sku"] == "PROD-001"
        # Product names should be Title Case and stripped
        assert cleaned_df.loc[0, "product_name"] == "Sabun Cuci"

    def test_orchestrated_cleaner_none_handling(self, base_config, logger):
        """Ensure the orchestrator handles None input gracefully."""
        orchestrator = DataCleaner(base_config, logger)
        result = orchestrator.process(None)
        assert result is None

    def test_missing_handler_drop_strategy(self, logger):
        """Verify the 'drop' strategy for critical missing data."""
        config = {
            "handle_missing": {
                "strategy": "drop",
                "columns": ["order_id"]
            }
        }
        df = pd.DataFrame({
            "order_id": ["A1", None, "C3"],
            "data": [1, 2, 3]
        })
        handler = MissingHandler(config, logger)
        cleaned_df = handler.handle(df)
        
        assert len(cleaned_df) == 2
        assert None not in cleaned_df["order_id"].values