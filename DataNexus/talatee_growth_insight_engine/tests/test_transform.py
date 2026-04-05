"""
Unit tests for transform modules.
Tests column mapping, date normalization, and feature engineering.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from unittest.mock import patch, MagicMock

from transform import map_columns_to_standard, normalize_date_columns, transform_features
from transform.column_mapper import auto_mapping_summary
from transform.date_normalizer import date_range_summary
from utils.config_loader import load_client_config


@pytest.fixture
def raw_transform_data():
    """Raw data for transform testing."""
    return pd.DataFrame({
        'transaction_date': ['2024-01-15', '2024/01/16', '15-01-2024', 'invalid'],
        'item_sku': ['SKU001', 'SKU002', 'SKU001', 'SKU003'],
        'item_name': ['Wireless Earbuds Pro', 'Smartphone Case', 'Power Bank', 'Case HP'],
        'unit_price': [250000, 45000, 150000, 35000],
        'qty': [2, 1, 1, 3],
        'sub_total': [500000, 45000, 150000, 105000],
        'source': ['Shopee', 'Tokopedia', 'Shopee', 'WhatsApp']
    })


@pytest.fixture
def transform_config():
    """Configuration for transform testing."""
    return {
        "client_id": "test_transform",
        "standard_columns": {
            "date": "transaction_date",
            "product_name": "item_name",
            "product_id": "item_sku",
            "price": "unit_price",
            "quantity": "qty",
            "total_amount": "sub_total",
            "platform": "source"
        },
        "feature_engineering": {
            "date_features": ["day_of_week", "month"],
            "rolling_windows": [7, 30]
        }
    }


def test_map_columns_to_standard(raw_transform_data, transform_config):
    """Test column standardization."""
    
    result = map_columns_to_standard(raw_transform_data, transform_config)
    
    assert result is not None
    assert not result.empty
    
    # Verify standard columns exist
    standard_cols = ['order_date', 'product_name', 'product_id', 'price', 
                    'quantity', 'total_amount', 'platform']
    for col in standard_cols:
        assert col in result.columns
    
    # Verify data preservation
    assert len(result) == len(raw_transform_data)
    assert (result['total_amount'] == raw_transform_data['sub_total']).all()


def test_column_mapper_fuzzy_matching(raw_transform_data):
    """Test fuzzy column matching."""
    
    config = {"standard_columns": {"date": "transaction_date"}}
    
    # Test with misspelled column names
    df_misspelled = raw_transform_data.rename(columns={
        'transaction_date': 'trans_date',
        'item_sku': 'prod_sku'
    })
    
    result = map_columns_to_standard(df_misspelled, config)
    
    assert 'order_date' in result.columns
    assert len(result) == len(df_misspelled)


def test_normalize_date_columns(raw_transform_data, transform_config):
    """Test date normalization."""
    
    result = normalize_date_columns(raw_transform_data, transform_config)
    
    assert result is not None
    
    # Check date column normalized
    assert pd.api.types.is_datetime64_any_dtype(result['order_date'])
    
    # Check date features created
    date_features = [col for col in result.columns if 'dayofweek' in col.lower() or 'month' in col.lower()]
    assert len(date_features) > 0
    
    # Verify date range
    assert result['order_date'].min() == pd.Timestamp('2024-01-15')
    assert result['order_date'].max() == pd.Timestamp('2024-01-16')


def test_date_normalizer_multiple_formats(raw_transform_data):
    """Test handling multiple date formats."""
    
    # Add more date formats
    raw_transform_data['mixed_date'] = ['01/15/2024', '2024-01-16', '15-Jan-2024']
    
    config = {"standard_columns": {"date": "mixed_date"}}
    result = map_columns_to_standard(raw_transform_data, config)
    result = normalize_date_columns(result, config)
    
    assert pd.api.types.is_datetime64_any_dtype(result['order_date'])
    assert len(result['order_date'].dropna()) == 3


def test_transform_features_pipeline(raw_transform_data, transform_config):
    """Test complete feature engineering pipeline."""
    
    # First standardize
    standardized = map_columns_to_standard(raw_transform_data, transform_config)
    date_normalized = normalize_date_columns(standardized, transform_config)
    
    # Then feature engineering
    features_result = transform_features(date_normalized, transform_config)
    
    assert features_result is not None
    
    # Check business metrics created
    business_metrics = ['avg_order_value', 'revenue_per_order']
    for metric in business_metrics:
        assert metric in features_result.columns
    
    # Check feature count increased
    assert len(features_result.columns) > len(date_normalized.columns)


def test_feature_engineering_business_metrics(raw_transform_data, transform_config):
    """Test specific business metric calculations."""
    
    standardized = map_columns_to_standard(raw_transform_data, transform_config)
    
    result = transform_features(standardized, transform_config)
    
    assert 'avg_order_value' in result.columns
    assert result['avg_order_value'].mean() > 0
    
    # Verify calculation logic
    expected_aov = raw_transform_data['sub_total'].sum() / len(raw_transform_data)
    np.testing.assert_almost_equal(result['avg_order_value'].mean(), expected_aov, decimal=0)


def test_auto_mapping_summary(raw_transform_data):
    """Test automatic mapping suggestions."""
    
    suggestions = auto_mapping_summary(raw_transform_data)
    
    assert isinstance(suggestions, dict)
    assert 'order_date' in suggestions
    assert suggestions['order_date']['best_match'] == 'transaction_date'


def test_date_range_summary(raw_transform_data):
    """Test date range quality report."""
    
    standardized = map_columns_to_standard(raw_transform_data, {"standard_columns": {"date": "transaction_date"}})
    date_normalized = normalize_date_columns(standardized, {})
    
    summary = date_range_summary(date_normalized)
    
    assert isinstance(summary, dict)
    assert 'date_span_days' in summary
    assert summary['date_span_days'] > 0


def test_transform_handles_missing_data():
    """Test transform pipeline with missing data."""
    
    df_missing = pd.DataFrame({
        'date': [None, '2024-01-01'],
        'amount': [100, None]
    })
    
    config = {"standard_columns": {"date": "date", "total_amount": "amount"}}
    
    result = map_columns_to_standard(df_missing, config)
    assert result is not None
    
    date_result = normalize_date_columns(result, config)
    assert date_result is not None
    
    features_result = transform_features(date_result, config)
    assert features_result is not None


def test_transform_pipeline_integration(raw_transform_data, transform_config):
    """Test full transform pipeline integration."""
    
    # Full pipeline
    standardized = map_columns_to_standard(raw_transform_data, transform_config)
    dates_normalized = normalize_date_columns(standardized, transform_config)
    features_added = transform_features(dates_normalized, transform_config)
    
    assert features_added is not None
    assert len(features_added) == len(raw_transform_data)
    
    # Verify data quality preserved
    assert features_added['total_amount'].sum() == raw_transform_data['sub_total'].sum()


@pytest.mark.parametrize("date_formats, expected_count", [
    (['2024-01-01', '2024-01-02'], 2),
    (['invalid', '2024-01-01'], 1),
    ([None, np.nan], 0)
])
def test_date_normalization_robustness(date_formats, expected_count):
    """Test date normalization with edge cases."""
    
    df = pd.DataFrame({'date_col': date_formats})
    config = {"standard_columns": {"date": "date_col"}}
    
    result = map_columns_to_standard(df, config)
    normalized = normalize_date_columns(result, config)
    
    valid_dates = normalized['order_date'].notna().sum()
    assert valid_dates == expected_count


class TestTransformErrorHandling:
    """Test error handling scenarios."""
    
    def test_empty_dataframe(self):
        """Test empty DataFrame handling."""
        
        empty_df = pd.DataFrame()
        config = {}
        
        result = map_columns_to_standard(empty_df, config)
        assert result is None
        
        result_features = transform_features(empty_df, config)
        assert result_features is None
    
    def test_missing_required_columns(self):
        """Test missing critical columns."""
        
        df_no_date = pd.DataFrame({'amount': [100, 200]})
        config = {"standard_columns": {"date": "missing_col"}}
        
        result = map_columns_to_standard(df_no_date, config)
        assert result is not None  # Should still work with warnings
        
        features_result = transform_features(result, config)
        assert features_result is not None  # Graceful degradation