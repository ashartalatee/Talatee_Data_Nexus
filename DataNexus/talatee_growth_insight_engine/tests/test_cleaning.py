"""
Unit tests for cleaning modules.
Tests data standardization, missing values, duplicates, and text cleaning.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from unittest.mock import patch, MagicMock

from cleaning import clean_and_standardize
from cleaning.missing_handler import handle_missing_values, generate_missing_report
from cleaning.duplicate_handler import remove_duplicates, detect_duplicates
from cleaning.text_cleaner import clean_text_columns, normalize_product_names
from utils.config_loader import load_client_config


@pytest.fixture
def dirty_sample_data():
    """Create dirty test data with common issues."""
    return pd.DataFrame({
        'Order_ID': ['ORD001', 'ORD001', np.nan, 'ORD002', 'ORD003'],
        'Product_SKU': ['SKU001', 'SKU001', 'SKU002', np.nan, 'SKU003'],
        'Product Name': [' Wireless Earbuds  Pro!!! ', 'wireless earbuds pro', 
                        'Smartphone Case XL', np.nan, 'Power Bank 10000mAh'],
        'Price': [250000, 250000, 45000, np.nan, 150000],
        'Quantity': [2, 2, 1, 3, np.nan],
        'Total': [500000, 500000, 45000, 0, 150000],
        'Date': ['2024-01-15', '2024/01/15', '15-01-2024', np.nan, '2024-01-20'],
        'Platform': ['Shopee', ' shopee ', np.nan, 'Tokopedia', 'Shopee']
    })


@pytest.fixture
def clean_config():
    """Clean configuration for testing."""
    return {
        "client_id": "test",
        "standard_columns": {
            "date": "Date",
            "product_name": "Product Name", 
            "product_id": "Product_SKU",
            "price": "Price",
            "quantity": "Quantity",
            "total_amount": "Total",
            "platform": "Platform"
        },
        "data_cleaning": {
            "handle_missing": {"strategy": "forward_fill", "threshold": 0.5},
            "remove_duplicates": {"columns": ["Order_ID", "Product_SKU"], "keep": "first"},
            "text_cleaning": {
                "columns": ["Product Name", "Platform"],
                "lowercase": True,
                "remove_special_chars": True
            }
        }
    }


def test_clean_and_standardize_full_pipeline(dirty_sample_data, clean_config):
    """Test complete cleaning pipeline."""
    
    result = clean_and_standardize(dirty_sample_data, clean_config)
    
    assert result is not None
    assert not result.empty
    
    # Column standardization
    expected_cols = ['order_date', 'product_name', 'product_id', 'price', 
                    'quantity', 'total_amount', 'platform']
    for col in expected_cols:
        assert col in result.columns
    
    # Duplicate removal (ORD001 should appear once)
    assert result[result['product_id'] == 'SKU001'].shape[0] == 1
    
    # Text cleaning
    assert result['product_name'].str.contains('wireless earbuds pro', case=False, na=False).all()
    assert result['platform'].str.strip().str.lower().eq('shopee').sum() >= 1


def test_missing_handler(dirty_sample_data, clean_config):
    """Test missing value handling."""
    
    result = handle_missing_values(dirty_sample_data, clean_config)
    
    assert result is not None
    
    # Check missing values reduced
    original_missing = dirty_sample_data.isnull().sum().sum()
    final_missing = result.isnull().sum().sum()
    assert final_missing < original_missing
    
    # Numeric columns should have values
    assert result['Price'].notna().all()
    assert result['Quantity'].notna().all()


def test_missing_report(dirty_sample_data):
    """Test missing data report generation."""
    
    report = generate_missing_report(dirty_sample_data)
    
    assert isinstance(report, pd.DataFrame)
    assert 'missing_pct' in report.columns
    assert len(report) == len(dirty_sample_data.columns)


def test_duplicate_handler(dirty_sample_data, clean_config):
    """Test duplicate detection and removal."""
    
    # Test detection
    duplicates = detect_duplicates(dirty_sample_data, ['Order_ID', 'Product_SKU'])
    assert isinstance(duplicates, dict)
    assert len(duplicates) > 0
    
    # Test removal
    result = remove_duplicates(dirty_sample_data, clean_config)
    
    assert len(result) < len(dirty_sample_data)
    assert result.duplicated(subset=['Order_ID', 'Product_SKU']).sum() == 0


def test_text_cleaner(dirty_sample_data, clean_config):
    """Test text cleaning."""
    
    result = clean_text_columns(dirty_sample_data, clean_config)
    
    assert result is not None
    
    # Check normalization
    product_names = result['Product Name'].dropna()
    assert product_names.str.len().min() > 0
    assert '!!!' not in product_names.values
    assert product_names.str.contains('wireless earbuds pro', case=False, na=False).all()


def test_normalize_product_names(dirty_sample_data):
    """Test product name normalization."""
    
    result = normalize_product_names(dirty_sample_data, 'Product Name')
    
    assert 'Product Name' in result.columns
    assert result['Product Name'].str.contains('pro', case=False, na=False).sum() >= 1
    assert result['Product Name'].nunique() <= dirty_sample_data['Product Name'].nunique()


def test_cleaning_handles_empty_data():
    """Test cleaning with empty data."""
    
    empty_df = pd.DataFrame()
    config = {}
    
    result = clean_and_standardize(empty_df, config)
    assert result is None
    
    result_missing = handle_missing_values(empty_df, config)
    assert result_missing is None


def test_cleaning_column_mapping():
    """Test automatic column mapping."""
    
    df = pd.DataFrame({
        'transaction_date': ['2024-01-01'],
        'item_name': ['Test Product'],
        'subtotal': [100000]
    })
    
    config = {
        "standard_columns": {
            "date": "transaction_date",
            "product_name": "item_name", 
            "total_amount": "subtotal"
        }
    }
    
    result = clean_and_standardize(df, config)
    
    assert 'order_date' in result.columns
    assert 'product_name' in result.columns
    assert 'total_amount' in result.columns


def test_high_missing_threshold():
    """Test column dropping with high missing threshold."""
    
    df = pd.DataFrame({
        'good_col': [1, 2, 3, 4],
        'mostly_missing': [1, np.nan, np.nan, np.nan]
    })
    
    config = {"data_cleaning": {"handle_missing": {"threshold": 0.6}}}
    
    result = handle_missing_values(df, config)
    
    assert 'mostly_missing' not in result.columns
    assert 'good_col' in result.columns


@pytest.mark.parametrize("text,expected", [
    (" Wireless Earbuds Pro!!! ", "wireless earbuds pro"),
    ("Smartphone Case XL (Black)", "smartphone case xl black"),
    (np.nan, np.nan),
    ("", "")
])
def test_single_text_cleaning(text, expected):
    """Test individual text cleaning."""
    
    from cleaning.text_cleaner import _clean_text_series
    
    series = pd.Series([text])
    result = _clean_text_series(series, lowercase=True, remove_special=True)
    
    if pd.isna(expected):
        assert pd.isna(result.iloc[0])
    else:
        assert result.iloc[0] == expected


class TestCleaningErrorHandling:
    """Test error handling in cleaning modules."""
    
    def test_invalid_config(self):
        """Test with invalid configuration."""
        
        df = pd.DataFrame({'test': [1, 2]})
        invalid_config = {"data_cleaning": "invalid"}
        
        result = clean_and_standardize(df, invalid_config)
        assert result is not None  # Should not crash
        
        result_missing = handle_missing_values(df, invalid_config)
        assert result_missing is not None
    
    def test_non_string_text_columns(self):
        """Test text cleaning with non-string data."""
        
        df = pd.DataFrame({'numbers': [1, 2, 3]})
        config = {"data_cleaning": {"text_cleaning": {"columns": ["numbers"]}}}
        
        result = clean_text_columns(df, config)
        assert result is not None
        assert 'numbers' in result.columns