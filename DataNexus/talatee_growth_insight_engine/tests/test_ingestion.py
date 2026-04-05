"""
Unit tests for ingestion module.
Tests data loading from all supported sources.
"""

import pytest
import pandas as pd
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas.testing as tm

from ingestion import load_all_sources, load_sample_data
from ingestion.shopee_loader import load_shopee_data
from ingestion.tokopedia_loader import load_tokopedia_data
from ingestion.tiktokshop_loader import load_tiktokshop_data
from ingestion.whatsapp_loader import load_whatsapp_data
from utils.config_loader import load_client_config


@pytest.fixture
def sample_config():
    """Sample client configuration for testing."""
    return {
        "client_id": "test_client",
        "data_sources": {
            "shopee": {"enabled": True, "file_path": "tests/sample_shopee.csv"},
            "tokopedia": {"enabled": True, "file_path": "tests/sample_tokopedia.csv"},
            "tiktokshop": {"enabled": True, "file_path": "tests/sample_tiktok.csv"},
            "whatsapp": {"enabled": True, "file_path": "tests/sample_whatsapp.csv"}
        },
        "marketplaces": ["shopee", "tokopedia", "whatsapp"]
    }


@pytest.fixture
def sample_data_path():
    """Ensure test data directory exists."""
    path = Path("tests")
    path.mkdir(exist_ok=True)
    return path


def test_load_sample_data(sample_data_path):
    """Test sample data generation."""
    df = load_sample_data("test", 100)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 100
    assert 'order_date' in df.columns
    assert 'total_amount' in df.columns
    assert df['total_amount'].sum() > 0
    assert df['platform'].nunique() >= 2


def test_load_all_sources_empty_config():
    """Test load_all_sources with no enabled sources."""
    config = {"data_sources": {"shopee": {"enabled": False}}}
    result = load_all_sources(config)
    assert result is None


@patch('ingestion.load_data._load_source_data')
def test_load_all_sources(mock_loader, sample_config):
    """Test multi-source loading."""
    
    # Mock successful loads
    mock_df1 = pd.DataFrame({'col1': [1, 2, 3]})
    mock_df2 = pd.DataFrame({'col2': [4, 5, 6]})
    
    mock_loader.side_effect = [mock_df1, mock_df2, None, None]
    
    result = load_all_sources(sample_config)
    
    assert result is not None
    assert len(result) == 6  # 3+3 rows
    assert 'platform' in result.columns
    assert result['platform'].nunique() == 2


def test_validate_raw_data(sample_data_path):
    """Test raw data validation."""
    from ingestion.load_data import validate_raw_data
    
    df = load_sample_data("test", 100)
    
    config = {"analytics": {"min_data_rows": 10}}
    result = validate_raw_data(df, config)
    
    assert result is True
    
    # Test with insufficient data
    small_df = df.head(5)
    result = validate_raw_data(small_df, config)
    assert result is False


@patch('pandas.read_csv')
def test_shopee_loader(mock_csv, sample_data_path):
    """Test Shopee loader."""
    
    mock_df = pd.DataFrame({
        'order_sn': ['ORD001'],
        'item_sku': ['SKU001'],
        'amount': [100000],
        'quantity': [2],
        'create_time': ['2024-01-01']
    })
    
    mock_csv.return_value = mock_df
    config = {"data_sources": {"shopee": {"enabled": True}}}
    
    result = load_shopee_data(config)
    
    assert result is not None
    assert 'order_id' in result.columns
    assert 'product_id' in result.columns
    assert 'platform' in result.columns
    assert result['platform'].iloc[0] == 'Shopee'


@patch('pandas.read_csv')
def test_tokopedia_loader(mock_csv, sample_data_path):
    """Test Tokopedia loader."""
    
    mock_df = pd.DataFrame({
        'order_id': ['ORD001'],
        'product_id': ['SKU001'],
        'subtotal': [100000],
        'quantity': [2]
    })
    
    mock_csv.return_value = mock_df
    config = {"data_sources": {"tokopedia": {"enabled": True}}}
    
    result = load_tokopedia_data(config)
    
    assert result is not None
    assert 'total_amount' in result.columns
    assert result['platform'].iloc[0] == 'Tokopedia'


@patch('pandas.read_csv')
def test_tiktokshop_loader(mock_csv, sample_data_path):
    """Test TikTok Shop loader."""
    
    mock_df = pd.DataFrame({
        'order_id': ['ORD001'],
        'product_id': ['SKU001'],
        'total_amount': [100000],
        'quantity_purchased': [2]
    })
    
    mock_csv.return_value = mock_df
    config = {"data_sources": {"tiktokshop": {"enabled": True}}}
    
    result = load_tiktokshop_data(config)
    
    assert result is not None
    assert result['platform'].iloc[0] == 'TikTok Shop'


@patch('pandas.read_csv')
def test_whatsapp_loader(mock_csv, sample_data_path):
    """Test WhatsApp loader."""
    
    mock_df = pd.DataFrame({
        'no_pesanan': ['WP001'],
        'nama_barang': ['Product A'],
        'harga': [50000],
        'jumlah': [2],
        'timestamp': ['2024-01-01']
    })
    
    mock_csv.return_value = mock_df
    config = {"data_sources": {"whatsapp": {"enabled": True}}}
    
    result = load_whatsapp_data(config)
    
    assert result is not None
    assert 'order_id' in result.columns
    assert 'total_amount' in result.columns
    assert result['platform'].iloc[0] == 'WhatsApp'


def test_ingestion_pipeline_integration(sample_data_path):
    """Test full ingestion pipeline."""
    
    # Create sample files for testing
    sample_shopee = pd.DataFrame({
        'order_sn': ['ORD001'], 'amount': [100000]
    })
    sample_shopee.to_csv(sample_data_path / "sample_shopee.csv", index=False)
    
    config = {
        "client_id": "test",
        "data_sources": {
            "shopee": {"enabled": True, "file_path": str(sample_data_path / "sample_shopee.csv")}
        }
    }
    
    result = load_all_sources(config)
    
    assert result is not None
    assert len(result) > 0
    assert 'platform' in result.columns


@pytest.mark.parametrize("missing_cols, expected", [
    (['total_amount'], False),
    (['platform'], False),
    ([], True)
])
def test_raw_data_validation_required_columns(missing_cols, expected, sample_data_path):
    """Test required columns validation."""
    from ingestion.load_data import validate_raw_data
    
    df = load_sample_data("test", 100)
    
    # Remove required columns
    for col in missing_cols:
        if col in df.columns:
            df = df.drop(columns=[col])
    
    config = {}
    result = validate_raw_data(df, config)
    assert result == expected


class TestIngestionErrorHandling:
    """Test error handling scenarios."""
    
    def test_file_not_found(self):
        """Test file not found handling."""
        config = {
            "data_sources": {
                "shopee": {"enabled": True, "file_path": "/nonexistent/file.csv"}
            }
        }
        result = load_all_sources(config)
        assert result is None  # Should gracefully handle missing files
    
    def test_empty_csv(self, tmp_path):
        """Test empty CSV handling."""
        empty_file = tmp_path / "empty.csv"
        empty_file.write_text("")
        
        config = {
            "data_sources": {
                "shopee": {"enabled": True, "file_path": str(empty_file)}
            }
        }
        result = load_all_sources(config)
        assert result is None
    
    def test_invalid_json_config(self):
        """Test invalid config handling."""
        invalid_config = {"data_sources": "invalid"}
        result = load_all_sources(invalid_config)
        assert result is None