"""
Pytest configuration for analytics engine tests.
"""

import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime


@pytest.fixture(autouse=True)
def cleanup_test_files():
    """Cleanup test files after each test."""
    yield
    test_path = Path("tests")
    for file in test_path.glob("*.csv"):
        if "sample_" in file.name or "test_" in file.name:
            file.unlink(missing_ok=True)
    
    # Cleanup export test directories
    export_path = Path("tests/export_test")
    if export_path.exists():
        for file in export_path.glob("*"):
            file.unlink(missing_ok=True)
        export_path.rmdir()


@pytest.fixture(scope="session")
def sample_test_data():
    """Generate consistent test data for session."""
    
    dates = pd.date_range('2024-01-01', periods=100, freq='H')
    
    return pd.DataFrame({
        'order_id': [f"ORD_{i:04d}" for i in range(100)],
        'product_id': np.random.choice(['P001', 'P002', 'P003'], 100),
        'total_amount': np.random.uniform(50000, 500000, 100).round(-2),
        'quantity': np.random.choice([1, 2, 3, 5], 100),
        'order_date': dates,
        'platform': np.random.choice(['Shopee', 'Tokopedia'], 100),
        'status': np.random.choice(['completed', 'cancelled'], 100, p=[0.8, 0.2])
    })


@pytest.fixture
def sample_export_data():
    """Sample data for export testing."""
    
    return {
        'metrics_df': pd.DataFrame({
            'platform': ['Shopee', 'Tokopedia'],
            'total_revenue': [1000000, 500000],
            'total_orders': [100, 50]
        }),
        'summary_df': pd.DataFrame({
            'Metric': ['Total Revenue', 'Total Orders'],
            'Value': ['Rp 1.5M', '150']
        }),
        'insights': [
            {'title': 'Test Insight 1', 'priority': 1},
            {'title': 'Test Insight 2', 'priority': 2}
        ]
    }