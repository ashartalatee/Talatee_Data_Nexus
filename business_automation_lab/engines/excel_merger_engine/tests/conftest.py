import pytest
import pandas as pd
import tempfile
import os

@pytest.fixture
def valid_dataframe():
    """Menyediakan struktur dataframe yang valid sesuai kontrak industri."""
    return pd.DataFrame({
        "sku": ["SKU-A", "SKU-B", "SKU-C"],
        "revenue": [150000.0, 275000.0, 89000.0],
        "qty": [10, 5, 2]
    })

@pytest.fixture
def invalid_dataframe_missing_col():
    """Menyediakan dataframe cacat (kehilangan kolom 'qty')."""
    return pd.DataFrame({
        "sku": ["SKU-A"],
        "revenue": [150000.0]
    })

@pytest.fixture
def invalid_dataframe_wrong_type():
    """Menyediakan dataframe cacat (kolom numerik diisi string text)."""
    return pd.DataFrame({
        "sku": ["SKU-A"],
        "revenue": ["seratus ribu"],  # Memicu crash jika dilakukan operasi matematika
        "qty": [10]
    })