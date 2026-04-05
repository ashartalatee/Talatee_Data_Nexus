"""
Multi-source data ingestion module.
Supports Shopee, Tokopedia, TikTokShop, and WhatsApp CSV/API loading.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from utils.logger import setup_logger, safe_log_dataframe
from utils.constants import SUPPORTED_SOURCES, RAW_DATA_DIR
from utils.config_loader import get_config_value, get_enabled_sources


def load_all_sources(config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Load and combine data from all enabled sources for a client.
    
    Args:
        config: Client configuration
        
    Returns:
        Combined DataFrame or None if no data loaded
    """
    logger = setup_logger("ingestion.load_data")
    logger.info("📥 Starting multi-source data ingestion...")
    
    enabled_sources = get_enabled_sources(config)
    if not enabled_sources:
        logger.warning("⚠️ No enabled data sources found")
        return None
    
    all_dataframes = []
    
    for source in enabled_sources:
        logger.info(f"📂 Loading {source}...")
        
        source_data = _load_source_data(source, config)
        if source_data is not None and not source_data.empty:
            source_data['platform'] = source  # Add source identifier
            all_dataframes.append(source_data)
            logger.info(f"✅ {source}: {len(source_data)} rows loaded")
        else:
            logger.warning(f"⚠️ No data from {source}")
    
    if not all_dataframes:
        logger.error("❌ No data loaded from any source")
        return None
    
    # Combine all sources
    logger.info(f"🔗 Combining {len(all_dataframes)} sources...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    safe_log_dataframe(logger, "combined_raw_data", combined_df)
    logger.info(f"✅ Total raw data: {len(combined_df)} rows, {len(combined_df.columns)} columns")
    
    return combined_df


def _load_source_data(source: str, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Load data from specific source based on config."""
    
    source_config = config['data_sources'].get(source, {})
    file_path = Path(source_config.get('file_path', f"{RAW_DATA_DIR}/{source}_{config['client_id']}.csv"))
    
    try:
        if not file_path.exists():
            logger = setup_logger(f"ingestion.{source}")
            logger.warning(f"📄 File not found: {file_path}")
            return None
        
        # Load CSV with flexible options
        df = pd.read_csv(
            file_path,
            encoding='utf-8',
            low_memory=False,
            on_bad_lines='skip'
        )
        
        logger = setup_logger(f"ingestion.{source}")
        safe_log_dataframe(logger, f"{source}_raw", df)
        
        return df
        
    except pd.errors.EmptyDataError:
        logger = setup_logger(f"ingestion.{source}")
        logger.warning(f"📄 Empty file: {file_path}")
        return None
    except Exception as e:
        logger = setup_logger(f"ingestion.{source}")
        logger.error(f"💥 Failed to load {source}: {str(e)}")
        return None


def load_sample_data(client_id: str = "client_a", n_rows: int = 1000) -> pd.DataFrame:
    """
    Generate sample data for testing/development.
    
    Args:
        client_id: Client identifier
        n_rows: Number of rows to generate
        
    Returns:
        Sample DataFrame
    """
    import numpy as np
    
    platforms = ['Shopee', 'Tokopedia', 'WhatsApp']
    statuses = ['completed', 'shipped', 'cancelled']
    
    np.random.seed(42)
    
    data = {
        'order_date': pd.date_range('2024-01-01', periods=n_rows, freq='H'),
        'order_id': [f"ORD_{i:08d}" for i in range(n_rows)],
        'product_id': np.random.choice(['PROD_001', 'PROD_002', 'PROD_003', 'PROD_004'], n_rows),
        'product_name': np.random.choice([
            'Wireless Earbuds Pro', 'Smartphone Case', 'Power Bank 10000mAh', 
            'Screen Protector', 'Charging Cable USB-C'
        ], n_rows),
        'price': np.random.uniform(5.0, 150.0, n_rows).round(2),
        'quantity': np.random.randint(1, 10, n_rows),
        'total_amount': np.random.uniform(10.0, 1000.0, n_rows).round(2),
        'platform': np.random.choice(platforms, n_rows),
        'status': np.random.choice(statuses, n_rows, p=[0.6, 0.3, 0.1]),
        'customer_id': [f"CUST_{i:06d}" for i in range(n_rows)]
    }
    
    df = pd.DataFrame(data)
    df['total_amount'] = (df['price'] * df['quantity']).round(2)
    
    # Save sample data
    sample_path = RAW_DATA_DIR / f"sample_{client_id}.csv"
    df.to_csv(sample_path, index=False)
    
    logger = setup_logger("ingestion.sample")
    safe_log_dataframe(logger, "sample_data", df.head())
    
    return df


def validate_raw_data(df: pd.DataFrame, config: Dict[str, Any]) -> bool:
    """
    Basic validation of raw data before processing.
    
    Args:
        df: Raw DataFrame
        config: Client configuration
        
    Returns:
        True if data passes basic validation
    """
    if df is None or df.empty:
        return False
    
    logger = setup_logger("ingestion.validation")
    
    # Check minimum required columns
    required_cols = ['total_amount', 'platform']
    missing_cols = set(required_cols) - set(df.columns)
    if missing_cols:
        logger.error(f"❌ Missing required columns: {missing_cols}")
        return False
    
    # Check data volume
    min_rows = get_config_value(config, 'analytics.min_data_rows', 10)
    if len(df) < min_rows:
        logger.warning(f"⚠️ Low data volume: {len(df)} rows < {min_rows}")
        return False
    
    # Check for negative values in key metrics
    if 'total_amount' in df.columns:
        negative_amounts = (df['total_amount'] < 0).sum()
        if negative_amounts > 0:
            logger.warning(f"⚠️ {negative_amounts} negative total_amount values")
    
    logger.info("✅ Raw data validation passed")
    return True