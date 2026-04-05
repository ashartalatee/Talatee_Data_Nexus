"""
WhatsApp CSV data loader.
Handles WhatsApp Business CSV exports with flexible parsing.
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value
from utils.constants import RAW_DATA_DIR
import re


def load_whatsapp_data(config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Load WhatsApp CSV data with flexible parsing.
    
    Args:
        config: Client configuration with WhatsApp settings
        
    Returns:
        WhatsApp DataFrame or None
    """
    logger = setup_logger("ingestion.whatsapp")
    whatsapp_config = config['data_sources']['whatsapp']
    
    # Check if enabled
    if not whatsapp_config.get('enabled', False):
        logger.info("⏭️ WhatsApp loader disabled")
        return None
    
    file_path = Path(whatsapp_config.get('file_path', ''))
    
    if not file_path.exists():
        logger.warning(f"📄 WhatsApp file not found: {file_path}")
        return None
    
    try:
        df = _load_whatsapp_csv(file_path, whatsapp_config, logger)
        if df is not None and not df.empty:
            df['platform'] = 'WhatsApp'
            safe_log_dataframe(logger, "whatsapp_data", df)
            logger.info(f"✅ WhatsApp loaded: {len(df)} rows")
        
        return df
        
    except Exception as e:
        logger.error(f"💥 WhatsApp load failed: {str(e)}", exc_info=True)
        return None


def _load_whatsapp_csv(file_path: Path, config: Dict[str, Any], 
                      logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Load and parse WhatsApp CSV with multiple format support."""
    
    delimiter = config.get('delimiter', ',')
    date_column = config.get('date_column', 'timestamp')
    
    try:
        # Try multiple encodings and delimiters
        encodings = ['utf-8-sig', 'utf-8', 'latin-1']
        delimiters = [delimiter, ',', ';', '\t']
        
        df = None
        for encoding in encodings:
            for delim in delimiters:
                try:
                    df = pd.read_csv(
                        file_path,
                        encoding=encoding,
                        sep=delim,
                        on_bad_lines='skip',
                        low_memory=False
                    )
                    if not df.empty:
                        logger.info(f"✅ WhatsApp CSV loaded with {encoding}/{delim}")
                        break
                except:
                    continue
            
            if df is not None and not df.empty:
                break
        
        if df is None or df.empty:
            logger.error("❌ Could not parse WhatsApp CSV with any encoding/delimiter")
            return None
        
        # Auto-detect WhatsApp format and standardize
        df = _standardize_whatsapp_format(df, date_column, logger)
        
        return df
        
    except Exception as e:
        logger.error(f"💥 WhatsApp CSV parsing failed: {str(e)}")
        return None


def _standardize_whatsapp_format(df: pd.DataFrame, date_column: str, 
                               logger: logging.Logger) -> pd.DataFrame:
    """Standardize WhatsApp CSV to common format."""
    
    # Common WhatsApp column patterns
    whatsapp_patterns = {
        'order_id': ['order_id', 'no_pesanan', 'invoice', 'nomor_pesanan'],
        'product_name': ['product', 'barang', 'nama_barang', 'item'],
        'product_id': ['product_id', 'kode_barang', 'sku'],
        'price': ['harga', 'price', 'unit_price'],
        'quantity': ['qty', 'quantity', 'jumlah'],
        'total_amount': ['total', 'jumlah_total', 'amount'],
        'customer_name': ['customer', 'pelanggan', 'nama_pelanggan'],
        'customer_phone': ['phone', 'no_hp', 'telepon'],
        'status': ['status', 'keterangan']
    }
    
    # Create standardized columns
    standardized_df = pd.DataFrame()
    
    # Date handling
    date_cols = _find_date_column(df, date_column)
    if date_cols:
        standardized_df['order_date'] = pd.to_datetime(df[date_cols[0]], errors='coerce')
    
    # Extract order ID from various formats
    order_id_col = _find_column(df, whatsapp_patterns['order_id'])
    if order_id_col:
        standardized_df['order_id'] = df[order_id_col].astype(str)
    
    # Extract phone-based customer ID
    phone_col = _find_column(df, whatsapp_patterns['customer_phone'])
    if phone_col:
        standardized_df['customer_id'] = df[phone_col].astype(str).str.replace(r'[^\d+]', '', regex=True)
    
    # Product info
    product_col = _find_column(df, whatsapp_patterns['product_name'])
    if product_col:
        standardized_df['product_name'] = df[product_col].astype(str)
    
    # Numeric fields
    price_col = _find_column(df, whatsapp_patterns['price'])
    if price_col:
        standardized_df['price'] = pd.to_numeric(df[price_col], errors='coerce')
    
    qty_col = _find_column(df, whatsapp_patterns['quantity'])
    if qty_col:
        standardized_df['quantity'] = pd.to_numeric(df[qty_col], errors='coerce').fillna(1).astype(int)
    
    total_col = _find_column(df, whatsapp_patterns['total_amount'])
    if total_col:
        standardized_df['total_amount'] = pd.to_numeric(df[total_col], errors='coerce')
    elif 'price' in standardized_df.columns and 'quantity' in standardized_df.columns:
        standardized_df['total_amount'] = (standardized_df['price'] * standardized_df['quantity']).round(2)
    
    # Fill missing order_id with generated values
    if 'order_id' not in standardized_df.columns or standardized_df['order_id'].isna().all():
        standardized_df['order_id'] = 'WHATSAPP_' + standardized_df.index.astype(str)
    
    # Generate product_id if missing
    if 'product_id' not in standardized_df.columns:
        standardized_df['product_id'] = (
            standardized_df['product_name'].fillna('unknown')
            .str[:20].str.replace(r'[^\w]', '_', regex=True)
        )
    
    # Clean status
    status_col = _find_column(df, whatsapp_patterns['status'])
    if status_col:
        standardized_df['status'] = df[status_col].astype(str).str.lower()
    
    logger.info(f"📋 WhatsApp standardized: {len(standardized_df)} rows, {len(standardized_df.columns)} cols")
    
    return standardized_df


def _find_date_column(df: pd.DataFrame, preferred: str) -> Optional[str]:
    """Find best date column candidate."""
    if preferred in df.columns:
        return preferred
    
    date_patterns = ['date', 'time', 'timestamp', 'tanggal']
    for col in df.columns.str.lower():
        if any(pattern in col for pattern in date_patterns):
            return col
    return None


def _find_column(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    """Find column matching any pattern."""
    for pattern in patterns:
        for col in df.columns:
            if pattern.lower() in col.lower():
                return col
    return None


def generate_whatsapp_sample(client_id: str = "client_a", n_rows: int = 500) -> pd.DataFrame:
    """
    Generate realistic WhatsApp sample data for testing.
    
    Args:
        client_id: Client identifier
        n_rows: Number of rows
        
    Returns:
        Sample WhatsApp DataFrame
    """
    import numpy as np
    
    np.random.seed(42)
    products = [
        'Sepatu Sport', 'Tas Selempang', 'Jam Tangan', 'Headset Wireless',
        'Powerbank', 'Kabel Charger', 'Case HP', 'Laptop Stand'
    ]
    
    data = {
        'timestamp': pd.date_range('2024-01-01', periods=n_rows, freq='4H'),
        'no_pesanan': [f"WP{i:06d}" for i in range(n_rows)],
        'nama_barang': np.random.choice(products, n_rows),
        'harga': np.random.uniform(25_000, 1_500_000, n_rows).round(-2),
        'jumlah': np.random.choice([1, 2, 3, 5], n_rows, p=[0.5, 0.3, 0.15, 0.05]),
        'no_hp': [f"+62{i:010d}" for i in np.random.randint(1000000000, 9999999999, n_rows)],
        'status': np.random.choice(['LUNAS', 'BELUM BAYAR', 'KIRIM'], n_rows, p=[0.6, 0.25, 0.15])
    }
    
    df = pd.DataFrame(data)
    df['total'] = (df['harga'] * df['jumlah']).round(-2)
    
    # Save sample
    sample_path = RAW_DATA_DIR / f"whatsapp_sample_{client_id}.csv"
    df.to_csv(sample_path, index=False, encoding='utf-8-sig')
    
    logger = setup_logger("ingestion.whatsapp.sample")
    safe_log_dataframe(logger, "whatsapp_sample", df.head(10))
    
    return df