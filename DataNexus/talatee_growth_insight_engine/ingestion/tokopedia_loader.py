"""
Tokopedia-specific data loader.
Handles Tokopedia CSV exports and API integration (placeholder).
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value
import requests


def load_tokopedia_data(config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Load Tokopedia data from CSV file or API.
    
    Args:
        config: Client configuration with Tokopedia settings
        
    Returns:
        Tokopedia DataFrame or None
    """
    logger = setup_logger("ingestion.tokopedia")
    tokopedia_config = config['data_sources']['tokopedia']
    
    # Check if enabled
    if not tokopedia_config.get('enabled', False):
        logger.info("⏭️ Tokopedia loader disabled")
        return None
    
    file_path = Path(tokopedia_config.get('file_path', ''))
    
    if file_path and file_path.exists():
        return _load_tokopedia_csv(file_path, logger)
    elif tokopedia_config.get('api_token') and tokopedia_config.get('shop_id'):
        return _load_tokopedia_api(tokopedia_config, config, logger)
    else:
        logger.warning("⚠️ No valid Tokopedia file path or API credentials")
        return None


def _load_tokopedia_csv(file_path: Path, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Load Tokopedia CSV export file."""
    try:
        logger.info(f"📄 Loading Tokopedia CSV: {file_path}")
        
        # Tokopedia CSV characteristics
        df = pd.read_csv(
            file_path,
            encoding='utf-8-sig',
            low_memory=False,
            dtype={
                'order_id': str,
                'product_id': str,
                'shop_id': str,
                'price': float,
                'quantity': int,
                'subtotal': float
            },
            parse_dates=['order_date', 'shipping_date']
        )
        
        # Standardize Tokopedia column names
        column_mapping = {
            'order_id': 'order_id',
            'product_id': 'product_id',
            'product_name': 'product_name',
            'price': 'price',
            'quantity': 'quantity', 
            'subtotal': 'total_amount',
            'order_date': 'order_date',
            'customer_id': 'customer_id',
            'order_status': 'status',
            'shop_id': 'shop_id'
        }
        
        # Apply mapping for available columns only
        available_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=available_mapping)
        df['platform'] = 'Tokopedia'
        
        # Ensure numeric columns
        numeric_cols = ['price', 'quantity', 'total_amount']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        safe_log_dataframe(logger, "tokopedia_csv", df)
        logger.info(f"✅ Tokopedia CSV loaded: {len(df)} rows")
        
        return df
        
    except Exception as e:
        logger.error(f"💥 Tokopedia CSV load failed: {str(e)}", exc_info=True)
        return None


def _load_tokopedia_api(tokopedia_config: Dict[str, Any], client_config: Dict[str, Any], 
                       logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Load Tokopedia data via Seller API (placeholder implementation)."""
    try:
        logger.info("🌐 Loading Tokopedia API data...")
        
        shop_id = tokopedia_config['shop_id']
        api_token = tokopedia_config['api_token']
        days_back = get_config_value(client_config, 'data_sources.tokopedia.date_range.days_back', 30)
        
        # API endpoint example
        url = f"https://fs.tokopedia.com/seller/v2/order/list"
        headers = {
            'Authorization': f'Bearer {api_token}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'shop_id': shop_id,
            'status': 'all',
            'page': 1,
            'per_page': 100
        }
        
        all_orders = []
        page = 1
        
        while True:
            params['page'] = page
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            orders = data.get('data', {}).get('orders', [])
            
            if not orders:
                break
                
            all_orders.extend(orders)
            page += 1
            
            if len(orders) < 100:  # Last page
                break
        
        if not all_orders:
            logger.warning("⚠️ No orders from Tokopedia API")
            return None
        
        # Normalize nested JSON structure
        df_list = []
        for order in all_orders:
            order_flat = {
                'order_id': order.get('order_id'),
                'product_id': order.get('product_id'),
                'product_name': order.get('product_name'),
                'price': order.get('price'),
                'quantity': order.get('quantity'),
                'total_amount': order.get('subtotal'),
                'order_date': pd.to_datetime(order.get('order_date')),
                'status': order.get('status'),
                'customer_id': order.get('customer_id'),
                'platform': 'Tokopedia'
            }
            df_list.append(order_flat)
        
        df = pd.DataFrame(df_list)
        
        safe_log_dataframe(logger, "tokopedia_api", df)
        logger.info(f"✅ Tokopedia API loaded: {len(df)} orders")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"🌐 Tokopedia API request failed: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"💥 Tokopedia API processing failed: {str(e)}", exc_info=True)
        return None


def tokopedia_column_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """
    Auto-detect and map Tokopedia-specific columns to standard format.
    
    Args:
        df: Raw Tokopedia DataFrame
        
    Returns:
        Column mapping dictionary
    """
    mapping = {}
    
    # Common Tokopedia column variations
    tokopedia_patterns = {
        'order_id': ['order_id', 'no_pesanan', 'invoice_no'],
        'product_id': ['product_id', 'sku', 'product_sku'],
        'product_name': ['product_name', 'nama_produk', 'item_name'],
        'total_amount': ['subtotal', 'total_amount', 'jumlah_total'],
        'quantity': ['quantity', 'qty', 'jumlah'],
        'price': ['price', 'harga', 'unit_price'],
        'order_date': ['order_date', 'tanggal_pesanan', 'created_at'],
        'customer_id': ['customer_id', 'buyer_id', 'id_pembeli'],
        'status': ['order_status', 'status_pesanan', 'status']
    }
    
    for standard_col, patterns in tokopedia_patterns.items():
        for pattern in patterns:
            if pattern in df.columns:
                mapping[pattern] = standard_col
                break
    
    return mapping