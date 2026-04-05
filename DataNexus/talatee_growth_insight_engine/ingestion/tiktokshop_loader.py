"""
TikTok Shop-specific data loader.
Handles TikTok Shop CSV exports and API integration (placeholder).
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value
import requests
import hmac
import hashlib
import time


def load_tiktokshop_data(config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Load TikTok Shop data from CSV file or API.
    
    Args:
        config: Client configuration with TikTok Shop settings
        
    Returns:
        TikTok Shop DataFrame or None
    """
    logger = setup_logger("ingestion.tiktokshop")
    tiktok_config = config['data_sources']['tiktokshop']
    
    # Check if enabled
    if not tiktok_config.get('enabled', False):
        logger.info("⏭️ TikTok Shop loader disabled")
        return None
    
    file_path = Path(tiktok_config.get('file_path', ''))
    
    if file_path and file_path.exists():
        return _load_tiktokshop_csv(file_path, logger)
    elif tiktok_config.get('app_key') and tiktok_config.get('app_secret') and tiktok_config.get('shop_id'):
        return _load_tiktokshop_api(tiktok_config, config, logger)
    else:
        logger.warning("⚠️ No valid TikTok Shop file path or API credentials")
        return None


def _load_tiktokshop_csv(file_path: Path, logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Load TikTok Shop CSV export file."""
    try:
        logger.info(f"📄 Loading TikTok Shop CSV: {file_path}")
        
        df = pd.read_csv(
            file_path,
            encoding='utf-8-sig',
            low_memory=False,
            dtype={
                'order_id': str,
                'product_id': str,
                'sku': str,
                'price': float,
                'quantity_purchased': int,
                'total_amount': float
            },
            parse_dates=['create_time', 'update_time']
        )
        
        # Standardize TikTok Shop column names
        column_mapping = {
            'order_id': 'order_id',
            'product_id': 'product_id',
            'product_name': 'product_title',
            'sku': 'product_id',
            'price': 'price',
            'quantity_purchased': 'quantity',
            'total_amount': 'total_amount',
            'create_time': 'order_date',
            'buyer_user_id': 'customer_id',
            'order_status': 'status'
        }
        
        # Apply mapping for available columns only
        available_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=available_mapping)
        df['platform'] = 'TikTok Shop'
        
        # Ensure numeric columns
        numeric_cols = ['price', 'quantity', 'total_amount']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        safe_log_dataframe(logger, "tiktokshop_csv", df)
        logger.info(f"✅ TikTok Shop CSV loaded: {len(df)} rows")
        
        return df
        
    except Exception as e:
        logger.error(f"💥 TikTok Shop CSV load failed: {str(e)}", exc_info=True)
        return None


def _load_tiktokshop_api(tiktok_config: Dict[str, Any], client_config: Dict[str, Any], 
                        logger: logging.Logger) -> Optional[pd.DataFrame]:
    """Load TikTok Shop data via Seller API (placeholder implementation)."""
    try:
        logger.info("🌐 Loading TikTok Shop API data...")
        
        app_key = tiktok_config['app_key']
        app_secret = tiktok_config['app_secret']
        shop_id = tiktok_config['shop_id']
        days_back = get_config_value(client_config, 'data_sources.tiktokshop.date_range.days_back', 30)
        
        # TikTok Shop API endpoint example
        timestamp = str(int(time.time()))
        params = {
            'app_key': app_key,
            'timestamp': timestamp,
            'shop_id': shop_id,
            'page_size': 50
        }
        
        # Generate signature
        query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])
        signature = hmac.new(
            app_secret.encode(),
            query_string.encode(),
            hashlib.sha256
        ).hexdigest()
        params['sign'] = signature
        
        url = "https://open-api.tiktok.com/order/list"
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        orders = data.get('data', {}).get('order_list', [])
        
        if not orders:
            logger.warning("⚠️ No orders from TikTok Shop API")
            return None
        
        # Flatten nested order items
        df_list = []
        for order in orders:
            order_items = order.get('order_items', [])
            for item in order_items:
                row = {
                    'order_id': order.get('order_id'),
                    'product_id': item.get('product_id', item.get('sku')),
                    'product_name': item.get('product_title'),
                    'price': float(item.get('price', 0)),
                    'quantity': int(item.get('quantity_purchased', 0)),
                    'total_amount': float(item.get('total_amount', 0)),
                    'order_date': pd.to_datetime(order.get('create_time')),
                    'status': order.get('order_status'),
                    'customer_id': order.get('buyer_user_id'),
                    'platform': 'TikTok Shop'
                }
                df_list.append(row)
        
        df = pd.DataFrame(df_list)
        
        safe_log_dataframe(logger, "tiktokshop_api", df)
        logger.info(f"✅ TikTok Shop API loaded: {len(df)} orders")
        
        return df
        
    except requests.exceptions.RequestException as e:
        logger.error(f"🌐 TikTok Shop API request failed: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"💥 TikTok Shop API processing failed: {str(e)}", exc_info=True)
        return None


def tiktokshop_column_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """
    Auto-detect and map TikTok Shop-specific columns to standard format.
    
    Args:
        df: Raw TikTok Shop DataFrame
        
    Returns:
        Column mapping dictionary
    """
    mapping = {}
    
    # Common TikTok Shop column variations
    tiktok_patterns = {
        'order_id': ['order_id', 'aw_order_id'],
        'product_id': ['product_id', 'sku', 'seller_sku'],
        'product_name': ['product_title', 'product_name', 'title'],
        'total_amount': ['total_amount', 'aw_total_amount'],
        'quantity': ['quantity_purchased', 'quantity', 'count'],
        'price': ['price', 'unit_price'],
        'order_date': ['create_time', 'order_create_time', 'created_at'],
        'customer_id': ['buyer_user_id', 'buyer_open_id'],
        'status': ['order_status', 'aw_order_status']
    }
    
    for standard_col, patterns in tiktok_patterns.items():
        for pattern in patterns:
            if pattern in df.columns:
                mapping[pattern] = standard_col
                break
    
    return mapping