"""
Multi-marketplace metrics calculation module.
Generates comprehensive KPIs across platforms, products, customers, and time.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional, List
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value
from utils.constants import CORE_METRICS


def generate_metrics(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Generate comprehensive business metrics across multiple dimensions.
    
    Args:
        df: Feature-engineered DataFrame
        config: Client configuration
        
    Returns:
        Metrics DataFrame with KPIs
    """
    logger = setup_logger("analysis.metrics")
    logger.info("📊 Generating comprehensive metrics...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    # Filter completed transactions only
    completed_mask = df['status'].str.contains('complete|delivered|lunas', case=False, na=False) if 'status' in df.columns else True
    df_metrics = df[completed_mask].copy()
    
    if df_metrics.empty:
        logger.warning("⚠️ No completed transactions found")
        return pd.DataFrame()
    
    metrics_dfs = []
    
    # Overall metrics
    overall_metrics = _calculate_overall_metrics(df_metrics)
    metrics_dfs.append(overall_metrics)
    
    # Platform metrics
    platform_metrics = _calculate_platform_metrics(df_metrics)
    metrics_dfs.append(platform_metrics)
    
    # Product metrics  
    product_metrics = _calculate_product_metrics(df_metrics, config)
    metrics_dfs.append(product_metrics)
    
    # Customer metrics
    customer_metrics = _calculate_customer_metrics(df_metrics)
    metrics_dfs.append(customer_metrics)
    
    # Time-based metrics
    time_metrics = _calculate_time_metrics(df_metrics)
    metrics_dfs.append(time_metrics)
    
    # Combine all metrics
    all_metrics = pd.concat(metrics_dfs, ignore_index=True)
    
    logger.info(f"✅ Generated {len(all_metrics)} metrics across {len(metrics_dfs)} categories")
    safe_log_dataframe(logger, "key_metrics", all_metrics.head(20))
    
    return all_metrics


def _calculate_overall_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Overall business metrics."""
    
    metrics = {
        'total_revenue': df['total_amount'].sum(),
        'total_orders': df['order_id'].nunique(),
        'total_units': df['quantity'].sum(),
        'total_customers': df['customer_id'].nunique(),
        'avg_order_value': df['total_amount'].sum() / df['order_id'].nunique(),
        'avg_units_per_order': df['quantity'].sum() / df['order_id'].nunique(),
        'conversion_rate': 1.0,  # Placeholder
        'date_range': f"{df['order_date'].min().date()} to {df['order_date'].max().date()}",
        'records_processed': len(df)
    }
    
    return pd.DataFrame([metrics])


def _calculate_platform_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Platform performance metrics."""
    
    if 'platform' not in df.columns:
        return pd.DataFrame()
    
    platform_stats = df.groupby('platform').agg({
        'total_amount': ['sum', 'count', 'mean'],
        'quantity': 'sum',
        'order_id': 'nunique',
        'customer_id': 'nunique'
    }).round(2)
    
    platform_stats.columns = [
        'platform_revenue', 'platform_orders', 'avg_order_value',
        'platform_units', 'unique_orders', 'unique_customers'
    ]
    
    platform_stats['platform_rev_share'] = (
        platform_stats['platform_revenue'] / platform_stats['platform_revenue'].sum() * 100
    ).round(2)
    
    platform_stats['aov'] = platform_stats['platform_revenue'] / platform_stats['platform_orders']
    platform_stats = platform_stats.reset_index()
    
    return platform_stats


def _calculate_product_metrics(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Top products performance."""
    
    top_n = get_config_value(config, 'analytics.top_n', 20)
    
    if 'product_id' not in df.columns or 'product_name' not in df.columns:
        return pd.DataFrame()
    
    product_stats = df.groupby(['product_id', 'product_name']).agg({
        'total_amount': ['sum', 'count', 'mean'],
        'quantity': 'sum',
        'order_id': 'nunique'
    }).round(2)
    
    product_stats.columns = [
        'product_revenue', 'product_orders', 'avg_price',
        'product_units', 'unique_orders'
    ]
    
    product_stats['revenue_per_unit'] = product_stats['product_revenue'] / product_stats['product_units']
    product_stats = product_stats.sort_values('product_revenue', ascending=False).head(top_n)
    
    # Add rankings
    product_stats['revenue_rank'] = range(1, len(product_stats) + 1)
    product_stats['popularity_rank'] = product_stats['product_orders'].rank(ascending=False).astype(int)
    
    return product_stats.reset_index()


def _calculate_customer_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Customer segmentation metrics."""
    
    if 'customer_id' not in df.columns:
        return pd.DataFrame()
    
    customer_stats = df.groupby('customer_id').agg({
        'total_amount': ['sum', 'count', 'mean'],
        'quantity': 'sum',
        'order_date': ['min', 'max', 'nunique']
    }).round(2)
    
    customer_stats.columns = [
        'customer_ltv', 'customer_orders', 'avg_order_value',
        'customer_units', 'first_order_date', 'last_order_date', 'order_days'
    ]
    
    customer_stats['recency_days'] = (pd.Timestamp.now() - customer_stats['last_order_date']).dt.days
    customer_stats['customer_tenure_days'] = (
        customer_stats['last_order_date'] - customer_stats['first_order_date']
    ).dt.days
    
    # RFM scores
    customer_stats['rfm_score'] = (
        (customer_stats['recency_days'].rank(pct=True) * 0.3 +
         (1/customer_stats['customer_orders'].rank(pct=True)) * 0.3 +
         customer_stats['customer_ltv'].rank(pct=True) * 0.4)
        .round(2) * 100
    )
    
    # Segmentation
    customer_stats['segment'] = pd.cut(
        customer_stats['rfm_score'], 
        bins=4, 
        labels=['Bronze', 'Silver', 'Gold', 'Platinum']
    )
    
    top_customers = customer_stats.nlargest(20, 'customer_ltv')
    
    return top_customers.reset_index(drop=True)


def _calculate_time_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Time-based trend metrics."""
    
    if 'order_date' not in df.columns:
        return pd.DataFrame()
    
    df['date_only'] = df['order_date'].dt.date
    daily_metrics = df.groupby('date_only').agg({
        'total_amount': 'sum',
        'order_id': 'nunique',
        'quantity': 'sum'
    }).round(2)
    
    daily_metrics.columns = ['daily_revenue', 'daily_orders', 'daily_units']
    daily_metrics['aov'] = daily_metrics['daily_revenue'] / daily_metrics['daily_orders']
    
    # Rolling trends
    daily_metrics['revenue_7d_avg'] = daily_metrics['daily_revenue'].rolling(7).mean()
    daily_metrics['orders_7d_avg'] = daily_metrics['daily_orders'].rolling(7).mean()
    
    # Growth rates
    daily_metrics['revenue_wow_growth'] = daily_metrics['daily_revenue'].pct_change(7) * 100
    daily_metrics['orders_wow_growth'] = daily_metrics['daily_orders'].pct_change(7) * 100
    
    recent_metrics = daily_metrics.tail(14).round(2)
    
    return recent_metrics.reset_index()


def calculate_growth_metrics(df: pd.DataFrame, periods: List[str] = ['7D', '30D']) -> pd.DataFrame:
    """
    Calculate growth metrics over multiple periods.
    
    Args:
        df: Input DataFrame
        periods: Growth periods to calculate
        
    Returns:
        Growth metrics DataFrame
    """
    if 'order_date' not in df.columns:
        return pd.DataFrame()
    
    df['date_only'] = df['order_date'].dt.date
    daily_data = df.groupby('date_only')['total_amount'].sum().reset_index()
    daily_data['date_only'] = pd.to_datetime(daily_data['date_only'])
    
    growth_metrics = []
    
    for period in periods:
        days = int(period[:-1])
        daily_data[f'{period}_ago'] = daily_data['total_amount'].shift(days)
        daily_data[f'{period}_growth'] = (
            (daily_data['total_amount'] - daily_data[f'{period}_ago']) / 
            daily_data[f'{period}_ago'].replace(0, np.nan) * 100
        )
        
        period_summary = {
            f'{period}_avg_revenue': daily_data['total_amount'].tail(days).mean(),
            f'{period}_total_revenue': daily_data['total_amount'].tail(days).sum(),
            f'{period}_avg_growth': daily_data[f'{period}_growth'].tail(days).mean(),
            f'{period}_growth_trend': daily_data[f'{period}_growth'].tail(7).mean()
        }
        growth_metrics.append(period_summary)
    
    return pd.DataFrame(growth_metrics)