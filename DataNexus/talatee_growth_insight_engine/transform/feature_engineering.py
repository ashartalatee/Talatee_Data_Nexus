"""
Comprehensive feature engineering module.
Creates business metrics, trends, aggregations, and time-based features.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value
from utils.constants import ROLLING_WINDOWS, GROWTH_METRICS, DATE_FEATURES


def transform_features(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Complete feature engineering pipeline.
    
    Args:
        df: Cleaned and standardized DataFrame
        config: Client configuration
        
    Returns:
        Feature-engineered DataFrame
    """
    logger = setup_logger("transform.feature_engineering")
    logger.info("🔬 Starting feature engineering...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    df_features = df.copy()
    
    # Pipeline stages
    stages = [
        ("Business Metrics", _create_business_metrics),
        ("Time Features", _create_time_features),
        ("Rolling Aggregations", _create_rolling_features),
        ("Platform Features", _create_platform_features),
        ("Product Features", _create_product_features),
        ("Customer Features", _create_customer_features)
    ]
    
    feature_count = 0
    for stage_name, stage_func in stages:
        logger.info(f"🔄 {stage_name}...")
        result = stage_func(df_features, config)
        if result is not None:
            df_features = pd.concat([df_features, result], axis=1)
            feature_count += len(result.columns)
    
    logger.info(f"✅ Feature engineering complete: {feature_count} new features created")
    safe_log_dataframe(logger, "features_sample", df_features.select_dtypes(include=[np.number]).tail())
    
    return df_features


def _create_business_metrics(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Create core business metrics."""
    
    features = pd.DataFrame(index=df.index)
    
    # Basic metrics
    features['avg_order_value'] = df['total_amount'] / df['quantity']
    features['revenue_per_order'] = df.groupby('order_id')['total_amount'].transform('sum')
    
    # Margin proxy (if cost data available or estimated)
    if 'cost' in df.columns:
        features['gross_margin'] = (df['total_amount'] - df['cost']) / df['total_amount']
    else:
        features['gross_margin'] = 0.3  # Default 30% margin
    
    # Status-based filtering
    completed_mask = df['status'].str.contains('complete|delivered|lunas', case=False, na=False)
    features['is_completed'] = completed_mask.astype(int)
    
    return features


def _create_time_features(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Enhanced time-based features."""
    
    date_features = get_config_value(config, 'feature_engineering.date_features', DATE_FEATURES)
    features = pd.DataFrame(index=df.index)
    
    if 'order_date' in df.columns:
        valid_dates = df['order_date'].dropna()
        
        # Lag features
        features['day_lag_1'] = df['order_date'].diff().dt.days.fillna(0)
        features['is_holiday_season'] = (
            (df['order_date'].dt.month.isin([12, 1])) | 
            (df['order_date'].dt.quarter == 4)
        ).astype(int)
        
        # Time since first order per customer
        first_order_date = df.groupby('customer_id')['order_date'].transform('min')
        features['days_since_first_order'] = (df['order_date'] - first_order_date).dt.days.fillna(0)
    
    return features


def _create_rolling_features(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Rolling window aggregations."""
    
    windows = get_config_value(config, 'feature_engineering.rolling_windows', ROLLING_WINDOWS)
    metrics = get_config_value(config, 'feature_engineering.growth_metrics', GROWTH_METRICS)
    
    features = pd.DataFrame(index=df.index)
    
    if 'order_date' not in df.columns:
        return features
    
    # Sort by date for rolling calculations
    df_sorted = df.sort_values('order_date').reset_index(drop=True)
    
    for window in windows:
        for metric in ['total_amount', 'quantity']:
            if metric in df_sorted.columns:
                # Rolling sum
                features[f'{metric}_rolling_{window}d'] = (
                    df_sorted.groupby(['customer_id', 'platform'])[metric]
                    .rolling(window=window, min_periods=1)
                    .sum()
                    .reset_index(0, drop=True)
                )
                
                # Rolling mean
                features[f'{metric}_rolling_avg_{window}d'] = (
                    df_sorted.groupby(['customer_id', 'platform'])[metric]
                    .rolling(window=window, min_periods=1)
                    .mean()
                    .reset_index(0, drop=True)
                )
    
    # Align back to original index
    features = features.reindex(df.index, fill_value=0)
    
    return features


def _create_platform_features(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Platform-specific features."""
    
    features = pd.DataFrame(index=df.index)
    
    if 'platform' not in df.columns:
        return features
    
    # Platform revenue share
    platform_rev = df.groupby('platform')['total_amount'].transform('sum')
    total_rev = df['total_amount'].sum()
    features['platform_rev_share'] = platform_rev / total_rev
    
    # Platform rank
    platform_order_count = df['platform'].map(
        df.groupby('platform').size().rank(ascending=False)
    )
    features['platform_rank'] = platform_order_count.fillna(0).astype(int)
    
    return features


def _create_product_features(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Product performance features."""
    
    features = pd.DataFrame(index=df.index)
    
    if 'product_id' not in df.columns:
        return features
    
    # Product performance ranks
    product_metrics = df.groupby('product_id').agg({
        'total_amount': ['count', 'sum', 'mean'],
        'quantity': 'sum'
    }).round(2)
    
    product_metrics.columns = ['product_orders', 'product_revenue', 'avg_order_value', 'product_units']
    product_metrics['revenue_rank'] = product_metrics['product_revenue'].rank(ascending=False)
    product_metrics['popularity_rank'] = product_metrics['product_orders'].rank(ascending=False)
    
    # Map back to transactions
    for metric in ['revenue_rank', 'popularity_rank']:
        features[metric] = df['product_id'].map(product_metrics[metric]).fillna(999)
    
    # Product recency
    product_last_sale = df.groupby('product_id')['order_date'].transform('max')
    features['product_recency_days'] = (pd.Timestamp.now() - product_last_sale).dt.days.fillna(0)
    
    return features


def _create_customer_features(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """Customer segmentation features."""
    
    features = pd.DataFrame(index=df.index)
    
    if 'customer_id' not in df.columns:
        return features
    
    # Customer lifetime value proxy
    customer_stats = df.groupby('customer_id')['total_amount'].agg([
        'count', 'sum', 'mean', 'std'
    ]).round(2)
    
    customer_stats.columns = ['customer_orders', 'customer_ltv', 'avg_customer_value', 'value_std']
    customer_stats['customer_segment'] = pd.qcut(customer_stats['customer_ltv'], 
                                               q=4, labels=['Bronze', 'Silver', 'Gold', 'Platinum'])
    
    # RFM scores (simplified)
    customer_stats['recency'] = (pd.Timestamp.now() - 
                                df.groupby('customer_id')['order_date'].transform('max')).dt.days
    customer_stats['frequency'] = customer_stats['customer_orders']
    customer_stats['monetary'] = customer_stats['customer_ltv']
    
    # Map back
    for metric in ['customer_ltv', 'customer_orders', 'recency']:
        features[metric] = df['customer_id'].map(customer_stats[metric]).fillna(0)
    
    features['customer_segment'] = df['customer_id'].map(customer_stats['customer_segment']).fillna('New')
    
    return features


def feature_importance_summary(df_features: pd.DataFrame) -> pd.DataFrame:
    """
    Generate feature summary with basic statistics.
    
    Args:
        df_features: Feature engineered DataFrame
        
    Returns:
        Feature summary DataFrame
    """
    numeric_features = df_features.select_dtypes(include=[np.number]).columns
    
    summary = pd.DataFrame({
        'feature': numeric_features,
        'dtype': df_features[numeric_features].dtypes,
        'missing_pct': (df_features[numeric_features].isnull().sum() / len(df_features) * 100).round(2),
        'unique_values': df_features[numeric_features].nunique(),
        'mean': df_features[numeric_features].mean().round(2),
        'std': df_features[numeric_features].std().round(2),
        'min': df_features[numeric_features].min().round(2),
        'max': df_features[numeric_features].max().round(2)
    }).sort_values('std', ascending=False)
    
    return summary.head(20)