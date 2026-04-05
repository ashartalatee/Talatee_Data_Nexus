"""
Date normalization module.
Handles multiple date formats, timezones, and date feature extraction.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value
from utils.constants import DATE_COLUMNS, DEFAULT_TIMEZONE


def normalize_date_columns(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Normalize all date columns to standard datetime format.
    
    Args:
        df: DataFrame with date columns
        config: Client configuration
        
    Returns:
        DataFrame with normalized dates
    """
    logger = setup_logger("transform.date_normalizer")
    logger.info("📅 Normalizing date columns...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    df_norm = df.copy()
    date_format = get_config_value(config, 'data_cleaning.date_format', '%Y-%m-%d')
    timezone = get_config_value(config, 'timezone', DEFAULT_TIMEZONE)
    
    logger.info(f"⚙️ Target format: {date_format}, Timezone: {timezone}")
    
    # Find and normalize date columns
    date_cols = _identify_date_columns(df_norm)
    
    stats = {'normalized': 0, 'failed': 0, 'new_features': 0}
    
    for col in date_cols:
        if col in df_norm.columns:
            result = _normalize_column(df_norm[col], date_format, timezone)
            df_norm[col] = result['normalized']
            stats['normalized'] += result['success_count']
            stats['failed'] += result['fail_count']
            
            # Add date features
            feature_cols = _extract_date_features(result['normalized'], col)
            for feature_col in feature_cols:
                df_norm[feature_col] = feature_cols[feature_col]
                stats['new_features'] += 1
    
    logger.info(f"✅ Date normalization: {stats['normalized']} success, {stats['failed']} failed")
    logger.info(f"✨ Added {stats['new_features']} date features")
    
    safe_log_dataframe(logger, "date_normalized", df_norm[[col for col in df_norm.columns if 'date' in col.lower()]].head())
    
    return df_norm


def _identify_date_columns(df: pd.DataFrame) -> List[str]:
    """Identify potential date columns."""
    
    candidates = []
    
    # Columns with date indicators in name
    date_keywords = ['date', 'time', 'tanggal', 'create', 'update', 'order']
    for col in df.columns:
        if any(keyword in col.lower() for keyword in date_keywords):
            candidates.append(col)
    
    # Object columns that look like dates
    for col in df.select_dtypes(include=['object']).columns:
        if col not in candidates:
            sample = df[col].dropna().head(10)
            if sample.apply(_is_likely_date).any():
                candidates.append(col)
    
    return list(set(candidates))


def _normalize_column(series: pd.Series, date_format: str, timezone: str) -> Dict[str, pd.Series]:
    """Normalize single date column."""
    
    # Try multiple common formats
    formats_to_try = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%m/%d/%Y',
        '%d-%m-%Y',
        '%Y%m%d',
        'ISO8601'
    ]
    
    normalized = pd.Series(dtype='datetime64[ns]')
    
    for fmt in formats_to_try:
        mask = series.isna() | normalized.notna()
        try_date = pd.to_datetime(series[mask], format=fmt, errors='coerce')
        
        if try_date.notna().sum() > normalized.notna().sum():
            normalized[mask] = try_date
            if normalized.notna().sum() == mask.sum():
                break
                
    # Final fallback
    remaining_mask = normalized.isna()
    if remaining_mask.any():
        fallback = pd.to_datetime(series[remaining_mask], errors='coerce', infer_datetime_format=True)
        normalized[remaining_mask] = fallback
    
    # Localize to timezone
    try:
        normalized = normalized.dt.tz_localize(None).dt.tz_localize(timezone, ambiguous='infer')
    except:
        pass  # Keep as naive datetime if timezone fails
    
    return {
        'normalized': normalized,
        'success_count': normalized.notna().sum(),
        'fail_count': normalized.isna().sum()
    }


def _is_likely_date(value: str) -> bool:
    """Check if value looks like a date."""
    
    if pd.isna(value):
        return False
    
    try:
        pd.to_datetime(value)
        return True
    except:
        return False


def _extract_date_features(date_series: pd.Series, prefix: str) -> Dict[str, pd.Series]:
    """
    Extract comprehensive date features.
    
    Args:
        date_series: Normalized datetime series
        prefix: Column prefix for feature names
        
    Returns:
        Dictionary of feature columns
    """
    features = {}
    
    valid_dates = date_series.dropna()
    if valid_dates.empty:
        return features
    
    # Basic components
    features[f'{prefix}_dayofweek'] = date_series.dt.dayofweek
    features[f'{prefix}_day'] = date_series.dt.day
    features[f'{prefix}_month'] = date_series.dt.month
    features[f'{prefix}_quarter'] = date_series.dt.quarter
    features[f'{prefix}_year'] = date_series.dt.year
    features[f'{prefix}_weekofyear'] = date_series.dt.isocalendar().week
    
    # Business features
    features[f'{prefix}_is_weekend'] = (date_series.dt.dayofweek >= 5).astype(int)
    features[f'{prefix}_is_month_start'] = date_series.dt.is_month_start.astype(int)
    features[f'{prefix}_is_month_end'] = date_series.dt.is_month_end.astype(int)
    features[f'{prefix}_days_in_month'] = date_series.dt.days_in_month
    
    # Time features (if datetime)
    if date_series.dt.hour.notna().any():
        features[f'{prefix}_hour'] = date_series.dt.hour
        features[f'{prefix}_is_peak_hour'] = ((date_series.dt.hour >= 18) | (date_series.dt.hour <= 9)).astype(int)
    
    return features


def date_range_summary(df: pd.DataFrame, date_col: str = 'order_date') -> Dict[str, Any]:
    """
    Generate date range quality summary.
    
    Args:
        df: DataFrame with date column
        date_col: Date column name
        
    Returns:
        Date range statistics
    """
    if date_col not in df.columns or df[date_col].isna().all():
        return {}
    
    valid_dates = df[date_col].dropna()
    
    return {
        'date_column': date_col,
        'valid_dates': len(valid_dates),
        'valid_pct': f"{len(valid_dates)/len(df)*100:.1f}%",
        'min_date': valid_dates.min(),
        'max_date': valid_dates.max(),
        'date_span_days': (valid_dates.max() - valid_dates.min()).days,
        'date_gaps': _detect_date_gaps(valid_dates).sum()
    }


def _detect_date_gaps(dates: pd.Series, freq: str = 'D') -> pd.Series:
    """Detect missing dates in time series."""
    
    date_range = pd.date_range(start=dates.min(), end=dates.max(), freq=freq)
    daily_counts = dates.resample(freq).size()
    gaps = date_range[~date_range.isin(dates)]
    
    return pd.Series(1, index=gaps)