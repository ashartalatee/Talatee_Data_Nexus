"""
Missing value handler module.
Configurable strategies: forward_fill, backward_fill, mean, median, drop.
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional
from utils.logger import setup_logger, safe_log_dataframe
from utils.constants import NUMERIC_COLUMNS, DATE_COLUMNS
from utils.config_loader import get_config_value


def handle_missing_values(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Handle missing values based on config strategy.
    
    Args:
        df: DataFrame with potential missing values
        config: Client configuration
        
    Returns:
        DataFrame with handled missing values
    """
    logger = setup_logger("cleaning.missing_handler")
    logger.info("🔍 Handling missing values...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    original_missing = df.isnull().sum().sum()
    logger.info(f"📊 Original missing values: {original_missing}")
    
    cleaned_df = df.copy()
    
    # Configurable settings
    strategy = get_config_value(config, 'data_cleaning.handle_missing.strategy', 'forward_fill')
    threshold = get_config_value(config, 'data_cleaning.handle_missing.threshold', 0.3)
    
    logger.info(f"⚙️ Strategy: {strategy}, Threshold: {threshold*100}%")
    
    # 1. Drop columns with excessive missing values
    cleaned_df = _drop_high_missing_columns(cleaned_df, threshold, logger)
    
    # 2. Handle row-wise missing (drop rows with all missing)
    rows_before = len(cleaned_df)
    cleaned_df = cleaned_df.dropna(how='all')
    logger.info(f"🗑️ Dropped {rows_before - len(cleaned_df)} all-missing rows")
    
    # 3. Column-specific strategies
    cleaned_df = _apply_column_strategies(cleaned_df, strategy, config, logger)
    
    # 4. Final fill for remaining NaNs
    cleaned_df = _final_fill_strategy(cleaned_df, strategy, logger)
    
    final_missing = cleaned_df.isnull().sum().sum()
    reduction = (1 - final_missing / original_missing * 100) if original_missing > 0 else 100
    
    logger.info(f"✅ Missing values handled: {original_missing} → {final_missing} ({reduction:.1f}% reduction)")
    
    return cleaned_df


def _drop_high_missing_columns(df: pd.DataFrame, threshold: float, 
                             logger: logging.Logger) -> pd.DataFrame:
    """Drop columns with missing values above threshold."""
    
    missing_pct = df.isnull().sum() / len(df)
    high_missing_cols = missing_pct[missing_pct > threshold].index.tolist()
    
    if high_missing_cols:
        logger.warning(f"🗑️ Dropping {len(high_missing_cols)} high-missing columns (> {threshold*100}%): {high_missing_cols[:5]}")
        df = df.drop(columns=high_missing_cols)
    
    return df


def _apply_column_strategies(df: pd.DataFrame, default_strategy: str, 
                           config: Dict[str, Any], logger: logging.Logger) -> pd.DataFrame:
    """Apply specific strategies to different column types."""
    
    df_clean = df.copy()
    
    # Numeric columns: mean/median
    numeric_strategy = get_config_value(config, 'data_cleaning.numeric_strategy', 'mean')
    for col in df.select_dtypes(include=['number']).columns:
        if col in df_clean.columns:
            df_clean[col] = _fill_numeric(df_clean[col], numeric_strategy)
    
    # Date columns: forward fill
    for col in DATE_COLUMNS.intersection(df_clean.columns):
        df_clean[col] = df_clean[col].fillna(method='ffill').fillna(method='bfill')
    
    # Categorical/text: mode or default
    cat_cols = df_clean.select_dtypes(include=['object', 'category']).columns
    for col in cat_cols:
        if col in df_clean.columns:
            df_clean[col] = _fill_categorical(df_clean[col])
    
    return df_clean


def _fill_numeric(series: pd.Series, strategy: str) -> pd.Series:
    """Fill numeric column based on strategy."""
    
    if strategy == 'mean':
        fill_value = series.mean()
    elif strategy == 'median':
        fill_value = series.median()
    else:
        fill_value = series.median()
    
    return series.fillna(fill_value)


def _fill_categorical(series: pd.Series, default_fill: str = 'Unknown') -> pd.Series:
    """Fill categorical column with mode or default."""
    
    if series.dtype == 'category':
        series = series.astype(str)
    
    # Get mode, if no mode use default
    mode_value = series.mode()
    fill_value = mode_value[0] if len(mode_value) > 0 else default_fill
    
    return series.fillna(fill_value)


def _final_fill_strategy(df: pd.DataFrame, strategy: str, logger: logging.Logger) -> pd.DataFrame:
    """Apply final fill strategy to remaining NaNs."""
    
    df_final = df.copy()
    
    if strategy == 'drop':
        rows_before = len(df_final)
        df_final = df_final.dropna()
        logger.info(f"🗑️ Final dropna: {rows_before - len(df_final)} rows dropped")
    
    elif strategy == 'zero':
        numeric_cols = df_final.select_dtypes(include=['number']).columns
        df_final[numeric_cols] = df_final[numeric_cols].fillna(0)
    
    elif strategy == 'forward_fill':
        df_final = df_final.fillna(method='ffill').fillna(method='bfill')
    
    else:  # default: forward_fill + backward_fill
        df_final = df_final.fillna(method='ffill').fillna(method='bfill')
    
    return df_final


def generate_missing_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate comprehensive missing data report.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Missing data summary DataFrame
    """
    if df.empty:
        return pd.DataFrame()
    
    missing_stats = pd.DataFrame({
        'column': df.columns,
        'missing_count': df.isnull().sum(),
        'missing_pct': (df.isnull().sum() / len(df) * 100).round(2),
        'dtype': df.dtypes,
        'unique_count': df.nunique()
    }).sort_values('missing_pct', ascending=False)
    
    return missing_stats


def get_missing_summary(missing_report: pd.DataFrame) -> Dict[str, Any]:
    """Get high-level missing data summary."""
    
    total_missing = missing_report['missing_count'].sum()
    total_cells = len(missing_report) * missing_report['missing_count'].index.nunique()
    overall_pct = (total_missing / total_cells * 100) if total_cells > 0 else 0
    
    high_missing = missing_report[missing_report['missing_pct'] > 10]
    
    return {
        'total_missing': int(total_missing),
        'overall_percentage': round(overall_pct, 2),
        'high_missing_columns': len(high_missing),
        'top_missing': high_missing.head(5).to_dict('records')
    }