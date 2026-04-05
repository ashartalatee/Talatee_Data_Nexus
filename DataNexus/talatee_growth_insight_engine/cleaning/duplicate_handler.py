"""
Duplicate detection and removal module.
Configurable duplicate handling with multiple key strategies.
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional, List
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value
from utils.constants import DUPLICATE_COLUMNS


def remove_duplicates(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Remove duplicates based on configurable column keys.
    
    Args:
        df: DataFrame potentially containing duplicates
        config: Client configuration
        
    Returns:
        Deduplicated DataFrame
    """
    logger = setup_logger("cleaning.duplicate_handler")
    logger.info("🔍 Detecting and removing duplicates...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    original_rows = len(df)
    
    # Configurable duplicate columns
    dup_columns = get_config_value(
        config, 'data_cleaning.remove_duplicates.columns', DUPLICATE_COLUMNS
    )
    
    # Filter to existing columns only
    dup_columns = [col for col in dup_columns if col in df.columns]
    
    if not dup_columns:
        logger.warning("⚠️ No duplicate columns specified or available")
        return df
    
    keep_strategy = get_config_value(config, 'data_cleaning.remove_duplicates.keep', 'first')
    
    logger.info(f"⚙️ Columns: {dup_columns}, Keep: {keep_strategy}")
    
    # Detect duplicates
    before_dup_count = df.duplicated(subset=dup_columns, keep=False).sum()
    logger.info(f"📊 Duplicate rows detected: {before_dup_count}")
    
    if before_dup_count == 0:
        logger.info("✅ No duplicates found")
        return df
    
    # Remove duplicates
    dedup_df = df.drop_duplicates(
        subset=dup_columns,
        keep=keep_strategy,
        ignore_index=True
    )
    
    removed_count = original_rows - len(dedup_df)
    removal_pct = (removed_count / original_rows * 100)
    
    logger.info(f"✅ Duplicates removed: {removed_count} ({removal_pct:.1f}%)")
    safe_log_dataframe(logger, "deduplicated", dedup_df)
    
    # Generate duplicate report
    dup_report = _generate_duplicate_report(df, dup_columns)
    logger.debug(f"📋 Duplicate report:\n{dup_report.to_string()}")
    
    return dedup_df


def _generate_duplicate_report(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Generate detailed duplicate analysis report."""
    
    dup_mask = df.duplicated(subset=columns, keep=False)
    dup_data = df[dup_mask].copy()
    
    if dup_data.empty:
        return pd.DataFrame()
    
    report = dup_data.groupby(columns).size().reset_index(name='duplicate_count')
    report = report[report['duplicate_count'] > 1].sort_values('duplicate_count', ascending=False)
    
    return report.head(10)


def detect_duplicates(df: pd.DataFrame, columns: Optional[List[str]] = None, 
                     threshold: int = 2) -> Dict[str, Any]:
    """
    Comprehensive duplicate detection across multiple strategies.
    
    Args:
        df: Input DataFrame
        columns: Specific columns to check (default: auto-detect)
        threshold: Minimum count to flag as duplicate
        
    Returns:
        Duplicate detection statistics
    """
    logger = setup_logger("cleaning.duplicates.detect")
    
    strategies = {
        'order_keys': ['order_id', 'product_id'],
        'transaction_keys': ['order_id', 'customer_id'],
        'product_keys': ['product_id', 'platform']
    }
    
    if columns:
        strategies['custom'] = columns
    
    results = {}
    
    for strategy_name, cols in strategies.items():
        available_cols = [col for col in cols if col in df.columns]
        if len(available_cols) == len(cols):
            dup_count = df.duplicated(subset=available_cols, keep=False).sum()
            unique_combos = df[available_cols].drop_duplicates().shape[0]
            results[strategy_name] = {
                'columns': available_cols,
                'duplicates': dup_count,
                'unique_combinations': unique_combos,
                'duplicate_pct': round(dup_count / len(df) * 100, 2)
            }
    
    logger.info(f"🔍 Duplicate detection: {len(results)} strategies checked")
    return results


def smart_dedupe(df: pd.DataFrame, config: Dict[str, Any], 
                strategies: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Advanced deduplication using multiple strategies sequentially.
    
    Args:
        df: Input DataFrame
        config: Configuration
        strategies: List of strategies to apply
        
    Returns:
        Deduplicated DataFrame
    """
    logger = setup_logger("cleaning.duplicates.smart")
    
    if strategies is None:
        strategies = ['strict', 'lenient']
    
    result_df = df.copy()
    applied_strategies = []
    
    for strategy in strategies:
        if strategy == 'strict':
            cols = ['order_id', 'product_id']
            keep = 'last'  # Keep most recent
        elif strategy == 'lenient':
            cols = ['order_id']
            keep = 'first'
        else:
            continue
        
        available_cols = [col for col in cols if col in result_df.columns]
        if len(available_cols) >= 1:
            rows_before = len(result_df)
            result_df = result_df.drop_duplicates(subset=available_cols, keep=keep, ignore_index=True)
            removed = rows_before - len(result_df)
            
            if removed > 0:
                applied_strategies.append(f"{strategy}: {removed} rows")
                logger.info(f"✅ {strategy}: removed {removed} duplicates")
    
    logger.info(f"🎯 Smart dedupe applied: {', '.join(applied_strategies)}")
    return result_df


def duplicate_summary_report(df_before: pd.DataFrame, df_after: pd.DataFrame, 
                           columns: List[str]) -> pd.DataFrame:
    """
    Generate before/after duplicate removal summary.
    
    Args:
        df_before: DataFrame before deduplication
        df_after: DataFrame after deduplication  
        columns: Duplicate detection columns
        
    Returns:
        Summary report DataFrame
    """
    summary = {
        'Metric': [
            'Total Rows Before',
            'Total Rows After', 
            'Rows Removed',
            'Reduction %',
            'Unique Combinations Before',
            'Unique Combinations After'
        ],
        'Value': [
            len(df_before),
            len(df_after),
            len(df_before) - len(df_after),
            f"{((len(df_before) - len(df_after)) / len(df_before) * 100):.1f}%",
            df_before[columns].drop_duplicates().shape[0],
            df_after[columns].drop_duplicates().shape[0]
        ]
    }
    
    return pd.DataFrame(summary)