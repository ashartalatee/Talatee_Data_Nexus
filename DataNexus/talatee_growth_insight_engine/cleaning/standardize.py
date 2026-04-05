"""
Main data standardization pipeline.
Orchestrates cleaning modules: missing values, duplicates, text cleaning, column mapping.
"""
from pathlib import Path
import pandas as pd
import logging
from typing import Dict, Any, Optional
from utils.logger import setup_logger, safe_log_dataframe, safe_dataframe_operation
from utils.config_loader import get_config_value
from utils.constants import STANDARD_COLUMNS, NUMERIC_COLUMNS, DATE_COLUMNS, REQUIRED_COLUMNS
from .missing_handler import handle_missing_values
from .duplicate_handler import remove_duplicates
from .text_cleaner import clean_text_columns
from transform.column_mapper import map_columns_to_standard
from transform.date_normalizer import normalize_date_columns


@safe_dataframe_operation
def clean_and_standardize(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Complete data standardization pipeline.
    
    Args:
        df: Raw DataFrame from ingestion
        config: Client configuration
        
    Returns:
        Cleaned and standardized DataFrame
    """
    logger = setup_logger("cleaning.standardize")
    logger.info("🧹 Starting full standardization pipeline...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    original_rows = len(df)
    logger.info(f"📊 Input: {original_rows} rows, {len(df.columns)} columns")
    
    # Pipeline stages
    pipeline_stages = [
        ("Column Mapping", map_columns_to_standard),
        ("Date Normalization", normalize_date_columns),
        ("Missing Values", handle_missing_values),
        ("Duplicates", remove_duplicates),
        ("Text Cleaning", clean_text_columns)
    ]
    
    cleaned_df = df.copy()
    
    for stage_name, stage_func in pipeline_stages:
        logger.info(f"🔄 Stage: {stage_name}")
        
        result = stage_func(cleaned_df, config)
        if result is None or result.empty:
            logger.error(f"❌ Stage failed: {stage_name}")
            return None
        
        cleaned_df = result
        safe_log_dataframe(logger, f"after_{stage_name.lower().replace(' ', '_')}", cleaned_df)
    
    # Final validation
    final_stats = _validate_standardized_data(cleaned_df, config, original_rows, logger)
    if not final_stats['valid']:
        logger.error("❌ Final validation failed")
        return None
    
    # Save cleaned data
    cleaned_path = _save_cleaned_data(cleaned_df, config, logger)
    
    logger.info(f"✅ Standardization complete: {final_stats}")
    logger.info(f"💾 Cleaned data saved: {cleaned_path}")
    
    return cleaned_df


def _validate_standardized_data(df: pd.DataFrame, config: Dict[str, Any], 
                               original_rows: int, logger: logging.Logger) -> Dict[str, Any]:
    """Validate standardized data meets quality standards."""
    
    stats = {
        'original_rows': original_rows,
        'final_rows': len(df),
        'final_columns': len(df.columns),
        'row_retention': round(len(df) / original_rows * 100, 1),
        'required_columns': True,
        'data_types_correct': True,
        'valid_dates': True,
        'valid_amounts': True
    }
    
    # Check required columns
    missing_required = REQUIRED_COLUMNS - set(df.columns)
    stats['required_columns'] = len(missing_required) == 0
    if missing_required:
        logger.warning(f"⚠️ Missing required columns: {missing_required}")
    
    # Check data types
    for col, dtype in STANDARD_COLUMNS.items():
        if col in df.columns:
            if dtype == 'numeric' and df[col].dtype not in ['float64', 'int64']:
                stats['data_types_correct'] = False
            elif col in DATE_COLUMNS and not pd.api.types.is_datetime64_any_dtype(df[col]):
                stats['data_types_correct'] = False
    
    # Check date range
    if 'order_date' in df.columns:
        valid_dates = df['order_date'].notna().sum()
        stats['valid_dates'] = valid_dates > 0
        stats['date_range'] = f"{df['order_date'].min()} to {df['order_date'].max()}"
    
    # Check amounts
    if 'total_amount' in df.columns:
        positive_amounts = (df['total_amount'] > 0).sum()
        stats['valid_amounts'] = positive_amounts > 0
        stats['positive_transactions'] = positive_amounts
    
    # Overall quality score
    quality_score = min(stats['row_retention'], 100)
    if stats['required_columns']:
        quality_score *= 0.9
    stats['quality_score'] = round(quality_score, 1)
    
    logger.info(f"📊 Quality: {stats['quality_score']}% | Rows retained: {stats['row_retention']}%")
    
    return stats


def _save_cleaned_data(df: pd.DataFrame, config: Dict[str, Any], 
                      logger: logging.Logger) -> Path:
    """Save cleaned data to processed location."""
    
    from utils.config_loader import get_output_dir
    
    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    cleaned_path = get_output_dir(config) / f"cleaned_{config['client_id']}_{timestamp}.parquet"
    
    # Save as efficient Parquet format
    df.to_parquet(cleaned_path, index=False, engine='pyarrow')
    
    # Also save CSV backup
    csv_path = cleaned_path.with_suffix('.csv')
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    logger.info(f"💾 Saved cleaned data: {cleaned_path}")
    return cleaned_path


def get_data_quality_report(df: pd.DataFrame, stats: Dict[str, Any]) -> pd.DataFrame:
    """
    Generate data quality summary report.
    
    Args:
        df: Cleaned DataFrame
        stats: Validation statistics
        
    Returns:
        Quality report DataFrame
    """
    report_data = {
        'Metric': [
            'Original Rows', 'Final Rows', 'Row Retention %',
            'Columns Count', 'Required Columns OK', 'Data Types OK',
            'Valid Dates', 'Positive Transactions', 'Quality Score %'
        ],
        'Value': [
            stats['original_rows'],
            stats['final_rows'], 
            f"{stats['row_retention']}%",
            stats['final_columns'],
            '✅' if stats['required_columns'] else '❌',
            '✅' if stats['data_types_correct'] else '❌',
            f"{stats.get('valid_dates', False)}",
            stats.get('positive_transactions', 0),
            f"{stats['quality_score']}%"
        ]
    }
    
    return pd.DataFrame(report_data)