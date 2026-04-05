"""
Intelligent column mapping module.
Automatically maps source-specific columns to standardized names.
"""

import pandas as pd
import logging
from typing import Dict, Any, Optional, List
from fuzzywuzzy import fuzz, process
from utils.logger import setup_logger, safe_log_dataframe
from utils.constants import STANDARD_COLUMNS
from utils.config_loader import get_config_value


def map_columns_to_standard(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Map source columns to standardized column names.
    
    Args:
        df: Raw DataFrame with source-specific columns
        config: Client configuration with standard_columns mapping
        
    Returns:
        DataFrame with standardized column names
    """
    logger = setup_logger("transform.column_mapper")
    logger.info("🔄 Mapping columns to standard format...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    standard_mapping = config.get('standard_columns', STANDARD_COLUMNS)
    mapped_cols = []
    
    df_mapped = pd.DataFrame()
    
    for standard_col, source_variations in standard_mapping.items():
        found_col = _find_best_column_match(df.columns, source_variations, standard_col, logger)
        
        if found_col and found_col in df.columns:
            df_mapped[standard_col] = df[found_col]
            mapped_cols.append(f"{found_col} → {standard_col}")
        else:
            # Try to infer
            inferred = _infer_column(df, standard_col)
            if inferred:
                df_mapped[standard_col] = inferred
                mapped_cols.append(f"[INFERRED] → {standard_col}")
            else:
                logger.warning(f"⚠️ No mapping found for '{standard_col}'")
    
    # Add unmapped columns with prefix
    unmapped_cols = [col for col in df.columns if col not in df_mapped.columns.values]
    for col in unmapped_cols:
        df_mapped[f"raw_{col}"] = df[col]
    
    logger.info(f"✅ Column mapping complete: {len(mapped_cols)} columns mapped")
    logger.info(f"📋 Mappings: {mapped_cols[:10]}{'...' if len(mapped_cols)>10 else ''}")
    
    safe_log_dataframe(logger, "column_mapped", df_mapped)
    
    return df_mapped


def _find_best_column_match(columns: pd.Index, variations: str, standard_name: str, 
                          logger: logging.Logger) -> Optional[str]:
    """
    Find best matching column using fuzzy matching.
    
    Args:
        columns: Available column names
        variations: Expected variations (comma-separated)
        standard_name: Target standard name
        
    Returns:
        Best matching column name or None
    """
    variations_list = [v.strip() for v in str(variations).split(',')]
    
    # Exact match first
    for col in columns:
        if col.lower() in [v.lower() for v in variations_list]:
            return col
    
    # Fuzzy matching
    if len(columns) > 0:
        best_match, score = process.extractOne(
            standard_name.lower(), 
            columns.str.lower(),
            scorer=fuzz.token_sort_ratio
        )
        
        if score > 80:  # Threshold for good match
            logger.debug(f"🔍 Fuzzy match: '{standard_name}' → '{best_match}' ({score})")
            return best_match
    
    return None


def _infer_column(df: pd.DataFrame, standard_col: str) -> Optional[pd.Series]:
    """Infer column content based on patterns."""
    
    if standard_col == 'order_date':
        date_cols = _find_date_like_columns(df)
        return pd.to_datetime(df[date_cols[0]], errors='coerce') if date_cols else None
    
    elif standard_col == 'total_amount':
        numeric_cols = df.select_dtypes(include=['number']).columns
        # Find column with values likely representing revenue
        for col in numeric_cols:
            if df[col].mean() > 0 and df[col].std() > 0:
                return df[col]
    
    elif standard_col == 'quantity':
        int_cols = df.select_dtypes(include=['int']).columns
        for col in int_cols:
            if df[col].median() <= 10:  # Reasonable quantity range
                return df[col]
    
    return None


def _find_date_like_columns(df: pd.DataFrame) -> List[str]:
    """Find columns likely containing dates."""
    
    date_indicators = ['date', 'time', 'tanggal', 'create', 'update', 'order']
    candidates = []
    
    for col in df.columns:
        col_lower = col.lower()
        if any(indicator in col_lower for indicator in date_indicators):
            candidates.append(col)
    
    # Check for date-like values
    for col in df.select_dtypes(include=['object']).columns:
        sample = df[col].dropna().head(10)
        if any(_looks_like_date(str(x)) for x in sample):
            candidates.append(col)
    
    return candidates[:3]  # Top 3 candidates


def _looks_like_date(text: str) -> bool:
    """Check if text looks like a date."""
    
    date_patterns = [
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}',  # MM/DD/YYYY or DD/MM/YYYY
        r'\d{4}-\d{1,2}-\d{1,2}',           # YYYY-MM-DD
        r'\d{1,2}:\d{2}',                   # Time
    ]
    
    return any(re.search(pattern, text) for pattern in date_patterns)


def generate_mapping_report(df_before: pd.DataFrame, df_after: pd.DataFrame, 
                          mapping_used: Dict[str, str]) -> pd.DataFrame:
    """
    Generate column mapping summary report.
    
    Args:
        df_before: Original DataFrame
        df_after: Mapped DataFrame
        mapping_used: Applied mapping
        
    Returns:
        Mapping report DataFrame
    """
    report_data = []
    
    for standard_col, source_col in mapping_used.items():
        if source_col in df_before.columns:
            before_unique = df_before[source_col].nunique()
            after_unique = df_after[standard_col].nunique()
            
            report_data.append({
                'Standard Column': standard_col,
                'Source Column': source_col,
                'Rows': len(df_before),
                'Unique Before': before_unique,
                'Unique After': after_unique,
                'Data Loss %': round((before_unique - after_unique) / before_unique * 100, 1) if before_unique > 0 else 0
            })
    
    return pd.DataFrame(report_data)


def auto_mapping_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate automatic mapping suggestions summary.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Mapping suggestions dictionary
    """
    suggestions = {}
    
    for standard_col, variations in STANDARD_COLUMNS.items():
        best_match = _find_best_column_match(df.columns, variations, standard_col, logging.getLogger("silent"))
        confidence = 100 if best_match and best_match.lower() in variations.lower() else 70
        
        suggestions[standard_col] = {
            'best_match': best_match,
            'alternatives': list(df.columns[:3]),  # Top 3 suggestions
            'confidence': confidence
        }
    
    return suggestions