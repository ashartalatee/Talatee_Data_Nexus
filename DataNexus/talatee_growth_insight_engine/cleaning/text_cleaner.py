"""
Text cleaning and normalization module.
Handles product names, customer names, and categorical text data.
"""

import pandas as pd
import re
import logging
import unicodedata
from typing import Dict, Any, Optional, List
from utils.logger import setup_logger, safe_log_dataframe
from utils.config_loader import get_config_value


def clean_text_columns(df: pd.DataFrame, config: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Clean text columns based on configuration.
    
    Args:
        df: DataFrame with text columns
        config: Client configuration
        
    Returns:
        DataFrame with cleaned text
    """
    logger = setup_logger("cleaning.text_cleaner")
    logger.info("🧹 Cleaning text columns...")
    
    if df is None or df.empty:
        logger.error("❌ Empty input DataFrame")
        return None
    
    text_config = get_config_value(config, 'data_cleaning.text_cleaning', {})
    columns = text_config.get('columns', ['product_name'])
    lowercase = text_config.get('lowercase', True)
    remove_special = text_config.get('remove_special_chars', True)
    
    logger.info(f"⚙️ Columns: {columns}, lowercase: {lowercase}, special_chars: {remove_special}")
    
    df_clean = df.copy()
    
    cleaned_count = 0
    for col in columns:
        if col in df_clean.columns:
            rows_before = df_clean[col].notna().sum()
            df_clean[col] = _clean_text_series(df_clean[col], lowercase, remove_special)
            rows_after = df_clean[col].notna().sum()
            cleaned_count += rows_before
            logger.info(f"✅ {col}: {rows_before} → {rows_after} cleaned")
    
    logger.info(f"🎉 Text cleaning complete: {cleaned_count} values processed")
    safe_log_dataframe(logger, "text_cleaned_sample", df_clean[columns].head())
    
    return df_clean


def _clean_text_series(series: pd.Series, lowercase: bool = True, 
                      remove_special: bool = True) -> pd.Series:
    """Clean a single text series."""
    
    def clean_text(text: str) -> str:
        if pd.isna(text):
            return text
        
        text = str(text).strip()
        
        # Normalize unicode
        text = unicodedata.normalize('NFKD', text)
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove special characters if requested
        if remove_special:
            text = re.sub(r'[^\w\s\-]', ' ', text)
        
        # Lowercase if requested
        if lowercase:
            text = text.lower()
        
        return text.strip()
    
    return series.map(clean_text)


def normalize_product_names(df: pd.DataFrame, product_col: str = 'product_name') -> pd.DataFrame:
    """
    Advanced product name normalization for better grouping.
    
    Args:
        df: DataFrame with product names
        product_col: Product name column
        
    Returns:
        DataFrame with normalized product names
    """
    logger = setup_logger("cleaning.text.product_normalize")
    
    if product_col not in df.columns:
        logger.warning(f"⚠️ Product column '{product_col}' not found")
        return df
    
    df_norm = df.copy()
    
    # Common normalization patterns
    patterns = {
        r'\s*(pro|premium|plus|ultra|new|original)\s*': '',  # Remove qualifiers
        r'\s*(xl|xxl|l|m|s|xs)\s*': '',  # Remove sizes
        r'\s*(hitam|putih|merah|biru|hitam)\s*': '',  # Common Indonesian colors
        r'\s*(murah|terbaik|terlaris)\s*': '',  # Promotional words
        r'\$?\s*[^)]{0,3}\s*\d{2,4}\s*[^)]{0,3}\$?': '',  # Remove short parenthetical codes
    }
    
    def normalize_product(text: str) -> str:
        if pd.isna(text):
            return text
        
        text = str(text)
        
        # Apply patterns
        for pattern, replacement in patterns.items():
            text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
        
        # Final cleanup
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    before_unique = df[product_col].nunique()
    df_norm[product_col] = df_norm[product_col].map(normalize_product)
    after_unique = df_norm[product_col].nunique()
    
    reduction = ((before_unique - after_unique) / before_unique * 100)
    logger.info(f"📦 Product normalization: {before_unique} → {after_unique} unique ({reduction:.1f}% reduction)")
    
    return df_norm


def extract_keywords(series: pd.Series, top_n: int = 20) -> pd.DataFrame:
    """
    Extract top keywords from text series for analysis.
    
    Args:
        series: Text series
        top_n: Number of top keywords
        
    Returns:
        Keywords DataFrame with counts
    """
    from collections import Counter
    import nltk
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    
    # Download required NLTK data (one-time)
    try:
        nltk.data.find('tokenizers/punkt')
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('punkt')
        nltk.download('stopwords')
    
    stop_words = set(stopwords.words('indonesian') + stopwords.words('english'))
    
    all_words = []
    for text in series.dropna():
        tokens = word_tokenize(str(text).lower())
        words = [w for w in tokens if w.isalpha() and len(w) > 2 and w not in stop_words]
        all_words.extend(words)
    
    word_counts = Counter(all_words)
    keywords_df = pd.DataFrame(
        word_counts.most_common(top_n),
        columns=['keyword', 'count']
    )
    
    return keywords_df


def standardize_categories(df: pd.DataFrame, category_col: str, 
                         mapping: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Standardize categorical values using mapping dictionary.
    
    Args:
        df: DataFrame
        category_col: Category column to standardize
        mapping: Dict mapping old values to new
        
    Returns:
        DataFrame with standardized categories
    """
    logger = setup_logger("cleaning.text.categories")
    
    if category_col not in df.columns:
        logger.warning(f"⚠️ Category column '{category_col}' not found")
        return df
    
    df_std = df.copy()
    
    if mapping:
        df_std[category_col] = df_std[category_col].map(mapping).fillna(df_std[category_col])
        logger.info(f"✅ Standardized {category_col} using mapping ({len(mapping)} rules)")
    
    # Create consistent categories
    unique_before = df_std[category_col].nunique()
    df_std[category_col] = df_std[category_col].astype(str).str.lower().str.strip()
    unique_after = df_std[category_col].nunique()
    
    logger.info(f"📂 {category_col}: {unique_before} → {unique_after} categories")
    
    return df_std


def text_stats(df: pd.DataFrame, text_columns: List[str]) -> Dict[str, Any]:
    """
    Generate text quality statistics.
    
    Args:
        df: DataFrame
        text_columns: List of text columns
        
    Returns:
        Text statistics dictionary
    """
    stats = {}
    
    for col in text_columns:
        if col in df.columns:
            series = df[col].astype(str)
            stats[col] = {
                'non_empty': (series != '').sum(),
                'avg_length': series.str.len().mean(),
                'unique_values': series.nunique(),
                'top_values': series.value_counts().head(5).to_dict(),
                'contains_numbers': (series.str.contains(r'\d', na=False)).sum(),
                'contains_special': (series.str.contains(r'[^\w\s]', na=False, regex=True)).sum()
            }
    
    return stats