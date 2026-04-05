import logging
import json
from typing import Dict, Any, Union, Optional, List
import pandas as pd
from pathlib import Path

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("column_mapper")

class ColumnMapper:
    """
    Production-grade module to map marketplace-specific columns to a 
    standardized schema (e.g., order_id, product_name, price, quantity, order_date).
    Supports direct dictionary mapping or external JSON mapping files.
    """

    def __init__(self, mapping_config: Dict[str, Any]):
        """
        :param mapping_config: Dictionary containing mapping logic per marketplace.
        """
        self.config = mapping_config
        logger.info("ColumnMapper initialized.")

    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardizes columns based on the detected marketplace source in the DataFrame.
        Expected column: '_marketplace_source' (added during ingestion).
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame passed to ColumnMapper.")
            return pd.DataFrame()

        if "_marketplace_source" not in df.columns:
            logger.error("Column '_marketplace_source' missing. Cannot apply mapping.")
            return df

        try:
            working_df = df.copy()
            marketplaces = working_df["_marketplace_source"].unique()
            final_parts = []

            for mkp in marketplaces:
                # Filter data for specific marketplace
                mkp_df = working_df[working_df["_marketplace_source"] == mkp].copy()
                
                # Retrieve the specific mapping dictionary
                mapping = self._get_mapping_for_marketplace(mkp)

                if not mapping:
                    logger.warning(f"No column mapping found for marketplace: {mkp}. Using raw columns.")
                    final_parts.append(mkp_df)
                    continue

                # Apply renaming
                logger.info(f"Applying column mapping for {mkp}: {list(mapping.values())}")
                mkp_df = mkp_df.rename(columns=mapping)
                
                final_parts.append(mkp_df)

            # Combine all mapped parts
            final_df = pd.concat(final_parts, axis=0, ignore_index=True, sort=False)
            
            # Final Schema Validation (Optional Check)
            self.validate_schema(final_df, ["order_id", "price", "quantity", "product_name"])
            
            return final_df

        except Exception as e:
            logger.error(f"Critical error during column mapping: {str(e)}", exc_info=True)
            return df

    def _get_mapping_for_marketplace(self, marketplace: str) -> Dict[str, str]:
        """
        Retrieves mapping dictionary from config. 
        Handles:
        1. Direct dict mapping: {"col_a": "price"}
        2. Marketplace object mapping: {"shopee": {"mapping": {...}}}
        3. External JSON file paths.
        """
        data = self.config.get(marketplace)

        if not data:
            return {}

        # Case 1: Data is a string (Path to JSON)
        if isinstance(data, str):
            return self._load_from_file(data)

        # Case 2: Data is a dict containing a 'mapping' key
        if isinstance(data, dict) and "mapping" in data:
            return data["mapping"]

        # Case 3: Data is the mapping dict itself
        if isinstance(data, dict):
            return data

        return {}

    def _load_from_file(self, file_path: str) -> Dict[str, str]:
        """Safely loads mapping from a JSON file."""
        path = Path(file_path)
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    content = json.load(f)
                    # If file has "mapping" key, use it, else use whole file
                    return content.get("mapping", content) if isinstance(content, dict) else {}
            except Exception as e:
                logger.error(f"Failed to load mapping file {path}: {e}")
                return {}
        logger.error(f"Mapping file not found: {path}")
        return {}

    def validate_schema(self, df: pd.DataFrame, required_cols: List[str]) -> bool:
        """
        Defensive check to ensure required standardized columns exist after mapping.
        """
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.warning(f"Schema validation: Missing standardized columns {missing}. "
                           f"Ensure JSON mapping matches source file headers.")
            return False
        logger.info("Schema validation passed for standardized columns.")
        return True