import pandas as pd
from core.logger import CustomLogger
from core.schema import DataSchema

class DataNormalizer:
    """
    Processor responsible for transforming raw marketplace data 
    into the Standard Data Schema using client-specific mappings.
    """

    def __init__(self, raw_df: pd.DataFrame, column_mapping: dict, channel_name: str):
        self.raw_df = raw_df.copy()
        self.column_mapping = column_mapping
        self.channel_name = channel_name
        self.logger = CustomLogger(name="DataNormalizer").get_logger()
        self.schema = DataSchema()

    def transform(self) -> pd.DataFrame:
        """
        Executes the normalization process: 
        Renaming, column selection, and metadata injection.
        """
        if self.raw_df.empty:
            self.logger.warning(f"Normalizer received empty data for channel: {self.channel_name}")
            return pd.DataFrame(columns=self.schema.get_required_columns())

        self.logger.info(f"Normalizing data for channel: {self.channel_name}")

        try:
            # 1. Rename columns based on mapping in client config
            normalized_df = self.raw_df.rename(columns=self.column_mapping)

            # 2. Add channel metadata
            normalized_df[self.schema.CHANNEL] = self.channel_name

            # 3. Ensure all required columns exist (fill missing with None)
            required_cols = self.schema.get_required_columns()
            for col in required_cols:
                if col not in normalized_df.columns:
                    self.logger.warning(f"Missing expected column '{col}' in {self.channel_name}. Filling with None.")
                    normalized_df[col] = None

            # 4. Filter only standard columns to maintain schema purity
            normalized_df = normalized_df[required_cols]

            # 5. Preliminary type alignment (Detailed cleaning happens in DataCleaner)
            normalized_df = self._initial_type_cast(normalized_df)

            self.logger.info(f"Normalization successful for {self.channel_name}")
            return normalized_df

        except Exception as e:
            self.logger.error(f"Error during normalization for {self.channel_name}: {str(e)}")
            raise e

    def _initial_type_cast(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs basic casting to ensure the DataFrame is ready for consolidation.
        """
        # Ensure Price and Quantity are numeric, coercing errors to NaN
        if self.schema.PRICE in df.columns:
            df[self.schema.PRICE] = pd.to_numeric(df[self.schema.PRICE], errors='coerce').fillna(0.0)
        
        if self.schema.QUANTITY in df.columns:
            df[self.schema.QUANTITY] = pd.to_numeric(df[self.schema.QUANTITY], errors='coerce').fillna(0).astype(int)
            
        return df