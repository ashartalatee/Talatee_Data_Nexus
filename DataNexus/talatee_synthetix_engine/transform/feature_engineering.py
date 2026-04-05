import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import numpy as np

# Internal Module Imports
from utils.logger import setup_custom_logger

# Initialize Logger
logger = setup_custom_logger("feature_engineering")

class FeatureEngineer:
    """
    Production-grade Feature Engineering module.
    Transforms standardized marketplace data into analytical datasets by 
    calculating GMV, revenue, and sales velocity.
    """

    def __init__(self, transform_config: Dict[str, Any]):
        """
        :param transform_config: Dictionary containing feature calculation settings.
        """
        self.config = transform_config
        self.features_to_build = self.config.get("feature_engineering", [])
        logger.info(f"FeatureEngineer initialized to build: {self.features_to_build}")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main orchestration for feature creation. 
        Applies calculations based on the client configuration.
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame passed to FeatureEngineer.")
            return pd.DataFrame()

        try:
            working_df = df.copy()
            
            # --- DEFENSIVE DATA TYPE CONVERSION ---
            # Menghindari AttributeError: 'int' object has no attribute 'fillna'
            # Kita pastikan kolom ada dan dikonversi ke Series numeric sebelum fillna
            
            for col in ['price', 'quantity']:
                if col in working_df.columns:
                    working_df[col] = pd.to_numeric(working_df[col], errors='coerce').fillna(0)
                else:
                    logger.warning(f"Required column '{col}' missing for calculations. Defaulting to 0.")
                    working_df[col] = 0.0

            # 1. Core Financial Features
            if "total_gmv" in self.features_to_build:
                working_df["total_gmv"] = working_df["price"] * working_df["quantity"]
                logger.debug("Feature 'total_gmv' calculated.")

            if "net_revenue" in self.features_to_build:
                # Ambil discount jika ada, jika tidak ada default ke 0
                discount_col = 'discount_amount'
                discount = pd.to_numeric(working_df.get(discount_col, 0), errors='coerce').fillna(0)
                working_df["net_revenue"] = (working_df["price"] * working_df["quantity"]) - discount

            # 2. Time-based Features
            if "order_date" in working_df.columns:
                working_df = self._add_time_features(working_df)

            # 3. Velocity and Aggregated Features
            if "order_velocity" in self.features_to_build:
                working_df = self._calculate_velocity(working_df)

            logger.info(f"Feature engineering complete. Current columns: {list(working_df.columns)}")
            return working_df

        except Exception as e:
            logger.error(f"Critical error in FeatureEngineering: {str(e)}", exc_info=True)
            # Kembalikan dataframe asli jika terjadi error fatal agar pipeline tidak berhenti total
            return df

    def _add_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds granular time components for trend analysis."""
        try:
            # Pastikan format datetime
            if not pd.api.types.is_datetime64_any_dtype(df["order_date"]):
                df["order_date"] = pd.to_datetime(df["order_date"], errors='coerce')

            # Drop baris dengan tanggal yang tidak valid agar tidak merusak ekstraksi dt
            temp_df = df.dropna(subset=["order_date"])
            
            df["order_hour"] = df["order_date"].dt.hour
            df["order_day_name"] = df["order_date"].dt.day_name()
            df["order_month"] = df["order_date"].dt.month
            df["order_year"] = df["order_date"].dt.year
            df["is_weekend"] = df["order_date"].dt.dayofweek.isin([5, 6]).astype(int)
            
            return df
        except Exception as e:
            logger.warning(f"Failed to add time features: {e}")
            return df

    def _calculate_velocity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculates sales velocity (items sold per day) per Product."""
        # Syarat minimal: harus ada nama produk dan tanggal
        if "product_name" not in df.columns or "order_date" not in df.columns:
            logger.warning("Skipping velocity calculation: 'product_name' or 'order_date' missing.")
            return df

        try:
            # Buat copy sementara untuk agregasi
            temp_df = df.copy()
            temp_df['date_only'] = temp_df['order_date'].dt.date
            
            # 1. Total quantity per produk per hari
            daily_sales = temp_df.groupby(['product_name', 'date_only'])['quantity'].sum().reset_index()
            
            # 2. Rata-rata penjualan harian (Velocity)
            velocity_map = daily_sales.groupby('product_name')['quantity'].mean().reset_index()
            velocity_map.columns = ['product_name', 'daily_velocity_avg']
            
            # 3. Gabungkan kembali ke dataframe utama
            df = df.merge(velocity_map, on='product_name', how='left')
            # Isi NaN velocity dengan 0 (untuk produk yang mungkin tidak punya data valid)
            df['daily_velocity_avg'] = df['daily_velocity_avg'].fillna(0)
            
            return df
        except Exception as e:
            logger.error(f"Velocity calculation failed: {e}")
            return df