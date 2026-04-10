import pandas as pd
import logging
import numpy as np

class FormatterModule:
    """
    Standardizes data formats including text casing, date-time parsing, 
    and numeric type enforcement to prevent common Pandas artifacts (like .0 on IDs).
    """

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def process(self, df: pd.DataFrame, rules: dict) -> pd.DataFrame:
        """
        Orchestrates all formatting tasks based on configuration rules.
        """
        # 1. Handle Numeric Integrity (Prevents 101.0 on IDs)
        # Dilakukan di awal agar tipe data konsisten sebelum diformat sebagai string
        df = self._enforce_numeric_types(df)

        # 2. Handle Text Standardization
        text_rules = rules.get("standardize_text", {})
        if text_rules.get("enabled", False):
            df = self._standardize_text(df, text_rules)

        # 3. Handle Date Formatting (Enhanced)
        date_rules = rules.get("date_formatting", {})
        if date_rules.get("enabled", False):
            df = self._parse_dates(df, date_rules)

        return df

    def _standardize_text(self, df: pd.DataFrame, rules: dict) -> pd.DataFrame:
        """
        Cleans whitespace and standardizes casing. 
        Handles 'nan' string artifacts from type conversion.
        """
        columns = rules.get("columns", [])
        case_type = rules.get("case", "title").lower()
        
        valid_cols = [c for c in columns if c in df.columns]
        self.logger.info(f"Standardizing text: {valid_cols} to {case_type}")

        for col in valid_cols:
            # Menggunakan copy untuk menghindari SettingWithCopyWarning
            # Convert ke string, hilangkan spasi di ujung
            df[col] = df[col].astype(str).str.strip()
            
            if case_type == "upper":
                df[col] = df[col].str.upper()
            elif case_type == "lower":
                df[col] = df[col].str.lower()
            else:
                df[col] = df[col].str.title()
            
            # KRUSIAL: Hapus artefak 'Nan' yang muncul karena .astype(str) pada data kosong
            df[col] = df[col].replace(['Nan', 'None', 'nan', 'null', 'N/A'], "")
        
        return df

    def _parse_dates(self, df: pd.DataFrame, rules: dict) -> pd.DataFrame:
        """
        Converts varied date strings into a uniform format.
        Uses dayfirst=False karena data input menggunakan format MM/DD/YY.
        """
        columns = rules.get("target_columns", [])
        output_format = rules.get("output_format", "%Y-%m-%d")

        valid_cols = [c for c in columns if c in df.columns]
        self.logger.info(f"Standardizing dates: {valid_cols}")

        for col in valid_cols:
            try:
                # errors='coerce' mengubah tanggal invalid menjadi NaT
                # dayfirst=False memastikan MM/DD/YY diparsing dengan benar
                temp_dt = pd.to_datetime(df[col], errors='coerce', dayfirst=False)
                
                # Format ke string dan pastikan baris kosong tetap kosong (bukan NaT)
                df[col] = temp_dt.dt.strftime(output_format).fillna("")
            except Exception as e:
                self.logger.error(f"Critical error parsing dates in column {col}: {str(e)}")
        
        return df

    def _enforce_numeric_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mencegah Pandas mengubah Integer menjadi Float (.0) saat ada baris kosong.
        Mencari kolom dengan nama 'id', 'amount', atau 'spent'.
        """
        for col in df.columns:
            # Penanganan ID (Mencegah 101.0)
            if "id" in col.lower():
                try:
                    # 'Int64' (Nullable Integer) menjaga angka tetap bulat meskipun ada NaN
                    df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                except Exception:
                    continue
            
            # Penanganan Nilai Uang / Transaksi
            elif any(key in col.lower() for key in ["amount", "spent", "price"]):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df