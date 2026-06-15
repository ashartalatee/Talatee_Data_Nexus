from typing import List, Dict, Tuple, Optional
import pandas as pd

class ExcelSchemaValidator:
    """
    Pure Data Validator: Memastikan file Excel yang masuk sesuai 
    dengan kontrak skema industri yang diharapkan sebelum di-merge.
    """
    def __init__(self, expected_schema: Dict[str, str]):
        # Contoh expected_schema: {"sku": "object", "revenue": "float64", "qty": "int64"}
        self.expected_schema = expected_schema

    def validate_structure(self, df: pd.DataFrame, file_name: str) -> Tuple[bool, Optional[str]]:
        """
        Memvalidasi keberadaan kolom dan kesesuaian tipe data standar.
        """
        # 1. Validasi Keberadaan Kolom (Header Match)
        missing_columns = [col for col in self.expected_schema.keys() if col not in df.columns]
        if missing_columns:
            return False, f"File {file_name} kehilangan kolom wajib: {missing_columns}"

        # 2. Validasi Tipe Data Struktural secara defensif
        for col, expected_type in self.expected_schema.items():
            if expected_type in ["float64", "int64"]:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    return False, f"Kolom '{col}' pada {file_name} tidak valid. Diharapkan numerik."
                    
        return True, None