import logging
from typing import List, Any
import pandas as pd

logger = logging.getLogger("PDFEngine.Processor")

class TableProcessor:
    """Bertanggung jawab mengolah data mentah dari extractor menjadi Dataframe terpadu."""
    
    @staticmethod
    def process_to_dataframe(raw_tables: List[List[List[Any]]]) -> pd.DataFrame:
        """
        Mengubah list tabel mentah menjadi satu DataFrame tunggal.
        Menangani perbedaan skema kolom secara dinamis (Schema Alignment).
        """
        if not raw_tables:
            return pd.DataFrame()
            
        dataframe_list = []
        
        for idx, table in enumerate(raw_tables):
            if not table or len(table) < 1:
                continue
                
            # Asumsikan baris pertama tabel adalah Header
            header = table[0]
            data_rows = table[1:]
            
            # Bersihkan header dari karakter newline atau spasi liar
            cleaned_header = [str(col).strip().replace('\n', ' ') if col else f"UNNAMED_{i}" for i, col in enumerate(header)]
            
            # Jika tabel tidak punya baris data (hanya header), skip
            if not data_rows:
                continue
                
            # Convert ke DataFrame
            df_temp = pd.DataFrame(data_rows, columns=cleaned_header)
            dataframe_list.append(df_temp)
            
        if not dataframe_list:
            return pd.DataFrame()
            
        # Poin Penting: Menggabungkan banyak dataframe dengan kolom berbeda secara otomatis.
        # Kolom yang tidak cocok akan diisi oleh NaN secara default.
        combined_df = pd.concat(dataframe_list, axis=0, ignore_index=True)
        
        # Data Cleaning dasar: hapus baris yang benar-benar kosong di semua kolom
        combined_df.dropna(how='all', inplace=True)
        
        return combined_df