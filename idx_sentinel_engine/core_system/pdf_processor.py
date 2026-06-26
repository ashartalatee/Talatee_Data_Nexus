import os
import re
import pandas as pd
import pdfplumber

class PDFProcessor:
    def __init__(self, config=None):
        self.config = config

    def extract_table_from_pdf(self, pdf_path: str) -> pd.DataFrame:
        """
        Membaca file PDF IDX dan mengekstrak tabel kepemilikan saham.
        Return: DataFrame dengan kolom standar [Emiten, Pemegang_Saham, Jumlah_Saham, Persentase]
        """
        raw_data = []
        
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    # Lewati header pada halaman pertama jika ada
                    for row in table:
                        # Bersihkan baris dari nilai None atau kosong
                        cleaned_row = [str(cell).strip() if cell else "" for cell in row]
                        
                        # Filter baris yang valid (Misal: baris harus mengandung kode emiten 4 huruf kapital)
                        if cleaned_row and self._is_valid_row(cleaned_row):
                            raw_data.append(cleaned_row)
        
        # Jika kolom PDF IDX umumnya ada 4 atau 5, sesuaikan di bawah ini
        # Asumsi standar: [Kode_Ticker, Nama_Emiten, Pemegang_Saham, Jumlah_Saham, Persentase]
        df = pd.DataFrame(raw_data)
        
        # Contoh normalisasi sederhana (sesuaikan indeks kolom dengan layout asli PDF IDX)
        if not df.empty:
            df.columns = ['Ticker', 'Nama_Emiten', 'Pemegang_Saham', 'Jumlah_Saham', 'Persentase']
            df['Jumlah_Saham'] = df['Jumlah_Saham'].apply(self._clean_numeric)
            df['Persentase'] = df['Persentase'].apply(self._clean_float)
            df['Ticker'] = df['Ticker'].str.upper()
        else:
            df = pd.DataFrame(columns=['Ticker', 'Nama_Emiten', 'Pemegang_Saham', 'Jumlah_Saham', 'Persentase'])
            
        return df

    def _is_valid_row(self, row):
        # Logika validasi: Kolom pertama biasanya berisi kode saham (4 huruf, contoh: BBCA, TLKM)
        if len(row) >= 4:
            return bool(re.match(r'^[A-Z]{4}$', row[0]))
        return False

    def _clean_numeric(self, val):
        cleaned = re.sub(r'[^\d]', '', val)
        return int(cleaned) if cleaned else 0

    def _clean_float(self, val):
        cleaned = val.replace(',', '.')
        cleaned = re.sub(r'[^\d.]', '', cleaned)
        return float(cleaned) if cleaned else 0.0