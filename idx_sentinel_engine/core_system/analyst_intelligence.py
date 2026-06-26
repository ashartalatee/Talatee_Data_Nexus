import pandas as pd

class AnalystIntelligence:
    def __init__(self):
        pass

    def calculate_daily_changes(self, df_yesterday: pd.DataFrame, df_today: pd.DataFrame) -> pd.DataFrame:
        """
        Membandingkan data kepemilikan saham kemarin dan hari ini.
        Menghasilkan analisa akumulasi/distribusi.
        """
        if df_yesterday.empty:
            # Jika tidak ada data kemarin, anggap semua data hari ini sebagai data awal (New Entry)
            df_analysis = df_today.copy()
            df_analysis['Saham_Kemarin'] = 0
            df_analysis['Persentase_Kemarin'] = 0.0
            df_analysis['Perubahan_Saham'] = df_analysis['Jumlah_Saham']
            df_analysis['Perubahan_Persentase'] = df_analysis['Persentase']
            df_analysis['Status'] = 'NEW_ENTRY'
            return df_analysis

        # Gabungkan data berdasarkan Ticker dan Pemegang_Saham (Composite Key)
        merged = pd.merge(
            df_today, 
            df_yesterday, 
            on=['Ticker', 'Pemegang_Saham'], 
            how='outer', 
            suffixes=('_today', '_yesterday')
        )

        # Isi nilai NaN dengan 0 (artinya baru masuk hari ini atau sudah keluar dari daftar)
        merged.fillna({'Jumlah_Saham_today': 0, 'Persentase_today': 0.0,
                       'Jumlah_Saham_yesterday': 0, 'Persentase_yesterday': 0.0}, inplace=True)

        # Hitung Delta Perubahan
        merged['Perubahan_Saham'] = merged['Jumlah_Saham_today'] - merged['Jumlah_Saham_yesterday']
        merged['Perubahan_Persentase'] = merged['Persentase_today'] - merged['Persentase_yesterday']

        # Berikan Label Status Analisis
        # Berikan Label Status Analisis
        def determine_status(row):
            if row['Jumlah_Saham_yesterday'] == 0:
                return 'NEW_ENTRY'  # Whale Baru
            elif row['Jumlah_Saham_today'] == 0:
                return 'EXIT'       # Whale Keluar
            elif row['Perubahan_Saham'] > 0:
                return 'AKUMULASI'  # Beli Lagi
            elif row['Perubahan_Saham'] < 0:
                return 'DISTRIBUSI' # Jual Sebagian
            return 'NO_CHANGE'

        merged['Status'] = merged.apply(determine_status, axis=1)
        
        # Filter hanya data yang ada perubahan saja untuk laporan utama
        df_changes = merged[merged['Status'] != 'NO_CHANGE'].copy()
        
        # Rapikan kolom akhir
        df_changes.rename(columns={
            'Nama_Emiten_today': 'Nama_Emiten',
            'Jumlah_Saham_today': 'Saham_Hari_Ini',
            'Persentase_today': 'Persentase_Hari_Ini',
            'Jumlah_Saham_yesterday': 'Saham_Kemarin',
            'Persentase_yesterday': 'Persentase_Kemarin'
        }, inplace=True)
        
        # Isi Nama Emiten yang hilang akibat outer join dari data kemarin
        df_changes['Nama_Emiten'] = df_changes['Nama_Emiten'].fillna(df_changes['Nama_Emiten_yesterday'])
        
        return df_changes[['Ticker', 'Nama_Emiten', 'Pemegang_Saham', 'Saham_Kemarin', 'Saham_Hari_Ini', 'Perubahan_Saham', 'Persentase_Kemarin', 'Persentase_Hari_Ini', 'Perubahan_Persentase', 'Status']]