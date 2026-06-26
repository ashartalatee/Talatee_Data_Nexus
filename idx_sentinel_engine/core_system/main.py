import os
import json
import glob
import pandas as pd
from pdf_processor import PDFProcessor
from analyst_intelligence import AnalystIntelligence
from excel_formatter import ExcelFormatter

def run_orchestrator():
    """
    Fungsi inti penggerak seluruh engine. 
    Dibuat dalam bentuk fungsi agar bisa dipanggil oleh local runner maupun telegram_bot.py.
    """
    # 1. Load Configuration
    config_path = "core_system/config.json"
    if not os.path.exists(config_path):
        return False, "File core_system/config.json tidak ditemukan!", None
        
    with open(config_path, "r") as f:
        config = json.load(f)
    
    # Ambil variabel path dari config
    input_dir = config["path"]["input_pdf_dir"]
    archive_dir = config["path"]["archive_pdf_dir"]
    output_dir = config["path"]["output_report_dir"]
    history_dir = config["path"]["history_dir"]
    
    # Pastikan semua folder yang dibutuhkan sudah terbentuk otomatis
    for folder in [input_dir, archive_dir, output_dir, history_dir]:
        os.makedirs(folder, exist_ok=True)
    
    # 2. Ambil berkas PDF di folder input (bisa memproses banyak file sekaligus secara batch)
    pdf_files = glob.glob(os.path.join(input_dir, "*.pdf"))
    if not pdf_files:
        return False, f"Folder '{input_dir}' kosong. Silakan letakkan berkas PDF IDX di sana.", None
        
    print(f"[MAIN] Menemukan {len(pdf_files)} file PDF untuk diproses.")
    
    # 3. Inisialisasi Modul Ingestion & Ekstraksi Data
    processor = PDFProcessor()
    
    # Gabungkan semua data hari ini jika user memasukkan beberapa file PDF sekaligus
    df_hari_ini_list = []
    for pdf_path in pdf_files:
        print(f"[MAIN] Mengekstrak data dari: {os.path.basename(pdf_path)}")
        df_extracted = processor.extract_table_from_pdf(pdf_path)
        if not df_extracted.empty:
            df_hari_ini_list.append(df_extracted)
            
    if not df_hari_ini_list:
        return False, "Gagal mengekstrak data valid dari PDF. Pastikan format dokumen sesuai standar IDX.", None
        
    df_hari_ini = pd.concat(df_hari_ini_list, ignore_index=True)
    
    # 4. Ambil Data Histori Pemrosesan Sebelumnya (Data Kemarin)
    history_file_path = os.path.join(history_dir, "master_database_kemarin.csv")
    if os.path.exists(history_file_path):
        print("[MAIN] Memuat data histori transaksi sebelumnya untuk komparasi...")
        df_kemarin = pd.read_csv(history_file_path)
    else:
        print("[MAIN] Histori data kemarin tidak ditemukan. Menginisialisasi pemrosesan perdana.")
        df_kemarin = pd.DataFrame()
        
    # 5. Eksekusi Intelligence Layer (Hitung Delta & Deteksi Whale)
    print("[MAIN] Menjalankan kalkulasi deteksi pergerakan saham...")
    analyst = AnalystIntelligence()
    df_analisis = analyst.calculate_daily_changes(df_kemarin, df_hari_ini)
    
    if df_analisis.empty:
        return True, "Proses selesai, namun tidak ditemukan adanya perubahan kepemilikan saham di atas 5% hari ini.", None
        
    # 6. Formatting & Generate Excel Master untuk User Non-IT
    output_excel_path = os.path.join(output_dir, "rekap_saham_master.xlsx")
    print(f"[MAIN] Menyusun laporan visual interaktif di: {output_excel_path}")
    formatter = ExcelFormatter()
    formatter.write_report(df_analisis, output_excel_path)
    
    # 7. Pencatatan Histori & Pengarsipan (Agar besok data hari ini diakui sebagai data 'kemarin')
    df_hari_ini.to_csv(history_file_path, index=False)
    
    # Pindahkan file PDF yang sudah diproses ke folder arsip agar folder input kembali bersih
    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        os.rename(pdf_path, os.path.join(archive_dir, filename))
        
    return True, f"Berhasil memproses {len(pdf_files)} berkas PDF. Silakan cek folder output.", output_excel_path

if __name__ == "__main__":
    print("[SYSTEM] Memulai eksekusi eksternal via Local Terminal...")
    success, msg, _ = run_orchestrator()
    print(f"\n[HASIL RUNNING] Status: {'SUKSES' if success else 'GAGAL'}")
    print(f"[PESAN SISTEM] : {msg}")