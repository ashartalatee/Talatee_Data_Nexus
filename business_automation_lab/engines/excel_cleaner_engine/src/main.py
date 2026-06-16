import os
import sys
import time
import yaml
import logging
from datetime import datetime

# ==============================================================================
# 🛡️ RUNTIME ENVIRONMENT GUARD
# ==============================================================================
# Mengunci root path agar Python memprioritaskan paket lokal di dalam folder 'src'
# Taktik ini mencegah ModuleNotFoundError terlepas dari direktori mana skrip dijalankan.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ENGINE_ROOT = os.path.dirname(CURRENT_DIR)

if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)
if ENGINE_ROOT not in sys.path:
    sys.path.insert(0, ENGINE_ROOT)

# ==============================================================================
# LOCAL MODULE IMPORTS (Menggunakan namespace 'infra_io' yang aman dari bentrok OS)
# ==============================================================================
try:
    from infra_io.reader import ExcelStreamReader
    from infra_io.writer import ExcelStreamWriter
    from core.cleaner import ExcelCleanerCore
except ModuleNotFoundError as import_err:
    print(f"[-] Import Failure: {str(import_err)}")
    print("[!] FATAL: Gagal memuat modul lokal internal.")
    print("    Pastikan Anda telah mengubah nama direktori fisik dari 'src/io/' menjadi 'src/infra_io/'")
    sys.exit(1)


# ==============================================================================
# LOGGING ORCHESTRATION WITH CONTEXT PROTECTION
# ==============================================================================
class TalateeContextFilter(logging.Filter):
    """
    Menjamin setiap baris log memiliki run_id default untuk mencegah KeyError
    saat dikoneksikan dengan sistem pemantauan logs atau debugging eksternal.
    """
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record):
        if not hasattr(record, 'run_id'):
            record.run_id = self.run_id
        return True


def setup_logging(run_id: str):
    """Membangun infrastruktur logging ganda (Main execution log & Error Quarantine)"""
    log_dir = os.path.join(ENGINE_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    log_format = "%(asctime)s [%(levelname)s] [RUN_ID: %(run_id)s] %(message)s"
    formatter = logging.Formatter(log_format)
    context_filter = TalateeContextFilter(run_id=run_id)

    # 1. Konfigurasi Global Base Logging (Konsol & File Utama)
    main_log_path = os.path.join(log_dir, "cleaner_execution.log")
    file_handler = logging.FileHandler(main_log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.addFilter(context_filter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(context_filter)

    root_logger = logging.getLogger("cleaner_engine")
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 2. Logger Khusus untuk Data Rusak yang Dikarantina (Error Quarantine)
    error_log_path = os.path.join(log_dir, "cleaner_errors.log")
    error_handler = logging.FileHandler(error_log_path, encoding='utf-8')
    error_handler.setFormatter(formatter)
    error_handler.addFilter(context_filter)
    
    error_logger = logging.getLogger("error_quarantine")
    error_logger.setLevel(logging.ERROR)
    error_logger.addHandler(error_handler)


# ==============================================================================
# PIPELINE CONFIGURATION LOADER
# ==============================================================================
def load_config() -> dict:
    """Memuat skema aturan pembersihan data dari berkas YAML eksternal dengan validasi blok."""
    config_path = os.path.join(ENGINE_ROOT, "config", "settings.yaml")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration profile missing at: {config_path}")
        
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = yaml.safe_load(f) or {}
        
    # Validasi integritas struktur file konfigurasi
    if "pipeline" not in config_data:
        raise KeyError("FATAL: Client config must contain a 'pipeline' block specifying operational thresholds.")
    if "columns_schema" not in config_data:
        raise KeyError("FATAL: Client config must contain a 'columns_schema' block defining cleaning rules.")
        
    return config_data


# ==============================================================================
# MAIN PIPELINE EXECUTION CIRCUIT
# ==============================================================================
def run_pipeline(input_file: str, output_file: str):
    """Mengeksekusi siklus pembersihan pipa data secara deterministik dan memory-safe."""
    # Menghasikan ID unik untuk setiap sesi eksekusi pipeline (Audit Trail)
    run_id = datetime.now().strftime("RUN-%Y%m%d-%H%M%S")
    start_time = time.time()
    
    setup_logging(run_id=run_id)
    logger = logging.getLogger("cleaner_engine")
    
    logger.info("=== TALATEE AUTOMATION: EXCEL CLEANER ENGINE STARTED ===")
    logger.info(f"Target Input Profile: {input_file}")
    logger.info(f"Target Output Profile: {output_file}")
    
    try:
        # 1. Inisialisasi Profil Aturan & Validasi Struktur Dokumen
        config = load_config()
        chunk_size = config.get("pipeline", {}).get("chunk_size", 50000)
        
        # 2. Inisialisasi Stream Core & I/O Components
        reader = ExcelStreamReader(file_path=input_file, chunk_size=chunk_size)
        writer = ExcelStreamWriter(output_path=output_file)
        cleaner = ExcelCleanerCore(config=config)
        
        chunk_count = 0
        total_rows_processed = 0
        
        # 3. Deterministic Chunking Loop (Sirkuit Utama Pemrosesan)
        for chunk in reader.stream_chunks():
            chunk_count += 1
            total_rows_processed += len(chunk)
            
            # Sanitasi data per chunk (Aman dari interupsi baris korup)
            cleaned_df = cleaner.clean_chunk(chunk, chunk_index=chunk_count)
            
            # Menulis langsung secara inkremental ke media penyimpanan (RAM Protection)
            writer.write_chunk(cleaned_df, chunk_index=chunk_count)
            
        duration = time.time() - start_time
        logger.info("=== PIPELINE EXECUTION SUCCESSFUL ===")
        logger.info(f"Total Rows Streamed       : {total_rows_processed}")
        logger.info(f"Total Chunks Handled      : {chunk_count}")
        logger.info(f"Total Processing Duration : {duration:.2f} seconds")
        logger.info(f"Cleaned Artifact Committed To: {output_file}")
        
    except Exception as pipeline_fault:
        logger.critical(
            f"Pipeline terminated due to critical unhandled system crash: {str(pipeline_fault)}", 
            exc_info=True
        )


# ==============================================================================
# LOCAL TESTING SUITE ENTRY
# ==============================================================================
if __name__ == "__main__":
    # Menghubungkan jalur data relatif terhadap folder root arsitektur proyek
    RAW_DATA_PATH = os.path.join(ENGINE_ROOT, "data", "01_raw", "tokopedia_sales_may_2026.xlsx")
    OUTPUT_DATA_PATH = os.path.join(ENGINE_ROOT, "data", "03_output", "cleaned_sales_may_2026.csv")
    
    if os.path.exists(RAW_DATA_PATH):
        run_pipeline(RAW_DATA_PATH, OUTPUT_DATA_PATH)
    else:
        print(f"[!] Simulasi dibatalkan. Berkas data mentah tidak ditemukan.")
        print(f"    Silakan tempatkan berkas Excel uji coba Anda di koordinat: {RAW_DATA_PATH}")