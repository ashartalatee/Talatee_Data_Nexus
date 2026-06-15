import sys
import logging
from core.context import EngineContext
from core.orchestrator import ExcelMergerOrchestrator
from services.validator import ExcelSchemaValidator
from drivers.excel_reader import SafeExcelReader
from drivers.file_system import LocalFileSystemDriver

# Setup logging sederhana untuk CLI
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Talatee.Main")

def run_pipeline():
    # 1. Inisialisasi Context & Infrastruktur Driver
    context = EngineContext(execution_mode="PORTFOLIO_DEMO")
    fs_driver = LocalFileSystemDriver(base_storage_path="storage")
    reader = SafeExcelReader()

    # 2. Tentukan Aturan Skema (Misal untuk data performa produk marketplace)
    expected_schema = {
        "sku": "object",
        "revenue": "float64",
        "qty": "int64"
    }
    validator = ExcelSchemaValidator(expected_schema=expected_schema)
    orchestrator = ExcelMergerOrchestrator(validator=validator)

    # 3. Pindai File Masuk
    input_files = fs_driver.scan_input_files()
    if not input_files:
        logger.info("Tidak ada file .xlsx baru yang ditemukan di folder storage/input/. Sistem idle.")
        sys.exit(0)

    # 4. Isolasi File ke Folder Processing sebelum di-read (Defensive Step)
    staged_files = []
    for file in input_files:
        staged_path = fs_driver.move_to_processing(file)
        staged_files.append(staged_path)

    # 5. Eksekusi Penggabungan melalui Orkestrator Core
    try:
        # Catatan: Di arsitektur penuh, orchestrator akan memanggil reader.read_file_safely
        # Kode ini memetakan logika eksekusi tingkat tinggi
        merged_dataframe = orchestrator.execute_merge(context, staged_files)
        
        # 6. Tulis Hasil Akhir ke Folder Output jika berhasil
        if not merged_dataframe.empty:
            output_path = f"storage/output/merged_report_{context.run_id}.xlsx"
            merged_dataframe.to_excel(output_path, index=False, engine="openpyxl")
            logger.info(f"🔥 SUKSES! File hasil penggabungan disimpan di: {output_path}")

            # Bersihkan berkas staging karena pemrosesan selesai sempurna
            for staged_file in staged_files:
                fs_driver.clean_processing_file(staged_file)
                
    except Exception as fatal_error:
        logger.error(f"Eksekusi pipeline gagal pada Run ID {context.run_id}: {str(fatal_error)}")
        logger.info("Berkas aman di folder storage/processing/ untuk keperluan investigasi manual.")

if __name__ == "__main__":
    run_pipeline()