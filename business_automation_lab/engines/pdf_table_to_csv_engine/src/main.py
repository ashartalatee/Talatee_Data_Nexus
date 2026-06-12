import logging
from pathlib import Path
from datetime import datetime
from extractor import PDFTableExtractor
from processor import TableProcessor

def setup_logging(log_dir: Path):
    """Mengonfigurasi infrastruktur logging global dengan audit trail terpusat."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"engine_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

class PDFAutomationEngine:
    """Orchestrator Core untuk memproses otomatisasi folder input ke output."""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.input_dir = base_dir / "input"
        self.output_dir = base_dir / "output"
        self.log_dir = base_dir / "logs"
        
        # Memastikan seluruh struktur folder wajib ada
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        setup_logging(self.log_dir)
        self.logger = logging.getLogger("PDFEngine.Main")

    def run(self):
        self.logger.info("=== PDF Table to CSV Automation Engine Dimulai ===")
        
        # Scan seluruh file .pdf di folder input
        pdf_files = list(self.input_dir.glob("*.pdf"))
        
        if not pdf_files:
            self.logger.info("Tidak ada file PDF yang ditemukan di folder input. Proses selesai.")
            return
            
        self.logger.info(f"Menemukan {len(pdf_files)} file PDF siap diproses.")
        
        for pdf_path in pdf_files:
            try:
                self.logger.info(f"Starting pipeline untuk file: {pdf_path.name}")
                
                # 1. Extraction Layer
                extraction_report = PDFTableExtractor.extract_all_tables(pdf_path)
                
                if extraction_report["tables_found"] == 0:
                    self.logger.warning(f"File {pdf_path.name} berhasil dibaca, tetapi tidak ada tabel terdeteksi.")
                    continue
                
                # 2. Processing Layer
                final_df = TableProcessor.process_to_dataframe(extraction_report["raw_data"])
                
                if final_df.empty:
                    self.logger.warning(f"Data pada file {pdf_path.name} kosong setelah difilter.")
                    continue
                
                # 3. Storage Layer
                output_file_name = f"{pdf_path.stem}_exported.csv"
                output_file_path = self.output_dir / output_file_name
                
                # Save dengan format UTF-8 BOM agar kompatibel penuh dengan Excel tanpa berantakan
                final_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
                
                # 4. Final Executive Audit Logging
                self.logger.info(
                    f"\n[REPORT SUCCESS] File Sukses Diproses:\n"
                    f"   - Nama File           : {extraction_report['file_name']}\n"
                    f"   - Jumlah Halaman      : {extraction_report['total_pages']}\n"
                    f"   - Jumlah Tabel        : {extraction_report['tables_found']}\n"
                    f"   - Total Baris Ekstrak : {len(final_df)}\n"
                    f"   - Lokasi Output       : {output_file_path}\n"
                )
                
            except Exception as main_err:
                # Isolasi error: jika satu file hancur/corrupt, pipeline tetap berjalan untuk file lain
                self.logger.critical(
                    f"Pipeline GAGAL memproses file '{pdf_path.name}'. "
                    f"Akan melanjutkan ke file berikutnya. Detail Error: {str(main_err)}", 
                    exc_info=True
                )
                
        self.logger.info("=== Seluruh Pipeline Selesai Dieksekusi ===")

if __name__ == "__main__":
    # Menentukan root project path secara dinamis
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    engine = PDFAutomationEngine(PROJECT_ROOT)
    engine.run()