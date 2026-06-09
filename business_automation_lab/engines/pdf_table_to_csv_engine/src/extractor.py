import logging
from pathlib import Path
from typing import Dict, List, Any
import pdfplumber

logger = logging.getLogger("PDFEngine.Extractor")

class PDFTableExtractor:
    """Bertanggung jawab penuh untuk membaca file PDF dan mengekstrak tabel per halaman."""
    
    @staticmethod
    def extract_all_tables(pdf_path: Path) -> Dict[str, Any]:
        """
        Membuka PDF dan mengekstrak seluruh tabel secara sekuensial.
        Menggunakan generator interior untuk menjaga stabilitas memori.
        """
        report = {
            "file_name": pdf_path.name,
            "total_pages": 0,
            "tables_found": 0,
            "raw_data": []
        }
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                report["total_pages"] = len(pdf.pages)
                logger.info(f"Memproses {pdf_path.name} | Total: {report['total_pages']} halaman.")
                
                for page_num, page in enumerate(pdf.pages, start=1):
                    # Ekstraksi tabel dari halaman aktif
                    tables = page.extract_tables()
                    
                    if tables:
                        for table in tables:
                            # Validasi jika tabel memiliki data (bukan list kosong)
                            if table and any(any(cell for cell in row) for row in table):
                                report["raw_data"].append(table)
                                report["tables_found"] += 1
                                
                logger.debug(f"Selesai ekstrak {pdf_path.name}: Menemukan {report['tables_found']} tabel.")
                
        except Exception as e:
            logger.error(f"Gagal melakukan ekstraksi pada file {pdf_path.name}. Error: {str(e)}")
            raise e
            
        return report