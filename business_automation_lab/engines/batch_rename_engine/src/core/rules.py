# src/core/rules.py
import re

class TransformerRules:
    """Mengelola mutasi penamaan secara deterministik tanpa merusak status sistem file."""
    
    @staticmethod
    def sanitize_name(name: str) -> str:
        """
        Membersihkan nama file menggunakan pendekatan Whitelist Alfanumerik ketat,
        menjamin tidak ada karakter ilegal atau duplikasi simbol pemisah.
        """
        # 1. Ubah spasi menjadi tunggal underscore
        clean = name.replace(" ", "_")
        
        # 2. Hapus semua karakter yang BUKAN alfanumerik, underscore (_), atau hyphen (-)
        clean = re.sub(r'[^a-zA-Z0-9_-]', '', clean)
        
        # 3. Tanggulangi kemunculan ganda/lebih dari karakter pembatas (_) akibat sanitasi simbol
        clean = re.sub(r'_{2,}', '_', clean)
        
        # 4. Bersihkan karakter pembatas jika tertinggal di ujung awal atau akhir teks (strip edge delimiters)
        clean = clean.strip('_')
        
        return clean

    @staticmethod
    def generate_new_name(base_name: str, prefix: str, suffix: str, index: int = None) -> str:
        """Menghasilkan nama baru secara aman, bersih, dan konsisten."""
        clean_base = TransformerRules.sanitize_name(base_name)
        formatted_index = f"_{index:03d}" if index is not None else ""
        return f"{prefix}{clean_base}{suffix}{formatted_index}"