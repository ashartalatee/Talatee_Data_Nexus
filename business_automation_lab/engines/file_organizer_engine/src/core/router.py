# src/core/router.py
from pathlib import Path
from typing import Dict, List, Any  # <-- Pastikan 'Any' ditambahkan di sini

class FileRouter:
    """Modul murni untuk memetakan file ke direktori tujuan berdasarkan matriks konfigurasi."""
    
    def __init__(self, routing_matrix: Dict[str, Any]):
        self.matrix = routing_matrix

    def determine_target_path(self, file_path: Path, destination_root: Path) -> Path:
        """Menentukan subfolder tujuan berdasarkan ekstensi berkas secara deterministik."""
        ext = file_path.suffix.lower()
        
        for category, rules in self.matrix.items():
            if ext in rules.get("extensions", []):
                return destination_root / rules.get("target_subfolder", "unclassified")
                
        return destination_root / "unclassified"