from pathlib import Path
from typing import List, Set
from src.exceptions.errors import TargetValidationError, FileCollisionError

class PreFlightValidator:
    """Melakukan audit integritas direktori dan struktur penamaan sebelum mutasi fisik."""
    
    def __init__(self, allowed_extensions: List[str], allow_overwrite: bool):
        self.allowed_extensions = [ext.lower() for ext in allowed_extensions]
        self.allow_overwrite = allow_overwrite

    def validate_directory(self, target_path: Path):
        """Memastikan target operasi adalah folder yang valid dan dapat diakses."""
        if not target_path.exists():
            raise TargetValidationError(f"Path target tidak ditemukan di sistem: {target_path}")
        if not target_path.is_dir():
            raise TargetValidationError(f"Path target bukan merupakan direktori/folder: {target_path}")

    def check_collisions(self, execution_plan: List[tuple]):
        """
        Memindai seluruh rencana eksekusi untuk memastikan tidak ada dua file berbeda 
         yang menghasilkan nama baru yang sama (mencegah data terhapus/saling tindih).
        """
        intended_targets: Set[Path] = set()
        
        for source_file, target_file in execution_plan:
            # 1. Cek apakah nama target sudah ada di disk dan overwrite dilarang
            if target_file.exists() and not self.allow_overwrite and source_file != target_file:
                raise FileCollisionError(
                    f"CRITICAL SAFETY VIOLATION: File target sudah ada di dalam disk: {target_file.name}"
                )
            
            # 2. Cek apakah ada duplikasi nama baru di dalam manifes rencana internal
            if target_file in intended_targets:
                raise FileCollisionError(
                    f"CRITICAL SAFETY VIOLATION: Terjadi tabrakan nama internal untuk target: {target_file.name}"
                )
            intended_targets.add(target_file)