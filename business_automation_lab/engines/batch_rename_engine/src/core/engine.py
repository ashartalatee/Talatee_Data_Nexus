from pathlib import Path
from typing import List, Any
from src.utils.logger import setup_logger
from src.core.rules import TransformerRules
from src.core.validator import PreFlightValidator
from src.exceptions.errors import RenameEngineException

logger = setup_logger()

class BatchRenameEngine:
    """Komponen Inti Orchestrator untuk memproses penggantian nama massal berintegritas tinggi."""
    
    def __init__(self, config: Any):
        self.config = config
        self.validator = PreFlightValidator(
            allowed_extensions=config.settings["naming_rules"]["allowed_extensions"],
            allow_overwrite=config.allow_overwrite
        )

    def compile_manifest(self, target_dir: Path, prefix: str, suffix: str, use_indexing: bool) -> List[tuple]:
        """Memetakan rencana perubahan nama file secara virtual (Source Path -> Target Path)."""
        self.validator.validate_directory(target_dir)
        allowed_exts = self.config.settings["naming_rules"]["allowed_extensions"]
        
        files = [f for f in target_dir.iterdir() if f.is_file() and f.suffix.lower() in allowed_exts]
        files.sort()  # Pengurutan konsisten untuk menjamin keakuratan indeks nomor urut

        execution_plan = []
        for index, file_path in enumerate(files, start=1):
            idx_param = index if use_indexing else None
            new_stem = TransformerRules.generate_new_name(file_path.stem, prefix, suffix, idx_param)
            new_filename = f"{new_stem}{file_path.suffix}"
            target_path = file_path.with_name(new_filename)
            execution_plan.append((file_path, target_path))
            
        return execution_plan

    def execute_batch(self, target_directory: str, prefix: str = "", suffix: str = "", use_indexing: bool = True):
        """Mengeksekusi penggantian nama massal dilindungi perlindungan penuh integritas data."""
        dir_path = Path(target_directory)
        
        try:
            plan = self.compile_manifest(dir_path, prefix, suffix, use_indexing)
            if not plan:
                logger.info("Tidak ada file yang cocok dengan kriteria ekstensi untuk diproses.")
                return

            # Validasi komprehensif sebelum menyentuh storage fisik
            self.validator.check_collisions(plan)

            if self.config.dry_run:
                logger.info(f"--- [SIMULASI DRY RUN AKTIF] Memeriksa {len(plan)} file ---")
                for src, dest in plan:
                    logger.info(f"[SIMULASI] Target: '{src.name}' -> Rencana baru: '{dest.name}'")
                logger.info("--- [DRY RUN SELESAI SANITASI] Tidak ada file asli yang bermutasi ---")
                return

            # Blok Eksekusi Fisik (Titik Tunggal Modifikasi Komponen Data)
            logger.info(f"Memulai mutasi data batch sesungguhnya untuk {len(plan)} item...")
            executed_history = []
            
            for src, dest in plan:
                if src == dest:
                    continue
                try:
                    src.rename(dest)
                    executed_history.append((src, dest))
                    logger.info(f"[BERHASIL] Mengubah: '{src.name}' -> '{dest.name}'")
                except Exception as e:
                    logger.error(f"[INKONSISTENSI OS] Gagal mengubah file {src.name}: {str(e)}. Memulai prosedur rollback otomatis!")
                    self._rollback(executed_history)
                    raise RenameEngineException("Batch dihentikan di tengah jalan. Status folder dikembalikan seperti semula.") from e

            logger.info("Seluruh proses batch rename sukses diselesaikan tanpa error.")

        except RenameEngineException as e:
            logger.error(f"Operasional Engine Terhambat: {str(e)}")

    def _rollback(self, executed_history: List[tuple]):
        """Mengembalikan file yang terlanjur diubah namanya jika proses mendadak interupsi/crash."""
        logger.warning("PROSEDUR REVERT / ROLLBACK DIJALANKAN...")
        for original_src, completed_dest in reversed(executed_history):
            try:
                if completed_dest.exists():
                    completed_dest.rename(original_src)
                    logger.info(f"[RESTORED] Mengembalikan: '{completed_dest.name}' -> '{original_src.name}'")
            except Exception as rollback_err:
                logger.critical(f"[FAIL ROLLBACK] Gagal mengembalikan berkas {completed_dest.name}: {str(rollback_err)}")