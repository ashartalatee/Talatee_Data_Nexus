# src/core/engine.py
import shutil
from typing import List, Tuple, Any
from pathlib import Path
from typing import List, Tuple
from src.utils.logger import setup_logger
from src.core.router import FileRouter
from src.core.validator import PreFlightValidator
from src.exceptions.errors import OrganizerEngineException

logger = setup_logger()

class FileOrganizerEngine:
    """Orchestrator utama yang bertanggung jawab atas pemindahan file massal yang aman."""
    
    def __init__(self, config: Any):
        self.config = config
        self.router = FileRouter(config.settings["routing_matrix"])
        self.validator = PreFlightValidator()

    def execute_organization(self, source_dir: str, target_dir: str):
        src_path = Path(source_dir)
        dest_path = Path(target_dir)
        
        try:
            self.validator.validate_source(src_path)
            
            # 1. Kumpulkan file yang akan dipindahkan
            files_to_process = [f for f in src_path.iterdir() if f.is_file()]
            if not files_to_process:
                logger.info("Folder sumber bersih. Tidak ada file yang perlu diorganisir.")
                return

            # 2. Susun Rencana Perjalanan Berkas (Manifest)
            movement_manifest: List[Tuple[Path, Path]] = []
            for file in files_to_process:
                target_folder = self.router.determine_target_path(file, dest_path)
                target_file_path = target_folder / file.name
                movement_manifest.append((file, target_file_path))

            # 3. Jalankan Proteksi Pre-Flight
            self.validator.check_destinations(movement_manifest)

            # Skenario Simulasi
            if self.config.dry_run:
                logger.info("--- [DRY RUN SIMULASI ORGANIZER] ---")
                for src, dest in movement_manifest:
                    logger.info(f"[RUTE] File '{src.name}' -> Masuk ke: '{dest.parent}'")
                return

            # 4. Mutasi Fisik dengan Track Record Rollback
            executed_moves: List[Tuple[Path, Path]] = []
            logger.info(f"Memulai pemindahan {len(movement_manifest)} file...")
            
            for src, dest in movement_manifest:
                # Buat folder tujuan jika diizinkan
                if self.config.settings["safety_settings"].get("create_missing_dirs", True):
                    dest.parent.mkdir(parents=True, exist_ok=True)
                
                try:
                    shutil.move(str(src), str(dest))
                    executed_moves.append((src, dest))
                    logger.info(f"[MOVED] '{src.name}' sukses dipindahkan.")
                except Exception as io_err:
                    logger.error(f"[FATAL I/O] Gagal memindahkan {src.name}: {str(io_err)}. Mengaktifkan rollback!")
                    self._rollback(executed_moves)
                    raise OrganizerEngineException("Proses interupsi sistem file. State dikembalikan.") from io_err

            logger.info("File Organizer Engine sukses merapikan direktori tanpa ada error.")

        except Exception as e:
            logger.error(f"Kegagalan Operasi Engine: {str(e)}")

    def _rollback(self, executed_moves: List[Tuple[Path, Path]]):
        """Mengembalikan file yang terlanjur pindah ke posisi semula jika terjadi kegagalan tengah jalan."""
        logger.warning("MEMULAI PROSEDUR PEMBATALAN MASSAL (ROLLBACK)...")
        for original_src, completed_dest in reversed(executed_moves):
            try:
                if completed_dest.exists():
                    shutil.move(str(completed_dest), str(original_src))
                    logger.info(f"[ROLLBACK] Berhasil mengembalikan '{completed_dest.name}'")
            except Exception as re_err:
                logger.critical(f"[CRITICAL FAILURE] Gagal mengembalikan file ke tempat asal: {str(re_err)}")