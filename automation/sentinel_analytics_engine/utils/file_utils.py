import logging
import shutil
from pathlib import Path
from typing import List, Optional, Union

class FileUtils:
    """
    Utility class for robust filesystem operations within the Talatee Sentinel Engine.
    Handles directory management, file pattern matching, and archival processes.
    """
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def ensure_directories(self, paths: List[Union[str, Path]]) -> None:
        """Ensures that required directory structures exist."""
        for path in paths:
            p = Path(path)
            if not p.exists():
                p.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Created directory: {p}")

    def list_files_by_pattern(self, directory: Path, pattern: str) -> List[Path]:
        """Returns a list of files matching a glob pattern."""
        if not directory.exists():
            self.logger.error(f"Directory not found: {directory}")
            return []
        
        files = list(directory.glob(pattern))
        self.logger.debug(f"Found {len(files)} files matching '{pattern}' in {directory}")
        return sorted(files)

    def archive_processed_file(self, file_path: Path, archive_dir: Path) -> bool:
        """
        Moves a processed file to an archive directory to prevent re-processing.
        Appends a timestamp to avoid naming collisions.
        """
        try:
            if not file_path.exists():
                return False

            self.ensure_directories([archive_dir])
            
            # Construct target path with simple collision avoidance
            target_path = archive_dir / f"{file_path.stem}_processed{file_path.suffix}"
            
            # If exists, append more specific identity
            if target_path.exists():
                from datetime import datetime
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                target_path = archive_dir / f"{file_path.stem}_{ts}{file_path.suffix}"

            shutil.move(str(file_path), str(target_path))
            self.logger.info(f"Archived file: {file_path.name} -> {target_path.name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to archive file {file_path}: {str(e)}")
            return False

    def safe_delete(self, file_path: Path) -> bool:
        """Deletes a file with error handling and logging."""
        try:
            if file_path.exists():
                file_path.unlink()
                self.logger.info(f"Successfully deleted temporary file: {file_path}")
                return True
            return False
        except Exception as e:
            self.logger.warning(f"Could not delete file {file_path}: {str(e)}")
            return False

    @staticmethod
    def get_project_root() -> Path:
        """Returns the absolute path to the project root directory."""
        return Path(__file__).parent.parent.absolute()