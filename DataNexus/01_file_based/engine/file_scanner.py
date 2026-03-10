from pathlib import Path
import os
from datetime import datetime


class FileScanner:

    def __init__(self, folders):
        self.folders = folders

    def scan(self):

        files_metadata = []

        for folder in self.folders:

            folder_path = Path(folder)

            if not folder_path.exists():
                continue

            for file in folder_path.glob("*"):

                # skip jika bukan file
                if not file.is_file():
                    continue

                filename = file.name.lower()

                # filter file sementara / lock
                if filename.startswith("~") or filename.startswith(".~") or "lock" in filename:
                    continue

                metadata = {
                    "file_name": file.name,
                    "file_type": file.suffix.lower(),
                    "file_path": str(file.resolve()),
                    "file_size": os.path.getsize(file),
                    "created_time": datetime.fromtimestamp(
                        os.path.getctime(file)
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                }

                files_metadata.append(metadata)

        return files_metadata