from pathlib import Path
import os
from datetime import datetime


def scan_folder(folder_path):

    files_data = []

    folder = Path(folder_path)

    if not folder.exists():
        return files_data

    for file in folder.glob("*"):

        if not file.is_file():
            continue

        filename = file.name.lower()

        # skip temporary / lock files
        if filename.startswith("~") or filename.startswith(".~") or "lock" in filename:
            continue

        metadata = {
            "path": file,  # <-- Path object (important for pipeline)
            "file_name": file.name,
            "file_type": file.suffix.lower(),
            "file_path": str(file.resolve()),
            "file_size": os.path.getsize(file),
            "created_time": datetime.fromtimestamp(
                os.path.getctime(file)
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }

        files_data.append(metadata)

    return files_data