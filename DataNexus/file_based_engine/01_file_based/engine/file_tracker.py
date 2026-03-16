import json
from pathlib import Path


def load_processed_files(path):

    path = Path(path)

    # ensure folder exists
    path.parent.mkdir(parents=True, exist_ok=True)

    # create file if not exists
    if not path.exists():
        with open(path, "w") as f:
            json.dump([], f)

    with open(path, "r") as f:
        return json.load(f)


def save_processed_files(files, path):

    path = Path(path)

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(files, f, indent=4)


def update_processed_files(new_files, metadata_path):

    processed_files = load_processed_files(metadata_path)

    for f in new_files:

        file_name = f["file_name"]

        if file_name not in processed_files:
            processed_files.append(file_name)

    save_processed_files(processed_files, metadata_path)