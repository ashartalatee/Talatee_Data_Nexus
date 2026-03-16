import pandas as pd
from pathlib import Path
from datetime import datetime


def generate_dataset_name(output_dir):

    today = datetime.now().strftime("%Y_%m_%d")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    existing_files = list(output_dir.glob(f"clean_dataset_{today}_v*.csv"))

    version = len(existing_files) + 1

    filename = f"clean_dataset_{today}_v{version}.csv"

    return output_dir / filename


def save_dataset(df, output_dir):

    if df is None or df.empty:
        raise ValueError("Dataset is empty. Nothing to save.")

    output_path = generate_dataset_name(output_dir)

    df.to_csv(output_path, index=False)

    print(f"\nDataset saved successfully:")
    print(output_path)

    return output_path