import pandas as pd
import os

def load_data(input_path="data/input"):
    """
    Load semua CSV di folder input, tambahkan kolom 'source' otomatis
    Return: single DataFrame gabungan
    """
    all_files = [f for f in os.listdir(input_path) if f.endswith(".csv")]
    data_frames = []

    for file in all_files:
        path = os.path.join(input_path, file)
        # Load CSV
        df = pd.read_csv(path)
        # Tambahkan kolom source berdasarkan nama file
        df["source"] = file.split(".")[0]  # shopee, tokopedia
        data_frames.append(df)
        print(f"✅ Loaded {file} ({df.shape[0]} rows)")

    # Gabungkan semua dataframe
    combined_df = pd.concat(data_frames, ignore_index=True)
    print(f"\n Total data combined: {combined_df.shape[0]} rows")
    return combined_df