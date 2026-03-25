import pandas as pd
import os

def detect_source(filename):
    filename = filename.lower()

    if "shopee" in filename:
        return "shopee"
    elif "tokopedia" in filename:
        return "tokopedia"
    elif "tiktok" in filename:
        return "tiktok"
    else:
        return "unknown"

def load_all_data(folder_path="data/raw"):
    all_data = []

    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            file_path = os.path.join(folder_path, file)

            df = pd.read_csv(file_path)
            df['source'] = detect_source(file)

            all_data.append(df)

    combined_df = pd.concat(all_data, ignore_index=True)

    return combined_df