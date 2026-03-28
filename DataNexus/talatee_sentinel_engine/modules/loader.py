import pandas as pd
import os

DATA_PATH = "data/raw"


def load_all_data():
    dataframes = []

    for file in os.listdir(DATA_PATH):
        if file.endswith(".csv"):
            file_path = os.path.join(DATA_PATH, file)
            print(f"📥 Loading file: {file}")

            try:
                df = pd.read_csv(file_path, encoding="latin1")

                # Normalize kolom awal
                df.columns = df.columns.str.lower().str.strip()

                df["source"] = file
                dataframes.append(df)

            except Exception as e:
                print(f"❌ Error loading {file}: {e}")

    if not dataframes:
        raise ValueError("🚫 Tidak ada data yang berhasil dimuat!")

    combined_df = pd.concat(dataframes, ignore_index=True)

    print("\n✅ Semua data berhasil digabung!")
    return combined_df