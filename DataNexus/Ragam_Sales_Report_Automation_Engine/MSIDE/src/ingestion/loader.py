import pandas as pd

def load_single_file(path, source_name):
    try:
        df = pd.read_csv(path)
        df["source"] = source_name
        print(f"✅ Loaded {source_name} ({len(df)} rows)")
        return df
    except Exception as e:
        print(f"❌ Error loading {source_name}: {e}")
        return pd.DataFrame()


def load_all_data(config):
    dataframes = []

    for source_name, path in config["data_sources"].items():
        df = load_single_file(path, source_name)
        dataframes.append(df)

    return dataframes