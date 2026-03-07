import pandas as pd


class DataCleaner:

    @staticmethod
    def clean_quotes(data):

        df = pd.DataFrame(data)

        # Remove duplicate quotes
        df = df.drop_duplicates(subset=["text"])

        # Normalize whitespace
        df["text"] = df["text"].str.strip()
        df["author"] = df["author"].str.strip()

        # Lowercase tags
        df["tags"] = df["tags"].str.lower()

        # Reset index
        df = df.reset_index(drop=True)

        return df