"""
Contoh pemakaian Google Sheets Importer Engine secara langsung di kode Python.
Jalankan: python examples/example_usage.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from sheets_importer.importer import GoogleSheetsImporter, GoogleSheetsImporterError  # noqa: E402


def main():
    # Ganti dengan link Google Sheets publik milikmu (Share -> Anyone with the link)
    sheet_url = "https://docs.google.com/spreadsheets/d/1XXXXXXXXXXXXXXXXXXXXX/edit#gid=0"

    importer = GoogleSheetsImporter()

    try:
        df = importer.from_public_link(sheet_url)
        print(df.head())

        output_path = importer.export(df, "output/data_export.csv", file_format="csv")
        print(f"\nData disimpan ke: {output_path}")
        print(f"Ringkasan: {importer.get_summary()}")
    except GoogleSheetsImporterError as e:
        print(f"Gagal import: {e}")


if __name__ == "__main__":
    main()
