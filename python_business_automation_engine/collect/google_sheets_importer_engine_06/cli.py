"""
CLI untuk Google Sheets Importer Engine — untuk pengguna teknis / dijadwalkan (cron).

Contoh pemakaian:
    python cli.py --url "https://docs.google.com/spreadsheets/d/xxxx/edit" --out data.csv
    python cli.py --url "<link>" --out data.xlsx --format excel
    python cli.py --url "<link>" --out data.csv --credentials credentials.json --worksheet "Sheet2"
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
from sheets_importer.importer import GoogleSheetsImporter, GoogleSheetsImporterError  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description="Google Sheets Importer Engine (CLI)")
    parser.add_argument("--url", required=True, help="Link Google Sheets")
    parser.add_argument("--out", required=True, help="Path file output, contoh: data.csv")
    parser.add_argument(
        "--format", default="csv", choices=["csv", "excel", "json"], help="Format output (default: csv)"
    )
    parser.add_argument("--credentials", default=None, help="Path ke credentials.json (untuk sheet privat)")
    parser.add_argument("--worksheet", default=None, help="Nama tab/worksheet (untuk sheet privat)")
    args = parser.parse_args()

    importer = GoogleSheetsImporter(credentials_path=args.credentials)

    try:
        df = importer.import_auto(args.url, worksheet_name=args.worksheet)
        output_path = importer.export(df, args.out, file_format=args.format)
        summary = importer.get_summary()

        print(f"[OK] Import berhasil ({summary['mode']} mode)")
        print(f"     {summary['rows']} baris x {summary['columns']} kolom")
        print(f"     Disimpan ke: {output_path}")
    except GoogleSheetsImporterError as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
