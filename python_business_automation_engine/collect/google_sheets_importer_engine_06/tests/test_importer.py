"""Unit test sederhana untuk GoogleSheetsImporter (tanpa perlu koneksi internet)."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from sheets_importer.importer import GoogleSheetsImporter, GoogleSheetsImporterError  # noqa: E402


VALID_URL = "https://docs.google.com/spreadsheets/d/1AbCdEfGhIjKlMnOpQrStUvWxYz/edit#gid=123456"
INVALID_URL = "https://example.com/not-a-sheet"


def test_extract_sheet_id():
    sheet_id = GoogleSheetsImporter.extract_sheet_id(VALID_URL)
    assert sheet_id == "1AbCdEfGhIjKlMnOpQrStUvWxYz"


def test_extract_sheet_id_invalid():
    with pytest.raises(GoogleSheetsImporterError):
        GoogleSheetsImporter.extract_sheet_id(INVALID_URL)


def test_extract_gid():
    assert GoogleSheetsImporter.extract_gid(VALID_URL) == "123456"
    assert GoogleSheetsImporter.extract_gid("https://docs.google.com/spreadsheets/d/abc/edit") == "0"


def test_build_export_url():
    importer = GoogleSheetsImporter()
    url = importer.build_export_url(VALID_URL, "csv")
    assert "1AbCdEfGhIjKlMnOpQrStUvWxYz" in url
    assert "format=csv" in url
    assert "gid=123456" in url


def test_export_csv(tmp_path):
    df = pd.DataFrame({"nama": ["Andi", "Budi"], "skor": [90, 85]})
    output_path = tmp_path / "out.csv"
    result_path = GoogleSheetsImporter.export(df, output_path, file_format="csv")

    assert Path(result_path).exists()
    df_check = pd.read_csv(result_path)
    assert list(df_check.columns) == ["nama", "skor"]
    assert len(df_check) == 2


def test_export_unsupported_format(tmp_path):
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(GoogleSheetsImporterError):
        GoogleSheetsImporter.export(df, tmp_path / "out.xyz", file_format="xyz")
