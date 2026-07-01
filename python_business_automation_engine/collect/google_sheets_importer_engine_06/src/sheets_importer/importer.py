"""
Google Sheets Importer Engine
==============================
Bagian dari "Python Business Automation Engine" — Collect Engine #6.

Engine ini mengimpor data dari Google Sheets secara otomatis, dengan dua mode:

1. PUBLIC MODE  -> Tanpa setup apapun. Cukup sheet di-share sebagai
                   "Anyone with the link can view". Cocok untuk pengguna non-IT.
2. PRIVATE MODE -> Untuk sheet privat/internal perusahaan, menggunakan
                   Google Service Account (credentials.json).

Author : (isi nama kamu di sini untuk portofolio)
License: MIT
"""

import io
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import requests


class GoogleSheetsImporterError(Exception):
    """Exception khusus untuk semua error yang berhubungan dengan proses import."""
    pass


class GoogleSheetsImporter:
    """Engine untuk mengimpor data dari Google Sheets ke dalam pandas DataFrame."""

    def __init__(self, credentials_path: Optional[str] = None):
        """
        Args:
            credentials_path: Path ke file credentials.json (Service Account).
                               Hanya diperlukan untuk mode privat.
        """
        self.credentials_path = credentials_path
        self._gspread_client = None
        self.last_import_info: dict = {}

    # ------------------------------------------------------------------
    # PUBLIC MODE — tanpa setup, paling mudah untuk non-IT
    # ------------------------------------------------------------------
    @staticmethod
    def extract_sheet_id(url: str) -> str:
        """Ambil SHEET_ID dari berbagai bentuk URL Google Sheets."""
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
        if not match:
            raise GoogleSheetsImporterError(
                "Link Google Sheets tidak valid. Pastikan link berbentuk: "
                "https://docs.google.com/spreadsheets/d/SHEET_ID/edit..."
            )
        return match.group(1)

    @staticmethod
    def extract_gid(url: str) -> str:
        """Ambil GID (id tab/worksheet) dari URL. Default '0' (tab pertama)."""
        match = re.search(r"[?#&]gid=([0-9]+)", url)
        return match.group(1) if match else "0"

    def build_export_url(self, sheet_url: str, file_format: str = "csv") -> str:
        sheet_id = self.extract_sheet_id(sheet_url)
        gid = self.extract_gid(sheet_url)
        fmt = file_format if file_format in ("csv", "xlsx", "ods", "tsv") else "csv"
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format={fmt}&gid={gid}"

    def from_public_link(self, sheet_url: str) -> pd.DataFrame:
        """Import data dari sheet publik. Tidak butuh API key / credentials sama sekali."""
        export_url = self.build_export_url(sheet_url, "csv")
        response = requests.get(export_url, timeout=30)

        if response.status_code != 200 or response.text.strip().startswith("<!DOCTYPE"):
            raise GoogleSheetsImporterError(
                "Gagal mengambil data. Pastikan sheet sudah di-set "
                "'Anyone with the link can view' lewat tombol Share di Google Sheets."
            )

        try:
            df = pd.read_csv(io.StringIO(response.text))
        except Exception as e:
            raise GoogleSheetsImporterError(f"Gagal membaca data sebagai tabel: {e}")

        self.last_import_info = {
            "mode": "public",
            "source": sheet_url,
            "rows": len(df),
            "columns": len(df.columns),
            "imported_at": datetime.now().isoformat(timespec="seconds"),
        }
        return df

    # ------------------------------------------------------------------
    # PRIVATE MODE — untuk sheet internal perusahaan
    # ------------------------------------------------------------------
    def _get_gspread_client(self):
        if self._gspread_client is not None:
            return self._gspread_client

        if not self.credentials_path:
            raise GoogleSheetsImporterError(
                "credentials_path belum diset. Lihat docs/SETUP_GOOGLE_API.md "
                "untuk cara membuat Service Account."
            )
        try:
            import gspread
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise GoogleSheetsImporterError(
                "Library belum terinstall. Jalankan: pip install gspread google-auth"
            )

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/drive.readonly",
        ]
        creds = Credentials.from_service_account_file(self.credentials_path, scopes=scopes)
        self._gspread_client = gspread.authorize(creds)
        return self._gspread_client

    def from_private_sheet(self, sheet_url: str, worksheet_name: Optional[str] = None) -> pd.DataFrame:
        """Import data dari sheet privat menggunakan Service Account."""
        client = self._get_gspread_client()
        sheet_id = self.extract_sheet_id(sheet_url)

        try:
            spreadsheet = client.open_by_key(sheet_id)
            worksheet = (
                spreadsheet.worksheet(worksheet_name) if worksheet_name else spreadsheet.sheet1
            )
            records = worksheet.get_all_records()
        except Exception as e:
            raise GoogleSheetsImporterError(
                f"Gagal mengakses sheet privat: {e}. Pastikan email Service Account "
                f"sudah ditambahkan sebagai Viewer/Editor pada sheet ini."
            )

        df = pd.DataFrame(records)
        self.last_import_info = {
            "mode": "private",
            "source": sheet_url,
            "worksheet": worksheet_name or "sheet1",
            "rows": len(df),
            "columns": len(df.columns),
            "imported_at": datetime.now().isoformat(timespec="seconds"),
        }
        return df

    # ------------------------------------------------------------------
    # UNIVERSAL HELPERS
    # ------------------------------------------------------------------
    def import_auto(self, sheet_url: str, worksheet_name: Optional[str] = None) -> pd.DataFrame:
        """Coba mode publik dulu (paling mudah). Fallback ke privat bila credentials tersedia."""
        try:
            return self.from_public_link(sheet_url)
        except GoogleSheetsImporterError:
            if self.credentials_path:
                return self.from_private_sheet(sheet_url, worksheet_name)
            raise

    @staticmethod
    def export(df: pd.DataFrame, output_path: Union[str, Path], file_format: str = "csv") -> str:
        """Simpan DataFrame ke file lokal (csv / excel / json)."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if file_format == "csv":
            df.to_csv(output_path, index=False)
        elif file_format == "excel":
            df.to_excel(output_path, index=False, engine="openpyxl")
        elif file_format == "json":
            df.to_json(output_path, orient="records", indent=2, force_ascii=False)
        else:
            raise GoogleSheetsImporterError(f"Format '{file_format}' tidak didukung.")

        return str(output_path)

    def get_summary(self) -> dict:
        """Info ringkas tentang import terakhir (untuk logging/laporan)."""
        return self.last_import_info
