"""
extractor.py
============
Core engine: PDF Extractor Engine.

Bagian dari "Python Business Automation Engine" — Engine #5 (COLLECT ENGINE).
Fungsi: mengambil TEKS, TABEL, GAMBAR, dan METADATA dari file PDF secara
otomatis, lalu menyimpannya dalam format yang siap dipakai (txt, xlsx, png,
json).

Didesain agar bisa dipakai lewat GUI (app.py) tanpa perlu tahu coding sama
sekali, tapi class PDFExtractorEngine di file ini juga bisa dipakai sendiri
lewat script Python biasa.

Library yang dipakai:
- pdfplumber -> ekstraksi teks & tabel (layout-aware)
- PyMuPDF (fitz) -> ekstraksi gambar embedded
- pypdf -> metadata dokumen
- pandas + openpyxl -> menyimpan tabel ke Excel multi-sheet
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import fitz  # PyMuPDF
import pandas as pd
import pdfplumber
from pypdf import PdfReader


ProgressCallback = Optional[Callable[[str], None]]


@dataclass
class ExtractionResult:
    """Ringkasan hasil ekstraksi satu file PDF."""

    source_pdf: str
    output_dir: str
    pages: int = 0
    text_saved: bool = False
    text_char_count: int = 0
    tables_saved: bool = False
    tables_found: int = 0
    images_saved: bool = False
    images_found: int = 0
    metadata_saved: bool = False
    errors: list[str] = field(default_factory=list)
    finished_at: str = ""

    def to_dict(self) -> dict:
        return {
            "source_pdf": self.source_pdf,
            "output_dir": self.output_dir,
            "pages": self.pages,
            "text_saved": self.text_saved,
            "text_char_count": self.text_char_count,
            "tables_saved": self.tables_saved,
            "tables_found": self.tables_found,
            "images_saved": self.images_saved,
            "images_found": self.images_found,
            "metadata_saved": self.metadata_saved,
            "errors": self.errors,
            "finished_at": self.finished_at,
        }


class PDFExtractorEngine:
    """Engine utama untuk mengekstrak konten dari satu file PDF."""

    def __init__(self, pdf_path: str, log: ProgressCallback = None):
        self.pdf_path = str(pdf_path)
        self._log = log or (lambda msg: None)

        if not os.path.isfile(self.pdf_path):
            raise FileNotFoundError(f"File tidak ditemukan: {self.pdf_path}")
        if not self.pdf_path.lower().endswith(".pdf"):
            raise ValueError("File harus berformat .pdf")

    def _say(self, msg: str) -> None:
        self._log(msg)

    # ------------------------------------------------------------------
    # METADATA
    # ------------------------------------------------------------------
    def extract_metadata(self) -> dict:
        reader = PdfReader(self.pdf_path)
        info = reader.metadata or {}
        file_stat = os.stat(self.pdf_path)

        meta = {
            "file_name": os.path.basename(self.pdf_path),
            "file_size_kb": round(file_stat.st_size / 1024, 2),
            "page_count": len(reader.pages),
            "title": str(info.title) if info and info.title else None,
            "author": str(info.author) if info and info.author else None,
            "subject": str(info.subject) if info and info.subject else None,
            "creator": str(info.creator) if info and info.creator else None,
            "producer": str(info.producer) if info and info.producer else None,
            "is_encrypted": reader.is_encrypted,
            "extracted_at": datetime.now().isoformat(timespec="seconds"),
        }
        return meta

    # ------------------------------------------------------------------
    # TEXT
    # ------------------------------------------------------------------
    def extract_text(self) -> str:
        chunks = []
        with pdfplumber.open(self.pdf_path) as pdf:
            total = len(pdf.pages)
            for i, page in enumerate(pdf.pages, start=1):
                self._say(f"  Membaca teks halaman {i}/{total}...")
                text = page.extract_text() or ""
                chunks.append(f"\n===== Halaman {i} =====\n{text}")
        return "\n".join(chunks).strip()

    # ------------------------------------------------------------------
    # TABLES
    # ------------------------------------------------------------------
    def extract_tables(self) -> dict[int, list[list[list[str]]]]:
        """Return {nomor_halaman: [tabel1, tabel2, ...]} tiap tabel = list of rows."""
        results: dict[int, list] = {}
        with pdfplumber.open(self.pdf_path) as pdf:
            total = len(pdf.pages)
            for i, page in enumerate(pdf.pages, start=1):
                self._say(f"  Mencari tabel di halaman {i}/{total}...")
                tables = page.extract_tables()
                if tables:
                    results[i] = tables
        return results

    # ------------------------------------------------------------------
    # IMAGES
    # ------------------------------------------------------------------
    def extract_images(self, output_dir: str) -> list[str]:
        saved_paths = []
        doc = fitz.open(self.pdf_path)
        for page_index in range(len(doc)):
            page = doc[page_index]
            images = page.get_images(full=True)
            for img_index, img in enumerate(images, start=1):
                xref = img[0]
                try:
                    pix = fitz.Pixmap(doc, xref)
                    if pix.n - pix.alpha > 3:  # CMYK -> RGB
                        pix = fitz.Pixmap(fitz.csRGB, pix)
                    filename = f"page{page_index + 1}_img{img_index}.png"
                    out_path = os.path.join(output_dir, filename)
                    pix.save(out_path)
                    saved_paths.append(out_path)
                    self._say(f"  Menyimpan gambar: {filename}")
                except Exception as e:  # noqa: BLE001
                    self._say(f"  [!] Lewati satu gambar (rusak): {e}")
        doc.close()
        return saved_paths

    # ------------------------------------------------------------------
    # ALL-IN-ONE
    # ------------------------------------------------------------------
    def extract_all(
        self,
        output_dir: str,
        do_text: bool = True,
        do_tables: bool = True,
        do_images: bool = True,
        do_metadata: bool = True,
    ) -> ExtractionResult:
        os.makedirs(output_dir, exist_ok=True)
        base_name = Path(self.pdf_path).stem

        result = ExtractionResult(source_pdf=self.pdf_path, output_dir=output_dir)

        with pdfplumber.open(self.pdf_path) as pdf:
            result.pages = len(pdf.pages)

        # metadata
        if do_metadata:
            try:
                self._say("[1/4] Mengambil metadata...")
                meta = self.extract_metadata()
                meta_path = os.path.join(output_dir, f"{base_name}_metadata.json")
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=2)
                result.metadata_saved = True
                self._say(f"    -> disimpan: {os.path.basename(meta_path)}")
            except Exception as e:  # noqa: BLE001
                result.errors.append(f"metadata: {e}")
                self._say(f"[!] Gagal ambil metadata: {e}")

        # text
        if do_text:
            try:
                self._say("[2/4] Mengekstrak teks...")
                text = self.extract_text()
                text_path = os.path.join(output_dir, f"{base_name}_text.txt")
                with open(text_path, "w", encoding="utf-8") as f:
                    f.write(text)
                result.text_saved = True
                result.text_char_count = len(text)
                self._say(f"    -> disimpan: {os.path.basename(text_path)} ({len(text)} karakter)")
            except Exception as e:  # noqa: BLE001
                result.errors.append(f"text: {e}")
                self._say(f"[!] Gagal ekstrak teks: {e}")

        # tables
        if do_tables:
            try:
                self._say("[3/4] Mengekstrak tabel...")
                tables_by_page = self.extract_tables()
                total_tables = sum(len(v) for v in tables_by_page.values())
                if total_tables > 0:
                    excel_path = os.path.join(output_dir, f"{base_name}_tables.xlsx")
                    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                        sheet_count = 0
                        for page_num, tables in tables_by_page.items():
                            for t_idx, table in enumerate(tables, start=1):
                                sheet_count += 1
                                sheet_name = f"p{page_num}_t{t_idx}"[:31]
                                if not table or not table[0]:
                                    continue
                                df = pd.DataFrame(table[1:], columns=table[0])
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
                    result.tables_saved = True
                    result.tables_found = total_tables
                    self._say(f"    -> disimpan: {os.path.basename(excel_path)} ({total_tables} tabel)")
                else:
                    self._say("    -> tidak ada tabel ditemukan di dokumen ini")
            except Exception as e:  # noqa: BLE001
                result.errors.append(f"tables: {e}")
                self._say(f"[!] Gagal ekstrak tabel: {e}")

        # images
        if do_images:
            try:
                self._say("[4/4] Mengekstrak gambar...")
                img_dir = os.path.join(output_dir, f"{base_name}_images")
                os.makedirs(img_dir, exist_ok=True)
                saved = self.extract_images(img_dir)
                if saved:
                    result.images_saved = True
                    result.images_found = len(saved)
                    self._say(f"    -> disimpan {len(saved)} gambar di folder {os.path.basename(img_dir)}/")
                else:
                    os.rmdir(img_dir)
                    self._say("    -> tidak ada gambar ditemukan di dokumen ini")
            except Exception as e:  # noqa: BLE001
                result.errors.append(f"images: {e}")
                self._say(f"[!] Gagal ekstrak gambar: {e}")

        result.finished_at = datetime.now().isoformat(timespec="seconds")

        summary_path = os.path.join(output_dir, f"{base_name}_summary.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, ensure_ascii=False, indent=2)

        return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Cara pakai: python extractor.py path/ke/file.pdf [folder_output]")
        sys.exit(1)

    pdf_file = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "./output"

    engine = PDFExtractorEngine(pdf_file, log=print)
    res = engine.extract_all(out_dir)
    print("\n=== SELESAI ===")
    print(json.dumps(res.to_dict(), indent=2, ensure_ascii=False))
