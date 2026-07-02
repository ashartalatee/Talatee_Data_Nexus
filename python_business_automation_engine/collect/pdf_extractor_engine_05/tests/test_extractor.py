"""
Unit test sederhana untuk PDFExtractorEngine.
Jalankan dengan: python -m pytest tests/ -v
(atau cukup: python -m unittest tests/test_extractor.py)
"""

import json
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from extractor import PDFExtractorEngine  # noqa: E402


def _make_sample_pdf(path: str):
    """Buat PDF contoh berisi teks + tabel sederhana, untuk keperluan test."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import cm
    from reportlab.pdfgen import canvas

    c = canvas.Canvas(path, pagesize=A4)
    width, height = A4
    c.setFont("Helvetica-Bold", 14)
    c.drawString(2 * cm, height - 2 * cm, "Dokumen Uji Coba")
    c.setFont("Helvetica", 10)
    c.drawString(2 * cm, height - 3 * cm, "Baris teks untuk pengujian ekstraksi.")

    data = [["Kolom A", "Kolom B"], ["1", "Satu"], ["2", "Dua"]]
    x0, y0 = 2 * cm, height - 5 * cm
    row_h = 0.7 * cm
    col_w = [3 * cm, 3 * cm]
    for r, row in enumerate(data):
        x = x0
        for ci, val in enumerate(row):
            c.rect(x, y0 - r * row_h, col_w[ci], row_h)
            c.drawString(x + 0.1 * cm, y0 - r * row_h + 0.2 * cm, str(val))
            x += col_w[ci]
    c.save()


class TestPDFExtractorEngine(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.mkdtemp()
        cls.pdf_path = os.path.join(cls.tmp_dir, "sample.pdf")
        _make_sample_pdf(cls.pdf_path)
        cls.out_dir = os.path.join(cls.tmp_dir, "output")

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_dir, ignore_errors=True)

    def test_invalid_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            PDFExtractorEngine(os.path.join(self.tmp_dir, "tidak_ada.pdf"))

    def test_non_pdf_extension_raises(self):
        fake = os.path.join(self.tmp_dir, "bukan_pdf.txt")
        with open(fake, "w") as f:
            f.write("halo")
        with self.assertRaises(ValueError):
            PDFExtractorEngine(fake)

    def test_extract_metadata(self):
        engine = PDFExtractorEngine(self.pdf_path)
        meta = engine.extract_metadata()
        self.assertEqual(meta["page_count"], 1)
        self.assertIn("file_size_kb", meta)

    def test_extract_text_contains_expected_string(self):
        engine = PDFExtractorEngine(self.pdf_path)
        text = engine.extract_text()
        self.assertIn("Dokumen Uji Coba", text)

    def test_extract_tables_finds_one_table(self):
        engine = PDFExtractorEngine(self.pdf_path)
        tables = engine.extract_tables()
        total = sum(len(v) for v in tables.values())
        self.assertGreaterEqual(total, 1)

    def test_extract_all_creates_expected_files(self):
        engine = PDFExtractorEngine(self.pdf_path)
        result = engine.extract_all(self.out_dir)

        self.assertTrue(result.text_saved)
        self.assertTrue(result.tables_saved)
        self.assertTrue(result.metadata_saved)
        self.assertEqual(len(result.errors), 0)

        base = "sample"
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, f"{base}_text.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, f"{base}_tables.xlsx")))
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, f"{base}_metadata.json")))
        self.assertTrue(os.path.exists(os.path.join(self.out_dir, f"{base}_summary.json")))

        with open(os.path.join(self.out_dir, f"{base}_summary.json")) as f:
            summary = json.load(f)
        self.assertEqual(summary["pages"], 1)


if __name__ == "__main__":
    unittest.main()
