import os
import json
import time
from playwright.sync_api import sync_playwright

class IDXScraper:
    def __init__(self, config_path: str = "core_system/config.json"):
        with open(config_path, "r") as f:
            self.config = json.load(f)
            
    def fetch_latest_pdf(self) -> str:
        """
        Membuka website IDX, mencari dokumen keterbukaan informasi kepemilikan saham terbaru,
        dan mengunduhnya langsung ke folder input.
        Return: Path file PDF yang berhasil di-download, atau None jika gagal.
        """
        url = self.config["scraper"]["idx_url"]
        keyword = self.config["scraper"]["search_keyword"]
        input_dir = self.config["path"]["input_pdf_dir"]
        headless = self.config["scraper"]["headless_mode"]
        
        os.makedirs(input_dir, exist_ok=True)
        
        print("[SCRAPER] Memulai browser automation via Playwright...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()
            
            try:
                # 1. Buka halaman Keterbukaan Informasi IDX
                page.goto(url, wait_until="networkidle")
                
                # 2. Cari kolom input kata kunci dan ketik keyword
                # Catatan: Selektor di bawah ini adalah contoh konseptual, sesuaikan dengan struktur DOM IDX terbaru jika berubah
                search_input = page.locator("input[placeholder*='Cari'], input[type='text']").first
                search_input.fill(keyword)
                search_input.press("Enter")
                
                print(f"[SCRAPER] Mencari dokumen dengan kata kunci: '{keyword}'")
                page.wait_for_timeout(3000) # Tunggu hasil pencarian merestrukturisasi tabel
                
                # 3. Cari link download/lampiran PDF pertama (terbaru)
                # Umumnya dokumen berbentuk baris tabel dengan tombol atau link unduh
                pdf_link = page.locator("a[href*='.pdf'], button:has-text('Unduh'), a:has-text('Download')").first
                
                if pdf_link.count() == 0:
                    print("[SCRAPER] Gagal menemukan dokumen PDF terbaru hari ini.")
                    browser.close()
                    return None
                
                # 4. Prosedur penanganan trigger download berkas di Playwright
                with page.expect_download() as download_info:
                    pdf_link.click()
                download = download_info.value
                
                # Simpan file dengan nama timestamp agar tidak bentrok
                filename = f"rekap_idx_{int(time.time())}.pdf"
                save_path = os.path.join(input_dir, filename)
                
                download.save_as(save_path)
                print(f"[SCRAPER] Sukses mengunduh file terbaru: {save_path}")
                
                browser.close()
                return save_path
                
            except Exception as e:
                print(f"[SCRAPER] Terjadi error saat scraping: {str(e)}")
                browser.close()
                return None