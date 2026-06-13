# src/main.py
import sys
import shutil
from pathlib import Path

# Daftarkan jalur modul agar keterbacaan package root aman
sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.settings import OrganizerConfig
from src.core.engine import FileOrganizerEngine

def setup_sandbox_environment(source: Path, target: Path):
    """Membersihkan sesi lama dan menyusun kembali berkas uji coba acak di folder staging."""
    for path in [source, target]:
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    # Generate mock files (Berkas representasi ekspor harian marketplace & administrasi bisnis)
    (source / "rekap_omset_shopee_januari.csv").write_text("data_omset", encoding="utf-8")
    (source / "matrix_biaya_ads_tokopedia.xlsx").write_text("data_ads", encoding="utf-8")
    (source / "invoice_supplier_tiktok.pdf").write_text("data_invoice", encoding="utf-8")
    (source / "kontrak_kerjasama_talent.docx").write_text("data_kontrak", encoding="utf-8")
    (source / "banner_promosi_gajian.png").write_text("data_gambar", encoding="utf-8")
    (source / "file_tidak_dikenal_sistem.xyz").write_text("unknown_payload", encoding="utf-8")

def main():
    print("==================================================================")
    print("Initializing Talatee Component: Level 3 File Organizer Engine v1.0")
    print("==================================================================\n")

    # 1. Muat Konfigurasi Aturan Pemetaan
    config = OrganizerConfig("config/rules.yaml")
    
    # 2. Bangun Instansiasi Engine
    engine = FileOrganizerEngine(config)
    
    # 3. Alokasikan Struktur Ruang Kerja Staging
    source_staging = Path("workspace_raw_staging")
    destination_vault = Path("workspace_organized_vault")
    setup_sandbox_environment(source_staging, destination_vault)

    # 4. Tahap 1: DRY RUN SIMULASI (Rencana rute pemetaan virtual)
    print("--- [TAHAP 1] SIMULASI RUTE DATA (DRY RUN) ---")
    config.dry_run = True
    engine.execute_organization(str(source_staging), str(destination_vault))

    # 5. Tahap 2: LIVE RUN MUTASI (Pengecekan ruang simpan & eksekusi pindah fisik berkas)
    print("\n--- [TAHAP 2] MATIKAN DRY RUN & EKSEKUSI DISTRIBUSI BERKAS ---")
    config.dry_run = False
    engine.execute_organization(str(source_staging), str(destination_vault))
    
    print("\n==================================================================")
    print("Struktur Folder Hasil Organisasi Data:")
    print("==================================================================")
    for item in destination_vault.rglob("*"):
        if item.is_file():
            print(f" -> [VAULT] {item.relative_to(destination_vault)}")

if __name__ == "__main__":
    main()