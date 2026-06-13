# src/main.py
import sys
import shutil
from pathlib import Path

# Memastikan module loading path terdaftar secara bersih di level root proyek
sys.path.append(str(Path(__file__).resolve().parents[1]))

from config.settings import EngineConfig
from src.core.engine import BatchRenameEngine

def prepare_mock_environment(workspace: Path):
    """
    Helper untuk membersihkan dan menyiapkan ulang workspace staging.
    Pendekatan ini menjamin isolasi data per sesi eksekusi.
    """
    # Jika folder sudah ada, hapus seluruh isinya untuk mencegah sisa file sesi lalu memicu collision
    if workspace.exists():
        shutil.rmtree(workspace)
        
    workspace.mkdir(parents=True, exist_ok=True)
    
    # Menyiapkan kembali berkas mentah untuk simulasi portofolio
    (workspace / "laporan penjualan bulanan shopee.csv").write_text("mock_data", encoding="utf-8")
    (workspace / "faktur customer tokopedia kuno.pdf").write_text("mock_data", encoding="utf-8")
    (workspace / "data_produk_tiktok_shop %&$.xlsx").write_text("mock_data", encoding="utf-8")

def main():
    print("==================================================================")
    print("Initializing Talatee Component: Level 2 Batch Rename Engine v1.0")
    print("==================================================================\n")
    
    # 1. Load Konfigurasi Sentral
    config = EngineConfig("config/config.yaml")
    
    # 2. Inisialisasi Core Automation Engine
    engine = BatchRenameEngine(config)
    
    # 3. Reset dan Siapkan Workspace Staging (Mencegah Pre-flight Collision)
    staging_workspace = Path("workspace_staging")
    prepare_mock_environment(staging_workspace)
    
    # 4. Eksekusi Tahap 1: DRY RUN (Aman, Hanya Simulasi Log)
    print("--- [TAHAP 1] MENJALANKAN SIMULASI DATA (DRY RUN) ---")
    config.dry_run = True  # Memastikan status awal adalah dry_run
    engine.execute_batch(
        target_directory="workspace_staging",
        prefix="PROD_DATA_",
        suffix="_CLEANSED",
        use_indexing=True
    )
    
    # 5. Eksekusi Tahap 2: LIVE RUN (Mengubah file asli secara aman)
    print("\n--- [TAHAP 2] MENONAKTIFKAN DRY RUN & EKSEKUSI MUTASI FISIK ---")
    config.dry_run = False  # Mengubah state config untuk eksekusi nyata
    engine.execute_batch(
        target_directory="workspace_staging",
        prefix="PROD_DATA_",
        suffix="_CLEANSED",
        use_indexing=True
    )

if __name__ == "__main__":
    main()