"""
Main entry point untuk Talatee Engine
Jalankan semua client sesuai konfigurasi
"""

from src.orchestrator.manager import run_all_clients

if __name__ == "__main__":
    try:
        print("[START] Running all clients...")
        run_all_clients()
        print("[DONE] All clients executed successfully.")
    except Exception as e:
        print(f"[FATAL ERROR] Engine failed: {e}")