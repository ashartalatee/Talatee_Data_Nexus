import os
import json
import logging

logger = logging.getLogger("TalateeEngine")

class ConfigLoader:
    def __init__(self, config_path: str = "config/secrets.example.json"):
        self.config_path = config_path
        self.config_data = {}

    def load_client_config(self) -> dict:
        """
        Memuat berkas konfigurasi klien dan memvalidasi keberadaan blok 'ingestion'.
        """
        if not os.path.exists(self.config_path):
            logger.warning(f"Berkas konfigurasi tidak ditemukan di {self.config_path}. Menggunakan default.")
            return {"ingestion": {"proxy_enabled": False, "timeout": 10}}

        with open(self.config_path, "r", encoding="utf-8") as f:
            try:
                self.config_data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error("Format JSON pada config rusak.")
                raise e

        # Enforcer: Validasi ketat arsitektur konfigurasi agar tidak memicu eror di hulu extractor
        if "ingestion" not in self.config_data:
            critical_msg = "FATAL: Client config must contain an 'ingestion' block for marketplace clients."
            logger.critical(critical_msg)
            raise ValueError(critical_msg)

        return self.config_data