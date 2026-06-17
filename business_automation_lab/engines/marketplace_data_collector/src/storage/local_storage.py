import os
import json
from datetime import datetime
from src.transformers.schemas import MarketplaceUnifiedModel

class LocalJsonStorage:
    def __init__(self, base_dir: str = "data"):
        self.raw_dir = os.path.join(base_dir, "raw")
        self.processed_dir = os.path.join(base_dir, "processed")
        self._ensure_directories()

    def _ensure_directories(self):
        """Membuat folder penyimpanan otomatis jika belum tersedia secara fisik."""
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)

    def save_raw(self, payload: dict, platform: str, run_id: str):
        """Menyimpan dump JSON respon asli dari marketplace untuk audit trail."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{platform}_{run_id}_raw.json"
        filepath = os.path.join(self.raw_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=4)

    def save_processed(self, model: MarketplaceUnifiedModel, run_id: str):
        """Menyimpan hasil data tervalidasi yang siap dikonsumsi sistem BI."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{model.platform}_{run_id}_clean.json"
        filepath = os.path.join(self.processed_dir, filename)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(model.model_dump_json(indent=4))