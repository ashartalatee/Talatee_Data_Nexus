import yaml
from pathlib import Path
from typing import Dict, Any

class EngineConfig:
    """Sistem pemuat konfigurasi terpusat. Mencegah crash akibat parameter kosong."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = Path(config_path)
        self.settings: Dict[str, Any] = self._load_yaml()
        self._validate_config()

    def _load_yaml(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Berkas konfigurasi sistem hilang di: {self.config_path}")
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _validate_config(self):
        """Memastikan semua blok konfigurasi wajib terisi dengan benar."""
        required_keys = ["naming_rules", "safety_settings"]
        for key in required_keys:
            if key not in self.settings:
                raise ValueError(f"CONFIG ERROR: Blok parameter '{key}' wajib ada di config.yaml")
        
        # Mapping parameter keamanan internal
        self.dry_run = self.settings["safety_settings"].get("dry_run", True)
        self.allow_overwrite = self.settings["safety_settings"].get("allow_overwrite", False)