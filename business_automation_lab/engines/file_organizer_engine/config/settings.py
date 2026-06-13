# config/settings.py
import yaml
from pathlib import Path
from typing import Dict, Any

class OrganizerConfig:
    """Sistem validasi skema aturan penataan dokumen secara terpusat."""

    def __init__(self, config_path: str = "config/rules.yaml"):
        self.config_path = Path(config_path)
        self.settings: Dict[str, Any] = self._load_yaml()
        self._validate_rules()

    def _load_yaml(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(f"Berkas aturan rules.yaml tidak ditemukan di: {self.config_path}")
        with open(self.config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _validate_rules(self):
        """Memastikan skema jalur distribusi data wajib memiliki rules dan matrix dasar."""
        if "safety_settings" not in self.settings or "routing_matrix" not in self.settings:
            raise ValueError("SCHEMA ERROR: Berkas rules.yaml wajib berisi 'safety_settings' dan 'routing_matrix'")
            
        self.dry_run = self.settings["safety_settings"].get("dry_run", True)