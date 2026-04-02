import json
from pathlib import Path


REQUIRED_FIELDS = ["client_name", "data_sources"]
DEFAULTS = {
    "schedule": {"type": "daily"},
    "analytics": {"top_n": 10}
}


def validate_config(config, file_path):
    """
    Validasi struktur config
    """
    for field in REQUIRED_FIELDS:
        if field not in config:
            print(f"[ERROR] Missing '{field}' in {file_path}")
            return False

    # Validasi data_sources
    if not isinstance(config["data_sources"], list):
        print(f"[ERROR] 'data_sources' must be list in {file_path}")
        return False

    return True


def apply_defaults(config):
    """
    Inject default value jika tidak ada
    """
    for key, value in DEFAULTS.items():
        if key not in config:
            config[key] = value
        else:
            # merge dict (biar tidak overwrite total)
            for sub_key, sub_val in value.items():
                if sub_key not in config[key]:
                    config[key][sub_key] = sub_val

    return config


def load_all_configs(config_folder="configs"):
    configs = []

    config_path = Path(config_folder)

    if not config_path.exists():
        print(f"[ERROR] Config folder not found: {config_folder}")
        return configs

    for file in config_path.glob("*.json"):
        print(f"[INFO] Loading config: {file}")

        try:
            content = file.read_text().strip()

            # ========================
            # SKIP FILE KOSONG
            # ========================
            if not content:
                print(f"[WARNING] Empty config skipped: {file}")
                continue

            config = json.loads(content)

            # ========================
            # VALIDATION
            # ========================
            if not validate_config(config, file):
                print(f"[WARNING] Invalid config skipped: {file}")
                continue

            # ========================
            # APPLY DEFAULT
            # ========================
            config = apply_defaults(config)

            # ========================
            # FILTER DATA SOURCE AKTIF
            # ========================
            active_sources = [
                src for src in config["data_sources"]
                if src.get("active", True)
            ]

            if not active_sources:
                print(f"[WARNING] No active data sources: {file}")
                continue

            config["data_sources"] = active_sources

            configs.append(config)

        except json.JSONDecodeError:
            print(f"[ERROR] Invalid JSON format: {file}")

        except Exception as e:
            print(f"[ERROR] Failed parsing {file}: {e}")

    print(f"[INFO] Total valid configs loaded: {len(configs)}")

    return configs