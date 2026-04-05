"""
Configuration loader for multi-client analytics engine.
FINAL VERSION (ALIGNED WITH SCHEDULER):
- Fail Fast Validation
- Flexible Daily Schedule
- Template Skip
- Duplicate Protection
- Auto Normalization
"""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from utils.logger import setup_logger, log_error_with_context


class ConfigValidator:
    """Strict validator for client configuration."""

    REQUIRED_KEYS = {
        'client_id', 'client_name', 'schedule', 'data_sources',
        'marketplaces', 'standard_columns', 'data_cleaning',
        'feature_engineering', 'analytics', 'export', 'paths'
    }

    VALID_SCHEDULE_TYPES = {'daily', 'weekly', 'monthly', 'custom'}
    VALID_MARKETPLACES = {'shopee', 'tokopedia', 'tiktokshop', 'whatsapp'}

    @staticmethod
    def validate(config: Dict[str, Any]) -> None:
        """Validate config. Raise error if invalid (FAIL FAST)."""

        # ✅ SKIP TEMPLATE
        if config.get("is_template") or config.get("client_id") == "YOUR_CLIENT_ID":
            raise ValueError("Template config detected — skipping")

        # ✅ REQUIRED KEYS
        missing_keys = ConfigValidator.REQUIRED_KEYS - set(config.keys())
        if missing_keys:
            raise ValueError(f"Missing required keys: {missing_keys}")

        # ======================
        # ✅ SCHEDULE VALIDATION
        # ======================
        schedule = config['schedule']

        if schedule.get('type') not in ConfigValidator.VALID_SCHEDULE_TYPES:
            raise ValueError(f"Invalid schedule type: {schedule.get('type')}")

        if schedule.get('enabled', True):

            # ✅ DAILY (UPDATED — FLEXIBLE)
            if schedule['type'] == 'daily':
                # optional run_on / days → no validation needed
                pass

            # WEEKLY
            elif schedule['type'] == 'weekly':
                if not schedule.get('day'):
                    raise ValueError(f"{config['client_id']}: weekly requires 'day'")

            # MONTHLY
            elif schedule['type'] == 'monthly':
                if not schedule.get('day_of_month'):
                    raise ValueError(f"{config['client_id']}: monthly requires 'day_of_month'")

            # CUSTOM
            elif schedule['type'] == 'custom':
                if not schedule.get('cron_expression'):
                    raise ValueError(f"{config['client_id']}: custom requires cron_expression")

        # ✅ TIME FORMAT
        if 'time' in schedule:
            try:
                datetime.strptime(schedule['time'], "%H:%M")
            except Exception:
                raise ValueError(f"Invalid time format: {schedule['time']}")

        # ======================
        # ✅ MARKETPLACE VALIDATION
        # ======================
        invalid_mp = set(config['marketplaces']) - ConfigValidator.VALID_MARKETPLACES
        if invalid_mp:
            raise ValueError(f"Invalid marketplaces: {invalid_mp}")


# ======================
# 🔥 NORMALIZATION
# ======================
def normalize_schedule(schedule: Dict[str, Any]) -> Dict[str, Any]:
    normalize_map = {
        "mon": "Monday", "tue": "Tuesday", "wed": "Wednesday",
        "thu": "Thursday", "fri": "Friday", "sat": "Saturday", "sun": "Sunday"
    }

    raw_days = schedule.get("run_on") or schedule.get("days")

    if raw_days:
        schedule["run_on"] = [
            normalize_map.get(d.lower(), d)
            for d in raw_days
            if isinstance(d, str)
        ]

    return schedule


# ======================
# 🚀 MAIN LOADER
# ======================
def load_client_config(client_id: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    logger = setup_logger("config_loader")
    logger.info("📋 Loading client configurations...")

    config_dir = Path("config")
    if not config_dir.exists():
        logger.error("❌ Config directory not found")
        return {}

    configs = {}
    seen = set()

    for config_file in config_dir.glob("*.json"):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)

            cid = config_data.get('client_id')

            if not cid:
                raise ValueError("Missing client_id")

            # ✅ SKIP TEMPLATE EARLY
            if config_data.get("is_template") or cid == "YOUR_CLIENT_ID":
                logger.info(f"⏭️ Skipping template: {config_file.name}")
                continue

            # ✅ DUPLICATE PROTECTION
            if cid in seen:
                logger.warning(f"⚠️ Duplicate client_id: {cid} — skipping")
                continue

            seen.add(cid)

            # ✅ NORMALIZE
            config_data['schedule'] = normalize_schedule(config_data.get('schedule', {}))

            # ✅ VALIDATE
            ConfigValidator.validate(config_data)

            configs[cid] = config_data
            logger.info(f"✅ Loaded config: {cid}")

        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {config_file}: {str(e)}")

        except Exception as e:
            context = {"file": config_file.name}
            log_error_with_context(logger, e, context)

    # ======================
    # 🎯 SINGLE CLIENT MODE
    # ======================
    if client_id:
        if client_id not in configs:
            logger.error(f"❌ Client not found: {client_id}")
            return {}

        configs = {client_id: configs[client_id]}
        logger.info(f"📋 Single client mode: {client_id}")

    logger.info(f"✅ Valid configs loaded: {len(configs)} clients")
    return configs


# ======================
# 🧩 UTIL FUNCTIONS
# ======================
def get_config_value(config: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    keys = key_path.split('.')
    value = config

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return default

    return value


def resolve_paths(config: Dict[str, Any], base_dir: Path = Path.cwd()) -> Dict[str, Path]:
    paths_config = config.get('paths', {})
    resolved_paths = {}

    path_mappings = {
        'raw_data': 'data/raw/{client_id}',
        'cleaned_data': 'data/cleaned/{client_id}',
        'processed_data': 'data/processed/{client_id}',
        'logs': 'logs/{client_id}'
    }

    for key, template in path_mappings.items():
        rel_path = paths_config.get(key, template)
        final_path = base_dir / rel_path.format(client_id=config['client_id'])

        final_path.mkdir(parents=True, exist_ok=True)
        resolved_paths[key] = final_path

    config['paths'] = resolved_paths
    return resolved_paths


def get_enabled_sources(config: Dict[str, Any]) -> List[str]:
    return [
        source for source, settings in config.get('data_sources', {}).items()
        if settings.get('enabled', False)
    ]


def get_output_dir(config: Dict[str, Any]) -> Path:
    output_dir = config.get('export', {}).get(
        'output_dir', f"reports/{config['client_id']}"
    )
    path = Path(output_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path