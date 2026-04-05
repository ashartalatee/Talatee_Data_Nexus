"""
Schedule validation for multi-client analytics engine.
Supports daily, weekly, monthly, and custom schedules per client.
FINAL VERSION (defensive + backward compatible + force_run support)
"""

import logging
from datetime import datetime, time, timedelta
from typing import Dict, Any, Optional
import croniter
from zoneinfo import ZoneInfo
from utils.logger import setup_logger


def should_run_pipeline(config: Dict[str, Any]) -> bool:
    """
    Check if pipeline should run based on client schedule configuration.
    """
    logger = setup_logger("scheduler")

    schedule_config = config.get('schedule', {})

    # ✅ FORCE RUN (GLOBAL OVERRIDE)
    if schedule_config.get("force_run", False):
        logger.info("⚡ Force run enabled — bypassing all schedule checks")
        return True

    if not schedule_config.get('enabled', True):
        logger.info("⏭️ Pipeline disabled in config")
        return False

    schedule_type = schedule_config.get('type', 'daily')

    try:
        match schedule_type:
            case 'daily':
                return _check_daily_schedule(schedule_config)
            case 'weekly':
                return _check_weekly_schedule(schedule_config)
            case 'monthly':
                return _check_monthly_schedule(schedule_config)
            case 'custom':
                return _check_cron_schedule(schedule_config)
            case _:
                logger.warning(f"⚠️ Unknown schedule type: {schedule_type}, defaulting to daily")
                return _check_daily_schedule(schedule_config)

    except Exception as e:
        logger.error(f"💥 Schedule check failed: {str(e)}")
        return True


def _check_daily_schedule(config: Dict[str, Any]) -> bool:
    """Check daily schedule (days of week + time)."""
    logger = setup_logger("scheduler")

    try:
        # ✅ FORCE RUN (LOCAL OVERRIDE)
        if config.get("force_run", False):
            logger.info("⚡ Force run enabled (daily) — bypassing schedule")
            return True

        tz_name = config.get("timezone")
        now = datetime.now(ZoneInfo(tz_name)) if tz_name else datetime.now()

        schedule_time = _parse_time(config.get('time', '09:00'))

        # ✅ SUPPORT BOTH: run_on (NEW) & days (LEGACY)
        raw_days = config.get('run_on') or config.get('days') or []

        normalize_map = {
            "mon": "Monday", "tue": "Tuesday", "wed": "Wednesday",
            "thu": "Thursday", "fri": "Friday", "sat": "Saturday", "sun": "Sunday"
        }

        normalized_days = []
        for d in raw_days:
            if not isinstance(d, str):
                continue
            d_lower = d.lower()
            normalized_days.append(normalize_map.get(d_lower, d))

        today = now.strftime("%A")

        logger.info(f"📅 Today: {today} | Allowed: {normalized_days}")

        if today not in normalized_days:
            logger.info(f"⏭️ Not scheduled for {today}")
            return False

        # ✅ SAFE TIME CHECK
        try:
            now_seconds = now.hour * 3600 + now.minute * 60 + now.second
            schedule_seconds = schedule_time.hour * 3600 + schedule_time.minute * 60

            time_diff = abs(now_seconds - schedule_seconds)
            within_window = time_diff <= 1800  # 30 minutes

        except Exception:
            logger.warning("⚠️ Time comparison failed, fallback to True")
            return True

        logger.info(f"⏰ Time check: now={now.time()} vs target={schedule_time} | within_window={within_window}")
        return within_window

    except Exception as e:
        logger.exception(f"💥 Daily schedule check failed: {e}")
        return False


def _check_weekly_schedule(config: Dict[str, Any]) -> bool:
    """Check weekly schedule (specific day + time)."""
    logger = setup_logger("scheduler")

    now = datetime.now()
    schedule_time = _parse_time(config.get('time', '09:00'))
    schedule_day = config.get('day', 'mon')

    day_map = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}
    target_weekday = day_map.get(schedule_day, 0)

    is_correct_day = now.weekday() == target_weekday
    time_ok = (now.time() >= schedule_time)

    logger.info(f"📅 Weekly check: target={schedule_day}, today={now.strftime('%A')}, time_ok={time_ok}")
    return is_correct_day and time_ok


def _check_monthly_schedule(config: Dict[str, Any]) -> bool:
    """Check monthly schedule (day of month + time)."""
    logger = setup_logger("scheduler")

    now = datetime.now()
    schedule_time = _parse_time(config.get('time', '09:00'))
    schedule_day = config.get('day_of_month', 1)

    is_correct_day = now.day == schedule_day
    time_ok = (now.time() >= schedule_time)

    logger.info(f"📅 Monthly check: target_day={schedule_day}, today={now.day}, time_ok={time_ok}")
    return is_correct_day and time_ok


def _check_cron_schedule(config: Dict[str, Any]) -> bool:
    """Check custom cron schedule."""
    logger = setup_logger("scheduler")

    cron_expr = config.get('cron_expression')
    if not cron_expr:
        logger.warning("⚠️ No cron_expression in custom schedule")
        return False

    try:
        now = datetime.now()
        cron = croniter.croniter(cron_expr, now)
        prev = cron.get_prev(datetime)
        next_run = cron.get_next(datetime)

        time_diff = (now - prev).total_seconds()
        should_run = time_diff <= 3600

        logger.info(f"📅 Cron '{cron_expr}': next={next_run}, should_run={should_run}")
        return should_run

    except Exception as e:
        logger.error(f"💥 Invalid cron expression '{cron_expr}': {str(e)}")
        return False


def _parse_time(time_str: str) -> time:
    """Parse time string to datetime.time object."""
    try:
        return datetime.strptime(time_str, "%H:%M").time()
    except Exception:
        return datetime.strptime("09:00", "%H:%M").time()


def get_next_run_time(config: Dict[str, Any]) -> Optional[datetime]:
    """Calculate next scheduled run time."""
    schedule_type = config.get('schedule', {}).get('type', 'daily')

    match schedule_type:
        case 'daily':
            return _next_daily_run(config)
        case 'weekly':
            return _next_weekly_run(config)
        case 'monthly':
            return _next_monthly_run(config)
        case 'custom':
            return _next_cron_run(config)

    return None


def _next_daily_run(config: Dict[str, Any]) -> datetime:
    now = datetime.now()
    schedule_time = _parse_time(config['schedule'].get('time', '09:00'))
    tomorrow = now + timedelta(days=1)
    return datetime.combine(tomorrow.date(), schedule_time)


def _next_weekly_run(config: Dict[str, Any]) -> datetime:
    now = datetime.now()
    schedule_day = config['schedule'].get('day', 'mon')

    day_map = {'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3, 'fri': 4, 'sat': 5, 'sun': 6}
    target_weekday = day_map.get(schedule_day, 0)

    days_ahead = (target_weekday - now.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7

    next_date = now + timedelta(days=days_ahead)
    schedule_time = _parse_time(config['schedule'].get('time', '09:00'))

    return datetime.combine(next_date.date(), schedule_time)