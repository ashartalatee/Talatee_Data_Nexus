from datetime import datetime


def should_run(schedule, last_run=None):
    """
    Smart scheduler:
    - Daily / Weekly / Monthly
    - Anti double run (optional last_run)
    - Validasi aman
    """

    if not schedule or "type" not in schedule:
        print("[WARNING] Invalid schedule config")
        return False

    now = datetime.now()
    schedule_type = schedule["type"].lower()

    # ========================
    # DAILY
    # ========================
    if schedule_type == "daily":
        if last_run:
            # hindari run 2x dalam hari yang sama
            if last_run.date() == now.date():
                print("[SKIP] Already run today")
                return False

        print("[RUN] Daily schedule matched")
        return True

    # ========================
    # WEEKLY
    # ========================
    if schedule_type == "weekly":
        days = schedule.get("days", [])

        # normalize ke lowercase
        days = [d.lower() for d in days]

        today = now.strftime("%A").lower()

        if today in days:
            if last_run and last_run.date() == now.date():
                print("[SKIP] Already run today (weekly)")
                return False

            print(f"[RUN] Weekly match: {today}")
            return True

        print(f"[SKIP] Weekly not matched: today={today}, expected={days}")
        return False

    # ========================
    # MONTHLY
    # ========================
    if schedule_type == "monthly":
        target_day = schedule.get("day", 1)

        # handle jika tanggal > jumlah hari bulan
        last_day_of_month = (datetime(now.year, now.month % 12 + 1, 1) - 
                             datetime(now.year, now.month, 1)).days

        run_day = min(target_day, last_day_of_month)

        if now.day == run_day:
            if last_run and last_run.date() == now.date():
                print("[SKIP] Already run today (monthly)")
                return False

            print(f"[RUN] Monthly match: day {run_day}")
            return True

        print(f"[SKIP] Monthly not matched: today={now.day}, target={run_day}")
        return False

    # ========================
    # UNKNOWN TYPE
    # ========================
    print(f"[WARNING] Unknown schedule type: {schedule_type}")
    return False