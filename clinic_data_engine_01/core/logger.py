import os
from datetime import datetime

LOG_FILE = "logs/app.log"

def log(message):
    os.makedirs("logs", exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"

    print(full_message)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(full_message + "\n")