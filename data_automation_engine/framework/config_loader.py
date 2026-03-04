import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 10))
    REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", 3))
    RATE_LIMIT_DELAY = int(os.getenv("RATE_LIMIT_DELAY", 2))
    DEFAULT_PAGES = int(os.getenv("DEFAULT_PAGES", 10))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")