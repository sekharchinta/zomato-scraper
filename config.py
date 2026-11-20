"""
Configuration settings for Zomato API Scraper.
Handles environment variables (.env), defaults, API endpoints, and directory paths.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
RESPONSES_DIR = OUTPUT_DIR / "responses"
DETAILS_DIR = BASE_DIR / "output_details"
REVIEWS_DIR = BASE_DIR / "output_reviews"
DELIVERY_DIR = BASE_DIR / "output_delivery"

# Ensure output directories exist
RESPONSES_DIR.mkdir(parents=True, exist_ok=True)
DETAILS_DIR.mkdir(parents=True, exist_ok=True)
REVIEWS_DIR.mkdir(parents=True, exist_ok=True)
DELIVERY_DIR.mkdir(parents=True, exist_ok=True)

# API Endpoints
BASE_URL = "https://www.zomato.com"
LOCATION_SEARCH_API_URL = "https://www.zomato.com/webroutes/location/search"
LOCATION_GET_API_URL = "https://www.zomato.com/webroutes/location/get"
SEARCH_API_URL = "https://www.zomato.com/webapi/searchapi.php"
CSRF_API_URL = "https://www.zomato.com/webroutes/auth/csrf"
RESTAURANTS_API_URL = "https://www.zomato.com/webroutes/search/home"
RESTAURANT_DETAILS_API_URL = "https://www.zomato.com/webroutes/getPage"
REVIEWS_API_URL = "https://www.zomato.com/webroutes/reviews/loadMore"

# Single Redis Queue Configuration
SINGLE_QUEUE_NAME = "zomato"

# Request Delays (0.4s for general requests, 1.0s for reviews)
DEFAULT_REQUEST_DELAY = 0.4
REVIEWS_REQUEST_DELAY = 1.0

# PostgreSQL Defaults
DEFAULT_POSTGRES_HOST = "127.0.0.1"
DEFAULT_POSTGRES_PORT = 5432
DEFAULT_POSTGRES_DB = "zomato-scraper"
DEFAULT_POSTGRES_USER = "postgres"
DEFAULT_POSTGRES_PASSWORD = ""
DEFAULT_POSTGRES_SCHEMA = "zomato"

# Slack Alert Defaults
DEFAULT_SLACK_WEBHOOK_URL = ""
DEFAULT_SLACK_BOT_TOKEN = ""
DEFAULT_SLACK_CHANNEL = ""
DEFAULT_SLACK_ALERT_ENABLED = True
DEFAULT_SLACK_MIN_SUCCESS_RATE = 99.0

# Legacy Mongo Placeholders (Deprecated - Project uses 100% PostgreSQL)
DEFAULT_MONGO_URI = ""
DEFAULT_MONGO_DB = ""
DEFAULT_MONGO_COLLECTION = ""
DEFAULT_MONGO_URLS_COLLECTION = ""
DEFAULT_MONGO_DETAILS_COLLECTION = ""
DEFAULT_MONGO_REVIEWS_COLLECTION = ""
DEFAULT_MONGO_DELIVERY_COLLECTION = ""

# Pagination defaults
START_PAGE = 1
START_OFFSET = 0
PAGE_INCREMENT = 1
OFFSET_INCREMENT = 12

DEFAULT_HEADERS = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "accept": "*/*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "origin": BASE_URL,
    "referer": BASE_URL + "/",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}


def load_env(env_path: Path = BASE_DIR / ".env") -> Dict[str, str]:
    """Parse .env file into key-value pairs without requiring external libraries."""
    env_vars: Dict[str, str] = {}
    if not env_path.exists():
        return env_vars

    with env_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip("\"'")
                env_vars[key] = val
                os.environ[key] = val
    return env_vars


class Config:
    """Consolidated configuration settings."""

    def __init__(self, env_path: Path = BASE_DIR / ".env", enable_mongo: Optional[bool] = None) -> None:
        env = load_env(env_path)

        self.city: str = os.getenv("CITY") or env.get("CITY") or env.get("SEARCH_QUERY") or "Hyderabad"

        raw_lat = os.getenv("LATITUDE") or env.get("LATITUDE")
        self.latitude: Optional[float] = float(raw_lat) if raw_lat else None

        raw_lon = os.getenv("LONGITUDE") or env.get("LONGITUDE")
        self.longitude: Optional[float] = float(raw_lon) if raw_lon else None

        self.start_page: int = int(os.getenv("START_PAGE") or env.get("START_PAGE") or START_PAGE)
        self.start_offset: int = int(os.getenv("START_OFFSET") or env.get("START_OFFSET") or START_OFFSET)
        self.page_increment: int = PAGE_INCREMENT
        self.offset_increment: int = OFFSET_INCREMENT

        # Max pages: defaults to None (unlimited, runs until page contains no restaurants)
        raw_max = os.getenv("MAX_PAGES") or env.get("MAX_PAGES")
        self.max_pages: Optional[int] = int(raw_max) if raw_max and int(raw_max) > 0 else None

        self.request_timeout: int = int(os.getenv("REQUEST_TIMEOUT") or env.get("REQUEST_TIMEOUT") or 30)
        self.request_delay: float = float(os.getenv("REQUEST_DELAY") or env.get("REQUEST_DELAY") or DEFAULT_REQUEST_DELAY)
        self.reviews_request_delay: float = float(os.getenv("REVIEWS_REQUEST_DELAY") or env.get("REVIEWS_REQUEST_DELAY") or REVIEWS_REQUEST_DELAY)
        raw_mrp = os.getenv("MAX_REVIEW_PAGES") or env.get("MAX_REVIEW_PAGES")
        self.max_review_pages: Optional[int] = int(raw_mrp) if raw_mrp and str(raw_mrp).isdigit() else None

        # Optional manual credentials override
        self.zomato_csrf: str = os.getenv("ZOMATO_CSRF") or env.get("ZOMATO_CSRF") or ""
        self.zomato_cookie: str = os.getenv("ZOMATO_COOKIE") or env.get("ZOMATO_COOKIE") or ""

        self.output_dir: Path = Path(os.getenv("OUTPUT_DIR") or env.get("OUTPUT_DIR") or OUTPUT_DIR)
        self.responses_dir: Path = self.output_dir / "responses"
        self.responses_dir.mkdir(parents=True, exist_ok=True)
        self.details_dir: Path = Path(os.getenv("DETAILS_DIR") or env.get("DETAILS_DIR") or DETAILS_DIR)
        self.details_dir.mkdir(parents=True, exist_ok=True)
        self.reviews_dir: Path = Path(os.getenv("REVIEWS_DIR") or env.get("REVIEWS_DIR") or REVIEWS_DIR)
        self.reviews_dir.mkdir(parents=True, exist_ok=True)
        self.delivery_dir: Path = Path(os.getenv("DELIVERY_DIR") or env.get("DELIVERY_DIR") or DELIVERY_DIR)
        self.delivery_dir.mkdir(parents=True, exist_ok=True)

        # Single Queue Configuration
        self.single_queue_name: str = os.getenv("SINGLE_QUEUE_NAME") or env.get("SINGLE_QUEUE_NAME") or SINGLE_QUEUE_NAME

        # PostgreSQL Configuration (Active DB)
        self.postgres_host: str = os.getenv("POSTGRES_HOST") or env.get("POSTGRES_HOST") or DEFAULT_POSTGRES_HOST
        self.postgres_port: int = int(os.getenv("POSTGRES_PORT") or env.get("POSTGRES_PORT") or DEFAULT_POSTGRES_PORT)
        self.postgres_db: str = os.getenv("POSTGRES_DB") or env.get("POSTGRES_DB") or DEFAULT_POSTGRES_DB
        self.postgres_user: str = os.getenv("POSTGRES_USER") or env.get("POSTGRES_USER") or DEFAULT_POSTGRES_USER
        self.postgres_password: str = os.getenv("POSTGRES_PASSWORD") or env.get("POSTGRES_PASSWORD") or DEFAULT_POSTGRES_PASSWORD
        self.postgres_schema: str = os.getenv("POSTGRES_SCHEMA") or env.get("POSTGRES_SCHEMA") or DEFAULT_POSTGRES_SCHEMA

        # Slack Alerts Configuration
        self.slack_webhook_url: str = os.getenv("SLACK_WEBHOOK_URL") or env.get("SLACK_WEBHOOK_URL") or DEFAULT_SLACK_WEBHOOK_URL
        self.slack_bot_token: str = os.getenv("SLACK_BOT_TOKEN") or env.get("SLACK_BOT_TOKEN") or DEFAULT_SLACK_BOT_TOKEN
        self.slack_channel: str = os.getenv("SLACK_CHANNEL") or env.get("SLACK_CHANNEL") or DEFAULT_SLACK_CHANNEL
        self.slack_alert_enabled: bool = (os.getenv("SLACK_ALERT_ENABLED") or env.get("SLACK_ALERT_ENABLED") or "true").lower() in ("true", "1", "yes")
        raw_min_sr = os.getenv("SLACK_MIN_SUCCESS_RATE") or env.get("SLACK_MIN_SUCCESS_RATE") or DEFAULT_SLACK_MIN_SUCCESS_RATE
        self.slack_min_success_rate: float = float(raw_min_sr)

        # Legacy Mongo flag (Strictly disabled - 100% PostgreSQL)
        self.enable_mongo: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "city": self.city,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_page": self.start_page,
            "start_offset": self.start_offset,
            "max_pages": self.max_pages,
            "max_review_pages": self.max_review_pages,
            "request_timeout": self.request_timeout,
            "request_delay": self.request_delay,
            "reviews_request_delay": self.reviews_request_delay,
            "output_dir": str(self.output_dir),
            "responses_dir": str(self.responses_dir),
            "details_dir": str(self.details_dir),
            "reviews_dir": str(self.reviews_dir),
            "delivery_dir": str(self.delivery_dir),
            "single_queue_name": self.single_queue_name,
            "postgres_host": self.postgres_host,
            "postgres_port": self.postgres_port,
            "postgres_db": self.postgres_db,
            "postgres_schema": self.postgres_schema,
        }
