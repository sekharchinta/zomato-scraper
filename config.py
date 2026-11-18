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

# MongoDB Defaults
DEFAULT_MONGO_URI = "mongodb://localhost:27017/"
DEFAULT_MONGO_DB = "zomato"
DEFAULT_MONGO_COLLECTION = "restaurants"
DEFAULT_MONGO_DETAILS_COLLECTION = "restaurants_data"
DEFAULT_MONGO_REVIEWS_COLLECTION = "reviews"
DEFAULT_MONGO_DELIVERY_COLLECTION = "delivery_data"

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
                if key not in os.environ:
                    os.environ[key] = val
    return env_vars


class Config:
    """Consolidated configuration settings."""

    def __init__(self, env_path: Path = BASE_DIR / ".env") -> None:
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
        self.request_delay: float = float(os.getenv("REQUEST_DELAY") or env.get("REQUEST_DELAY") or 1.0)
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

        # MongoDB configuration
        self.mongo_uri: str = os.getenv("MONGO_URI") or env.get("MONGO_URI") or DEFAULT_MONGO_URI
        self.mongo_db: str = os.getenv("MONGO_DB") or env.get("MONGO_DB") or DEFAULT_MONGO_DB
        self.mongo_collection: str = os.getenv("MONGO_COLLECTION") or env.get("MONGO_COLLECTION") or DEFAULT_MONGO_COLLECTION
        self.mongo_details_collection: str = (
            os.getenv("MONGO_DETAILS_COLLECTION")
            or env.get("MONGO_DETAILS_COLLECTION")
            or DEFAULT_MONGO_DETAILS_COLLECTION
        )
        self.mongo_reviews_collection: str = (
            os.getenv("MONGO_REVIEWS_COLLECTION")
            or env.get("MONGO_REVIEWS_COLLECTION")
            or DEFAULT_MONGO_REVIEWS_COLLECTION
        )
        self.mongo_delivery_collection: str = (
            os.getenv("MONGO_DELIVERY_COLLECTION")
            or env.get("MONGO_DELIVERY_COLLECTION")
            or DEFAULT_MONGO_DELIVERY_COLLECTION
        )
        raw_enable_mongo = os.getenv("ENABLE_MONGO") or env.get("ENABLE_MONGO") or "true"
        self.enable_mongo: bool = raw_enable_mongo.lower() not in ("false", "0", "no", "off")

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
            "output_dir": str(self.output_dir),
            "responses_dir": str(self.responses_dir),
            "details_dir": str(self.details_dir),
            "reviews_dir": str(self.reviews_dir),
            "delivery_dir": str(self.delivery_dir),
            "mongo_uri": self.mongo_uri,
            "mongo_db": self.mongo_db,
            "mongo_collection": self.mongo_collection,
            "mongo_details_collection": self.mongo_details_collection,
            "mongo_reviews_collection": self.mongo_reviews_collection,
            "mongo_delivery_collection": self.mongo_delivery_collection,
            "enable_mongo": self.enable_mongo,
        }
