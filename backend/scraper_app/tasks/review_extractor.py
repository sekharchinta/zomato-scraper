"""
tasks/review_extractor.py
Reviews Extraction Logic for Zomato Scraper:
1. Generates review API endpoints and query parameters.
2. Extracts reviews across pages via REVIEWS_API_URL with rate-limiting and pause checks.
3. Zero MongoDB, Zero SQLite - 100% PostgreSQL schema 'zomato'.
"""

import logging
import re
import time
from typing import Any, Dict, List, Optional

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    RESTAURANT_DETAILS_API_URL,
    REVIEWS_API_URL,
)
from scraper import ZomatoScraper
from scraper_app.tasks.base import check_and_wait_if_paused, get_active_scraper

logger = logging.getLogger("review_extractor")


def generate_review_api_urls(
    restaurant_id: int,
    page_url: str,
    page: int = 1,
    sort: str = "dd",
    filter_type: str = "reviews-dining",
) -> Dict[str, Any]:
    """
    Generates review URLs and parameters for a restaurant.
    Used for calling the Zomato reviews API and constructing referer URLs.
    """
    clean_url = str(page_url or "").strip()
    clean_url = re.sub(r"^https?://[^/]+", "", clean_url)
    if clean_url and not clean_url.startswith("/"):
        clean_url = f"/{clean_url}"
    clean_base = re.sub(r"/(?:info|order|photos|menu|reviews)/?$", "", clean_url)
    reviews_path = f"{clean_base}/reviews"

    return {
        "res_id": int(restaurant_id),
        "clean_base": clean_base,
        "reviews_path": reviews_path,
        "load_more_url": REVIEWS_API_URL,
        "load_more_params": {
            "res_id": int(restaurant_id),
            "page": page,
            "sort": sort,
            "filter": filter_type,
        },
        "get_page_url": RESTAURANT_DETAILS_API_URL,
        "get_page_params": {
            "page_url": f"{reviews_path}?page={page}",
            "location": "",
            "isMobile": 0,
        },
        "html_url": f"{BASE_URL}{reviews_path}?page={page}",
        "referer": f"{BASE_URL}{reviews_path}",
    }


def extract_restaurant_reviews(
    restaurant_id: int,
    page_url: str,
    max_review_pages: Optional[int] = 10,
    scraper: Optional[ZomatoScraper] = None,
    batch_requestid: Optional[Any] = None,
    job_record: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """
    Extracts reviews for a restaurant across multiple pages from REVIEWS_API_URL.
    Checks and waits if batch is paused or terminated.
    Updates job_record pagination fields if provided.
    Returns:
        List of raw review dicts.
    """
    if not restaurant_id or (max_review_pages is not None and max_review_pages <= 0):
        return []

    if scraper is None:
        scraper = get_active_scraper()

    clean_path = str(page_url or "").strip()
    clean_path = re.sub(r"^https?://[^/]+", "", clean_path)
    if not clean_path.startswith("/"):
        clean_path = f"/{clean_path}"

    review_urls_info = generate_review_api_urls(restaurant_id=restaurant_id, page_url=clean_path)
    reviews_list: List[Dict[str, Any]] = []
    rev_page = 1
    limit_pages = max_review_pages or 10

    while rev_page <= limit_pages:
        if batch_requestid and not check_and_wait_if_paused(batch_requestid, f"Reviews page {rev_page} for {clean_path}"):
            break

        rev_resp = None
        params = {
            "res_id": restaurant_id,
            "page": rev_page,
            "sort": "dd",
            "filter": "reviews-dining",
        }
        csrf = scraper.ensure_csrf_token()
        rev_headers = {
            "user-agent": DEFAULT_HEADERS["user-agent"],
            "referer": review_urls_info.get("referer", BASE_URL),
            "x-zomato-csrft": csrf,
        }

        for att in range(1, 3):
            try:
                rev_resp = scraper.session.get(
                    REVIEWS_API_URL,
                    params=params,
                    headers=rev_headers,
                    timeout=scraper.config.request_timeout,
                )
                if rev_resp.status_code == 200:
                    break
                time.sleep(att * 0.5)
            except Exception:
                time.sleep(0.5)

        page_reviews = []
        if rev_resp and rev_resp.status_code == 200:
            try:
                rev_data = rev_resp.json()
                if isinstance(rev_data, dict):
                    entities = rev_data.get("entities", {})
                    if isinstance(entities, dict) and "REVIEWS" in entities:
                        for r_id, r_body in entities["REVIEWS"].items():
                            if isinstance(r_body, dict):
                                page_reviews.append(r_body)
                    elif "reviews" in rev_data and isinstance(rev_data["reviews"], list):
                        page_reviews = rev_data["reviews"]
            except Exception:
                pass

        # Fallback to direct HTML reviews page with extract_zomato_preloaded_state if API failed or 401'd
        if not page_reviews and rev_page == 1 and review_urls_info.get("clean_base"):
            try:
                rev_html_url = f"{BASE_URL}{review_urls_info['clean_base']}/reviews?page={rev_page}"
                html_resp = scraper.session.get(rev_html_url, headers=rev_headers, timeout=scraper.config.request_timeout)
                if html_resp.status_code == 200:
                    from scraper import extract_zomato_preloaded_state
                    st = extract_zomato_preloaded_state(html_resp.text)
                    if st and isinstance(st, dict):
                        entities = st.get("entities", {})
                        if isinstance(entities, dict) and "REVIEWS" in entities:
                            for r_id, r_body in entities["REVIEWS"].items():
                                if isinstance(r_body, dict):
                                    page_reviews.append(r_body)
            except Exception as h_err:
                logger.warning("[REVIEW_EXTRACTOR] HTML reviews fallback error for %s: %s", clean_path, h_err)

        if not page_reviews:
            break

        reviews_list.extend(page_reviews)
        if job_record:
            job_record.last_review_page = rev_page
            job_record.total_review_pages = rev_page
            job_record.save(update_fields=["last_review_page", "total_review_pages", "updated_at"])

        rev_page += 1
        time.sleep(1.0)  # 1.0s delay between reviews requests

    return reviews_list
