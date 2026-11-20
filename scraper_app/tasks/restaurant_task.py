"""
Task 2: Extract Restaurant Raw Details.
Queue: restaurants_queue

Fetches comprehensive restaurant data (cuisines, timings, phone numbers,
ratings, pricing, highlights) using getPage API or HTML preloaded state fallback.
Stores full response in 'restaurants_data' and triggers simultaneous parsing.
"""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import django_rq
from django.db import models

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    RESTAURANT_DETAILS_API_URL,
)
from scraper_app.models import RestaurantRecord, ScrapeRequest
from scraper_app.parser import parse_and_sync_simultaneously
from scraper_app.tasks.base import (
    DEFAULT_RETRY,
    extract_zomato_preloaded_state,
    get_active_scraper,
    save_task_fallback_data,
    update_request_progress,
)

logger = logging.getLogger("ZomatoTasks")


def extract_restaurant_data_task(
    request_id: str,
    restaurant_id: int,
    page_url: str,
    name: str = "",
    city: str = "",
    location: str = "",
    location_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Worker task for 'restaurants_queue':
    1. Extracts raw restaurant data using getPage API.
    2. Falls back to HTML preloaded state if API returns 401.
    3. Saves raw data in collection 'restaurants_data' with _id = restaurant_id.
    4. Pushes review API job into 'reviews_queue' if not yet enqueued.
    5. Triggers simultaneous delivery_data parsing.
    """
    # Lazy import to avoid circular dependency
    from scraper_app.tasks.review_task import extract_restaurant_reviews_task

    logger.info("Starting Task 2: Details extraction for ID %s (%s)", restaurant_id, name)
    col_data = get_restaurants_data_collection()
    col_urls = get_urls_collection()
    col_reviews = get_reviews_collection()
    queue_reviews = django_rq.get_queue("reviews_queue")

    # Set polite delay to prevent burst rate limiting
    time.sleep(0.3)

    clean_url = str(page_url).strip()
    if clean_url.startswith("http"):
        clean_url = re.sub(r"^https?://[^/]+", "", clean_url)
    if not clean_url.startswith("/"):
        clean_url = f"/{clean_url}"

    try:
        scraper = get_active_scraper()
        csrf_token = scraper.ensure_csrf_token()
        headers = dict(scraper.session.headers)
        headers.update(DEFAULT_HEADERS)
        headers["referer"] = f"{BASE_URL}{clean_url}"
        if csrf_token:
            headers["x-zomato-csrft"] = csrf_token

        params = {
            "page_url": clean_url,
            "location": "",
            "isMobile": 0,
        }

        raw_json: Dict[str, Any] = {}
        resp = None

        # Enforce 0.4s delay for every request
        time.sleep(0.4)

        # Try API with up to 3 attempts
        for attempt in range(1, 4):
            try:
                resp = scraper.session.get(
                    RESTAURANT_DETAILS_API_URL,
                    params=params,
                    headers=headers,
                    timeout=scraper.config.request_timeout,
                )
                if resp.status_code == 200:
                    try:
                        raw_json = resp.json()
                        break
                    except Exception:
                        pass
                elif resp.status_code in (401, 429):
                    time.sleep(0.5 * attempt)
                    csrf_token = scraper.ensure_csrf_token(force_refresh=True)
                    if csrf_token:
                        headers["x-zomato-csrft"] = csrf_token
            except Exception as exc:
                if attempt == 3:
                    logger.warning("Failed getPage for ID %s on attempt 3: %s", restaurant_id, exc)
                time.sleep(0.3 * attempt)

        # If getPage API returned 401 or non-200, extract complete data from HTML preloaded state
        if not raw_json or getattr(resp, "status_code", 0) != 200:
            logger.info("getPage API returned non-200 for ID %s; extracting full details via web page HTML...", restaurant_id)
            for attempt in range(1, 4):
                try:
                    full_url = f"{BASE_URL}{clean_url}"
                    resp_html = scraper.session.get(full_url, headers=headers, timeout=scraper.config.request_timeout)
                    if resp_html.status_code == 200:
                        state = extract_zomato_preloaded_state(resp_html.text)
                        if state:
                            curr = state.get("pages", {}).get("restaurant", {})
                            sections = {}
                            res_id_found = None
                            for rk, rv in curr.items():
                                if isinstance(rv, dict) and "sections" in rv:
                                    sections = rv["sections"]
                                    res_id_found = rk
                                    break

                            raw_json = {
                                "page_data": {"sections": sections},
                                "page_info": {"resId": int(res_id_found) if res_id_found and res_id_found.isdigit() else restaurant_id},
                                "entities": state.get("entities", {}),
                                "status": "success",
                            }
                            break
                    elif resp_html.status_code in (429, 503) and attempt < 3:
                        time.sleep(1.0 * attempt)
                except Exception as exc:
                    if attempt == 3:
                        logger.warning("HTML details extraction fallback failed for ID %s: %s", restaurant_id, exc)
                    time.sleep(0.5 * attempt)

        scraper.close()

        if not raw_json:
            raw_json = {
                "status": getattr(resp, "status_code", "failed"),
                "fallback": True,
                "restaurant_id": restaurant_id,
                "name": name,
            }

        # Save directly to PostgreSQL model RestaurantRecord
        RestaurantRecord.objects.update_or_create(
            restaurant_id=int(restaurant_id),
            defaults={
                "name": name,
                "city": city,
                "location": location,
                "location_key": location_key or "",
                "page_url": clean_url,
                "raw_data": {"details": raw_json},
                "details_data": raw_json.get("page_data", {}).get("sections", {}) if isinstance(raw_json, dict) else {},
                "status": "details_extracted",
            }
        )
        logger.info("Saved hotel details for '%s' (ID: %s) to PostgreSQL", name, restaurant_id)

        # Update ScrapeRequest details count
        try:
            ScrapeRequest.objects.filter(request_id=request_id).update(
                scraped_details=models.F("scraped_details") + 1
            )
        except Exception:
            try:
                req = ScrapeRequest.objects.get(request_id=request_id)
                req.scraped_details += 1
                req.save(update_fields=["scraped_details"])
            except Exception:
                pass

        # Push review API job into Queue 3: reviews_queue (if not already processed or enqueued)
        clean_base = re.sub(r"/(?:info|order|photos|menu|reviews)/?$", "", clean_url)
        reviews_page_url = f"{clean_base}/reviews"

        if not col_reviews.find_one({"_id": int(restaurant_id)}):
            try:
                queue_reviews.enqueue(
                    extract_restaurant_reviews_task,
                    request_id=request_id,
                    restaurant_id=int(restaurant_id),
                    page_url=reviews_page_url,
                    name=name,
                    city=city,
                    location=location,
                    location_key=location_key,
                    retry=DEFAULT_RETRY,
                )
            except Exception as exc:
                logger.warning("Could not enqueue review job for ID %s: %s", restaurant_id, exc)

        # Initial delivery_data sync
        parsed_doc = parse_and_sync_simultaneously(
            restaurant_id=int(restaurant_id),
            request_id=request_id,
            location_key=location_key,
        )

        update_request_progress(request_id)
        return {"status": "success", "restaurant_id": restaurant_id}

    except Exception as exc:
        save_task_fallback_data(
            "extract_restaurant_data_task",
            {
                "request_id": request_id,
                "restaurant_id": restaurant_id,
                "page_url": clean_url,
                "name": name,
                "city": city,
                "location": location,
                "location_key": location_key,
            },
            exc=exc,
        )
        raise
