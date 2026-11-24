"""
Base module for Zomato Scraper RQ Tasks.
Contains shared helpers, state extractors, scraper instantiation,
retry specifications, and fallback persistence handlers.
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import django_rq
from django.db import models
from django.utils import timezone as dj_timezone
from rq.job import Retry

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    LOCATION_GET_API_URL,
    RESTAURANTS_API_URL,
    RESTAURANT_DETAILS_API_URL,
    REVIEWS_API_URL,
    Config,
)
from scraper import ZomatoScraper, extract_restaurant_page_url
from scraper_app.models import RestaurantRecord, ScrapeBatch, ScrapeRequest
from scraper_app.parsers.parser import parse_and_sync_simultaneously

logger = logging.getLogger("ZomatoTasks")

# Standard 3-retry configuration with exponential backoff (1s, 2s, 4s)
DEFAULT_RETRY = Retry(max=3, interval=[1, 2, 4])


def extract_zomato_preloaded_state(html_text: str) -> Optional[Dict[str, Any]]:
    """
    Extracts the window.__PRELOADED_STATE__ JSON object embedded inside Zomato
    HTML pages. This contains complete data for entities (REVIEWS, RATING,
    RESTAURANTS) and sections (SECTION_BASIC_INFO, SECTION_RES_DETAILS,
    SECTION_RES_CONTACT, SECTION_REVIEWS).
    """
    if not html_text:
        return None

    # Pattern: window.__PRELOADED_STATE__ = JSON.parse("...") or window.__PRELOADED_STATE__ = {...};
    patterns = [
        r'window\.__PRELOADED_STATE__\s*=\s*JSON\.parse\((".*?")\);',
        r"window\.__PRELOADED_STATE__\s*=\s*JSON\.parse\(('.*?')\);",
        r"window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});\s*(?:window\.|<\/script>)",
    ]

    for p in patterns:
        m = re.search(p, html_text, re.DOTALL)
        if m:
            raw_val = m.group(1).strip()
            # If wrapped in JSON.parse string literal
            if raw_val.startswith('"') or raw_val.startswith("'"):
                try:
                    unescaped = json.loads(raw_val)
                    return json.loads(unescaped)
                except Exception:
                    pass
            else:
                try:
                    return json.loads(raw_val)
                except Exception:
                    pass

    return None


def get_active_scraper() -> ZomatoScraper:
    """Instantiate and initialize ZomatoScraper with fresh CSRF token."""
    config = Config()
    scraper = ZomatoScraper(config=config)
    try:
        scraper.ensure_csrf_token()
    except Exception as exc:
        logger.warning("Could not auto-refresh CSRF token: %s", exc)
    return scraper


def update_request_progress(request_id: str) -> None:
    """Updates the progress percentage and status of a ScrapeRequest."""
    try:
        req = ScrapeRequest.objects.get(request_id=request_id)
        if req.status in ("completed", "failed") and req.is_cached:
            return

        total = req.total_restaurants
        if total == 0:
            return

        det = req.scraped_details
        rev = req.scraped_reviews
        deliv = req.parsed_delivery

        # Compute weighted progress: 10% URL extraction, 40% details, 25% reviews, 25% parsing
        progress = int(10 + (det / total * 40) + (rev / total * 25) + (deliv / total * 25))
        progress = min(progress, 100)
        req.progress = progress

        # Only complete when Task 1 has finished discovering URLs and all details & delivery documents are parsed
        if req.status != "extracting_urls" and deliv >= total and det >= total:
            req.status = "completed"
            req.progress = 100
        elif req.status != "extracting_urls":
            if deliv > 0:
                req.status = "parsing"
            elif rev > 0:
                req.status = "extracting_reviews"
            elif det > 0:
                req.status = "fetching_details"

        req.save(update_fields=["progress", "status", "updated_at"])
    except ScrapeRequest.DoesNotExist:
        pass
    except Exception as exc:
        logger.warning("Error updating request progress: %s", exc)


def save_task_fallback_data(task_name: str, kwargs: Dict[str, Any], exc: Optional[Exception] = None) -> None:
    """
    Saves fetched / partial data to the database when a task completely exhausts all retries,
    allowing the pipeline to gracefully advance to the next step without hanging.
    """
    restaurant_id = kwargs.get("restaurant_id")
    request_id = kwargs.get("request_id")
    name = kwargs.get("name", "")
    city = kwargs.get("city", "")
    location = kwargs.get("location", "")
    location_key = kwargs.get("location_key")

    logger.warning(
        "Final fallback saving for task '%s' (ID=%s, request=%s): %s",
        task_name,
        restaurant_id,
        request_id,
        exc,
    )

    if task_name == "extract_restaurant_reviews_task" and restaurant_id:
        RestaurantRecord.objects.update_or_create(
            restaurant_id=int(restaurant_id),
            defaults={
                "name": name,
                "city": city,
                "location": location,
                "location_key": location_key or "",
                "reviews": [],
                "total_reviews": 0,
                "status": "failed_all_retries",
                "scraped_at": dj_timezone.now(),
            }
        )

        if request_id:
            try:
                ScrapeRequest.objects.filter(request_id=request_id).update(
                    scraped_reviews=models.F("scraped_reviews") + 1,
                    parsed_delivery=models.F("parsed_delivery") + 1,
                )
            except Exception:
                pass
            update_request_progress(request_id)

    elif task_name == "extract_restaurant_data_task" and restaurant_id:
        RestaurantRecord.objects.update_or_create(
            restaurant_id=int(restaurant_id),
            defaults={
                "name": name,
                "city": city,
                "location": location,
                "location_key": location_key or "",
                "page_url": kwargs.get("page_url", ""),
                "status": "failed_all_retries",
                "scraped_at": dj_timezone.now(),
            }
        )

        if request_id:
            try:
                ScrapeRequest.objects.filter(request_id=request_id).update(
                    scraped_details=models.F("scraped_details") + 1,
                    parsed_delivery=models.F("parsed_delivery") + 1,
                )
            except Exception:
                pass
            update_request_progress(request_id)

    elif task_name == "extract_location_urls_task" and request_id:
        try:
            req = ScrapeRequest.objects.get(request_id=request_id)
            if req.status == "extracting_urls":
                req.status = "completed" if req.total_restaurants == 0 else "fetching_details"
                req.save(update_fields=["status", "updated_at"])
        except Exception:
            pass


def check_batch_active(batch_id: Any, task_desc: str = "") -> bool:
    """
    Checks if a batch is active or was terminated/cancelled.
    Returns:
        True: batch is active and ready to proceed.
        False: batch was terminated or deleted; task should abort immediately.
    """
    b_id_str = str(batch_id or "").strip()
    if not b_id_str:
        return True

    try:
        batch = ScrapeBatch.objects.filter(batch_id=b_id_str).first()
    except Exception:
        try:
            batch = ScrapeBatch.objects.filter(request_id=b_id_str).first()
        except Exception:
            batch = None

    if not batch:
        logger.info("[CHECK_STATE] Batch %s does not exist in PostgreSQL. Aborting '%s'.", b_id_str, task_desc)
        return False

    if batch.status == "terminated":
        logger.info("[CHECK_STATE] Batch %s has been TERMINATED by user. Aborting '%s'.", b_id_str, task_desc)
        return False

    return True


# Backward-compatible alias
check_and_wait_if_paused = check_batch_active

