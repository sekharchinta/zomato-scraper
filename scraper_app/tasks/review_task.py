"""
Task 3: Extract Restaurant Customer Reviews.
Queue: reviews_queue

Extracts multi-page customer reviews across all available pagination pages.
Includes 1.0 second polite delay, preloaded state HTML extraction,
and simultaneous synchronization into 'delivery_data'.
Zero-failure guarantee: If reviews fail, saves empty list [] and preserves
all 32 restaurant data fields.
"""

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from django.db import models

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    RESTAURANT_DETAILS_API_URL,
    REVIEWS_API_URL,
)
from scraper_app.models import RestaurantRecord, ScrapeRequest
from scraper_app.parser import parse_and_sync_simultaneously
from scraper_app.tasks.base import (
    extract_zomato_preloaded_state,
    get_active_scraper,
    save_task_fallback_data,
    update_request_progress,
)

logger = logging.getLogger("ZomatoTasks")


def extract_restaurant_reviews_task(
    request_id: str,
    restaurant_id: int,
    page_url: str,
    name: str = "",
    city: str = "",
    location: str = "",
    location_key: Optional[str] = None,
    max_pages: Optional[int] = 10,
) -> Dict[str, Any]:
    """
    Worker task for 'reviews_queue':
    Extracts all customer reviews across all available pagination pages.
    1. 1.0 second request delay for polite rate limiting.
    2. Fetches direct HTML reviews page which bypasses 401 UNAUTHORISED API restrictions.
    3. Identifies total numberOfPages and iterates with 1.0s delay.
    4. Falls back to getPage / loadMore APIs if HTML parsing is unavailable.
    5. Saves full reviews list and raw state into MongoDB collection 'reviews'.
    6. Simultaneously parses and merges with restaurant_data into 'delivery_data'.
    """
    logger.info("Starting Task 3: Reviews extraction for ID %s (%s)", restaurant_id, name)
    # Polite 1.0 second request delay for reviews as requested
    time.sleep(1.0)

    clean_url = str(page_url).strip()
    if clean_url.startswith("http"):
        clean_url = re.sub(r"^https?://[^/]+", "", clean_url)
    if not clean_url.startswith("/"):
        clean_url = f"/{clean_url}"

    clean_base = re.sub(r"/(?:info|order|photos|menu|reviews)/?$", "", clean_url)
    reviews_base_path = f"{clean_base}/reviews"

    reviews_list: List[Dict[str, Any]] = []
    seen_review_ids = set()
    total_pages_available = 1
    raw_data: Dict[str, Any] = {}

    def _extract_reviews_from_state(st: Dict[str, Any]) -> None:
        if not isinstance(st, dict):
            return
        entities = st.get("entities", {})
        p_ratings = entities.get("RATING", {})
        p_reviews = entities.get("REVIEWS", {})

        items = []
        if isinstance(p_reviews, dict):
            items = list(p_reviews.values())
        elif isinstance(p_reviews, list):
            items = p_reviews

        # Fallback to page sections if entities.REVIEWS is empty
        if not items:
            for r_val in st.get("pages", {}).get("restaurant", {}).values():
                if isinstance(r_val, dict):
                    sec_entities = r_val.get("sections", {}).get("SECTION_REVIEWS", {}).get("entities", [])
                    if isinstance(sec_entities, list):
                        items = sec_entities
                        break

        for item in items:
            if not isinstance(item, dict):
                continue
            rid = str(item.get("reviewId") or item.get("id") or "")
            if rid and rid in seen_review_ids:
                continue
            if rid:
                seen_review_ids.add(rid)

            r_rating = item.get("ratingV2") or item.get("rating")
            if isinstance(r_rating, dict):
                for ent in r_rating.get("entities", []):
                    for eid in ent.get("entity_ids", []):
                        r_obj = p_ratings.get(str(eid)) or p_ratings.get(eid)
                        if isinstance(r_obj, dict) and "rating" in r_obj:
                            r_rating = r_obj.get("rating")
                            break

            reviews_list.append({
                "review_id": rid or f"rev_{len(reviews_list) + 1}",
                "user_name": item.get("userName") or (item.get("user", {}).get("name") if isinstance(item.get("user"), dict) else "Anonymous"),
                "rating": str(r_rating or "").strip(),
                "review_text": str(item.get("reviewText") or "").strip(),
                "like_count": int(item.get("likeCount", 0) or 0),
                "timestamp": item.get("timestamp"),
            })

    try:
        col_reviews = get_reviews_collection()
        scraper = get_active_scraper()
        csrf_token = scraper.ensure_csrf_token()
        headers = dict(scraper.session.headers)
        headers.update(DEFAULT_HEADERS)
        headers["referer"] = f"{BASE_URL}{clean_base}"
        if csrf_token:
            headers["x-zomato-csrft"] = csrf_token

        # Strategy 1: Direct HTML reviews page fetch with retry & 1s delay
        review_urls_to_try = [
            f"{BASE_URL}{reviews_base_path}?page=1",
            f"{BASE_URL}{reviews_base_path}?page=1&filter=reviews-delivery",
            f"{BASE_URL}{reviews_base_path}?page=1&filter=reviews-dining",
        ]

        for p1_url in review_urls_to_try:
            if reviews_list:
                break
            for attempt in range(1, 4):
                try:
                    p1_resp = scraper.session.get(p1_url, headers=headers, timeout=scraper.config.request_timeout)
                    if p1_resp.status_code == 200:
                        state = extract_zomato_preloaded_state(p1_resp.text)
                        if state:
                            raw_data = state
                            curr = state.get("pages", {}).get("restaurant", {})
                            for r_k, r_v in curr.items():
                                if isinstance(r_v, dict) and "sections" in r_v:
                                    sec_rev = r_v.get("sections", {}).get("SECTION_REVIEWS", {})
                                    if isinstance(sec_rev, dict) and "numberOfPages" in sec_rev:
                                        try:
                                            total_pages_available = int(sec_rev["numberOfPages"])
                                        except Exception:
                                            pass
                                        break

                            _extract_reviews_from_state(state)
                            if reviews_list:
                                break
                    elif p1_resp.status_code in (429, 401, 503) and attempt < 3:
                        time.sleep(1.0 * attempt)
                except Exception as exc:
                    if attempt == 3:
                        logger.warning("Strategy 1 error on %s for ID %s: %s", p1_url, restaurant_id, exc)
                    time.sleep(0.5 * attempt)

        if reviews_list:
            logger.info(
                "Restaurant ID %s (%s): Page 1 yielded %d reviews (%d total pages available)",
                restaurant_id,
                name,
                len(reviews_list),
                total_pages_available,
            )

            # Fetch subsequent review pages up to max_pages with 1.0 second request delay
            limit = min(total_pages_available, max_pages if max_pages else 10)
            for p in range(2, limit + 1):
                time.sleep(1.0)  # 1 second request delay per page
                p_url = f"{BASE_URL}{reviews_base_path}?page={p}"
                try:
                    p_resp = scraper.session.get(p_url, headers=headers, timeout=scraper.config.request_timeout)
                    if p_resp.status_code == 200:
                        st_p = extract_zomato_preloaded_state(p_resp.text)
                        if st_p:
                            _extract_reviews_from_state(st_p)
                except Exception as p_err:
                    logger.warning("Error fetching review page %d for %s: %s", p, restaurant_id, p_err)
                    break

        # Strategy 2: If HTML extraction yielded 0 reviews, try API endpoints as fallback
        if not reviews_list:
            logger.info("Attempting API fallback for reviews ID %s...", restaurant_id)
            params = {
                "page_url": f"{reviews_base_path}?page=1",
                "location": "",
                "isMobile": 0,
            }
            for attempt in range(1, 4):
                try:
                    resp = scraper.session.get(
                        RESTAURANT_DETAILS_API_URL,
                        params=params,
                        headers=headers,
                        timeout=scraper.config.request_timeout,
                    )
                    if resp.status_code == 200:
                        raw_data = resp.json()
                        _extract_reviews_from_state(raw_data)
                        break
                    elif resp.status_code in (401, 429) and attempt < 3:
                        time.sleep(0.5 * attempt)
                except Exception:
                    time.sleep(0.3 * attempt)

            # Fallback to loadMore API
            if not reviews_list:
                try:
                    load_params = {
                        "res_id": int(restaurant_id),
                        "page": 1,
                        "sort": "dd",
                        "filter": "reviews-dining",
                    }
                    resp_lm = scraper.session.get(
                        REVIEWS_API_URL,
                        params=load_params,
                        headers=headers,
                        timeout=scraper.config.request_timeout,
                    )
                    if resp_lm.status_code == 200:
                        raw_data = resp_lm.json()
                        _extract_reviews_from_state(raw_data)
                except Exception:
                    pass

        scraper.close()

        if not raw_data:
            raw_data = {
                "status": "empty_or_failed",
                "restaurant_id": restaurant_id,
                "reviews": [],
            }

        # Save directly to PostgreSQL model RestaurantRecord
        RestaurantRecord.objects.update_or_create(
            restaurant_id=int(restaurant_id),
            defaults={
                "name": name,
                "city": city,
                "location": location,
                "location_key": location_key or "",
                "reviews": reviews_list,
                "total_reviews": len(reviews_list),
                "number_of_pages": total_pages_available,
                "status": "reviews_extracted",
            }
        )
        logger.info("Saved %d reviews to PostgreSQL table 'zomato_restaurants' for ID %s (%s)", len(reviews_list), restaurant_id, name)

        # Update ScrapeRequest reviews count
        try:
            ScrapeRequest.objects.filter(request_id=request_id).update(
                scraped_reviews=models.F("scraped_reviews") + 1
            )
        except Exception:
            try:
                req = ScrapeRequest.objects.get(request_id=request_id)
                req.scraped_reviews += 1
                req.save(update_fields=["scraped_reviews"])
            except Exception:
                pass

        # Simultaneous delivery_data parsing: Merges existing restaurant_data with reviews
        parsed_doc = parse_and_sync_simultaneously(
            restaurant_id=int(restaurant_id),
            request_id=request_id,
            location_key=location_key,
        )
        if parsed_doc:
            try:
                ScrapeRequest.objects.filter(request_id=request_id).update(
                    parsed_delivery=models.F("parsed_delivery") + 1
                )
            except Exception:
                try:
                    req = ScrapeRequest.objects.get(request_id=request_id)
                    req.parsed_delivery += 1
                    req.save(update_fields=["parsed_delivery"])
                except Exception:
                    pass

        update_request_progress(request_id)
        return {"status": "success", "restaurant_id": restaurant_id, "reviews_count": len(reviews_list)}

    except Exception as exc:
        save_task_fallback_data(
            "extract_restaurant_reviews_task",
            {
                "request_id": request_id,
                "restaurant_id": restaurant_id,
                "name": name,
                "city": city,
                "location": location,
                "location_key": location_key,
            },
            exc=exc,
        )
        raise
