"""
tasks/restaurant_extractor.py
Restaurant Extraction Logic and Pipeline Task for Zomato Scraper:
1. Extracts raw restaurant details via RESTAURANT_DETAILS_API_URL and preloaded HTML fallback.
2. Coordinates review extraction via tasks/review_extractor.py.
3. Coordinates parsing via parsers/parser.py.
4. Persists raw responses and parsed records into 'zomato_restaurants' table in PostgreSQL.
5. Updates job stage in 'zomato_jobs' and batch progress in 'zomato_batches'.
6. Zero MongoDB, Zero SQLite - 100% PostgreSQL schema 'zomato'.
"""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from django.conf import settings
from django.db import transaction
from django.utils import timezone
from rq import get_current_job

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    RESTAURANT_DETAILS_API_URL,
    SINGLE_QUEUE_NAME,
)
from scraper import extract_zomato_preloaded_state, ZomatoScraper
from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch
from scraper_app.parsers.parser import build_delivery_document
from scraper_app.tasks.base import check_and_wait_if_paused, get_active_scraper
from scraper_app.tasks.review_extractor import extract_restaurant_reviews

logger = logging.getLogger("restaurant_extractor")


def _handle_failed_urls_after_queue_completion(batch: ScrapeBatch) -> int:
    """
    Runs AFTER queue completion of all pushed jobs for a batch:
    If all pushed jobs are finished and any jobs failed during the initial run
    (secondary_attempt_done=False), pushes ONLY those failed jobs into the single
    queue 'zomato' for worker processing based on status.
    """
    if not batch:
        return 0
    try:
        from scraper_app.workers.failed_jobs import FailedJobsHandler
        handler = FailedJobsHandler()
        res = handler.process_failed_jobs_for_batch(batch_id=str(batch.requestid))
        return res.get("requeued_count", 0)
    except Exception as exc:
        logger.warning("[RESTAURANT_EXTRACTOR] Error in _handle_failed_urls_after_queue_completion: %s", exc)
        return 0


def extract_restaurant_details(
    restaurant_url: str,
    scraper: Optional[ZomatoScraper] = None,
    max_retries: int = 3,
) -> Tuple[Optional[Dict[str, Any]], Optional[int], str]:
    """
    Extracts raw restaurant details via RESTAURANT_DETAILS_API_URL.
    Falls back to preloaded HTML state if the API fails or returns empty.
    Returns:
        (restaurant_details_raw, restaurant_id, restaurant_name)
    """
    if scraper is None:
        scraper = get_active_scraper()

    clean_path = str(restaurant_url or "").strip()
    clean_path = re.sub(r"^https?://[^/]+", "", clean_path)
    if not clean_path.startswith("/"):
        clean_path = f"/{clean_path}"

    restaurant_details_raw: Optional[Dict[str, Any]] = None
    csrf = scraper.ensure_csrf_token()
    headers = dict(DEFAULT_HEADERS)
    headers["x-zomato-csrft"] = csrf
    headers["referer"] = f"{BASE_URL}{clean_path}"

    for attempt in range(1, max_retries + 1):
        try:
            resp = scraper.session.get(
                RESTAURANT_DETAILS_API_URL,
                params={"page_url": clean_path, "location": "", "isMobile": 0},
                headers=headers,
                timeout=scraper.config.request_timeout,
            )
            if resp.status_code == 200:
                try:
                    restaurant_details_raw = resp.json()
                    break
                except Exception:
                    pass
            elif resp.status_code == 401 and attempt < max_retries:
                scraper.warm_session(force=True)
                headers["x-zomato-csrft"] = scraper.ensure_csrf_token(force_refresh=True)
            time.sleep(attempt * 0.5)
        except Exception as req_err:
            logger.warning("[RESTAURANT_EXTRACTOR] Attempt %d details error for %s: %s", attempt, clean_path, req_err)
            time.sleep(attempt * 0.5)

    # Fallback to HTML preloaded state if API failed
    if not restaurant_details_raw:
        try:
            html_resp = scraper.session.get(f"{BASE_URL}{clean_path}", headers=DEFAULT_HEADERS, timeout=10)
            if html_resp.status_code == 200:
                st = extract_zomato_preloaded_state(html_resp.text)
                if st:
                    pages = st.get("pages", {})
                    rest_dict = pages.get("restaurant", {})
                    current = pages.get("current", {})

                    # Extract sections from pages.restaurant
                    sections = {}
                    res_id_found = None
                    if isinstance(rest_dict, dict):
                        for rk, rv in rest_dict.items():
                            if isinstance(rv, dict) and "sections" in rv:
                                sections = rv["sections"]
                                res_id_found = rk
                                break
                        if not sections:
                            for rk, rv in rest_dict.items():
                                if isinstance(rv, dict):
                                    sections = rv
                                    res_id_found = rk
                                    break

                    found_id = None
                    if res_id_found and str(res_id_found).isdigit():
                        found_id = int(res_id_found)
                    elif current.get("resId") and str(current.get("resId")).isdigit():
                        found_id = int(current["resId"])

                    # Extract real name from SECTION_BASIC_INFO or pageTitle (never use "restaurant")
                    res_title = ""
                    if isinstance(sections.get("SECTION_BASIC_INFO"), dict):
                        res_title = sections["SECTION_BASIC_INFO"].get("name") or ""
                    if not res_title and current.get("pageTitle"):
                        res_title = current["pageTitle"].split(",")[0].strip()
                    if not res_title and current.get("title") and current.get("title") != "Order Online":
                        res_title = current["title"]

                    page_info_dict = dict(current) if isinstance(current, dict) else {}
                    if res_title:
                        page_info_dict["name"] = res_title
                    if found_id:
                        page_info_dict["resId"] = found_id

                    if sections or current:
                        restaurant_details_raw = {
                            "page_data": {"sections": sections},
                            "page_info": page_info_dict,
                            "sections": sections,
                        }
        except Exception as html_err:
            logger.warning("[RESTAURANT_EXTRACTOR] HTML fallback failed for %s: %s", clean_path, html_err)

    if not restaurant_details_raw:
        return None, None, ""

    # Resolve restaurant ID and name from raw payload
    page_info = restaurant_details_raw.get("page_info", {}) if isinstance(restaurant_details_raw.get("page_info"), dict) else {}
    sections = restaurant_details_raw.get("page_data", {}).get("sections", {}) if isinstance(restaurant_details_raw.get("page_data"), dict) else restaurant_details_raw.get("sections", {})
    basic_info = sections.get("SECTION_BASIC_INFO", {}) if isinstance(sections, dict) else {}

    raw_id = (
        basic_info.get("res_id")
        or page_info.get("resId")
        or (restaurant_details_raw.get("info", {}).get("resId") if isinstance(restaurant_details_raw.get("info"), dict) else None)
    )
    restaurant_id: Optional[int] = int(raw_id) if raw_id and str(raw_id).isdigit() else None
    res_name = basic_info.get("name") or page_info.get("name") or ""
    if res_name.strip().lower() == "restaurant":
        res_name = ""

    return restaurant_details_raw, restaurant_id, res_name


def process_restaurant_pipeline_job(
    batch_id: str,
    job_record_id: int,
    restaurant_url: str,
    name: str = "",
    city: str = "",
    location: str = "",
    location_key: str = "",
    max_review_pages: Optional[int] = 10,
    resume_stage: Optional[str] = None,
    retry: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Worker pipeline task executed by workers from the single queue 'zomato'.
    1. Extracts raw restaurant data via getPage (extract_restaurant_details).
    2. Extracts reviews via reviews/loadMore (extract_restaurant_reviews).
    3. Persists raw responses and parsed records into 'zomato_restaurants' table in DB.
    """
    rq_job = get_current_job()
    rq_job_id = rq_job.id if rq_job else ""

    scraper = get_active_scraper()

    # 1. Fetch Job Record & Batch
    job_record = RestaurantJob.objects.filter(id=job_record_id).first()
    batch = ScrapeBatch.objects.filter(requestid=batch_id).first()

    if not batch:
        logger.error("[RESTAURANT_EXTRACTOR] Batch %s does not exist. Aborting.", batch_id)
        return {"status": "cancelled", "message": "Batch was deleted or cancelled."}

    if not job_record:
        # Auto-heal missing job record in PostgreSQL
        job_record = RestaurantJob.objects.create(
            batch=batch,
            name=name or "Restaurant",
            page_url=restaurant_url,
            stage="pending",
        )

    # Respect user pause / termination
    if not check_and_wait_if_paused(batch.requestid, f"Pipeline execution for {restaurant_url}"):
        logger.info("[RESTAURANT_EXTRACTOR] Execution stopped because batch %s was terminated.", batch.requestid)
        job_record.stage = "terminated"
        job_record.save(update_fields=["stage", "updated_at"])
        return {"status": "terminated", "job_id": job_record.id}

    job_record.rq_job_id = rq_job_id
    if not job_record.started_at:
        job_record.started_at = timezone.now()

    # Clean URL path for API
    clean_path = str(restaurant_url or "").strip()
    clean_path = re.sub(r"^https?://[^/]+", "", clean_path)
    if not clean_path.startswith("/"):
        clean_path = f"/{clean_path}"

    restaurant_details_raw: Optional[Dict[str, Any]] = None
    restaurant_id: Optional[int] = job_record.restaurant_id
    reviews_list: List[Dict[str, Any]] = []

    # =========================================================================
    # PHASE 1: RESTAURANT RAW DATA EXTRACTION
    # =========================================================================
    if resume_stage != "reviews":
        job_record.stage = "processing_details"
        job_record.save(update_fields=["stage", "rq_job_id", "started_at", "updated_at"])

        restaurant_details_raw, fetched_id, res_name = extract_restaurant_details(
            restaurant_url=clean_path,
            scraper=scraper,
            max_retries=3,
        )

        if not restaurant_details_raw:
            job_record.stage = "details_failed"
            job_record.error_message = "Failed to fetch restaurant details from API and HTML fallback."
            if job_record.secondary_attempt_done:
                job_record.stage = "failed_all_retries"
            job_record.save(update_fields=["stage", "error_message", "updated_at"])
            _handle_failed_urls_after_queue_completion(batch)
            return {"status": "failed", "stage": job_record.stage, "job_id": job_record.id}

        if fetched_id:
            restaurant_id = fetched_id
            job_record.restaurant_id = restaurant_id
            job_record.save(update_fields=["restaurant_id", "updated_at"])

        chosen_name = res_name or job_record.name or name or "Restaurant"
        if chosen_name.strip().lower() == "restaurant" and job_record.name and job_record.name.lower() != "restaurant":
            chosen_name = job_record.name
        job_record.name = chosen_name
        job_record.stage = "details_done"
        job_record.save(update_fields=["name", "stage", "updated_at"])

    # =========================================================================
    # PHASE 2: REVIEWS EXTRACTION FOR THE SAME RESTAURANT
    # =========================================================================
    job_record.stage = "processing_reviews"
    job_record.save(update_fields=["stage", "updated_at"])

    if restaurant_id and (max_review_pages is None or max_review_pages > 0):
        reviews_list = extract_restaurant_reviews(
            restaurant_id=restaurant_id,
            page_url=clean_path,
            max_review_pages=max_review_pages,
            scraper=scraper,
            batch_requestid=batch.requestid,
            job_record=job_record,
        )

    # =========================================================================
    # PHASE 3: PARSING & PERSISTENCE INTO 'zomato_restaurants' TABLE
    # =========================================================================
    # Parse raw responses using separate parsing engine (parser.py)
    delivery_doc = build_delivery_document(
        restaurant_id=restaurant_id or job_record.id,
        detail_data=restaurant_details_raw or {},
        reviews_data={"reviews": reviews_list},
        city=city,
        location=location,
        location_key=location_key,
    ) or {}

    raw_combined_payload = {
        "restaurant_details": restaurant_details_raw or {},
        "reviews": reviews_list,
        "page_url": clean_path,
        "scraped_at": timezone.now().isoformat(),
    }

    final_doc_name = delivery_doc.get("name") or job_record.name or name or "Restaurant"
    if final_doc_name.strip().lower() == "restaurant" and job_record.name and job_record.name.lower() != "restaurant":
        final_doc_name = job_record.name

    # Upsert into PostgreSQL 'zomato_restaurants' table
    with transaction.atomic():
        restaurant_record, _ = RestaurantRecord.objects.update_or_create(
            requestid=batch,
            pageurl=clean_path,
            defaults={
                "restaurant_id": restaurant_id,
                "name": final_doc_name,
                "city": city,
                "location": location,
                "location_key": location_key,
                "address": delivery_doc.get("address", ""),
                "phone_number": delivery_doc.get("phone_numbers", ""),
                "timing": delivery_doc.get("timing", ""),
                "rating": delivery_doc.get("rating", ""),
                "votes": delivery_doc.get("votes") or 0,
                "cuisines": delivery_doc.get("cuisines", ""),
                "costof_two": delivery_doc.get("average_cost_for_two") or delivery_doc.get("cost_for_two", ""),
                "canonical_url": delivery_doc.get("canonical_url", ""),
                "total_reviews": len(reviews_list),
                "reviews": reviews_list,
                "raw_data": raw_combined_payload,
                "delivery_document": delivery_doc,
            },
        )

    # 4. Mark Job Completed and Update Batch Progress
    job_record.stage = "completed"
    job_record.completed_at = timezone.now()
    job_record.save(update_fields=["stage", "completed_at", "updated_at"])

    # Update ScrapeBatch progress
    batch.refresh_from_db()
    batch.completed_jobs = RestaurantJob.objects.filter(batch=batch, stage="completed").count()
    batch.failed_jobs = RestaurantJob.objects.filter(batch=batch, stage="failed_all_retries").count()
    batch.scraped_details = RestaurantRecord.objects.filter(requestid=batch).count()
    batch.scraped_reviews = sum(
        len(r.reviews) for r in RestaurantRecord.objects.filter(requestid=batch).only("reviews") if r.reviews
    )

    tot = batch.total_restaurants or 1
    done_count = batch.completed_jobs + batch.failed_jobs
    batch.progress = min(100, int((done_count / tot) * 100))
    batch.save(update_fields=["progress", "completed_jobs", "failed_jobs", "scraped_details", "scraped_reviews", "updated_at"])

    # Check for queue completion of pushed jobs and trigger retry for any failed URLs
    _handle_failed_urls_after_queue_completion(batch)

    logger.info(
        "[RESTAURANT_EXTRACTOR] Successfully processed job %d (%s) -> Stored in 'zomato_restaurants' (Progress: %d%%)",
        job_record.id,
        clean_path,
        batch.progress,
    )

    return {
        "status": "success",
        "job_id": job_record.id,
        "restaurant_id": restaurant_id,
        "restaurant_record_id": restaurant_record.id,
        "reviews_count": len(reviews_list),
    }
