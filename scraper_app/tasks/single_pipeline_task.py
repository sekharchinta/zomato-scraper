"""
Streamlined Single-Queue Worker Task:
Processes each restaurant completely inside a single job:
1. Phase 1: Fetches restaurant URL raw data (hotel details, cuisines, contact, ratings).
2. Phase 2: Generates review URLs for the same restaurant and extracts multi-page reviews with 1s delay.
3. Phase 3: Combines details and reviews into a single combined record and upserts into PostgreSQL 'zomato_restaurants'.
4. Retry & Requeue: Supports resuming from failure point (details vs reviews) and graceful fallback.
"""

import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from django.db import transaction, models
from django.utils import timezone
from rq import get_current_job

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    RESTAURANT_DETAILS_API_URL,
    REVIEWS_API_URL,
    Config,
)
from scraper import extract_zomato_preloaded_state
from scraper_app.parser import build_delivery_document
from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch
from scraper_app.tasks.base import check_and_wait_if_paused, get_active_scraper

logger = logging.getLogger("ZomatoSinglePipeline")


def generate_review_api_urls(
    restaurant_id: int,
    page_url: str,
    page: int = 1,
    sort: str = "dd",
    filter_type: str = "reviews-dining",
) -> Dict[str, Any]:
    """
    Generates review URLs and parameters from restaurant details:
    - load_more: Dedicated reviews pagination endpoint (reviews/loadMore)
    - get_page: Restaurant page router for reviews tab (getPage?page_url=.../reviews)
    - html_url: Public HTML webpage for preloaded state fallback
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
) -> Dict[str, Any]:
    """
    Worker function executed by RQ workers from 'zomato'.
    Processes a restaurant URL through all 3 stages:
    1. Details Extraction
    2. Reviews Extraction for the same restaurant
    3. Combined PostgreSQL persistence
    """
    rq_job = get_current_job()
    rq_job_id = rq_job.id if rq_job else ""

    scraper = get_active_scraper()
    clean_url = str(restaurant_url).strip()
    clean_url = re.sub(r"^https?://[^/]+", "", clean_url)
    if not clean_url.startswith("/"):
        clean_url = f"/{clean_url}"
    clean_base = re.sub(r"/(?:info|order|photos|menu|reviews)/?$", "", clean_url)

    # 0. Check if batch is paused or terminated
    if not check_and_wait_if_paused(batch_id, f"pipeline for {clean_url}"):
        scraper.close()
        logger.info("[TERMINATED_JOB] Batch %s is terminated or cancelled. Skipping job for '%s'.", batch_id, name or clean_url)
        return {
            "status": "cancelled",
            "message": f"Batch {batch_id} was terminated or cancelled.",
        }

    # 1. Lookup or auto-heal RestaurantJob
    job_record = None
    if job_record_id:
        # Try direct PK lookup
        job_record = RestaurantJob.objects.filter(id=job_record_id).first()
        if not job_record:
            # Try looking up by Zomato restaurant_id and batch_id
            job_record = RestaurantJob.objects.filter(
                batch_id=batch_id,
                restaurant_id=job_record_id,
            ).first()

    if not job_record:
        # Check if the batch still exists in PostgreSQL
        batch = ScrapeBatch.objects.filter(batch_id=batch_id).first()
        if not batch:
            scraper.close()
            logger.info(
                "[CANCELLED_JOB] Batch %s was deleted or cancelled from PostgreSQL. Skipping orphaned job for '%s' (job_record_id=%s).",
                batch_id,
                name or clean_url,
                job_record_id,
            )
            return {
                "status": "cancelled",
                "message": f"Batch {batch_id} was deleted or cancelled from database.",
            }

        # Batch exists! Auto-heal by creating the missing RestaurantJob record
        clean_res_id = int(job_record_id) if job_record_id and str(job_record_id).isdigit() and int(job_record_id) > 1000 else None
        job_record = RestaurantJob.objects.create(
            batch=batch,
            restaurant_id=clean_res_id,
            name=name or clean_url,
            page_url=clean_url,
            stage="pending",
            rq_job_id=rq_job_id,
        )
        logger.info(
            "[AUTO_HEAL] Created missing RestaurantJob (PK=%s, res_id=%s) for batch %s ('%s')",
            job_record.id,
            clean_res_id,
            batch_id,
            name or clean_url,
        )
    else:
        job_record.rq_job_id = rq_job_id
        job_record.save(update_fields=["rq_job_id", "updated_at"])

    logger.info(
        "Starting Streamlined Job %s for '%s' (batch=%s, resume_stage=%s)",
        job_record_id,
        name or clean_url,
        batch_id,
        resume_stage,
    )

    details_data: Dict[str, Any] = {}
    res_id: Optional[int] = job_record.restaurant_id
    res_name: str = name or job_record.name

    # =========================================================================
    # PHASE 1: Fetch Restaurant Details (Skip if resuming from reviews failure)
    # =========================================================================
    skip_details = (resume_stage == "reviews_failed" and res_id is not None)

    if skip_details:
        existing_rec = RestaurantRecord.objects.filter(restaurant_id=res_id).first()
        if existing_rec:
            if not details_data and existing_rec.raw_data and isinstance(existing_rec.raw_data, dict):
                details_data = existing_rec.raw_data.get("details", {})
            if not details_data and existing_rec.details_data:
                details_data = {"page_data": {"sections": existing_rec.details_data}}
            if not res_name and existing_rec.name:
                res_name = existing_rec.name
    else:
        job_record.stage = "processing_details"
        job_record.save(update_fields=["stage", "updated_at"])

        details_success = False
        headers = dict(scraper.session.headers)
        headers.update(DEFAULT_HEADERS)
        headers["referer"] = f"{BASE_URL}{clean_base}"

        for attempt in range(1, 4):
            try:
                # Enforce 0.4s delay for every request
                time.sleep(0.4)
                logger.info(
                    "[TASK %s | PHASE 1] Fetching live restaurant details for '%s' (0.4s delay, attempt %d/3)...",
                    job_record_id,
                    res_name or clean_url,
                    attempt,
                )

                csrf = scraper.ensure_csrf_token()
                if csrf:
                    headers["x-zomato-csrft"] = csrf

                # Tier 1: Try getPage API
                resp = scraper.session.get(
                    RESTAURANT_DETAILS_API_URL,
                    params={"page_url": clean_base, "location": "", "isMobile": 0},
                    headers=headers,
                    timeout=scraper.config.request_timeout,
                )
                if resp.status_code == 200:
                    details_data = resp.json()
                    details_success = True
                    break

                # Tier 2: If 401 or non-200, try direct HTML preloaded state
                full_html_url = f"{BASE_URL}{clean_base}"
                h_resp = scraper.session.get(full_html_url, headers=headers, timeout=scraper.config.request_timeout)
                if h_resp.status_code == 200:
                    st = extract_zomato_preloaded_state(h_resp.text)
                    if st:
                        res_page = st.get("pages", {}).get("restaurant", {})
                        sections = {}
                        id_found = None
                        for rk, rv in res_page.items():
                            if isinstance(rv, dict) and "sections" in rv:
                                sections = rv["sections"]
                                id_found = rk
                                break
                        details_data = {
                            "page_data": {"sections": sections},
                            "page_info": {"resId": int(id_found) if id_found and id_found.isdigit() else None, "pageUrl": clean_base},
                            "entities": st.get("entities", {}),
                            "status": "success",
                        }
                        details_success = True
                        break

                if attempt < 3:
                    time.sleep(attempt * 0.4)
            except Exception as exc:
                logger.warning("[TASK %s | PHASE 1] Attempt %d/3 failed for '%s': %s", job_record_id, clean_url, attempt, exc)
                if attempt < 3:
                    time.sleep(attempt * 0.4)

        if not details_success or not details_data:
            job_record.stage = "details_failed"
            job_record.error_message = f"Failed fetching restaurant details after 3 attempts"
            job_record.save(update_fields=["stage", "error_message", "updated_at"])
            scraper.close()
            raise RuntimeError(f"Details extraction failed for '{clean_url}' after 3 retries")

        # Extract resolved restaurant ID and info
        p_info = details_data.get("page_info", {})
        res_id = p_info.get("resId")
        if not res_id:
            sec_basic = details_data.get("page_data", {}).get("sections", {}).get("SECTION_BASIC_INFO", {})
            res_id = sec_basic.get("res_id") or sec_basic.get("id")
        if not res_id:
            # Hash-based fallback ID if missing from API
            res_id = abs(hash(clean_base)) % (10**9)

        res_id = int(res_id)
        sec_basic = details_data.get("page_data", {}).get("sections", {}).get("SECTION_BASIC_INFO", {})
        if not res_name and sec_basic.get("name"):
            res_name = sec_basic.get("name")

        job_record.restaurant_id = res_id
        job_record.name = res_name or job_record.name
        job_record.stage = "details_done"
        job_record.save(update_fields=["restaurant_id", "name", "stage", "updated_at"])
        logger.info("Phase 1 Complete: Details extracted for '%s' (ID=%s)", res_name, res_id)

    # =========================================================================
    # PHASE 2: Fetch Customer Reviews for the Same Restaurant
    # =========================================================================
    if not check_and_wait_if_paused(batch_id, f"reviews for {res_name}"):
        scraper.close()
        return {"status": "terminated", "message": f"Batch {batch_id} was terminated."}

    job_record.stage = "processing_reviews"
    job_record.save(update_fields=["stage", "updated_at"])

    # Polite 1.0s delay between restaurant operations
    time.sleep(1.0)

    reviews_list: List[Dict[str, Any]] = []
    seen_review_ids = set()
    total_pages_available = 1
    reviews_raw_payloads = []

    def _collect_review_entities(raw_obj: Dict[str, Any]) -> None:
        if not isinstance(raw_obj, dict):
            return
        entities = raw_obj.get("entities", {})
        p_ratings = entities.get("RATING", {})
        p_reviews = entities.get("REVIEWS", {})

        items = []
        if isinstance(p_reviews, dict):
            items = list(p_reviews.values())
        elif isinstance(p_reviews, list):
            items = p_reviews

        if not items:
            for r_val in raw_obj.get("pages", {}).get("restaurant", {}).values():
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

    # Determine starting page (resume from last successful page if requeued)
    start_page = 1
    if resume_stage == "reviews_failed" and job_record.last_review_page > 0:
        start_page = job_record.last_review_page + 1
        logger.info("Resuming reviews extraction from page %d for restaurant ID %s", start_page, res_id)

    # Step A: Fetch Review Page 1 / metadata if start_page == 1
    review_api_info = generate_review_api_urls(res_id, clean_base, page=1)
    headers = dict(scraper.session.headers)
    headers.update(DEFAULT_HEADERS)
    headers["referer"] = review_api_info["referer"]

    if start_page == 1:
        p1_success = False
        time.sleep(1.0)  # 1.0s delay for reviews
        logger.info("[TASK %s | PHASE 2] Fetching review page 1 & metadata for '%s' (ID=%s, 1.0s delay)...", job_record_id, res_name, res_id)
        for attempt in range(1, 4):
            try:
                # Try getPage reviews endpoint
                resp_p1 = scraper.session.get(
                    review_api_info["get_page_url"],
                    params=review_api_info["get_page_params"],
                    headers=headers,
                    timeout=scraper.config.request_timeout,
                )
                if resp_p1.status_code == 200:
                    p1_data = resp_p1.json()
                    reviews_raw_payloads.append(p1_data)
                    _collect_review_entities(p1_data)
                    sec_rev = p1_data.get("page_data", {}).get("sections", {}).get("SECTION_REVIEWS", {})
                    if isinstance(sec_rev, dict) and "numberOfPages" in sec_rev:
                        total_pages_available = int(sec_rev["numberOfPages"])
                    p1_success = True
                    break

                # HTML preloaded state fallback for reviews
                resp_html = scraper.session.get(review_api_info["html_url"], headers=headers, timeout=scraper.config.request_timeout)
                if resp_html.status_code == 200:
                    st_p1 = extract_zomato_preloaded_state(resp_html.text)
                    if st_p1:
                        reviews_raw_payloads.append(st_p1)
                        _collect_review_entities(st_p1)
                        res_p = st_p1.get("pages", {}).get("restaurant", {})
                        for rk, rv in res_p.items():
                            if isinstance(rv, dict) and "sections" in rv:
                                sec_rev = rv.get("sections", {}).get("SECTION_REVIEWS", {})
                                if isinstance(sec_rev, dict) and "numberOfPages" in sec_rev:
                                    total_pages_available = int(sec_rev["numberOfPages"])
                                break
                        p1_success = True
                        break

                if attempt < 3:
                    time.sleep(attempt * 1.0)
            except Exception as e_p1:
                logger.warning("Reviews page 1 attempt %d failed for %s: %s", attempt, res_id, e_p1)
                if attempt < 3:
                    time.sleep(attempt * 1.0)

        job_record.last_review_page = 1
        job_record.total_review_pages = total_pages_available
        job_record.save(update_fields=["last_review_page", "total_review_pages", "updated_at"])

    # Step B: Paginate from start_page to numberOfPages (or max_review_pages limit)
    limit = min(total_pages_available, max_review_pages if max_review_pages else 10)
    for p in range(max(2, start_page), limit + 1):
        if not check_and_wait_if_paused(batch_id, f"reviews page {p} for {res_name}"):
            scraper.close()
            return {"status": "terminated", "message": f"Batch {batch_id} was terminated."}

        time.sleep(1.0)  # Polite 1.0s delay per review page
        logger.info(
            "[TASK %s | PHASE 2] Fetching review page %d/%d for '%s' (ID=%s, 1.0s delay)...",
            job_record_id,
            p,
            limit,
            res_name,
            res_id,
        )
        p_api = generate_review_api_urls(res_id, clean_base, page=p)
        page_success = False

        for attempt in range(1, 4):
            try:
                # 1. Try webroutes/reviews/loadMore API
                resp_more = scraper.session.get(
                    p_api["load_more_url"],
                    params=p_api["load_more_params"],
                    headers=headers,
                    timeout=scraper.config.request_timeout,
                )
                if resp_more.status_code == 200:
                    more_data = resp_more.json()
                    reviews_raw_payloads.append(more_data)
                    _collect_review_entities(more_data)
                    page_success = True
                    break

                # 2. Try getPage?page_url=.../reviews?page=P
                resp_gp = scraper.session.get(
                    p_api["get_page_url"],
                    params=p_api["get_page_params"],
                    headers=headers,
                    timeout=scraper.config.request_timeout,
                )
                if resp_gp.status_code == 200:
                    gp_data = resp_gp.json()
                    reviews_raw_payloads.append(gp_data)
                    _collect_review_entities(gp_data)
                    page_success = True
                    break

                # 3. Try HTML preloaded state
                h_p = scraper.session.get(p_api["html_url"], headers=headers, timeout=scraper.config.request_timeout)
                if h_p.status_code == 200:
                    st_p = extract_zomato_preloaded_state(h_p.text)
                    if st_p:
                        reviews_raw_payloads.append(st_p)
                        _collect_review_entities(st_p)
                        page_success = True
                        break

                if attempt < 3:
                    time.sleep(attempt * 1.0)
            except Exception as e_p:
                logger.warning("Review page %d attempt %d failed for %s: %s", p, attempt, res_id, e_p)
                if attempt < 3:
                    time.sleep(attempt * 1.0)

        if not page_success:
            # Review page failed after 3 attempts -> record failure state for requeue
            job_record.stage = "reviews_failed"
            job_record.last_review_page = p - 1
            job_record.error_message = f"Failed fetching review page {p} after 3 retries"
            job_record.save(update_fields=["stage", "last_review_page", "error_message", "updated_at"])
            scraper.close()
            raise RuntimeError(f"Review page {p} failed for ID {res_id} after 3 retries")

        job_record.last_review_page = p
        job_record.save(update_fields=["last_review_page", "updated_at"])

    scraper.close()
    logger.info("Phase 2 Complete: %d reviews collected across %d pages for ID %s", len(reviews_list), job_record.last_review_page, res_id)

    # =========================================================================
    # PHASE 3: Combine Restaurant & Review Data and Upsert into PostgreSQL
    # =========================================================================
    sections = details_data.get("page_data", {}).get("sections", {})
    sec_basic = sections.get("SECTION_BASIC_INFO", {})
    sec_contact = sections.get("SECTION_RES_CONTACT", {})
    sec_details = sections.get("SECTION_RES_DETAILS", {})
    sec_rating = sections.get("SECTION_RATING_DATA", {})

    cuisines_list = [c.get("name") for c in sec_basic.get("cuisine", []) if isinstance(c, dict)]
    cuisines_str = ", ".join(filter(None, cuisines_list))

    phone_list = sec_contact.get("phoneDetails", {}).get("phoneStr", "")
    timing_str = sec_basic.get("timing", {}).get("customTimingText", "")
    address_str = sec_contact.get("address", "")
    rating_val = str(sec_rating.get("rating_value") or sec_basic.get("rating", {}).get("aggregate_rating") or "")
    votes_val = str(sec_rating.get("votes_count") or sec_basic.get("rating", {}).get("votes") or "")
    cost_val = str(sec_basic.get("costText", {}).get("text") or sec_details.get("CFT_DETAILS", {}).get("cft", "") or "")

    delivery_doc = build_delivery_document(
        detail_data=details_data,
        reviews_data=reviews_list,
        restaurant_id=res_id,
        request_id=str(batch_id),
        city=city,
        location=location,
        location_key=location_key,
    ) or {}

    cuisines_str = delivery_doc.get("cuisines") or cuisines_str
    cost_val = str(delivery_doc.get("average_cost_for_two") or cost_val)
    address_str = delivery_doc.get("address") or address_str
    phone_list = delivery_doc.get("phone_numbers") or phone_list
    timing_str = delivery_doc.get("timing") or timing_str
    rating_val = delivery_doc.get("rating") or rating_val
    votes_val = str(delivery_doc.get("votes") or votes_val)
    canonical_str = delivery_doc.get("canonical_url") or f"{BASE_URL}{clean_base}"

    logger.info(
        "[TASK %s | PHASE 3] Upserting live combined record to PostgreSQL table 'zomato_restaurants' for '%s' (ID=%s, %d reviews)...",
        job_record_id,
        res_name,
        res_id,
        len(reviews_list),
    )

    with transaction.atomic():
        record, created = RestaurantRecord.objects.update_or_create(
            restaurant_id=res_id,
            defaults={
                "batch_id": batch_id,
                "name": res_name,
                "city": city,
                "location": location,
                "location_key": location_key,
                "page_url": clean_base,
                "canonical_url": canonical_str,
                "phone_numbers": phone_list,
                "address": address_str,
                "cuisines": cuisines_str,
                "timing": timing_str,
                "rating": rating_val,
                "votes": votes_val,
                "cost_for_two": cost_val,
                "total_reviews": len(reviews_list),
                "number_of_pages": total_pages_available,
                "pages_scraped": job_record.last_review_page,
                "reviews": reviews_list,
                "details_data": sections,
                "raw_data": {"details": details_data, "reviews": reviews_raw_payloads},
                "delivery_document": delivery_doc,
                "status": "completed",
                "scraped_at": timezone.now(),
            },
        )

        job_record.stage = "completed"
        job_record.save(update_fields=["stage", "updated_at"])

        # Update ScrapeBatch tracking metrics
        batch = ScrapeBatch.objects.filter(batch_id=batch_id).first()
        if batch:
            batch.completed_jobs = models.F("completed_jobs") + 1
            batch.scraped_details = models.F("scraped_details") + 1
            batch.scraped_reviews = models.F("scraped_reviews") + len(reviews_list)
            batch.parsed_delivery = models.F("parsed_delivery") + 1
            batch.save(update_fields=["completed_jobs", "scraped_details", "scraped_reviews", "parsed_delivery", "updated_at"])

            # Recalculate progress percentage
            batch.refresh_from_db()
            if not batch.started_at:
                batch.started_at = timezone.now()
                batch.save(update_fields=["started_at"])

            if batch.status == "terminated":
                pass
            else:
                total = max(batch.total_restaurants, 1)
                done = batch.completed_jobs + batch.failed_jobs
                batch.progress = min(100, int((done / total) * 100))
                if batch.status != "extracting_urls" and batch.total_restaurants > 0 and done >= batch.total_restaurants:
                    if batch.status != "completed":
                        from scraper_app.producers.scrape_producer import complete_or_terminate_batch_and_advance
                        complete_or_terminate_batch_and_advance(batch, status="completed", delay_seconds=5.0)
                elif batch.status != "completed" and batch.status != "extracting_urls":
                    batch.status = "processing_restaurants"
                    batch.save(update_fields=["progress", "status", "updated_at"])

    logger.info(
        "Successfully Completed Job %s for '%s' (ID=%s): %d reviews saved to PostgreSQL table 'zomato_restaurants'",
        job_record_id,
        res_name,
        res_id,
        len(reviews_list),
    )

    return {
        "status": "success",
        "restaurant_id": res_id,
        "name": res_name,
        "total_reviews": len(reviews_list),
        "pages_scraped": job_record.last_review_page,
    }
