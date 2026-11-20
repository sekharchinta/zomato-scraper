"""
Producer Module for Zomato Scraper (Single Redis Queue Architecture).
Zero MongoDB, Zero SQLite - All state, caching, and batch tracking are in PostgreSQL.

Features:
1. Real-time live discovery and streaming enqueueing into the SINGLE Redis queue ('zomato').
2. Jobs are pushed directly into Redis queue 'zomato' immediately upon page discovery.
3. Generates unique UUID batch_id for end-to-end traceability of every scrape batch.
4. Creates PostgreSQL tracking records:
   - ScrapeBatch in 'zomato_batches'
   - RestaurantJob in 'zomato_restaurant_jobs'
5. Enqueues each restaurant into the SINGLE Redis queue ('zomato')
   with 3-retry configuration (DEFAULT_RETRY).
"""

import json
import logging
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import django_rq
from django.conf import settings
from django.db import transaction
from rq.job import Job, Retry
from rq.registry import FailedJobRegistry, StartedJobRegistry

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    DEFAULT_REQUEST_DELAY,
    RESTAURANTS_API_URL,
    SINGLE_QUEUE_NAME,
    Config,
)
from scraper import (
    ZomatoScraper,
    extract_restaurant_page_url,
    extract_zomato_preloaded_state,
)
from scraper_app.models import (
    RestaurantJob,
    RestaurantRecord,
    ScrapeBatch,
    generate_location_key,
)
from scraper_app.tasks.base import DEFAULT_RETRY, get_active_scraper
from scraper_app.tasks.single_pipeline_task import process_restaurant_pipeline_job

logger = logging.getLogger("ZomatoProducer")


def clear_queue_with_delay(queue: Any, queue_name: str, delay_seconds: float = 5.0, force_wait: bool = False) -> bool:
    """
    Clears all jobs in the Redis queue and failed job registry, then pauses for
    delay_seconds (default 5.0s) to allow Redis and active worker loops to settle cleanly
    before any new jobs are enqueued, avoiding race conditions or queue conflicts.
    If force_wait=True, always pauses for delay_seconds even if queue was already empty.
    """
    queue_len = len(queue)
    if queue_len > 0:
        logger.info(
            "[QUEUE_CLEANUP] Clearing %d existing job(s) from queue '%s' before pushing new jobs...",
            queue_len,
            queue_name,
        )
        queue.empty()
    try:
        registry = FailedJobRegistry(queue=queue)
        for jid in registry.get_job_ids():
            registry.remove(jid)
    except Exception as reg_err:
        logger.warning("[QUEUE_CLEANUP] Error clearing FailedJobRegistry: %s", reg_err)

    if queue_len > 0 or force_wait:
        logger.info(
            "[QUEUE_CLEANUP] Queue '%s' cleared. Waiting %.1f seconds for workers to settle before pushing new jobs...",
            queue_name,
            delay_seconds,
        )
        time.sleep(delay_seconds)
        logger.info("[QUEUE_CLEANUP] %.1f-second wait complete. Queue '%s' is clean and ready for new jobs.", delay_seconds, queue_name)
        return True
    else:
        logger.info("[QUEUE_CLEANUP] Queue '%s' is already clean (0 jobs). Proceeding without delay.", queue_name)
        return False


def complete_or_terminate_batch_and_advance(
    batch: ScrapeBatch,
    status: str,
    error_message: str = "",
    delay_seconds: float = 5.0,
) -> Optional[ScrapeBatch]:
    """
    Called whenever a batch completes or is terminated by the user:
    1. Updates PostgreSQL records: sets status ('completed' or 'terminated'), completed_at = now().
       If terminated, marks pending/in-flight jobs as 'terminated'.
    2. Empties the Redis single queue ('zomato') and FailedJobRegistry.
    3. Waits delay_seconds (default 5.0s) for workers to settle cleanly.
    4. Finds the earliest queued request in PostgreSQL ('status__in=["queued", "pending"]').
    5. If found, starts processing the next queued request (start_queued_batch).
    6. Returns the next ScrapeBatch or None if no queued requests remain.
    """
    from django.utils import timezone as dj_timezone
    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)

    # 1. Update PostgreSQL status
    with transaction.atomic():
        if status == "terminated":
            batch.jobs.filter(stage__in=["pending", "processing_details", "processing_reviews"]).update(stage="terminated")
        batch.status = status
        if error_message:
            batch.error_message = error_message
        batch.completed_at = dj_timezone.now()
        if not batch.started_at:
            batch.started_at = batch.created_at
        if status == "completed":
            batch.progress = 100
            batch.save(update_fields=["status", "progress", "completed_at", "updated_at"])
        else:
            batch.save(update_fields=["status", "error_message", "started_at", "completed_at", "updated_at"])

    # Evaluate Slack alerts for failed request or success rate < 99%
    try:
        from scraper_app.alerts import check_and_alert_batch_status
        check_and_alert_batch_status(batch, error_message=error_message)
    except Exception as alert_err:
        logger.warning("[SLACK_ALERT] Error checking alerts for batch %s: %s", batch.batch_id, alert_err)

    # 2. Empty queue in Redis and wait 5 seconds for workers to settle
    clear_queue_with_delay(queue, queue_name, delay_seconds=delay_seconds, force_wait=True)

    # 3. Find and start next queued batch in PostgreSQL
    next_batch = ScrapeBatch.objects.filter(
        status__in=["queued", "pending"]
    ).exclude(batch_id=batch.batch_id).order_by("created_at").first()

    if next_batch:
        logger.info(
            "[ADVANCE_BATCH] Batch %s is %s. Starting next queued batch %s (%s, %s)...",
            batch.batch_id, status, next_batch.batch_id, next_batch.city, next_batch.location
        )
        start_queued_batch(next_batch)
        next_batch.refresh_from_db()
        return next_batch

    logger.info("[ADVANCE_BATCH] Batch %s is %s. No further queued requests in PostgreSQL.", batch.batch_id, status)
    return None


def resolve_locations_list(
    city: str,
    location_raw: Any,
    is_all_locations: bool,
) -> List[Dict[str, Any]]:
    """Resolves the list of locations to scrape (single locality vs all localities in city)."""
    locations_list: List[Dict[str, Any]] = []
    if is_all_locations:
        try:
            scraper = ZomatoScraper(config=Config(enable_mongo=False))
            all_sugs = scraper.get_location_suggestions(city)
            scraper.close()
            locations_list = all_sugs if all_sugs else [{"entity_title": city, "latitude": None, "longitude": None}]
        except Exception:
            locations_list = [{"entity_title": city, "latitude": None, "longitude": None}]
    else:
        if isinstance(location_raw, dict):
            locations_list = [location_raw]
        else:
            loc_title = str(location_raw or city).strip()
            locations_list = [{"entity_title": loc_title, "latitude": None, "longitude": None}]
    return locations_list


def _enqueue_single_restaurant(
    batch: ScrapeBatch,
    queue: Any,
    batch_id_str: str,
    city: str,
    location_title: str,
    location_key: str,
    item: Dict[str, Any],
) -> Optional[RestaurantJob]:
    """Helper creating RestaurantJob in PostgreSQL and immediately enqueueing into Redis queue 'zomato'."""
    url = item.get("url", "")
    if not url:
        return None

    res_id = item.get("res_id")
    clean_res_id = int(res_id) if res_id and str(res_id).isdigit() else None
    name = item.get("name", "") or "Restaurant"

    job_record = RestaurantJob.objects.create(
        batch=batch,
        restaurant_id=clean_res_id,
        name=name,
        page_url=url,
        stage="pending",
    )

    rq_job = queue.enqueue(
        process_restaurant_pipeline_job,
        batch_id=batch_id_str,
        job_record_id=job_record.id,
        restaurant_url=url,
        name=name,
        city=city,
        location=location_title,
        location_key=location_key,
        max_review_pages=10,
        retry=DEFAULT_RETRY,
    )

    job_record.rq_job_id = rq_job.id
    job_record.save(update_fields=["rq_job_id", "updated_at"])
    return job_record


def discover_and_enqueue_restaurants(
    scraper: ZomatoScraper,
    city: str,
    locations_list: List[Dict[str, Any]],
    batch: ScrapeBatch,
    queue: Any,
    batch_id_str: str,
    location_title: str,
    location_key: str,
    max_pages: Optional[int] = None,
) -> int:
    """
    Discovers restaurant URLs across pagination and IMMEDIATELY enqueues each page into
    the SINGLE Redis queue ('zomato') and PostgreSQL tracking tables.
    Returns total number of restaurants enqueued.
    """
    discovered_count = 0
    seen_urls = set()

    for loc_item in locations_list:
        loc_title = loc_item.get("entity_title") or loc_item.get("entity_name") or city
        lat = loc_item.get("latitude")
        lon = loc_item.get("longitude")

        try:
            resolved_loc = scraper.resolve_location(
                city=city,
                lat=lat,
                lon=lon,
                location=loc_title,
            )
        except Exception as exc:
            logger.warning("Could not resolve location '%s': %s", loc_title, exc)
            resolved_loc = {
                "entity_id": "0",
                "entity_type": "subzone",
                "city_id": "6",
                "country_id": "1",
                "country_name": "India",
                "place_id": "0",
                "cell_id": "0",
                "delivery_subzone_id": 0,
                "place_type": "DSZ",
                "city_name": city,
                "location_name": loc_title,
                "folder_name": f"{city}_{loc_title}".lower().replace(" ", "_"),
                "latitude": lat or 17.3850,
                "longitude": lon or 78.4867,
                "user_defined_latitude": lat or 17.3850,
                "user_defined_longitude": lon or 78.4867,
                "exact_location_url": f"{BASE_URL}/{city.lower().replace(' ', '-')}/restaurants",
            }

        page = 1
        offset = 0
        pages_scraped = 0
        search_metadata: Optional[Dict[str, Any]] = None

        while True:
            if max_pages is not None and pages_scraped >= max_pages:
                break

            resp = None
            for attempt in range(1, 4):
                try:
                    csrf = scraper.ensure_csrf_token()
                    payload = scraper.build_restaurants_payload(
                        resolved_loc,
                        page=page,
                        offset=offset,
                        search_metadata=search_metadata,
                    )
                    headers = {
                        "user-agent": DEFAULT_HEADERS["user-agent"],
                        "origin": BASE_URL,
                        "referer": resolved_loc.get("exact_location_url", BASE_URL),
                        "x-zomato-csrft": csrf,
                        "content-type": "application/json",
                    }
                    resp = scraper.session.post(
                        RESTAURANTS_API_URL,
                        json=payload,
                        headers=headers,
                        timeout=scraper.config.request_timeout,
                    )
                    if resp.status_code == 200:
                        break
                    elif resp.status_code in (429, 503) and attempt < 3:
                        time.sleep(attempt * 1.0)
                except Exception as req_err:
                    logger.warning("[PRODUCER] Attempt %d error querying restaurants API: %s", attempt, req_err)
                    if attempt < 3:
                        time.sleep(attempt * 0.5)

            if not resp or resp.status_code != 200:
                break

            try:
                data = resp.json()
            except Exception:
                break

            sections = data.get("sections", {})
            restaurants = sections.get("SECTION_SEARCH_RESULT", []) if isinstance(sections, dict) else []
            meta_info = sections.get("SECTION_SEARCH_META_INFO", {}) if isinstance(sections, dict) else {}

            if not restaurants:
                break

            page_enqueued = 0
            for item in restaurants:
                res_obj = item.get("restaurant", {}) if isinstance(item, dict) else {}
                url = extract_restaurant_page_url(item)
                name = res_obj.get("name") or item.get("info", {}).get("name") or "Restaurant"
                res_id = res_obj.get("id") or item.get("info", {}).get("resId")

                if url and url not in seen_urls:
                    seen_urls.add(url)
                    job = _enqueue_single_restaurant(
                        batch=batch,
                        queue=queue,
                        batch_id_str=batch_id_str,
                        city=city,
                        location_title=location_title,
                        location_key=location_key,
                        item={"name": name, "url": url, "res_id": res_id},
                    )
                    if job:
                        discovered_count += 1
                        page_enqueued += 1

            # Update batch in PostgreSQL with real-time count
            batch.total_restaurants = discovered_count
            batch.save(update_fields=["total_restaurants", "updated_at"])
            logger.info(
                "[PRODUCER] Page %d enqueued %d new restaurant job(s) into single queue '%s' (Total queued: %d)",
                page,
                page_enqueued,
                queue.name,
                discovered_count,
            )

            # Stopping Condition 1: If 0 new restaurants were enqueued from this page (wrap-around repeat), stop
            if page_enqueued == 0:
                logger.info(
                    "[PRODUCER] Page %d for '%s' returned 0 new unique restaurants (end of unique results / wrap-around). Stopping pagination.",
                    page,
                    location_title,
                )
                break

            # Extract searchMetaData for next page's payload
            search_metadata = meta_info.get("searchMetaData") if isinstance(meta_info, dict) else {}
            if not isinstance(search_metadata, dict):
                search_metadata = {}

            # Stopping Condition 2: Explicit hasMore=False flag (searchMetaData or meta_info)
            has_more = None
            if isinstance(search_metadata, dict) and "hasMore" in search_metadata:
                has_more = search_metadata["hasMore"]
            elif isinstance(meta_info, dict) and "hasMore" in meta_info:
                has_more = meta_info["hasMore"]

            if has_more is False:
                logger.info(
                    "[PRODUCER] hasMore is False on Page %d for '%s'. Stopping pagination.",
                    page,
                    location_title,
                )
                break

            # Advance offset using solr_offset if present, or add 12
            next_offset = None
            postback = search_metadata.get("postbackParams")
            if isinstance(postback, str):
                try:
                    pb = json.loads(postback)
                    if isinstance(pb, dict) and isinstance(pb.get("solr_offset"), int):
                        next_offset = pb["solr_offset"]
                except Exception:
                    pass
            elif isinstance(postback, dict) and isinstance(postback.get("solr_offset"), int):
                next_offset = postback["solr_offset"]

            if next_offset is not None and next_offset > offset:
                offset = next_offset
            else:
                offset += 12

            page += 1
            pages_scraped += 1
            time.sleep(0.4)  # 0.4s delay for every request

    # Universal HTML Fallback: If 0 restaurants found from API, check city restaurants HTML page
    if discovered_count == 0:
        try:
            city_slug = city.lower().replace(" ", "-")
            url = f"{BASE_URL}/{city_slug}/restaurants"
            resp = scraper.session.get(url, headers=DEFAULT_HEADERS, timeout=10)
            if resp.status_code == 200:
                st = extract_zomato_preloaded_state(resp.text)
                if st:
                    search_sec = st.get("pages", {}).get("search", {})
                    for p_path, p_val in search_sec.items():
                        res_list = p_val.get("sections", {}).get("SECTION_SEARCH_RESULT", [])
                        for item in res_list:
                            url = extract_restaurant_page_url(item)
                            res_obj = item.get("restaurant", {}) if isinstance(item, dict) else {}
                            name = res_obj.get("name") or item.get("info", {}).get("name") or "Restaurant"
                            res_id = res_obj.get("id") or item.get("info", {}).get("resId")
                            if url and url not in seen_urls:
                                seen_urls.add(url)
                                job = _enqueue_single_restaurant(
                                    batch=batch,
                                    queue=queue,
                                    batch_id_str=batch_id_str,
                                    city=city,
                                    location_title=location_title,
                                    location_key=location_key,
                                    item={"name": name, "url": url, "res_id": res_id},
                                )
                                if job:
                                    discovered_count += 1

            batch.total_restaurants = discovered_count
            batch.save(update_fields=["total_restaurants", "updated_at"])
        except Exception as exc:
            logger.warning("[PRODUCER] HTML fallback discovery error: %s", exc)

    return discovered_count


def dispatch_scrape_request(
    city: str,
    location_raw: Any = None,
    selected_fields: Optional[Any] = None,
    force_refresh: bool = False,
    max_pages: Optional[int] = None,
    restaurant_url: Optional[str] = None,
    sync_discovery: bool = False,
    clear_queue: bool = False,
) -> Dict[str, Any]:
    """
    Main Producer Entrypoint (100% Live Scraping):
    1. Generates unique UUID batch_id for end-to-end traceability.
    2. Creates ScrapeBatch in PostgreSQL ('zomato_batches') immediately.
    3. Connects to the SINGLE Redis Queue ('zomato').
    4. If direct restaurant_url provided: enqueues restaurant pipeline job directly into 'zomato'.
    5. If city/location provided: enqueues extract_location_urls_task into the SINGLE queue 'zomato'
       so URL extraction runs entirely inside RQ workers (or synchronously if sync_discovery=True).
    6. Returns queued confirmation with batch_id immediately (<50ms).
    """
    city_str = str(city or "").strip()
    rest_url_str = str(restaurant_url or "").strip()

    if not city_str and not rest_url_str:
        return {"status": "error", "message": "City name or restaurant URL is required", "status_code": 400}

    is_all_locations = False
    location_title = "All Locations"

    if isinstance(location_raw, dict):
        location_title = location_raw.get("entity_title") or location_raw.get("entity_name") or "Selected Location"
        if location_raw.get("entity_type") == "all":
            is_all_locations = True
    elif isinstance(location_raw, str) and location_raw.strip():
        location_title = location_raw.strip()
        if location_title.upper() in ("ALL", "ALL LOCATIONS", f"ALL LOCATIONS IN {city_str.upper()}"):
            is_all_locations = True
    elif not rest_url_str:
        is_all_locations = True
        location_title = f"All Locations in {city_str.title()}"

    fields = selected_fields or []
    if isinstance(fields, str):
        fields = [f.strip() for f in fields.split(",") if f.strip()]

    # Generate unique UUID for every batch of jobs and deterministic location_key
    batch_uuid = uuid.uuid4()
    batch_id_str = str(batch_uuid)
    location_key = generate_location_key(city_str or "direct", location_raw)

    # 1. Create ScrapeBatch in PostgreSQL ('zomato_batches')
    batch = ScrapeBatch.objects.create(
        batch_id=batch_uuid,
        request_id=batch_id_str,
        location_key=location_key,
        city=city_str or "Direct URL",
        location=location_title if not rest_url_str else "Direct Restaurant URL",
        status="queued",
        progress=0,
        total_restaurants=0,
        completed_jobs=0,
        failed_jobs=0,
        is_cached=False,
        fields_selected=fields,
    )

    # 2. Get Redis Queue ('zomato') on Redis DB 1
    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)

    # Case A: Direct restaurant URL scraping
    if rest_url_str:
        # Before giving the fresh restaurant URL, check and clear the Redis queue,
        # waiting 5 seconds to ensure all existing jobs are cleared and workers settle cleanly
        # before pushing new jobs to prevent concurrency conflicts.
        clear_queue_with_delay(queue, queue_name, delay_seconds=5.0)

        logger.info(
            "[PRODUCER] Enqueueing direct restaurant URL %s into single queue '%s' (batch=%s)",
            rest_url_str,
            queue_name,
            batch_id_str,
        )
        job_record = RestaurantJob.objects.create(
            batch=batch,
            name=city_str or "Restaurant",
            page_url=rest_url_str,
            stage="pending",
        )
        rq_job = queue.enqueue(
            process_restaurant_pipeline_job,
            batch_id=batch_id_str,
            job_record_id=job_record.id,
            restaurant_url=rest_url_str,
            name=city_str or "Restaurant",
            city=city_str,
            location=location_title,
            location_key=location_key,
            max_review_pages=10,
            retry=DEFAULT_RETRY,
        )
        job_record.rq_job_id = rq_job.id
        job_record.save(update_fields=["rq_job_id", "updated_at"])

        batch.total_restaurants = 1
        batch.save(update_fields=["total_restaurants", "updated_at"])

        return {
            "status": "queued",
            "batch_id": batch_id_str,
            "request_id": batch_id_str,
            "location_key": location_key,
            "cached": False,
            "city": city_str,
            "location": location_title,
            "total_restaurants": 1,
            "queue": queue_name,
            "rq_job_id": rq_job.id,
            "message": f"Enqueued single restaurant job into single queue '{queue_name}' with UUID batch {batch_id_str}.",
            "status_code": 200,
        }

    # Case B: City / Location scraping
    if clear_queue:
        clear_queue_with_delay(queue, queue_name, delay_seconds=5.0)

    locations_list = resolve_locations_list(city_str, location_raw, is_all_locations)

    if sync_discovery:
        # Synchronous discovery mode (for CLI scripts or unit tests)
        logger.info(
            "[PRODUCER] Running SYNCHRONOUS discovery for batch %s: city='%s', location='%s'",
            batch_id_str,
            city_str,
            location_title,
        )
        scraper = get_active_scraper()
        total_discovered = discover_and_enqueue_restaurants(
            scraper=scraper,
            city=city_str,
            locations_list=locations_list,
            batch=batch,
            queue=queue,
            batch_id_str=batch_id_str,
            location_title=location_title,
            location_key=location_key,
            max_pages=max_pages,
        )
        scraper.close()

        if total_discovered == 0:
            batch.status = "completed"
            batch.progress = 100
            batch.save(update_fields=["status", "progress", "updated_at"])
            return {
                "status": "completed",
                "batch_id": batch_id_str,
                "request_id": batch_id_str,
                "location_key": location_key,
                "cached": False,
                "city": city_str,
                "location": location_title,
                "total_restaurants": 0,
                "queue": queue_name,
                "message": f"No restaurants discovered for location '{location_title}' in {city_str}.",
                "status_code": 200,
            }

        return {
            "status": "queued",
            "batch_id": batch_id_str,
            "request_id": batch_id_str,
            "location_key": location_key,
            "cached": False,
            "city": city_str,
            "location": location_title,
            "total_restaurants": total_discovered,
            "queue": queue_name,
            "message": f"Enqueued {total_discovered} restaurant jobs into single queue '{queue_name}' with UUID batch {batch_id_str}.",
            "status_code": 200,
        }

    # Default: Queue-driven asynchronous URL discovery
    # Enqueues extract_location_urls_task into the SINGLE Redis queue ('zomato')
    logger.info(
        "[PRODUCER] Enqueueing URL extraction task into single queue '%s' for batch %s: city='%s', location='%s'",
        queue_name,
        batch_id_str,
        city_str,
        location_title,
    )
    from scraper_app.tasks.location_task import extract_location_urls_task

    rq_job = queue.enqueue(
        extract_location_urls_task,
        batch_id=batch_id_str,
        city=city_str,
        locations_list=locations_list,
        location_raw=location_raw,
        location_key=location_key,
        location_title=location_title,
        max_pages=max_pages,
        job_timeout=1800,
        retry=DEFAULT_RETRY,
    )

    return {
        "status": "queued",
        "batch_id": batch_id_str,
        "request_id": batch_id_str,
        "location_key": location_key,
        "cached": False,
        "city": city_str,
        "location": location_title,
        "total_restaurants": 0,
        "queue": queue_name,
        "rq_job_id": rq_job.id,
        "message": f"Enqueued URL extraction job into single queue '{queue_name}' with UUID batch {batch_id_str}.",
        "status_code": 200,
    }


def enqueue_pipeline_restaurant_job(
    batch_id: str,
    job_record_id: int,
    restaurant_url: str,
    name: str = "",
    city: str = "",
    location: str = "",
    location_key: str = "",
    max_review_pages: int = 10,
    resume_stage: Optional[str] = None,
) -> Any:
    """Enqueues a restaurant pipeline job into the single queue ('zomato')."""
    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)
    return queue.enqueue(
        process_restaurant_pipeline_job,
        batch_id=batch_id,
        job_record_id=job_record_id,
        restaurant_url=restaurant_url,
        name=name,
        city=city,
        location=location,
        location_key=location_key,
        max_review_pages=max_review_pages,
        resume_stage=resume_stage,
        retry=DEFAULT_RETRY,
    )


def enqueue_location_task(*args: Any, **kwargs: Any) -> Any:
    """Legacy helper for backward compatibility."""
    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)
    return None


def enqueue_restaurant_task(*args: Any, **kwargs: Any) -> Any:
    """Legacy helper for backward compatibility."""
    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)
    return None


def enqueue_review_task(*args: Any, **kwargs: Any) -> Any:
    """Legacy helper for backward compatibility."""
    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)
    return None


def start_queued_batch(batch_or_id: Any) -> Dict[str, Any]:
    """
    Guarantees the specified queued batch starts execution immediately.
    Checks if active RQ jobs already exist in Redis for this batch;
    if not, enqueues the location URL extraction task or pending restaurant jobs
    and updates batch status to 'extracting_urls' or 'processing_restaurants'.
    """
    batch = None
    if isinstance(batch_or_id, ScrapeBatch):
        batch = batch_or_id
    else:
        clean_id = str(batch_or_id or "").strip()
        try:
            batch = ScrapeBatch.objects.get(batch_id=clean_id)
        except Exception:
            try:
                batch = ScrapeBatch.objects.get(request_id=clean_id)
            except Exception:
                return {"status": "error", "message": f"Batch '{batch_or_id}' not found"}

    if not batch:
        return {"status": "error", "message": "Batch not found"}

    batch.refresh_from_db()
    if batch.status in ["completed", "failed", "terminated"]:
        return {
            "status": "already_finished",
            "batch_id": str(batch.batch_id),
            "batch_status": batch.status,
            "message": f"Batch {batch.batch_id} is already {batch.status}.",
        }

    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)
    batch_id_str = str(batch.batch_id)

    # Check if there is an active job in Redis queue or StartedJobRegistry for this batch
    has_active_job = False
    try:
        for q_j in queue.jobs:
            if q_j.kwargs.get("batch_id") == batch_id_str:
                has_active_job = True
                break

        if not has_active_job:
            started_reg = StartedJobRegistry(queue=queue)
            for jid in started_reg.get_job_ids():
                try:
                    sj = Job.fetch(jid, connection=queue.connection)
                    if sj and sj.kwargs.get("batch_id") == batch_id_str:
                        has_active_job = True
                        break
                except Exception:
                    pass
    except Exception as check_err:
        logger.warning("[START_QUEUED_BATCH] Error checking Redis active jobs: %s", check_err)

    if has_active_job:
        logger.info("[START_QUEUED_BATCH] Batch %s already has active job(s) in queue '%s'.", batch_id_str, queue_name)
        if batch.status in ["queued", "pending"]:
            batch.status = "extracting_urls" if batch.total_restaurants == 0 else "processing_restaurants"
            batch.save(update_fields=["status", "updated_at"])
        return {
            "status": "success",
            "batch_id": batch_id_str,
            "batch_status": batch.status,
            "message": f"Batch {batch_id_str} is already active in Redis.",
        }

    # No active job in Redis -> Enqueue jobs to start execution!
    logger.info("[START_QUEUED_BATCH] Enqueueing jobs to start execution for batch %s (%s, %s)", batch_id_str, batch.city, batch.location)
    from django.utils import timezone as dj_timezone
    if not batch.started_at:
        batch.started_at = dj_timezone.now()

    # Case A: RestaurantJob records already exist in DB
    if batch.jobs.exists():
        pending_jobs = batch.jobs.filter(stage__in=["pending", "details_failed", "reviews_failed"])
        enqueued_count = 0
        for p_job in pending_jobs:
            rq_j = queue.enqueue(
                process_restaurant_pipeline_job,
                batch_id=batch_id_str,
                job_record_id=p_job.id,
                restaurant_url=p_job.page_url,
                name=p_job.name,
                city=batch.city,
                location=batch.location,
                location_key=batch.location_key,
                max_review_pages=10,
                retry=DEFAULT_RETRY,
            )
            p_job.rq_job_id = rq_j.id
            p_job.stage = "pending"
            p_job.save(update_fields=["rq_job_id", "stage", "updated_at"])
            enqueued_count += 1

        batch.status = "processing_restaurants"
        batch.save(update_fields=["status", "started_at", "updated_at"])
        return {
            "status": "success",
            "batch_id": batch_id_str,
            "batch_status": "processing_restaurants",
            "enqueued_jobs": enqueued_count,
            "message": f"Enqueued {enqueued_count} restaurant job(s) into single queue '{queue_name}' for batch {batch_id_str}.",
        }

    # Case B: Direct URL without RestaurantJob yet
    if batch.city and ("zomato.com" in batch.city or batch.city.startswith("http://") or batch.city.startswith("https://") or re.search(r"/(?:order|info|reviews|photos)", batch.city)):
        rest_url_str = batch.city
        job_record = RestaurantJob.objects.create(
            batch=batch,
            name=batch.location or "Restaurant",
            page_url=rest_url_str,
            stage="pending",
        )
        rq_job = queue.enqueue(
            process_restaurant_pipeline_job,
            batch_id=batch_id_str,
            job_record_id=job_record.id,
            restaurant_url=rest_url_str,
            name=batch.location or "Restaurant",
            city=batch.city,
            location=batch.location,
            location_key=batch.location_key,
            max_review_pages=10,
            retry=DEFAULT_RETRY,
        )
        job_record.rq_job_id = rq_job.id
        job_record.save(update_fields=["rq_job_id", "updated_at"])
        batch.total_restaurants = 1
        batch.status = "processing_restaurants"
        batch.save(update_fields=["total_restaurants", "status", "started_at", "updated_at"])
        return {
            "status": "success",
            "batch_id": batch_id_str,
            "batch_status": "processing_restaurants",
            "message": f"Enqueued single restaurant job for batch {batch_id_str}.",
        }

    # Case C: Location search
    from scraper_app.tasks.location_task import extract_location_urls_task
    locations_list = resolve_locations_list(batch.city, batch.location, is_all_locations=False)
    rq_job = queue.enqueue(
        extract_location_urls_task,
        batch_id=batch_id_str,
        city=batch.city,
        locations_list=locations_list,
        location_raw=batch.location,
        location_key=batch.location_key,
        location_title=batch.location,
        max_pages=None,
        job_timeout=1800,
        retry=DEFAULT_RETRY,
    )
    batch.status = "extracting_urls"
    batch.save(update_fields=["status", "started_at", "updated_at"])
    return {
        "status": "success",
        "batch_id": batch_id_str,
        "batch_status": "extracting_urls",
        "message": f"Enqueued URL extraction job into single queue '{queue_name}' for batch {batch_id_str}.",
    }
