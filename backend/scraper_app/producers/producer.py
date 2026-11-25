"""
producer.py
Main Producer Module:
1. Dispatches scrape requests, creating ScrapeBatch records with requestid PK and lat/long.
2. Provides push_urls_to_queue(request_id) called by url_extractor to push all discovered
   restaurant jobs from 'zomato_jobs' into the single Redis queue ('zomato').
3. Handles sequential batch advancement, queue pausing, and queue management.
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional
import django_rq
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from rq.job import Job
from rq.registry import FailedJobRegistry

from config import Config, DEFAULT_REQUEST_DELAY, SINGLE_QUEUE_NAME
from scraper import ZomatoScraper, slugify
from scraper_app.models import LatLongData, RestaurantJob, ScrapeBatch, generate_location_key
from scraper_app.tasks.base import DEFAULT_RETRY, check_and_wait_if_paused

logger = logging.getLogger("producer")


def enqueue_pipeline_restaurant_job(
    batch_id: str,
    job_record_id: Optional[int] = None,
    restaurant_url: str = "",
    name: str = "",
    city: str = "",
    location: str = "",
    location_key: str = "",
    queue_name: Optional[str] = None,
    retry: Optional[Any] = None,
    **extra_kwargs,
) -> Job:
    """Enqueues a restaurant pipeline job with retry configuration."""
    target_queue = queue_name or getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(target_queue)
    from scraper_app.tasks.restaurant_extractor import process_restaurant_pipeline_job
    retry_obj = retry if retry is not None else DEFAULT_RETRY
    return queue.enqueue(
        process_restaurant_pipeline_job,
        batch_id=batch_id,
        job_record_id=job_record_id,
        restaurant_url=restaurant_url,
        name=name,
        city=city,
        location=location,
        location_key=location_key,
        retry=retry_obj,
        **extra_kwargs,
    )


def clear_queue_with_delay(queue: Any, queue_name: str, delay_seconds: float = 5.0, force_wait: bool = False) -> bool:
    """Safely empties the Redis queue and allows active workers to settle cleanly."""
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


def push_urls_to_queue(request_id: str) -> Dict[str, Any]:
    """
    Pushes all extracted restaurant URLs/jobs in 'zomato_jobs' for a given request_id
    into the single Redis queue ('zomato') so worker.py can process them.
    Called directly by tasks/url_extractor.py after URL discovery finishes.
    """
    batch = ScrapeBatch.objects.filter(requestid=request_id).first()
    if not batch:
        logger.error("[PRODUCER] Cannot push URLs to queue: batch %s not found.", request_id)
        return {"status": "error", "message": f"Batch {request_id} not found"}

    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)

    # Import worker job pipeline
    from scraper_app.tasks.restaurant_extractor import process_restaurant_pipeline_job

    jobs_to_enqueue = RestaurantJob.objects.filter(batch=batch, stage="pending").order_by("id")
    total_jobs = jobs_to_enqueue.count()

    logger.info(
        "[PRODUCER] Pushing %d extracted restaurant jobs for batch %s into single queue '%s'...",
        total_jobs,
        request_id,
        queue_name,
    )

    if total_jobs == 0:
        # No restaurants found in this batch
        batch.total_restaurants = 0
        batch.status = "completed"
        batch.progress = 100
        batch.completed_at = timezone.now()
        batch.save(update_fields=["status", "progress", "total_restaurants", "completed_at", "updated_at"])
        complete_or_terminate_batch_and_advance(batch, status="completed", delay_seconds=1.0)
        return {
            "status": "completed",
            "request_id": str(batch.requestid),
            "enqueued_jobs": 0,
            "message": "0 restaurants found for this location.",
        }

    jobs_list = list(jobs_to_enqueue)
    prepared_data_list = []
    for job_record in jobs_list:
        job_data = queue.prepare_data(
            process_restaurant_pipeline_job,
            kwargs={
                "batch_id": str(batch.requestid),
                "job_record_id": job_record.id,
                "restaurant_url": job_record.page_url,
                "name": job_record.name,
                "city": batch.city,
                "location": batch.location,
                "location_key": batch.location_key,
                "max_review_pages": 10,
                "retry": DEFAULT_RETRY,
            },
            timeout=getattr(settings, "RQ_DEFAULT_TIMEOUT", 360),
            result_ttl=getattr(settings, "RQ_RESULT_TTL", 500),
        )
        prepared_data_list.append(job_data)

    # Push all jobs at once in a single atomic batch into the queue
    enqueued_rq_jobs = queue.enqueue_many(prepared_data_list)
    enqueued_count = len(enqueued_rq_jobs)

    # Bulk update all job records with their respective rq_job_id in PostgreSQL
    now_ts = timezone.now()
    for job_rec, rq_job in zip(jobs_list, enqueued_rq_jobs):
        job_rec.rq_job_id = rq_job.id
        job_rec.updated_at = now_ts

    RestaurantJob.objects.bulk_update(jobs_list, ["rq_job_id", "updated_at"], batch_size=500)

    batch.status = "processing_restaurants"
    batch.total_restaurants = total_jobs
    batch.save(update_fields=["status", "total_restaurants", "updated_at"])

    logger.info(
        "[PRODUCER] Successfully pushed all %d jobs at once into queue '%s' for request_id=%s. Workers can now start processing.",
        len(enqueued_rq_jobs),
        queue_name,
        request_id,
    )

    return {
        "status": "queued",
        "request_id": str(batch.requestid),
        "total_jobs": total_jobs,
        "enqueued_jobs": enqueued_count,
        "queue": queue_name,
    }


def resolve_locations_list(
    city: str,
    location_raw: Any,
    is_all_locations: bool,
    request_id: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Resolves the list of locations to scrape (single locality vs all localities in city)."""
    locations_list: List[Dict[str, Any]] = []
    if is_all_locations:
        # Check if locations were already discovered and stored in latlong_data for this request_id or city
        if request_id:
            try:
                stored = list(
                    LatLongData.objects.filter(requestid=request_id).exclude(entity_type="all").values(
                        "entity_id", "entity_type", "entity_title", "entity_name", "latitude", "longitude", "place_id", "cell_id", "delivery_subzone_id"
                    )
                )
                if stored:
                    filtered_stored = [
                        s for s in stored
                        if " - " not in str(s.get("entity_title") or s.get("entity_name") or "")
                    ]
                    return filtered_stored if filtered_stored else stored
            except Exception:
                pass

        try:
            stored = list(
                LatLongData.objects.filter(city__iexact=city).exclude(entity_type="all").values(
                    "entity_id", "entity_type", "entity_title", "entity_name", "latitude", "longitude", "place_id", "cell_id", "delivery_subzone_id"
                )
            )
            if stored:
                filtered_stored = [
                    s for s in stored
                    if " - " not in str(s.get("entity_title") or s.get("entity_name") or "")
                ]
                return filtered_stored if filtered_stored else stored
        except Exception:
            pass

        try:
            from scraper_app.tasks.location_extractor import extract_locations_and_store
            sugs = extract_locations_and_store(city)
            filtered_sugs = [
                s for s in (sugs or [])
                if " - " not in str(s.get("entity_title") or s.get("entity_name") or "")
            ]
            locations_list = filtered_sugs if filtered_sugs else sugs if sugs else [{"entity_title": city, "latitude": None, "longitude": None}]
        except Exception:
            locations_list = [{"entity_title": city, "latitude": None, "longitude": None}]
    else:
        if isinstance(location_raw, dict):
            locations_list = [location_raw]
        else:
            loc_title = str(location_raw or city).strip()
            # Check if latlong was stored for this locality
            stored_loc = None
            if request_id:
                stored_loc = LatLongData.objects.filter(requestid=request_id, entity_title__iexact=loc_title).first()
            if not stored_loc:
                stored_loc = LatLongData.objects.filter(entity_title__iexact=loc_title).first()

            if stored_loc:
                locations_list = [{
                    "entity_id": stored_loc.entity_id,
                    "entity_type": stored_loc.entity_type,
                    "entity_title": stored_loc.entity_title,
                    "entity_name": stored_loc.entity_name,
                    "latitude": float(stored_loc.latitude) if stored_loc.latitude else None,
                    "longitude": float(stored_loc.longitude) if stored_loc.longitude else None,
                    "place_id": stored_loc.place_id,
                    "cell_id": stored_loc.cell_id,
                    "delivery_subzone_id": stored_loc.delivery_subzone_id,
                }]
            else:
                locations_list = [{"entity_title": loc_title, "latitude": None, "longitude": None}]

    return locations_list


def dispatch_scrape_request(
    city: str,
    location_raw: Any = None,
    selected_fields: Optional[Any] = None,
    force_refresh: bool = False,
    max_pages: Optional[int] = None,
    restaurant_url: Optional[str] = None,
    sync_discovery: bool = False,
    clear_queue: bool = False,
    request_id: Optional[Any] = None,
    user: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Main Producer Entrypoint (100% Live Scraping):
    1. Resolves requestid (uses provided request_id from location search or creates new UUID).
    2. Stores lat/long with requestid (primary key) in PostgreSQL ('zomato_batches').
    3. Enqueues extract_urls_from_latlongs (tasks/url_extractor.py) into the single queue 'zomato'.
    4. Returns queued confirmation with request_id immediately (<50ms).
    """
    city_str = str(city or "").strip()
    rest_url_str = str(restaurant_url or "").strip()

    if not city_str and not rest_url_str:
        return {"status": "error", "message": "City name or restaurant URL is required", "status_code": 400}

    # Resolve unified request UUID
    batch_uuid: uuid.UUID
    if isinstance(request_id, uuid.UUID):
        batch_uuid = request_id
    elif isinstance(request_id, str) and request_id.strip():
        try:
            batch_uuid = uuid.UUID(request_id.strip())
        except ValueError:
            batch_uuid = uuid.uuid4()
    elif isinstance(location_raw, dict) and location_raw.get("request_id"):
        try:
            batch_uuid = uuid.UUID(str(location_raw["request_id"]).strip())
        except ValueError:
            batch_uuid = uuid.uuid4()
    else:
        batch_uuid = uuid.uuid4()

    # Prevent primary key collision if a batch with this request_id already exists in zomato_batches
    if ScrapeBatch.objects.filter(requestid=batch_uuid).exists():
        batch_uuid = uuid.uuid4()
    batch_id_str = str(batch_uuid)

    is_all_locations = False
    location_title = "All Locations"
    lat: Optional[float] = None
    lon: Optional[float] = None

    if isinstance(location_raw, dict):
        location_title = location_raw.get("entity_title") or location_raw.get("entity_name") or "Selected Location"
        if location_raw.get("entity_type") == "all":
            is_all_locations = True
        try:
            if location_raw.get("latitude") is not None:
                lat = float(location_raw["latitude"])
            if location_raw.get("longitude") is not None:
                lon = float(location_raw["longitude"])
        except (ValueError, TypeError):
            pass
    elif isinstance(location_raw, str) and location_raw.strip():
        location_title = location_raw.strip()
        if location_title.upper() in ("ALL", "ALL LOCATIONS", f"ALL LOCATIONS IN {city_str.upper()}"):
            is_all_locations = True
    elif not rest_url_str:
        is_all_locations = True
        location_title = f"All Locations in {city_str.title()}"

    # If coordinates not yet determined, lookup stored latlongs in PostgreSQL
    if (lat is None or lon is None) and not rest_url_str:
        stored_coord = LatLongData.objects.filter(requestid=batch_uuid).exclude(latitude__isnull=True).first()
        if not stored_coord and location_title:
            stored_coord = LatLongData.objects.filter(entity_title__iexact=location_title).exclude(latitude__isnull=True).first()
        if not stored_coord and city_str:
            stored_coord = LatLongData.objects.filter(search_query__iexact=city_str).exclude(latitude__isnull=True).first()
        if stored_coord:
            lat = float(stored_coord.latitude) if stored_coord.latitude else None
            lon = float(stored_coord.longitude) if stored_coord.longitude else None

    fields = selected_fields or []
    if isinstance(fields, str):
        fields = [f.strip() for f in fields.split(",") if f.strip()]

    location_key = generate_location_key(city_str or "direct", location_raw)

    # 1. Create ScrapeBatch in PostgreSQL ('zomato_batches') storing lat/long with requestid PK
    batch = ScrapeBatch.objects.create(
        batch_id=batch_uuid,
        request_id=batch_id_str,
        user=user,
        location_key=location_key,
        city=city_str or "Direct URL",
        location=location_title if not rest_url_str else "Direct Restaurant URL",
        latitude=lat,
        longitude=lon,
        status="queued",
        progress=0,
        total_restaurants=0,
        completed_jobs=0,
        failed_jobs=0,
        is_cached=False,
        fields_selected=fields,
    )

    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)

    # Case A: Direct restaurant URL scraping
    if rest_url_str:
        clear_queue_with_delay(queue, queue_name, delay_seconds=5.0)
        from scraper_app.tasks.restaurant_extractor import process_restaurant_pipeline_job

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
            "latitude": lat,
            "longitude": lon,
            "total_restaurants": 1,
            "queue": queue_name,
            "rq_job_id": rq_job.id,
            "message": f"Enqueued single restaurant job into queue '{queue_name}' with UUID requestid {batch_id_str}.",
            "status_code": 200,
        }

    # Case B: City / Location scraping
    if clear_queue:
        clear_queue_with_delay(queue, queue_name, delay_seconds=5.0)

    locations_list = resolve_locations_list(city_str, location_raw, is_all_locations, request_id=batch_uuid)

    # Enqueue tasks/url_extractor.py to discover all URLs from latlongs and push into queue
    logger.info(
        "[PRODUCER] Enqueueing URL extractor task into single queue '%s' for batch %s: city='%s', location='%s' (lat=%s, lon=%s)",
        queue_name,
        batch_id_str,
        city_str,
        location_title,
        lat,
        lon,
    )
    from scraper_app.tasks.url_extractor import extract_urls_from_latlongs

    rq_job = queue.enqueue(
        extract_urls_from_latlongs,
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
        "latitude": lat,
        "longitude": lon,
        "total_restaurants": 0,
        "queue": queue_name,
        "rq_job_id": rq_job.id,
        "message": f"Enqueued URL extractor for '{location_title}' in {city_str} with UUID requestid {batch_id_str}.",
        "status_code": 200,
    }


def start_queued_batch(queued_batch: Any) -> Dict[str, Any]:
    """Transitions a queued batch in PostgreSQL to active and enqueues url_extractor."""
    if not queued_batch:
        return {"status": "error", "message": "Batch is empty"}

    with transaction.atomic():
        if isinstance(queued_batch, (str, uuid.UUID)):
            batch_obj = ScrapeBatch.objects.select_for_update().filter(requestid=queued_batch).first()
            if not batch_obj:
                return {"status": "error", "message": f"Batch {queued_batch} not found"}
            queued_batch = batch_obj
        else:
            queued_batch = ScrapeBatch.objects.select_for_update().filter(requestid=queued_batch.requestid).first()

        if not queued_batch:
            return {"status": "error", "message": "Batch not found"}

        # Guard 1: If batch is already running or active, do NOT double-enqueue!
        if queued_batch.status in ["extracting_urls", "processing_restaurants", "fetching_details"]:
            return {
                "status": "already_running",
                "message": f"Batch is already running in status '{queued_batch.status}'",
                "batch_id": str(queued_batch.requestid),
            }

        # Guard 2: If batch is already finished or terminated, return status
        if queued_batch.status not in ["queued", "pending"]:
            return {
                "status": "already_finished",
                "message": f"Batch is already in status '{queued_batch.status}'",
                "batch_id": str(queued_batch.requestid),
            }

        # Guard 3: Check if restaurant jobs ALREADY exist in zomato_jobs for this batch!
        from scraper_app.models import RestaurantJob
        existing_jobs = RestaurantJob.objects.filter(batch=queued_batch)
        if existing_jobs.exists():
            queued_batch.status = "processing_restaurants"
            queued_batch.started_at = queued_batch.started_at or timezone.now()
            queued_batch.save(update_fields=["status", "started_at", "updated_at"])
            push_res = push_urls_to_queue(str(queued_batch.requestid))
            return {
                "status": "success",
                "message": f"Resumed {existing_jobs.count()} existing jobs for batch {queued_batch.requestid}",
                "batch_id": str(queued_batch.requestid),
                "enqueued_jobs": push_res.get("enqueued_jobs", 0),
            }

        queued_batch.status = "extracting_urls"
        queued_batch.started_at = timezone.now()
        queued_batch.save(update_fields=["status", "started_at", "updated_at"])

        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        queue = django_rq.get_queue(queue_name)

        from scraper_app.tasks.url_extractor import extract_urls_from_latlongs
        rq_job = queue.enqueue(
            extract_urls_from_latlongs,
            batch_id=str(queued_batch.requestid),
            city=queued_batch.city,
            location_raw=queued_batch.location,
            location_key=queued_batch.location_key,
            location_title=queued_batch.location,
            job_timeout=1800,
            retry=DEFAULT_RETRY,
        )
        return {
            "status": "success",
            "message": f"Started queued batch for {queued_batch.city} ({queued_batch.location})",
            "batch_id": str(queued_batch.requestid),
            "rq_job_id": rq_job.id if rq_job else None,
        }


def complete_or_terminate_batch_and_advance(
    batch: ScrapeBatch,
    status: str = "completed",
    delay_seconds: float = 5.0,
    error_message: str = "",
) -> Optional[ScrapeBatch]:
    """Finishes the batch, clears the queue, and advances to the next queued batch."""
    batch.refresh_from_db()
    batch.status = status
    if status == "completed":
        batch.progress = 100
    if not batch.completed_at:
        batch.completed_at = timezone.now()
    if error_message:
        batch.error_message = error_message
    batch.save(update_fields=["status", "progress", "completed_at", "error_message", "updated_at"])

    queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    queue = django_rq.get_queue(queue_name)
    clear_queue_with_delay(queue, queue_name, delay_seconds=delay_seconds, force_wait=True)

    next_batch = ScrapeBatch.objects.filter(
        status__in=["queued", "pending"]
    ).exclude(requestid=batch.requestid).order_by("created_at").first()

    if next_batch:
        logger.info("[PRODUCER] Starting next queued batch %s (%s, %s)...", next_batch.requestid, next_batch.city, next_batch.location)
        start_queued_batch(next_batch)
        next_batch.refresh_from_db()
        return next_batch

    return None
