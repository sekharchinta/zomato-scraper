"""
Task: Extract Location Restaurant URLs.
Single Queue: zomato (Redis DB 1)
Database: PostgreSQL schema 'zomato' (Zero MongoDB, Zero SQLite)

Worker task for 'zomato':
1. Receives batch_id, city, location details.
2. Discovers all restaurant URLs across pagination for the requested locality/city (with 0.4s delay).
3. For each restaurant URL discovered:
   - Creates a RestaurantJob record in PostgreSQL ('zomato_restaurant_jobs').
   - Immediately enqueues process_restaurant_pipeline_job into the SAME single queue ('zomato').
   - Updates batch.total_restaurants in PostgreSQL in real time.
4. If 0 restaurants returned by API, falls back to city restaurants HTML page preloaded state.
"""

import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime
from django.utils import timezone

import django_rq
from django.conf import settings

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    RESTAURANTS_API_URL,
    SINGLE_QUEUE_NAME,
    Config,
)
from scraper import (
    extract_restaurant_page_url,
    extract_zomato_preloaded_state,
)
from scraper_app.models import RestaurantJob, ScrapeBatch
from scraper_app.tasks.base import (
    DEFAULT_RETRY,
    check_and_wait_if_paused,
    get_active_scraper,
    save_task_fallback_data,
)
from scraper_app.tasks.single_pipeline_task import process_restaurant_pipeline_job

logger = logging.getLogger("ZomatoLocationTask")


def extract_location_urls_task(
    batch_id: str,
    city: str,
    locations_list: Optional[List[Dict[str, Any]]] = None,
    location_raw: Optional[Any] = None,
    location_key: Optional[str] = None,
    location_title: Optional[str] = None,
    max_pages: Optional[int] = None,
    request_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    RQ Worker task for single queue 'zomato':
    Discovers all restaurant URLs across pagination and pushes each restaurant
    job into the SAME single queue ('zomato').
    """
    logger.info(
        "[URL_EXTRACTION] Starting URL extraction task for batch %s: city='%s', location='%s'",
        batch_id,
        city,
        location_title,
    )

    # 1. Lookup ScrapeBatch in PostgreSQL
    batch: Optional[ScrapeBatch] = None
    if batch_id:
        try:
            batch = ScrapeBatch.objects.filter(batch_id=batch_id).first()
        except Exception:
            pass
    if not batch and request_id:
        try:
            batch = ScrapeBatch.objects.filter(request_id=request_id).first()
        except Exception:
            pass

    if not batch:
        logger.warning(
            "[URL_EXTRACTION] Batch %s does not exist in PostgreSQL (deleted or cancelled). Aborting extraction.",
            batch_id or request_id,
        )
        return {
            "status": "cancelled",
            "batch_id": batch_id or request_id,
            "message": "Batch does not exist in PostgreSQL. Aborting extraction.",
        }

    batch.status = "extracting_urls"
    batch.progress = 5
    if not batch.started_at:
        batch.started_at = timezone.now()
    batch.save(update_fields=["status", "progress", "started_at", "updated_at"])

    try:
        scraper = get_active_scraper()
        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        queue_zomato = django_rq.get_queue(queue_name)

        # 2. Resolve locations list if not pre-populated
        if not locations_list:
            from scraper_app.producers.scrape_producer import resolve_locations_list
            is_all = False
            if isinstance(location_raw, dict) and location_raw.get("entity_type") == "all":
                is_all = True
            elif isinstance(location_raw, str) and location_raw.upper() in ("ALL", "ALL LOCATIONS", f"ALL LOCATIONS IN {city.upper()}"):
                is_all = True
            elif not location_raw:
                is_all = True
            locations_list = resolve_locations_list(city, location_raw, is_all)

        total_discovered_urls = 0
        seen_urls = set()

        # 3. Discover restaurant URLs for each location
        for loc_item in locations_list:
            loc_name = loc_item.get("entity_title") or loc_item.get("entity_name") or city
            lat = loc_item.get("latitude")
            lon = loc_item.get("longitude")

            try:
                resolved_loc = scraper.resolve_location(
                    city=city,
                    lat=lat,
                    lon=lon,
                    location=loc_name,
                )
            except Exception as exc:
                logger.warning("Primary resolve_location failed for '%s': %s. Attempting fallback...", loc_name, exc)
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
                    "location_name": loc_name,
                    "folder_name": f"{city}_{loc_name}".lower().replace(" ", "_"),
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
                if not check_and_wait_if_paused(batch.batch_id, f"URL extraction page {page} for {loc_name}"):
                    logger.info("[URL_EXTRACTION] Extraction stopped because batch %s was terminated.", batch.batch_id)
                    scraper.close()
                    return {
                        "status": "terminated",
                        "batch_id": str(batch.batch_id),
                        "message": "Extraction stopped because batch was terminated.",
                    }

                if max_pages is not None and pages_scraped >= max_pages:
                    logger.info("Reached max_pages limit (%d) for %s. Stopping pagination.", max_pages, loc_name)
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
                        logger.warning("[URL_EXTRACTION] Attempt %d error querying restaurants API for %s: %s", attempt, loc_name, req_err)
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
                    url = extract_restaurant_page_url(item)
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)

                    res_obj = item.get("restaurant", {}) if isinstance(item, dict) else {}
                    name = res_obj.get("name") or item.get("info", {}).get("name") or "Restaurant"
                    res_id = res_obj.get("id") or item.get("info", {}).get("resId")
                    clean_res_id = int(res_id) if res_id and str(res_id).isdigit() else None

                    # Create PostgreSQL tracking record
                    job_record = RestaurantJob.objects.create(
                        batch=batch,
                        restaurant_id=clean_res_id,
                        name=name,
                        page_url=url,
                        stage="pending",
                    )

                    # Enqueue restaurant pipeline task into the SINGLE queue 'zomato'
                    rq_job = queue_zomato.enqueue(
                        process_restaurant_pipeline_job,
                        batch_id=str(batch.batch_id),
                        job_record_id=job_record.id,
                        restaurant_url=url,
                        name=name,
                        city=resolved_loc.get("city_name", city),
                        location=resolved_loc.get("location_name", loc_name),
                        location_key=location_key or "",
                        max_review_pages=10,
                        retry=DEFAULT_RETRY,
                    )
                    job_record.rq_job_id = rq_job.id
                    job_record.save(update_fields=["rq_job_id", "updated_at"])

                    total_discovered_urls += 1
                    page_enqueued += 1

                # Update live counter in PostgreSQL so dashboard displays real-time restaurant count
                if batch:
                    batch.total_restaurants = total_discovered_urls
                    batch.save(update_fields=["total_restaurants", "updated_at"])

                logger.info(
                    "[URL_EXTRACTION] Page %d enqueued %d restaurant job(s) into single queue '%s' (Batch total: %d)",
                    page,
                    page_enqueued,
                    queue_name,
                    total_discovered_urls,
                )

                # Stopping Condition 1: If 0 new restaurants were enqueued from this page (wrap-around repeat), stop
                if page_enqueued == 0:
                    logger.info(
                        "[URL_EXTRACTION] Page %d for '%s' returned 0 new restaurants (end of unique results / wrap-around). Stopping pagination.",
                        page,
                        loc_name,
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
                        "[URL_EXTRACTION] hasMore is False on Page %d for '%s'. Stopping pagination.",
                        page,
                        loc_name,
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

        # 4. Universal HTML Fallback: If 0 restaurants found from API, check city restaurants HTML page
        if total_discovered_urls == 0:
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
                                if not url or url in seen_urls:
                                    continue
                                seen_urls.add(url)
                                res_obj = item.get("restaurant", {}) if isinstance(item, dict) else {}
                                name = res_obj.get("name") or item.get("info", {}).get("name") or "Restaurant"
                                res_id = res_obj.get("id") or item.get("info", {}).get("resId")
                                clean_res_id = int(res_id) if res_id and str(res_id).isdigit() else None

                                job_record = RestaurantJob.objects.create(
                                    batch=batch,
                                    restaurant_id=clean_res_id,
                                    name=name,
                                    page_url=url,
                                    stage="pending",
                                )

                                rq_job = queue_zomato.enqueue(
                                    process_restaurant_pipeline_job,
                                    batch_id=str(batch.batch_id),
                                    job_record_id=job_record.id,
                                    restaurant_url=url,
                                    name=name,
                                    city=city,
                                    location=location_title or city,
                                    location_key=location_key or "",
                                    max_review_pages=10,
                                    retry=DEFAULT_RETRY,
                                )
                                job_record.rq_job_id = rq_job.id
                                job_record.save(update_fields=["rq_job_id", "updated_at"])

                                total_discovered_urls += 1

                if batch:
                    batch.total_restaurants = total_discovered_urls
                    batch.save(update_fields=["total_restaurants", "updated_at"])
            except Exception as exc:
                logger.warning("[URL_EXTRACTION] HTML fallback discovery error: %s", exc)

        scraper.close()

        # 5. Update batch status in PostgreSQL
        if batch:
            batch.refresh_from_db()
            if batch.status == "terminated":
                logger.info("[URL_EXTRACTION] Batch %s is marked terminated. Leaving status untouched.", batch_id)
                return {
                    "status": "terminated",
                    "batch_id": str(batch.batch_id),
                    "message": "Batch was terminated by user.",
                }
            batch.total_restaurants = total_discovered_urls
            if total_discovered_urls == 0:
                if batch.status != "completed":
                    from scraper_app.producers.scrape_producer import complete_or_terminate_batch_and_advance
                    complete_or_terminate_batch_and_advance(batch, status="completed", delay_seconds=5.0)
            else:
                batch.status = "processing_restaurants"
                done_count = (batch.completed_jobs or 0) + (batch.failed_jobs or 0)
                if done_count >= total_discovered_urls:
                    if batch.status != "completed":
                        from scraper_app.producers.scrape_producer import complete_or_terminate_batch_and_advance
                        complete_or_terminate_batch_and_advance(batch, status="completed", delay_seconds=5.0)
                else:
                    batch.progress = max(batch.progress, int((done_count / total_discovered_urls) * 100))
                    batch.save(update_fields=["status", "progress", "total_restaurants", "updated_at"])

        logger.info(
            "[URL_EXTRACTION] Task complete: %d restaurant job(s) enqueued directly into single queue '%s' (batch=%s)",
            total_discovered_urls,
            queue_name,
            batch_id,
        )
        return {
            "status": "success",
            "batch_id": batch_id,
            "total_urls": total_discovered_urls,
        }

    except Exception as exc:
        logger.error("[URL_EXTRACTION] Failed to extract URLs for batch %s: %s", batch_id, exc)
        if batch:
            batch.status = "failed"
            batch.error_message = f"URL extraction failed: {str(exc)}"
            batch.save(update_fields=["status", "error_message", "updated_at"])
        save_task_fallback_data(
            "extract_location_urls_task",
            {"request_id": batch_id, "city": city, "location_key": location_key},
            exc=exc,
        )
        raise exc


# Backward compatibility alias
discover_and_enqueue_restaurant_urls_job = extract_location_urls_task
