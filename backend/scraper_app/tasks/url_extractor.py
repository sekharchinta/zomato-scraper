"""
tasks/url_extractor.py
Extracts restaurant URLs from the lat/long coordinates chosen by the user,
stores each URL job into the 'zomato_jobs' PostgreSQL database table,
and calls producer.py upon completion to push all extracted URLs into the queue.
Zero MongoDB, Zero SQLite - 100% PostgreSQL schema 'zomato'.
"""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional
import django_rq
from django.conf import settings
from django.utils import timezone

from config import (
    BASE_URL,
    DEFAULT_HEADERS,
    RESTAURANTS_API_URL,
    SINGLE_QUEUE_NAME,
)
from scraper import (
    ZomatoScraper,
    build_locus_cookie,
    extract_restaurant_page_url,
    extract_zomato_preloaded_state,
    fetch_city_html_fallback,
)
from scraper_app.models import LatLongData, RestaurantJob, ScrapeBatch
from scraper_app.tasks.base import check_and_wait_if_paused, get_active_scraper

logger = logging.getLogger("url_extractor")

DEFAULT_RETRY = getattr(settings, "RQ_RETRY_CONFIG", None)


class UserURLExtractor:
    """
    Isolated per-user URL Extractor.
    Executes URL discovery inside a user's dedicated worker pool using an isolated
    ZomatoScraper HTTP session (cookies, locus geofence, CSRF token) and pushes
    discovered jobs strictly through UserProducer(user_id=...).
    """

    def __init__(self, batch_id: str, user_id: Optional[Any] = None) -> None:
        self.batch_id = str(batch_id)
        self.user_id = user_id

    def run(
        self,
        city: str,
        locations_list: Optional[List[Dict[str, Any]]] = None,
        location_raw: Any = None,
        location_key: Optional[str] = None,
        location_title: Optional[str] = None,
        max_pages: Optional[int] = None,
    ) -> Dict[str, Any]:
        return _execute_user_url_extraction(
            batch_id=self.batch_id,
            city=city,
            locations_list=locations_list,
            location_raw=location_raw,
            location_key=location_key,
            location_title=location_title,
            max_pages=max_pages,
            explicit_user_id=self.user_id,
        )


def extract_urls_from_latlongs(
    batch_id: str,
    city: str,
    locations_list: Optional[List[Dict[str, Any]]] = None,
    location_raw: Any = None,
    location_key: Optional[str] = None,
    location_title: Optional[str] = None,
    max_pages: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Extracts all restaurant URLs for the chosen location coordinates,
    persists each URL into the 'zomato_jobs' table in PostgreSQL,
    and calls UserProducer to push all URLs into the user's dedicated Redis queue.
    """
    return _execute_user_url_extraction(
        batch_id=batch_id,
        city=city,
        locations_list=locations_list,
        location_raw=location_raw,
        location_key=location_key,
        location_title=location_title,
        max_pages=max_pages,
    )


def _execute_user_url_extraction(
    batch_id: str,
    city: str,
    locations_list: Optional[List[Dict[str, Any]]] = None,
    location_raw: Any = None,
    location_key: Optional[str] = None,
    location_title: Optional[str] = None,
    max_pages: Optional[int] = None,
    explicit_user_id: Optional[Any] = None,
) -> Dict[str, Any]:
    batch = ScrapeBatch.objects.filter(requestid=batch_id).first()
    if not batch:
        logger.error("[URL_EXTRACTOR] ScrapeBatch '%s' not found. Aborting.", batch_id)
        return {"status": "error", "message": f"Batch {batch_id} not found"}

    batch.status = "extracting_urls"
    batch.progress = 5
    if not batch.started_at:
        batch.started_at = timezone.now()
    batch.save(update_fields=["status", "progress", "started_at", "updated_at"])

    logger.info("[URL_EXTRACTOR] Starting URL extraction for batch %s (%s, %s)", batch_id, city, location_title)

    # 1. Resolve locations list if needed
    if not locations_list:
        from scraper_app.producers.producer import resolve_locations_list
        is_all = False
        if isinstance(location_raw, dict) and location_raw.get("entity_type") == "all":
            is_all = True
        elif isinstance(location_raw, str) and location_raw.upper() in ("ALL", "ALL LOCATIONS", f"ALL LOCATIONS IN {city.upper()}"):
            is_all = True
        elif not location_raw:
            is_all = True
        locations_list = resolve_locations_list(city, location_raw, is_all, request_id=batch.requestid)

    is_all_batch = False
    batch_target = str(location_title or batch.location or location_raw or "").strip().upper()
    if batch_target in ("ALL", "ALL LOCATIONS", f"ALL LOCATIONS IN {city.upper()}", city.upper()):
        is_all_batch = True

    scraper = get_active_scraper()
    total_discovered_urls = 0

    try:
        # Pre-fetch city spatial fallback to ensure valid placeId, cellId, deliverySubzoneId
        city_curr_data: Dict[str, Any] = {}
        try:
            city_sugs = fetch_city_html_fallback(scraper.session, city)
            if city_sugs and city_sugs[0].get("raw_suggestion"):
                city_curr_data = city_sugs[0]["raw_suggestion"]
        except Exception as city_err:
            logger.warning("[URL_EXTRACTOR] Could not pre-fetch city spatial IDs: %s", city_err)

        default_city_id = str(city_curr_data.get("cityId") or "0")
        default_place_id = str(city_curr_data.get("placeId") or "0")
        default_cell_id = str(city_curr_data.get("cellId") or "0")
        default_subzone_id = int(city_curr_data.get("deliverySubzoneId") or 0)
        city_lat = float(city_curr_data.get("latitude")) if city_curr_data.get("latitude") else None
        city_lon = float(city_curr_data.get("longitude")) if city_curr_data.get("longitude") else None

        for loc_item in locations_list:
            loc_name = loc_item.get("entity_title") or loc_item.get("entity_name") or city
            lat = loc_item.get("latitude") or city_lat
            lon = loc_item.get("longitude") or city_lon

            # Update batch latitude and longitude if not yet set
            if lat is not None and getattr(batch, "latitude", None) is None and hasattr(batch, "latitude"):
                try:
                    batch.latitude = float(lat)
                    batch.save(update_fields=["latitude", "updated_at"])
                except Exception:
                    pass
            if lon is not None and getattr(batch, "longitude", None) is None and hasattr(batch, "longitude"):
                try:
                    batch.longitude = float(lon)
                    batch.save(update_fields=["longitude", "updated_at"])
                except Exception:
                    pass

            try:
                resolved_loc = scraper.resolve_location(city=city, lat=lat, lon=lon, location=loc_name)
            except Exception as exc:
                logger.warning("[URL_EXTRACTOR] Primary resolve_location failed for '%s': %s", loc_name, exc)
                resolved_loc = {
                    "entity_id": default_city_id,
                    "entity_type": "city",
                    "city_id": default_city_id,
                    "country_id": "1",
                    "country_name": "India",
                    "place_id": default_place_id,
                    "cell_id": default_cell_id,
                    "delivery_subzone_id": default_subzone_id,
                    "place_type": "DSZ",
                    "city_name": city,
                    "location_name": loc_name,
                    "folder_name": f"{city}_{loc_name}".lower().replace(" ", "_"),
                    "latitude": lat or city_lat or 17.3850,
                    "longitude": lon or city_lon or 78.4867,
                    "user_defined_latitude": lat or city_lat or 17.3850,
                    "user_defined_longitude": lon or city_lon or 78.4867,
                    "exact_location_url": f"{BASE_URL}/{city.lower().replace(' ', '-')}/restaurants",
                }

            # Ensure spatial IDs are non-zero to prevent 400 Parameter errors
            if (not resolved_loc.get("place_id") or str(resolved_loc.get("place_id")) == "0") and default_place_id != "0":
                resolved_loc["place_id"] = default_place_id
            if (not resolved_loc.get("cell_id") or str(resolved_loc.get("cell_id")) == "0") and default_cell_id != "0":
                resolved_loc["cell_id"] = default_cell_id
            if (not resolved_loc.get("delivery_subzone_id") or resolved_loc.get("delivery_subzone_id") == 0) and default_subzone_id != 0:
                resolved_loc["delivery_subzone_id"] = default_subzone_id

            # Store resolved latlongs in PostgreSQL 'latlong_data' table linked to requestid
            try:
                r_lat = resolved_loc.get("latitude") or lat
                r_lon = resolved_loc.get("longitude") or lon
                if r_lat is not None and r_lon is not None:
                    LatLongData.objects.get_or_create(
                        requestid=batch.requestid,
                        entity_title=loc_name,
                        defaults={
                            "search_query": loc_name,
                            "city": city,
                            "latitude": float(r_lat),
                            "longitude": float(r_lon),
                            "place_id": str(resolved_loc.get("place_id") or ""),
                            "cell_id": str(resolved_loc.get("cell_id") or ""),
                            "delivery_subzone_id": int(resolved_loc.get("delivery_subzone_id") or 0),
                        },
                    )
            except Exception as latlong_err:
                logger.warning("[URL_EXTRACTOR] Error recording latlong to PostgreSQL: %s", latlong_err)

            # Standard format: Discover at exact location coordinates without uniqueness filtering
            discovered = _scrape_location_coordinates(
                scraper=scraper,
                batch=batch,
                resolved_loc=resolved_loc,
                loc_name=loc_name,
                city=city,
                is_all_batch=is_all_batch,
                max_pages=max_pages,
            )
            total_discovered_urls += discovered

        # Fallback: If 0 restaurants discovered via API, attempt public HTML preloaded state
        if total_discovered_urls == 0:
            logger.info("[URL_EXTRACTOR] 0 restaurants discovered via API for %s. Attempting public HTML preloaded fallback...", city)
            try:
                html_resp = scraper.session.get(
                    f"{BASE_URL}/{city.lower().replace(' ', '-')}/restaurants",
                    headers={"user-agent": DEFAULT_HEADERS["user-agent"]},
                    timeout=15,
                )
                if html_resp.status_code == 200:
                    state = extract_zomato_preloaded_state(html_resp.text)
                    if state and isinstance(state, dict):
                        pages = state.get("pages", {})
                        sections = {}
                        # 1. Search across pages.search (e.g. pages['search']['/palakollu/restaurants']['sections'])
                        search_dict = pages.get("search", {})
                        if isinstance(search_dict, dict):
                            for path_key, path_val in search_dict.items():
                                if isinstance(path_val, dict) and "sections" in path_val:
                                    sections = path_val["sections"]
                                    break
                        # 2. Search in pages.restaurant
                        if not sections and isinstance(pages.get("restaurant"), dict):
                            sections = pages["restaurant"].get("sections", {})
                        # 3. Search in pages.current
                        if not sections and isinstance(pages.get("current"), dict):
                            sections = pages["current"].get("sections", {})

                        res_list = sections.get("SECTION_SEARCH_RESULT", []) if isinstance(sections, dict) else []
                        seen_batch_urls = set(RestaurantJob.objects.filter(batch=batch).values_list("page_url", flat=True))
                        for item in res_list:
                            url = extract_restaurant_page_url(item)
                            if not url or url in seen_batch_urls:
                                continue
                            seen_batch_urls.add(url)
                            res_obj = item.get("restaurant", {}) if isinstance(item, dict) else {}
                            name = res_obj.get("name") or item.get("info", {}).get("name") or "Restaurant"
                            res_id = res_obj.get("id") or item.get("info", {}).get("resId")
                            clean_res_id = int(res_id) if res_id and str(res_id).isdigit() else None
                            RestaurantJob.objects.create(
                                batch=batch,
                                restaurant_id=clean_res_id,
                                name=name,
                                page_url=url,
                                stage="pending",
                            )
                            total_discovered_urls += 1
            except Exception as html_err:
                logger.warning("[URL_EXTRACTOR] HTML preloaded fallback error: %s", html_err)

        scraper.close()

        # Update total restaurants count in ScrapeBatch
        batch.refresh_from_db()
        batch.total_restaurants = total_discovered_urls
        batch.save(update_fields=["total_restaurants", "updated_at"])

        logger.info(
            "[URL_EXTRACTOR] Finished extraction: Discovered %d unique restaurant URLs for batch %s in 'zomato_jobs'",
            total_discovered_urls,
            batch_id,
        )

        # 4. Call UserProducer (isolated per user) to push all extracted URLs into the user's queue
        from scraper_app.producers.producer import UserProducer, push_urls_to_queue
        owner_user_id = explicit_user_id if explicit_user_id is not None else getattr(batch, "user_id", None)
        if owner_user_id is not None:
            dispatch_result = UserProducer(user_id=owner_user_id).push_urls_to_queue(request_id=batch_id)
        else:
            dispatch_result = push_urls_to_queue(request_id=batch_id)

        return {
            "status": "success",
            "batch_id": batch_id,
            "total_urls": total_discovered_urls,
            "dispatch": dispatch_result,
        }

    except Exception as exc:
        logger.error("[URL_EXTRACTOR] Error extracting URLs for batch %s: %s", batch_id, exc)
        scraper.close()
        batch.status = "failed"
        batch.error_message = f"URL extraction failed: {str(exc)}"
        batch.save(update_fields=["status", "error_message", "updated_at"])
        raise exc


def _scrape_location_coordinates(
    scraper: ZomatoScraper,
    batch: ScrapeBatch,
    resolved_loc: Dict[str, Any],
    loc_name: str,
    city: str,
    is_all_batch: bool,
    max_pages: Optional[int],
) -> int:
    """Queries Zomato's search/home API and saves discovered URLs to zomato_jobs without uniqueness restrictions."""
    extract_url_func = extract_restaurant_page_url
    pause_check_func = check_and_wait_if_paused

    # 1. Set delivery geofence cookies on scraper session matching the delivery cURL
    c_id = str(resolved_loc.get("city_id") or resolved_loc.get("entity_id") or "0")
    dsz_id = resolved_loc.get("delivery_subzone_id") or resolved_loc.get("place_id") or 0
    c_lat = float(resolved_loc.get("latitude") or 17.3850)
    c_lon = float(resolved_loc.get("longitude") or 78.4867)
    try:
        locus_cookie = build_locus_cookie(city_id=c_id, lat=c_lat, lon=c_lon, dsz_id=dsz_id, city_name=city)
        scraper.session.cookies.set("locus", locus_cookie, domain=".zomato.com")
        scraper.session.cookies.set("fbcity", str(c_id), domain=".zomato.com")
        scraper.session.cookies.set("ltv", str(c_id), domain=".zomato.com")
        scraper.session.cookies.set("lty", "city", domain=".zomato.com")
    except Exception as locus_err:
        logger.warning("[URL_EXTRACTOR] Error setting delivery locus cookie: %s", locus_err)

    page = 1
    offset = 0
    pages_scraped = 0
    search_metadata: Optional[Dict[str, Any]] = None
    discovered_count = 0
    last_page_urls: List[str] = []
    seen_batch_urls: set = set(RestaurantJob.objects.filter(batch=batch).values_list("page_url", flat=True))

    while True:
        if not pause_check_func(batch.batch_id, f"URL extraction page {page} for {loc_name}"):
            logger.info("[URL_EXTRACTOR] Extraction stopped because batch %s was terminated.", batch.batch_id)
            return discovered_count

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
                    context="delivery",
                )
                referer_url = resolved_loc.get("exact_location_url") or f"{BASE_URL}/{city.lower().replace(' ', '-')}/restaurants"
                if not referer_url.endswith("?category=1") and "/restaurants" in referer_url:
                    referer_url = f"{referer_url}?category=1"

                headers = {
                    "user-agent": DEFAULT_HEADERS["user-agent"],
                    "origin": BASE_URL,
                    "referer": referer_url,
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
                logger.warning("[URL_EXTRACTOR] Attempt %d error querying API for %s: %s", attempt, loc_name, req_err)
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

        # PRIMARY STOPPING RULE: Stop ONLY when SECTION_SEARCH_RESULT is empty
        if not restaurants or len(restaurants) == 0:
            logger.info("[URL_EXTRACTOR] SECTION_SEARCH_RESULT is empty on page %d for %s. Reached end of catalog.", page, loc_name)
            break

        current_page_urls = []
        for item in restaurants:
            url = extract_url_func(item)
            if not url:
                continue

            current_page_urls.append(url)

            # Prevent duplicate jobs within the same batch (e.g. sponsored ads repeated across pages)
            if url in seen_batch_urls:
                continue
            seen_batch_urls.add(url)

            res_obj = item.get("info", {}) or item.get("restaurant", {})
            name = res_obj.get("name") or "Restaurant"
            res_id = res_obj.get("resId") or res_obj.get("id")
            clean_res_id = int(res_id) if res_id and str(res_id).isdigit() else None

            # Persist job record in 'zomato_jobs' table (zero filters, no uniqueness restriction: extract all)
            RestaurantJob.objects.create(
                batch=batch,
                restaurant_id=clean_res_id,
                name=name,
                page_url=url,
                stage="pending",
            )
            discovered_count += 1

        # Real-time counter update
        batch.total_restaurants = RestaurantJob.objects.filter(batch=batch).count()
        batch.save(update_fields=["total_restaurants", "updated_at"])

        # Loop guard: If current page returned exact identical URLs as previous page, break
        if current_page_urls and current_page_urls == last_page_urls:
            logger.info("[URL_EXTRACTOR] Page %d returned identical items as previous page. Stopping pagination.", page)
            break
        last_page_urls = current_page_urls

        # Check hasMore (supports both meta_info.hasMore and meta_info.searchMetaData.hasMore)
        search_metadata = meta_info.get("searchMetaData") if isinstance(meta_info, dict) else {}
        if not isinstance(search_metadata, dict):
            search_metadata = {}

        has_more = meta_info.get("hasMore")
        if has_more is None:
            has_more = search_metadata.get("hasMore")

        if has_more is False:
            logger.info("[URL_EXTRACTOR] hasMore is False on page %d for %s. Reached end of catalog.", page, loc_name)
            break

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
        time.sleep(0.4)

    return discovered_count


# Backward compatibility alias
extract_location_urls_task = extract_urls_from_latlongs
