"""
Views & API Endpoints for Zomato Scraper Dashboard.
Zero MongoDB, Zero SQLite - All data querying and statistics are handled
exclusively through PostgreSQL Django Models (ScrapeBatch, RestaurantRecord).
All scraping is 100% LIVE (No caching).
"""

import csv
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from django.db import models, transaction
from django.conf import settings
from django.http import Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import render
from django.utils import timezone as dj_timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST, require_http_methods
import django_rq
from rq.job import Job
from rq.registry import FailedJobRegistry

from config import Config, SINGLE_QUEUE_NAME
from scraper import ZomatoScraper, fetch_city_html_fallback
from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch, ScrapeRequest, generate_location_key
from scraper_app.parser import (
    ALL_FIELDS,
    DEFAULT_FIELDS,
    EXAMPLE_SCHEMA_DOCUMENT,
    reparse_delivery_data_for_request,
)
from scraper_app.producers import (
    clear_queue_with_delay,
    complete_or_terminate_batch_and_advance,
    dispatch_scrape_request,
    start_queued_batch,
)
from scraper_app.tasks import extract_location_urls_task

logger = logging.getLogger("ZomatoViews")


def dashboard_view(request: HttpRequest) -> HttpResponse:
    """Renders the main interactive User Dashboard."""
    stats = ScrapeBatch.get_collection_stats()
    recent_requests = ScrapeBatch.objects.all()[:10]
    context = {
        "title": "Zomato Restaurant & Review Scraper Dashboard",
        "stats": stats,
        "recent_requests": recent_requests,
        "default_fields": DEFAULT_FIELDS,
        "all_fields": ALL_FIELDS,
        "example_schema_json": json.dumps(EXAMPLE_SCHEMA_DOCUMENT, indent=2),
    }
    return render(request, "dashboard.html", context)


@require_GET
def get_collection_stats_api(request: HttpRequest) -> JsonResponse:
    """Returns dynamic counts directly from PostgreSQL Django models."""
    return JsonResponse({
        "status": "success",
        "stats": ScrapeBatch.get_collection_stats(),
    })


@require_GET
def search_locations_api(request: HttpRequest) -> JsonResponse:
    """
    Search API for location auto-complete (100% LIVE SCRAPING, NO CACHING):
    Takes 'query' (e.g. city or locality name), queries Zomato's location API live,
    and returns suggestions with lat/lon plus an option for 'All locations'.
    Guards against 401 errors and uses preloaded HTML fallback.
    """
    query = request.GET.get("query", "").strip()
    if not query:
        return JsonResponse({"status": "error", "message": "Query parameter is required"}, status=400)

    formatted_suggestions = []
    try:
        scraper = ZomatoScraper(config=Config())
        suggestions = scraper.get_location_suggestions(query)
        scraper.close()

        for s in suggestions:
            formatted_suggestions.append({
                "entity_id": s.get("entity_id", 0),
                "entity_type": s.get("entity_type", ""),
                "entity_title": s.get("entity_title") or s.get("entity_name") or query.title(),
                "entity_name": s.get("entity_name") or s.get("entity_title") or query.title(),
                "latitude": s.get("latitude"),
                "longitude": s.get("longitude"),
            })
    except Exception as exc:
        logger.warning("Error searching locations for '%s': %s. Attempting fallback.", query, exc)

    # Secondary check: if still empty, invoke direct HTML preloaded state fallback
    if not formatted_suggestions:
        try:
            html_sugs = fetch_city_html_fallback(None, query)
            for s in html_sugs:
                formatted_suggestions.append({
                    "entity_id": s.get("entity_id", 0),
                    "entity_type": s.get("entity_type", "city"),
                    "entity_title": s.get("entity_title") or s.get("entity_name") or query.title(),
                    "entity_name": s.get("entity_name") or s.get("entity_title") or query.title(),
                    "latitude": s.get("latitude"),
                    "longitude": s.get("longitude"),
                })
        except Exception as exc2:
            logger.warning("Secondary HTML fallback failed for '%s': %s", query, exc2)

    first_lat = formatted_suggestions[0].get("latitude") if formatted_suggestions else None
    first_lon = formatted_suggestions[0].get("longitude") if formatted_suggestions else None

    # If all tiers return empty, provide a minimal fallback object with null coordinates
    if not formatted_suggestions:
        formatted_suggestions.append({
            "entity_id": 0,
            "entity_type": "city",
            "entity_title": query.title(),
            "entity_name": f"{query.title()}, India",
            "latitude": None,
            "longitude": None,
        })

    resp_payload = {
        "status": "success",
        "city": query,
        "total": len(formatted_suggestions),
        "suggestions": formatted_suggestions,
        "all_option": {
            "entity_id": 0,
            "entity_type": "all",
            "entity_title": f"All Locations in {query.title()}",
            "entity_name": f"All Locations in {query.title()}",
            "latitude": first_lat,
            "longitude": first_lon,
        },
    }

    return JsonResponse(resp_payload)


@require_GET
def get_schema_fields_api(request: HttpRequest) -> JsonResponse:
    """Returns example document format and lists of default and customizable fields."""
    return JsonResponse({
        "status": "success",
        "default_fields": DEFAULT_FIELDS,
        "all_fields": ALL_FIELDS,
        "example_format": EXAMPLE_SCHEMA_DOCUMENT,
    })


@csrf_exempt
@require_POST
def start_scrape_api(request: HttpRequest) -> JsonResponse:
    """
    Producer Endpoint (100% LIVE SCRAPING):
    Handles inputs: city, location (single or all), selected fields.
    Delegates directly to scraper_app.producers.dispatch_scrape_request.
    """
    try:
        data = json.loads(request.body.decode("utf-8")) if request.body else {}
    except ValueError:
        data = request.POST.dict()

    city = str(data.get("city", "")).strip()
    location_raw = data.get("location")
    selected_fields = data.get("selected_fields")
    restaurant_url = data.get("restaurant_url") or data.get("url")

    # If the user pasted a restaurant URL or path into the city field
    if not restaurant_url and city:
        if "zomato.com" in city or city.startswith("http://") or city.startswith("https://") or re.search(r"/(?:order|info|reviews|photos)", city):
            restaurant_url = city

    max_pages_val = data.get("max_pages")
    max_pages = int(max_pages_val) if max_pages_val is not None and int(max_pages_val) > 0 else None
    clear_queue_val = bool(data.get("clear_queue", False))

    result = dispatch_scrape_request(
        city=city,
        location_raw=location_raw,
        selected_fields=selected_fields,
        force_refresh=True,
        max_pages=max_pages,
        restaurant_url=restaurant_url,
        clear_queue=clear_queue_val,
    )
    status_code = result.pop("status_code", 200)
    return JsonResponse(result, status=status_code)


def get_batch_elapsed_seconds(b: Optional[ScrapeBatch]) -> int:
    """
    Computes elapsed processing duration for a scrape batch.
    Returns 0 if the batch is in 'queued' or 'pending' state.
    When active or completed, measures duration from started_at.
    """
    if not b:
        return 0
    if b.status in ["queued", "pending"]:
        return 0
    if b.started_at:
        end_time = b.completed_at if (b.completed_at and b.status in ["completed", "terminated", "failed"]) else dj_timezone.now()
        return max(0, int((end_time - b.started_at).total_seconds()))
    if b.status in ["completed", "terminated", "failed"]:
        end_time = b.completed_at or b.updated_at or b.created_at
        return max(0, int((end_time - b.created_at).total_seconds()))
    if b.status in ["extracting_urls", "processing_restaurants", "fetching_details"]:
        b.started_at = dj_timezone.now()
        b.save(update_fields=["started_at"])
        return 0
    return max(0, int((dj_timezone.now() - b.created_at).total_seconds()))


@require_GET
def scrape_status_api(request: HttpRequest, request_id: str) -> JsonResponse:
    """
    Returns live progress bar, active running status, and queued batches.
    Supports special keyword 'active' to dynamically locate the currently running batch.
    """
    req = None
    if request_id == "active":
        # Look for currently running/processing batch first
        req = ScrapeBatch.objects.filter(
            status__in=["extracting_urls", "processing_restaurants", "fetching_details"]
        ).order_by("-updated_at").first()
        if not req:
            # Check if there are queued batches waiting: auto-start the earliest queued batch!
            queued = ScrapeBatch.objects.filter(status__in=["queued", "pending"]).order_by("created_at").first()
            if queued:
                queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
                queue = django_rq.get_queue(queue_name)
                clear_queue_with_delay(queue, queue_name, delay_seconds=5.0, force_wait=True)
                start_queued_batch(queued)
                queued.refresh_from_db()
                req = queued
        if not req:
            # Fallback to the latest batch overall
            req = ScrapeBatch.objects.order_by("-updated_at").first()
        if not req:
            return JsonResponse({"status": "error", "message": "No active batches found"}, status=404)
    else:
        try:
            req = ScrapeBatch.objects.get(request_id=request_id)
        except ScrapeBatch.DoesNotExist:
            try:
                req = ScrapeBatch.objects.get(batch_id=request_id)
            except Exception:
                # Batch not found (e.g. deleted/cancelled or invalid ID).
                # Provide currently running batch and queued batches so frontend can auto-recover seamlessly.
                running_batch = ScrapeBatch.objects.filter(
                    status__in=["extracting_urls", "processing_restaurants", "fetching_details"]
                ).order_by("-updated_at").first()
                running_batch_info = None
                if running_batch:
                    running_batch_info = {
                        "batch_id": str(running_batch.batch_id),
                        "request_id": running_batch.request_id,
                        "city": running_batch.city,
                        "location": running_batch.location,
                        "status": running_batch.status,
                        "progress": running_batch.progress,
                        "total_restaurants": running_batch.total_restaurants,
                        "completed_jobs": running_batch.completed_jobs,
                        "failed_jobs": running_batch.failed_jobs,
                        "elapsed_seconds": get_batch_elapsed_seconds(running_batch),
                    }
                queued_qs = ScrapeBatch.objects.filter(status__in=["queued", "pending"]).order_by("created_at")
                queued_batches = [
                    {
                        "batch_id": str(b.batch_id),
                        "request_id": b.request_id,
                        "city": b.city,
                        "location": b.location,
                        "status": b.status,
                        "created_at": b.created_at.isoformat(),
                        "time_str": b.created_at.strftime("%H:%M:%S"),
                        "elapsed_seconds": 0,
                    }
                    for b in queued_qs
                ]
                return JsonResponse({
                    "status": "not_found",
                    "message": f"Request ID '{request_id}' not found in PostgreSQL.",
                    "active_running_batch": running_batch_info,
                    "queued_batches": queued_batches,
                }, status=404)

    # If the requested batch is queued, check if there is an active running batch:
    # If NO other batch is running, automatically kick off execution for this queued batch!
    if req.status in ["queued", "pending"]:
        has_running = ScrapeBatch.objects.filter(
            status__in=["extracting_urls", "processing_restaurants", "fetching_details"]
        ).exclude(batch_id=req.batch_id).exists()
        if not has_running:
            start_queued_batch(req)
            req.refresh_from_db()

    # Compute live elapsed seconds
    elapsed = get_batch_elapsed_seconds(req)

    # Look up any queued batches in PostgreSQL waiting to be processed
    queued_qs = ScrapeBatch.objects.filter(
        status__in=["queued", "pending"]
    ).order_by("created_at")
    queued_batches = [
        {
            "batch_id": str(b.batch_id),
            "request_id": b.request_id,
            "city": b.city,
            "location": b.location,
            "status": b.status,
            "created_at": b.created_at.isoformat(),
            "time_str": b.created_at.strftime("%H:%M:%S"),
            "elapsed_seconds": 0,
        }
        for b in queued_qs
        if str(b.batch_id) != str(req.batch_id)
    ]

    # Look up currently running/processing batch in PostgreSQL
    running_batch = ScrapeBatch.objects.filter(
        status__in=["extracting_urls", "processing_restaurants", "fetching_details"]
    ).order_by("-updated_at").first()

    # If NO batch is running, and there is a queued batch waiting:
    if not running_batch:
        queued = ScrapeBatch.objects.filter(status__in=["queued", "pending"]).order_by("created_at").first()
        if queued:
            logger.info("[STATUS_API] No active running batch. Auto-starting queued batch %s (%s, %s)...",
                        queued.batch_id, queued.city, queued.location)
            queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
            queue = django_rq.get_queue(queue_name)
            clear_queue_with_delay(queue, queue_name, delay_seconds=5.0, force_wait=True)
            start_queued_batch(queued)
            queued.refresh_from_db()
            running_batch = queued

    running_batch_info = None
    if running_batch:
        running_batch_info = {
            "batch_id": str(running_batch.batch_id),
            "request_id": running_batch.request_id,
            "city": running_batch.city,
            "location": running_batch.location,
            "status": running_batch.status,
            "progress": running_batch.progress,
            "total_restaurants": running_batch.total_restaurants,
            "completed_jobs": running_batch.completed_jobs,
            "failed_jobs": running_batch.failed_jobs,
            "elapsed_seconds": get_batch_elapsed_seconds(running_batch),
        }

    # Dual-layer safety net: ensure completion alerts are evaluated when batch finishes
    if req.status == "completed":
        try:
            from scraper_app.alerts import check_and_alert_batch_status
            check_and_alert_batch_status(req)
        except Exception:
            pass

    return JsonResponse({
        "status": "success",
        "request_id": req.request_id,
        "batch_id": str(req.batch_id),
        "city": req.city,
        "location": req.location,
        "task_status": req.status,
        "progress": req.progress,
        "total_restaurants": req.total_restaurants,
        "completed_jobs": req.completed_jobs,
        "failed_jobs": req.failed_jobs,
        "scraped_details": req.scraped_details,
        "scraped_reviews": req.scraped_reviews,
        "parsed_delivery": req.parsed_delivery,
        "is_cached": False,
        "fields_selected": req.fields_selected,
        "elapsed_seconds": elapsed,
        "error_message": req.error_message,
        "created_at": req.created_at.isoformat(),
        "updated_at": req.updated_at.isoformat(),
        "queued_batches": queued_batches,
        "active_running_batch": running_batch_info,
    })


@require_GET
def get_scrape_data_api(request: HttpRequest, request_id: str) -> JsonResponse:
    """
    Returns extracted live data from PostgreSQL 'zomato_restaurants' for the requested request_id/batch_id.
    Filters/projects only the user-selected or default fields.
    Data access handled by Django models only.
    """
    try:
        req = ScrapeBatch.objects.get(request_id=request_id)
    except ScrapeBatch.DoesNotExist:
        try:
            req = ScrapeBatch.objects.get(batch_id=request_id)
        except Exception:
            return JsonResponse({"status": "error", "message": "Request ID not found"}, status=404)

    # If the requested batch is still queued, viewing queued data is disabled
    if req.status in ["queued", "pending"]:
        return JsonResponse({
            "status": "queued",
            "message": "This request is queued in Redis and has not started extraction yet. Viewing queued requests is disabled.",
            "data": [],
            "fields": [],
            "total_count": 0,
        })

    # Determine fields
    custom_fields_param = request.GET.get("fields")
    if custom_fields_param:
        fields = [f.strip() for f in custom_fields_param.split(",") if f.strip()]
    elif req.fields_selected and len(req.fields_selected) > 0:
        fields = req.fields_selected
    else:
        fields = DEFAULT_FIELDS

    limit = int(request.GET.get("limit", 200))
    skip = int(request.GET.get("skip", 0))

    records = RestaurantRecord.get_delivery_records(
        query={"batch_id": req.batch_id},
        fields=fields,
        limit=limit,
        skip=skip,
    )
    if not records:
        if req.location_key:
            records = RestaurantRecord.get_delivery_records(
                query={"location_key": req.location_key},
                fields=fields,
                limit=limit,
                skip=skip,
            )
        if not records and req.city:
            records = RestaurantRecord.get_delivery_records(
                query={"city": req.city},
                fields=fields,
                limit=limit,
                skip=skip,
            )

    # Format output records with requested fields
    formatted_list = []
    for doc in records:
        row: Dict[str, Any] = {}
        for f in fields:
            row[f] = doc.get(f)
        if "restaurant_id" not in row and doc.get("_id"):
            row["restaurant_id"] = doc.get("_id")
        formatted_list.append(row)

    return JsonResponse({
        "status": "success",
        "request_id": request_id,
        "batch_id": str(req.batch_id),
        "total_records": len(formatted_list),
        "fields": fields,
        "data": formatted_list,
    })


@require_GET
def export_csv_api(request: HttpRequest, request_id: str) -> HttpResponse:
    """
    Exports extracted delivery data from PostgreSQL in CSV format with user-selected or default fields.
    Data access handled by Django models only.
    """
    try:
        req = ScrapeBatch.objects.get(request_id=request_id)
    except ScrapeBatch.DoesNotExist:
        try:
            req = ScrapeBatch.objects.get(batch_id=request_id)
        except Exception:
            raise Http404("Request ID not found")

    custom_fields_param = request.GET.get("fields")
    if custom_fields_param:
        fields = [f.strip() for f in custom_fields_param.split(",") if f.strip()]
    elif req.fields_selected and len(req.fields_selected) > 0:
        fields = req.fields_selected
    else:
        fields = DEFAULT_FIELDS

    records = RestaurantRecord.get_delivery_records(
        query={"batch_id": req.batch_id},
        fields=fields,
    )
    if not records:
        if req.location_key:
            records = RestaurantRecord.get_delivery_records(
                query={"location_key": req.location_key},
                fields=fields,
            )
        if not records and req.city:
            records = RestaurantRecord.get_delivery_records(
                query={"city": req.city},
                fields=fields,
            )

    response = HttpResponse(content_type="text/csv")
    filename = f"zomato_{req.city}_{req.location}_{req.request_id[:10]}.csv".replace(" ", "_")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'

    writer = csv.writer(response)
    writer.writerow(fields)

    for doc in records:
        row = []
        for f in fields:
            val = doc.get(f)
            if isinstance(val, (list, dict)):
                if f == "reviews" and isinstance(val, list):
                    # Format reviews summary for CSV
                    val_str = f"{len(val)} reviews: " + "; ".join(
                        f"[{r.get('user_name', 'User')} ({r.get('rating', '-')})]: {r.get('review_text', '')[:60]}"
                        for r in val[:3]
                    )
                else:
                    val_str = json.dumps(val, ensure_ascii=False)
            else:
                val_str = str(val if val is not None else "")
            row.append(val_str)
        writer.writerow(row)

    return response


@csrf_exempt
@require_http_methods(["POST", "DELETE"])
def delete_batch_api(request: HttpRequest, request_id: str) -> JsonResponse:
    """
    Cancels and deletes a queued or active scrape request:
    1. Removes all associated RQ jobs for this batch from the Redis single queue ('zomato')
       and the FailedJobRegistry.
    2. Deletes all linked RestaurantJob records in PostgreSQL ('zomato_restaurant_jobs').
    3. Deletes all partial or extracted RestaurantRecord records in PostgreSQL ('zomato_restaurants').
    4. Deletes the ScrapeBatch record itself in PostgreSQL ('zomato_batches').
    """
    clean_id = str(request_id or "").strip()
    req = None

    try:
        req = ScrapeBatch.objects.get(request_id=clean_id)
    except ScrapeBatch.DoesNotExist:
        try:
            req = ScrapeBatch.objects.get(batch_id=uuid.UUID(clean_id) if len(clean_id) == 36 else clean_id)
        except Exception:
            pass

    if not req:
        return JsonResponse({"status": "error", "message": f"Request ID or Batch '{request_id}' not found."}, status=404)

    batch_id_str = str(req.batch_id)
    city = req.city
    location = req.location
    cancelled_jobs = 0

    # 1. Clean up Redis Queue jobs for this batch
    try:
        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        queue = django_rq.get_queue(queue_name)

        # Cancel jobs known from RestaurantJob tracking records
        job_rq_ids = list(RestaurantJob.objects.filter(batch=req).exclude(rq_job_id="").values_list("rq_job_id", flat=True))
        for jid in job_rq_ids:
            try:
                rq_j = Job.fetch(jid, connection=queue.connection)
                if rq_j:
                    rq_j.cancel()
                    rq_j.delete()
                    cancelled_jobs += 1
            except Exception:
                pass

        # Scan active queue for any unhandled or URL extraction jobs matching this batch_id
        try:
            for q_job in queue.jobs:
                b_id = q_job.kwargs.get("batch_id")
                if b_id and str(b_id) == batch_id_str:
                    q_job.cancel()
                    q_job.delete()
                    cancelled_jobs += 1
        except Exception as q_scan_err:
            logger.warning("Error scanning queue jobs for batch %s: %s", batch_id_str, q_scan_err)

        # Remove from FailedJobRegistry if present
        try:
            registry = FailedJobRegistry(queue=queue)
            for jid in registry.get_job_ids():
                try:
                    fj = Job.fetch(jid, connection=queue.connection)
                    if fj and fj.kwargs.get("batch_id") == batch_id_str:
                        registry.remove(fj)
                        fj.delete()
                except Exception:
                    pass
        except Exception:
            pass

    except Exception as redis_err:
        logger.error("Error clearing Redis jobs for batch %s: %s", batch_id_str, redis_err)

    # 2. Delete database records in PostgreSQL
    with transaction.atomic():
        # Delete related RestaurantJob records
        req.jobs.all().delete()

        # Delete any RestaurantRecord entries associated with this batch
        RestaurantRecord.objects.filter(batch=req).delete()

        # Delete the ScrapeBatch record
        req.delete()

    logger.info(
        "Successfully cancelled and deleted batch %s (%s, %s) from Redis and PostgreSQL (%d jobs removed).",
        batch_id_str,
        city,
        location,
        cancelled_jobs,
    )

    return JsonResponse({
        "status": "success",
        "message": f"Successfully deleted queued request for {city} ({location}) from Redis queue ({cancelled_jobs} job(s) removed) and database.",
        "batch_id": batch_id_str,
        "city": city,
        "location": location,
    })





@csrf_exempt
@require_http_methods(["POST"])
def terminate_batch_api(request: HttpRequest, request_id: str) -> JsonResponse:
    """
    Terminates the currently running or queued scrape request:
    1. Cancels and removes all pending RQ jobs for this batch from the Redis single queue ('zomato')
       and FailedJobRegistry.
    2. Marks remaining pending/in-flight RestaurantJob records as 'terminated' in PostgreSQL.
    3. Sets ScrapeBatch.status = 'terminated' in PostgreSQL.
    4. KEEPS all extracted RestaurantRecord records intact in PostgreSQL for inspection and CSV export.
    5. Returns information about the next queued batch (if any) so frontend can advance seamlessly.
    """
    clean_id = str(request_id or "").strip()
    req = None
    try:
        req = ScrapeBatch.objects.get(request_id=clean_id)
    except ScrapeBatch.DoesNotExist:
        try:
            req = ScrapeBatch.objects.get(batch_id=uuid.UUID(clean_id) if len(clean_id) == 36 else clean_id)
        except Exception:
            pass

    if not req:
        return JsonResponse({"status": "error", "message": f"Request ID or Batch '{request_id}' not found."}, status=404)

    batch_id_str = str(req.batch_id)
    city = req.city
    location = req.location

    # Complete/terminate batch in PostgreSQL, empty Redis queue, wait 5 seconds,
    # and automatically advance to the next queued request in PostgreSQL!
    next_batch = complete_or_terminate_batch_and_advance(
        batch=req,
        status="terminated",
        error_message="Process terminated by user.",
        delay_seconds=5.0,
    )

    next_batch_info = None
    if next_batch:
        next_batch_info = {
            "batch_id": str(next_batch.batch_id),
            "request_id": next_batch.request_id,
            "city": next_batch.city,
            "location": next_batch.location,
            "status": next_batch.status,
        }

    logger.info(
        "Successfully terminated batch %s (%s, %s). Redis queue emptied, 5s settle wait complete. Next batch: %s",
        batch_id_str,
        city,
        location,
        next_batch_info["batch_id"] if next_batch_info else "None",
    )

    return JsonResponse({
        "status": "success",
        "message": f"Successfully terminated scrape process for {city} ({location}). Redis queue emptied. Partial data retained in database.",
        "batch_id": batch_id_str,
        "batch_status": "terminated",
        "city": city,
        "location": location,
        "next_batch": next_batch_info,
    })


@csrf_exempt
@require_http_methods(["POST"])
def start_queued_batch_api(request: HttpRequest, request_id: str) -> JsonResponse:
    """Explicitly triggers execution of a queued batch in Redis and PostgreSQL."""
    res = start_queued_batch(request_id)
    status_code = 200 if res.get("status") in ["success", "already_finished"] else 400
    return JsonResponse(res, status=status_code)
