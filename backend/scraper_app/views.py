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
from scraper_app.models import LatLongData, RestaurantJob, RestaurantRecord, ScrapeBatch, ScrapeRequest, generate_location_key
from scraper_app.parsers.parser import (
    ALL_FIELDS,
    DEFAULT_FIELDS,
    EXAMPLE_SCHEMA_DOCUMENT,
    reparse_delivery_data_for_request,
)
from scraper_app.producers.producer import (
    clear_queue_with_delay,
    complete_or_terminate_batch_and_advance,
    dispatch_scrape_request,
    start_queued_batch,
)
from scraper_app.tasks.url_extractor import extract_urls_from_latlongs as extract_location_urls_task

logger = logging.getLogger("ZomatoViews")


def react_app_view(request: HttpRequest) -> HttpResponse:
    """Renders the compiled React SPA entry point."""
    frontend_dist = getattr(settings, "FRONTEND_DIST_DIR", getattr(settings, "ROOT_DIR", settings.BASE_DIR.parent) / "frontend" / "dist")
    index_file = frontend_dist / "index.html"
    if index_file.exists():
        return render(request, "index.html")
    return HttpResponse(
        """<!DOCTYPE html>
        <html>
        <head><title>Zomato Scraper - React Frontend</title></head>
        <body style="font-family: sans-serif; display:flex; align-items:center; justify-content:center; height:100vh; margin:0; background:#f8fafc;">
            <div style="background:white; padding:32px; border-radius:12px; box-shadow:0 4px 6px rgba(0,0,0,0.05); text-align:center; max-width:520px;">
                <h2 style="color:#e23744; margin-top:0;">Zomato Scraper - React Frontend</h2>
                <p style="color:#475569;">The production bundle (<code>frontend/dist/index.html</code>) is currently being generated.</p>
                <div style="margin-top:20px; padding:12px; background:#f1f5f9; border-radius:8px; font-family:monospace; font-size:13px; text-align:left;">
                    <b>Development Mode:</b><br>
                    cd frontend &amp;&amp; npm run dev<br>
                    Open: <a href="http://localhost:5173" style="color:#e23744;">http://localhost:5173</a><br><br>
                    <b>Production Mode:</b><br>
                    cd frontend &amp;&amp; npm run build
                </div>
            </div>
        </body>
        </html>""",
        content_type="text/html",
    )


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
    Takes 'query' (e.g. city or locality name), delegates to location_extractor,
    stores results and lat/longs into the 'latlong_data' database table linked to requestid,
    and returns suggestions with lat/long and unified request_id plus an option for 'All locations'.
    """
    query = request.GET.get("query", "").strip()
    if not query:
        return JsonResponse({"status": "error", "message": "Query parameter is required"}, status=400)

    req_id = request.GET.get("request_id", "").strip() or str(uuid.uuid4())
    user = request.user if getattr(request, "user", None) and request.user.is_authenticated else None
    from scraper_app.tasks.location_extractor import extract_locations_and_store
    formatted_suggestions = extract_locations_and_store(query, request_id=req_id, user=user)

    first_lat = formatted_suggestions[0].get("latitude") if formatted_suggestions else None
    first_lon = formatted_suggestions[0].get("longitude") if formatted_suggestions else None

    # If all tiers return empty, provide a minimal fallback object with null coordinates
    if not formatted_suggestions:
        formatted_suggestions.append({
            "request_id": req_id,
            "entity_id": 0,
            "entity_type": "city",
            "entity_title": query.title(),
            "entity_name": f"{query.title()}, India",
            "latitude": None,
            "longitude": None,
        })

    all_option_item = {
        "request_id": req_id,
        "entity_id": "0",
        "entity_type": "all",
        "entity_title": f"All Locations in {query.title()}",
        "entity_name": f"All Locations in {query.title()}",
        "latitude": first_lat,
        "longitude": first_lon,
    }

    # Persist "All Locations in <City>" into latlong_data sharing the SAME unified batch requestid
    try:
        LatLongData.objects.create(
            requestid=uuid.UUID(req_id),
            user=user,
            search_query=query,
            city=query.title(),
            entity_id="0",
            entity_type="all",
            entity_title=f"All Locations in {query.title()}",
            entity_name=f"All Locations in {query.title()}",
            latitude=first_lat,
            longitude=first_lon,
            raw_data={"all_locations": True, "total_subzones": len(formatted_suggestions)},
        )
    except Exception as all_err:
        logger.warning("[VIEWS] Error saving all_locations to latlong_data: %s", all_err)

    resp_payload = {
        "status": "success",
        "request_id": req_id,
        "city": query,
        "total": len(formatted_suggestions),
        "suggestions": formatted_suggestions,
        "all_option": all_option_item,
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
    request_id_val = data.get("request_id") or data.get("batch_id")
    if not request_id_val and isinstance(location_raw, dict):
        request_id_val = location_raw.get("request_id")

    result = dispatch_scrape_request(
        city=city,
        location_raw=location_raw,
        selected_fields=selected_fields,
        force_refresh=True,
        max_pages=max_pages,
        restaurant_url=restaurant_url,
        clear_queue=clear_queue_val,
        request_id=request_id_val,
        user=request.user if getattr(request, "user", None) and request.user.is_authenticated else None,
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

    # Safety check: if all jobs for this batch are finished, ensure status is completed
    if (
        req.status not in ["completed", "terminated", "failed"]
        and req.total_restaurants > 0
        and (req.completed_jobs + req.failed_jobs) >= req.total_restaurants
    ):
        req.status = "completed"
        req.progress = 100
        if not req.completed_at:
            req.completed_at = dj_timezone.now()
        req.save(update_fields=["status", "progress", "completed_at", "updated_at"])

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

    # Look up recent completed batches to keep history constantly fresh
    recent_completed_qs = ScrapeBatch.objects.filter(
        models.Q(status="completed") |
        models.Q(status__in=["terminated", "failed"], total_restaurants__gt=0) |
        models.Q(progress=100, total_restaurants__gt=0)
    ).exclude(
        models.Q(status__in=["terminated", "failed"], total_restaurants=0)
    ).order_by(
        models.F("completed_at").desc(nulls_last=True),
        models.F("updated_at").desc(),
        models.F("created_at").desc()
    )[:25]
    completed_batches_data = []
    for cb in recent_completed_qs:
        c_count = RestaurantRecord.objects.filter(requestid=cb).count()
        if c_count == 0:
            c_res_ids = list(RestaurantJob.objects.filter(batch=cb, stage="completed").values_list("restaurant_id", flat=True))
            if c_res_ids:
                c_count = RestaurantRecord.objects.filter(restaurant_id__in=c_res_ids).count()
        if c_count == 0 and (cb.status == "completed" or cb.progress == 100):
            c_count = cb.completed_jobs or cb.total_restaurants or cb.scraped_details
        completed_batches_data.append({
            "batch_id": str(cb.batch_id),
            "request_id": cb.request_id,
            "city": cb.city,
            "location": cb.location,
            "status": cb.status,
            "total_restaurants": cb.total_restaurants,
            "completed_jobs": cb.completed_jobs,
            "records_count": c_count,
            "created_at": cb.created_at.isoformat() if cb.created_at else None,
            "completed_at": cb.completed_at.isoformat() if cb.completed_at else None,
            "elapsed_seconds": get_batch_elapsed_seconds(cb),
            "csv_url": f"/api/export-csv/{cb.batch_id}/",
        })

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
        "latitude": float(req.latitude) if getattr(req, "latitude", None) else None,
        "longitude": float(req.longitude) if getattr(req, "longitude", None) else None,
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
        "completed_batches": completed_batches_data,
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

    # STRICT BATCH ISOLATION: Query only records linked directly to this batch.
    # Do NOT fall back to city or location_key as that causes records from previous
    # batches to glitchily display while a new queue is extracting.
    records = RestaurantRecord.get_delivery_records(
        query={"batch_id": req.batch_id},
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
    Data access handled strictly per batch for clean isolation and robust downloading.
    """
    clean_id = str(request_id or "").strip()
    req = None
    try:
        req = ScrapeBatch.objects.get(batch_id=uuid.UUID(clean_id) if len(clean_id) == 36 else clean_id)
    except Exception:
        try:
            req = ScrapeBatch.objects.get(request_id=clean_id)
        except Exception:
            raise Http404("Request ID not found")

    custom_fields_param = request.GET.get("fields")
    if custom_fields_param:
        fields = [f.strip() for f in custom_fields_param.split(",") if f.strip()]
    elif req.fields_selected and len(req.fields_selected) > 0:
        fields = req.fields_selected
    else:
        fields = DEFAULT_FIELDS

    # STRICT BATCH ISOLATION: Only export records belonging to this specific batch
    records = RestaurantRecord.get_delivery_records(
        query={"batch_id": req.batch_id},
        fields=fields,
    )

    clean_city = re.sub(r"[^\w\-]", "_", str(req.city or "city")).strip("_")
    clean_loc = re.sub(r"[^\w\-]", "_", str(req.location or "all")).strip("_")
    short_id = str(req.batch_id)[:8]
    filename = f"zomato_{clean_city}_{clean_loc}_{short_id}.csv"

    response = HttpResponse(content_type="text/csv")
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    response["Access-Control-Expose-Headers"] = "Content-Disposition"

    # Prepend UTF-8 BOM so Microsoft Excel & spreadsheet tools render characters (like ₹) correctly
    response.write("\ufeff")

    writer = csv.writer(response)
    writer.writerow(fields)

    for doc in records:
        row = []
        for f in fields:
            val = doc.get(f)
            if isinstance(val, (list, dict)):
                if f == "reviews" and isinstance(val, list):
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


@require_GET
def get_completed_batches_api(request: HttpRequest) -> JsonResponse:
    """
    Returns recent completed and terminated batches with their record counts and download URLs.
    Used by frontend to display completed requests history and provide instant CSV download buttons.
    """
    limit = int(request.GET.get("limit", 25))
    batches = ScrapeBatch.objects.filter(
        models.Q(status="completed") |
        models.Q(status__in=["terminated", "failed"], total_restaurants__gt=0) |
        models.Q(progress=100, total_restaurants__gt=0)
    ).exclude(
        models.Q(status__in=["terminated", "failed"], total_restaurants=0)
    ).order_by(
        models.F("completed_at").desc(nulls_last=True),
        models.F("updated_at").desc(),
        models.F("created_at").desc()
    )[:limit]

    data = []
    for b in batches:
        rec_count = RestaurantRecord.objects.filter(requestid=b).count()
        if rec_count == 0:
            res_ids = list(RestaurantJob.objects.filter(batch=b, stage="completed").values_list("restaurant_id", flat=True))
            if res_ids:
                rec_count = RestaurantRecord.objects.filter(restaurant_id__in=res_ids).count()
        if rec_count == 0 and (b.status == "completed" or b.progress == 100):
            rec_count = b.completed_jobs or b.total_restaurants or b.scraped_details

        data.append({
            "batch_id": str(b.batch_id),
            "request_id": b.request_id,
            "city": b.city,
            "location": b.location,
            "status": b.status,
            "total_restaurants": b.total_restaurants,
            "completed_jobs": b.completed_jobs,
            "records_count": rec_count,
            "created_at": b.created_at.isoformat() if b.created_at else None,
            "completed_at": b.completed_at.isoformat() if b.completed_at else None,
            "elapsed_seconds": get_batch_elapsed_seconds(b),
            "csv_url": f"/api/export-csv/{b.batch_id}/",
        })

    return JsonResponse({
        "status": "success",
        "total": len(data),
        "batches": data,
    })


@csrf_exempt
@require_http_methods(["POST"])
def test_slack_alert_api(request: HttpRequest) -> JsonResponse:
    """Test endpoint to verify Slack webhook alert delivery."""
    from scraper_app.alerts import send_slack_message, is_slack_alert_enabled
    if not is_slack_alert_enabled():
        return JsonResponse({"status": "error", "message": "Slack alerts are disabled in configuration (SLACK_ALERT_ENABLED=false)"}, status=400)

    text = "⚡ *Zomato Scraper Test Alert*\nSlack webhook integration is operational and ready to receive extraction alerts."
    sent, info = send_slack_message(text=text, color="#2563eb")
    if sent:
        return JsonResponse({"status": "success", "message": "Slack alert sent successfully!"})
    return JsonResponse({"status": "error", "message": info}, status=500)


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

    # Mark remaining pending/in-flight RestaurantJob records as 'terminated' in PostgreSQL
    RestaurantJob.objects.filter(batch=req).exclude(stage="completed").update(stage="terminated")

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
    status_code = 200 if res.get("status") in ["success", "already_finished", "already_running"] else 400
    return JsonResponse(res, status=status_code)
