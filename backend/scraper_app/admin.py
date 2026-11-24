"""
Django Admin Configuration for Zomato Scraper Application.
Provides clean, standard, 100% plain-text display without colors or decorative markup:
1. Batches (ScrapeBatch) - shows completed jobs / total jobs in plain text, record count, and duration
2. Extracted Data (RestaurantRecord) - plain text restaurant attributes, ratings, and reviews
3. Pipeline Jobs (RestaurantJob) - individual restaurant jobs, stages, and URLs
4. Location Coordinates (LatLongData) - location queries and coordinate pairs
"""

import json
from django.contrib import admin

from scraper_app.models import LatLongData, RestaurantJob, RestaurantRecord, ScrapeBatch
from scraper_app.views import get_batch_elapsed_seconds


# Custom Admin Headers
admin.site.site_header = "Zomato Scraper Operations"
admin.site.site_title = "Zomato Scraper Admin"
admin.site.index_title = "Data Extraction, Batches & Job Monitoring"


# ==============================================================================
# 0. LATLONG DATA ADMIN (LatLongData)
# ==============================================================================
@admin.register(LatLongData)
class LatLongDataAdmin(admin.ModelAdmin):
    list_display = (
        "batch_id",
        "search_query",
        "city",
        "entity_title",
        "latitude",
        "longitude",
        "created_at",
    )
    list_filter = ("city", "entity_type", "created_at")
    search_fields = ("requestid", "search_query", "city", "entity_title", "entity_name")
    readonly_fields = ("requestid", "created_at", "updated_at")
    ordering = ("-created_at",)
    list_per_page = 50

    def batch_id(self, obj: LatLongData) -> str:
        return str(obj.requestid)[:8] if obj.requestid else "-"
    batch_id.short_description = "Batch ID"
    batch_id.admin_order_field = "requestid"


# ==============================================================================
# 1. BATCHES ADMIN (ScrapeBatch)
# ==============================================================================
@admin.register(ScrapeBatch)
class ScrapeBatchAdmin(admin.ModelAdmin):
    list_display = (
        "batch_id_short",
        "location_text",
        "status_text",
        "jobs_progress",
        "saved_records",
        "duration",
        "created_at_text",
        "completed_at_text",
    )
    list_filter = ("status", "city", "created_at")
    search_fields = ("requestid", "city", "location", "location_key")
    readonly_fields = (
        "requestid",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
        "jobs_progress",
    )
    ordering = ("-created_at",)
    list_per_page = 25

    fieldsets = (
        ("Batch Identification", {
            "fields": ("requestid", "city", "location", "status")
        }),
        ("Jobs & Extraction Progress", {
            "fields": ("jobs_progress", "progress", "total_restaurants", "completed_jobs", "failed_jobs", "scraped_details", "scraped_reviews")
        }),
        ("Coordinates & Spatial Data", {
            "classes": ("collapse",),
            "fields": ("latitude", "longitude", "location_key", "fields")
        }),
        ("Execution Timestamps", {
            "fields": ("created_at", "started_at", "completed_at", "updated_at", "error_message")
        }),
    )

    def batch_id_short(self, obj: ScrapeBatch) -> str:
        return str(obj.requestid)[:8]
    batch_id_short.short_description = "Batch ID"
    batch_id_short.admin_order_field = "requestid"

    def location_text(self, obj: ScrapeBatch) -> str:
        city = (obj.city or "").title()
        loc = obj.location or "All"
        if loc and loc.lower() != city.lower():
            return f"{city} ({loc})"
        return city
    location_text.short_description = "City & Location"
    location_text.admin_order_field = "city"

    def status_text(self, obj: ScrapeBatch) -> str:
        return str(obj.status or "queued").replace("_", " ").title()
    status_text.short_description = "Status"
    status_text.admin_order_field = "status"

    def jobs_progress(self, obj: ScrapeBatch) -> str:
        total = obj.total_restaurants or 0
        done = obj.completed_jobs or 0
        pct = min(100, int((done / total * 100))) if total > 0 else (100 if obj.status == "completed" else 0)
        return f"{done} / {total} ({pct}%)"
    jobs_progress.short_description = "Completed / Total Jobs"
    jobs_progress.admin_order_field = "completed_jobs"

    def saved_records(self, obj: ScrapeBatch) -> int:
        count = RestaurantRecord.objects.filter(requestid=obj).count()
        if count == 0 and (obj.status == "completed" or obj.progress == 100):
            return obj.completed_jobs or 0
        return count
    saved_records.short_description = "Saved Records"

    def duration(self, obj: ScrapeBatch) -> str:
        secs = get_batch_elapsed_seconds(obj)
        if secs <= 0:
            return "-"
        m, s = divmod(secs, 60)
        h, m = divmod(m, 60)
        if h > 0:
            return f"{h}h {m}m {s}s"
        if m > 0:
            return f"{m}m {s}s"
        return f"{s}s"
    duration.short_description = "Duration"

    def created_at_text(self, obj: ScrapeBatch) -> str:
        if not obj.created_at:
            return "-"
        return obj.created_at.strftime("%b %d, %H:%M")
    created_at_text.short_description = "Created"
    created_at_text.admin_order_field = "created_at"

    def completed_at_text(self, obj: ScrapeBatch) -> str:
        if not obj.completed_at:
            return "-"
        return obj.completed_at.strftime("%b %d, %H:%M")
    completed_at_text.short_description = "Completed"
    completed_at_text.admin_order_field = "completed_at"


# ==============================================================================
# 2. JOBS - EXTRACTED DATA ADMIN (RestaurantRecord)
# ==============================================================================
@admin.register(RestaurantRecord)
class RestaurantRecordAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "city",
        "location",
        "rating_display",
        "cost_display",
        "phone_number",
        "total_reviews",
        "status",
        "scraped_at",
    )
    list_filter = ("status", "city", "rating", "scraped_at")
    search_fields = (
        "requestid__requestid",
        "name",
        "city",
        "location",
        "phone_number",
        "canonical_url",
        "pageurl",
        "address",
        "restaurant_id",
    )
    readonly_fields = (
        "id",
        "requestid",
        "scraped_at",
        "created_at",
        "updated_at",
        "reviews_display",
        "details_data_display",
        "delivery_document_display",
    )
    ordering = ("-scraped_at",)
    list_per_page = 50

    fieldsets = (
        ("Request & Batch Information", {
            "fields": ("requestid", "status", "scraped_at", "location_key", "created_at", "updated_at")
        }),
        ("Restaurant Details", {
            "fields": ("name", "restaurant_id", "city", "location", "address", "phone_number")
        }),
        ("Web URLs", {
            "fields": ("pageurl", "canonical_url")
        }),
        ("Dining & Operations", {
            "fields": ("cuisines", "timing", "rating", "votes", "costof_two")
        }),
        ("Customer Reviews", {
            "fields": ("total_reviews", "number_of_pages", "pages_scraped", "reviews_display")
        }),
        ("Normalized Delivery & Raw Payloads", {
            "classes": ("collapse",),
            "fields": ("delivery_document_display", "details_data_display")
        }),
    )

    def batch_id(self, obj: RestaurantRecord) -> str:
        return str(obj.requestid_id)[:8] if obj.requestid_id else "-"
    batch_id.short_description = "Batch ID"
    batch_id.admin_order_field = "requestid__requestid"

    def rating_display(self, obj: RestaurantRecord) -> str:
        r = obj.rating or "-"
        votes = obj.votes or "0"
        if r and r != "-":
            return f"{r} ({votes} votes)"
        return "-"
    rating_display.short_description = "Rating"
    rating_display.admin_order_field = "rating"

    def cost_display(self, obj: RestaurantRecord) -> str:
        cost = str(obj.costof_two or "").strip()
        if not cost:
            return "-"
        if not cost.startswith("₹"):
            return f"₹{cost}"
        return cost
    cost_display.short_description = "Cost for Two"

    def reviews_display(self, obj: RestaurantRecord) -> str:
        if not obj.reviews:
            return "No reviews recorded."
        lines = []
        for r in obj.reviews:
            user = r.get("user_name", "Anonymous")
            rating = r.get("rating", "-")
            text = r.get("review_text", "")
            lines.append(f"[{rating}] {user}: {text}")
        return "\n\n".join(lines)
    reviews_display.short_description = "Customer Reviews"

    def delivery_document_display(self, obj: RestaurantRecord) -> str:
        if not obj.delivery_document:
            return "No delivery document recorded."
        return json.dumps(obj.delivery_document, indent=2, ensure_ascii=False)
    delivery_document_display.short_description = "Parsed Delivery Document"

    def details_data_display(self, obj: RestaurantRecord) -> str:
        if not obj.details_data:
            return "No details recorded."
        return json.dumps(obj.details_data, indent=2, ensure_ascii=False)
    details_data_display.short_description = "Raw Details Data"


# ==============================================================================
# 3. INPUTS ADMIN (RestaurantJob: Input URL, Restaurant Name, Stage, and Retries)
# ==============================================================================
@admin.register(RestaurantJob)
class RestaurantJobAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "batch__requestid",
        "stage_display",
        "retries_display",
        "restaurant_id",
        "created_at_text",
    )
    list_filter = ("stage", "secondary_attempt_done", "created_at")
    search_fields = ("name", "page_url", "batch__requestid", "restaurant_id")
    readonly_fields = ("name", "page_url", "batch", "restaurant_id", "stage", "created_at", "updated_at")
    ordering = ("-id",)
    list_per_page = 40

    def batch_id(self, obj: RestaurantJob) -> str:
        return str(obj.batch_id)[:8] if obj.batch_id else "-"
    batch_id.short_description = "Batch ID"
    batch_id.admin_order_field = "batch__requestid"

    def stage_display(self, obj: RestaurantJob) -> str:
        return str(obj.stage or "pending").replace("_", " ").title()
    stage_display.short_description = "Stage"
    stage_display.admin_order_field = "stage"

    def retries_display(self, obj: RestaurantJob) -> str:
        count = obj.retries_count or 0
        if obj.secondary_attempt_done:
            return f"{count} (Requeued)"
        return str(count)
    retries_display.short_description = "Retries"
    retries_display.admin_order_field = "retries_count"

    def created_at_text(self, obj: RestaurantJob) -> str:
        if not obj.created_at:
            return "-"
        return obj.created_at.strftime("%b %d, %H:%M")
    created_at_text.short_description = "Created"
    created_at_text.admin_order_field = "created_at"
