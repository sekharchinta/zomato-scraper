"""
Django Admin Configuration for Zomato Scraper Application.
Provides monitoring, search, and filtering with clean, standard plain-text display:
1. Batches (ScrapeBatch)
2. Jobs (Extracted Data) (RestaurantRecord)
3. Inputs (URLs & Names) (RestaurantJob)
"""

import json
from django.contrib import admin

from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch


# ==============================================================================
# 1. BATCHES ADMIN (ScrapeBatch)
# ==============================================================================
@admin.register(ScrapeBatch)
class ScrapeBatchAdmin(admin.ModelAdmin):
    list_display = (
        "requestid",
        "city",
        "location",
        "status",
        "progress",
        "total_restaurants",
        "completed_jobs",
        "failed_jobs",
        "parsed_delivery",
        "created_at",
        "started_at",
        "completed_at",
    )
    list_filter = ("status", "city", "created_at")
    search_fields = ("requestid", "city", "location", "location_key")
    readonly_fields = (
        "requestid",
        "created_at",
        "updated_at",
        "started_at",
        "completed_at",
    )
    ordering = ("-created_at",)
    list_per_page = 25


# ==============================================================================
# 2. JOBS - EXTRACTED DATA ADMIN (RestaurantRecord)
# ==============================================================================
@admin.register(RestaurantRecord)
class RestaurantRecordAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "request_id",
        "city",
        "location",
        "rating",
        "votes",
        "costof_two",
        "phone_number",
        "total_reviews",
        "status",
        "scraped_at",
    )
    list_filter = ("status", "city", "rating", "scraped_at")
    # Searching by request UUID returns all records for that request
    search_fields = (
        "requestid__requestid",
        "name",
        "city",
        "location",
        "phone_number",
        "canonical_url",
        "pageurl",
        "address",
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
            "fields": ("name", "city", "location", "address", "phone_number")
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

    def request_id(self, obj: RestaurantRecord) -> str:
        return str(obj.requestid_id or "-")
    request_id.short_description = "Request ID"
    request_id.admin_order_field = "requestid__requestid"

    def reviews_display(self, obj: RestaurantRecord) -> str:
        if not obj.reviews:
            return "No reviews recorded."
        lines = []
        for r in obj.reviews:
            user = r.get("user_name", "Anonymous")
            rating = r.get("rating", "-")
            text = r.get("review_text", "")
            likes = r.get("like_count", 0)
            lines.append(f"[{rating}] {user} ({likes} likes): {text}")
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
# 3. INPUTS ADMIN (RestaurantJob: ONLY Input URL and Restaurant Name)
# ==============================================================================
@admin.register(RestaurantJob)
class RestaurantJobAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "page_url",
        "request_id",
        "stage",
        "restaurant_id",
        "created_at",
    )
    list_filter = ("stage", "created_at")
    search_fields = ("name", "page_url", "batch__requestid", "restaurant_id")
    readonly_fields = ("name", "page_url", "batch", "restaurant_id", "stage", "created_at", "updated_at")
    ordering = ("-id",)
    list_per_page = 30

    def request_id(self, obj: RestaurantJob) -> str:
        if not obj.batch_id:
            return "-"
        return str(obj.batch_id)
    request_id.short_description = "Request ID"
    request_id.admin_order_field = "batch__requestid"
