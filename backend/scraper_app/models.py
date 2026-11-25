"""
Django PostgreSQL Models for Zomato Scraper Application.
Database: 'zomato-scraper' | Schema: 'zomato'
Zero SQLite, Zero MongoDB - All tracking, jobs, and restaurant data
are persisted into PostgreSQL tables in the 'zomato' schema.
"""

import re
import uuid
from typing import Any, Dict, List, Optional
from django.contrib.auth.models import User
from django.db import models
from django.utils import timezone


def generate_location_key(city: str, location: Any) -> str:
    """
    Creates a deterministic unique key for the requested location.
    Used to check if a location has already been scraped in PostgreSQL.
    Examples:
        - city='Hyderabad', location='Banjara Hills' -> 'loc_hyderabad_banjara_hills'
        - city='Bengaluru', location='ALL' -> 'loc_bengaluru_all'
        - city='Mumbai', location={'entity_id': 123, 'entity_title': 'Bandra'} -> 'loc_mumbai_bandra_123'
    """
    clean_city = re.sub(r"[^a-z0-9]+", "_", str(city or "").strip().lower()).strip("_")
    if isinstance(location, dict):
        entity_id = location.get("entity_id")
        title = location.get("entity_title") or location.get("entity_name") or ""
        clean_title = re.sub(r"[^a-z0-9]+", "_", str(title).strip().lower()).strip("_")
        if location.get("entity_type") == "all" or clean_title in ("all", "all_locations"):
            return f"loc_{clean_city}_all"
        if entity_id:
            return f"loc_{clean_city}_{clean_title}_{entity_id}"
        return f"loc_{clean_city}_{clean_title}" if clean_title else f"loc_{clean_city}"

    loc_str = str(location or "").strip()
    clean_loc = re.sub(r"[^a-z0-9]+", "_", loc_str.lower()).strip("_")
    if not clean_loc or clean_loc in ("all", "all_locations", f"all_locations_in_{clean_city}"):
        return f"loc_{clean_city}_all"

    return f"loc_{clean_city}_{clean_loc}"


class BatchQuerySet(models.QuerySet):
    """Custom QuerySet enabling transparent query aliases for ScrapeBatch."""

    def _remap_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        new_kwargs = {}
        for k, v in kwargs.items():
            if k == "batch_id" or k.startswith("batch_id__"):
                new_key = "requestid" + k[len("batch_id"):]
                new_kwargs[new_key] = v
            elif k == "request_id" or k.startswith("request_id__"):
                new_key = "requestid" + k[len("request_id"):]
                new_kwargs[new_key] = v
            elif k == "fields_selected" or k.startswith("fields_selected__"):
                new_key = "fields" + k[len("fields_selected"):]
                new_kwargs[new_key] = v
            else:
                new_kwargs[k] = v
        return new_kwargs

    def filter(self, *args, **kwargs):
        return super().filter(*args, **self._remap_kwargs(kwargs))

    def exclude(self, *args, **kwargs):
        return super().exclude(*args, **self._remap_kwargs(kwargs))

    def get(self, *args, **kwargs):
        return super().get(*args, **self._remap_kwargs(kwargs))

    def update_or_create(self, defaults=None, **kwargs):
        remapped_defaults = self._remap_kwargs(defaults) if defaults else {}
        remapped_kwargs = self._remap_kwargs(kwargs)
        return super().update_or_create(defaults=remapped_defaults, **remapped_kwargs)


class ScrapeBatch(models.Model):
    """
    Tracks lifecycle and progress of every location scraping batch in 'zomato.zomato_batches'.
    Identified by a unique UUID requestid as primary key.
    """
    STATUS_CHOICES = [
        ("queued", "Queued in RQ"),
        ("in_progress", "In Progress"),
        ("extracting_urls", "Extracting Restaurant URLs"),
        ("processing_restaurants", "Processing Restaurants & Reviews"),
        ("terminated", "Terminated"),
        ("completed", "Completed"),
        ("failed", "Failed"),
        ("cached", "Cached Result"),
    ]

    requestid = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False, db_index=True, db_column="requestid")
    user = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="batches",
        help_text="Authenticated user who launched this batch",
    )
    city = models.CharField(max_length=120)
    location = models.CharField(max_length=255)
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default="queued")
    progress = models.IntegerField(default=0, help_text="Progress percentage 0-100")
    total_restaurants = models.IntegerField(default=0, help_text="Total restaurant jobs in this batch")
    parsed_delivery = models.IntegerField(default=0, help_text="Delivery documents parsed")
    completed_jobs = models.IntegerField(default=0, help_text="Completed restaurant jobs")
    failed_jobs = models.IntegerField(default=0, help_text="Failed jobs after retries")
    fields = models.JSONField(default=list, blank=True, db_column="fields")
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True, help_text="Timestamp when batch execution actually started")
    completed_at = models.DateTimeField(null=True, blank=True, help_text="Timestamp when batch execution finished or terminated")

    # Coordinates of chosen location
    latitude = models.DecimalField(max_digits=12, decimal_places=8, null=True, blank=True, help_text="Latitude of chosen location")
    longitude = models.DecimalField(max_digits=12, decimal_places=8, null=True, blank=True, help_text="Longitude of chosen location")

    # Additional metadata fields
    error_message = models.TextField(blank=True, default="")
    is_cached = models.BooleanField(default=False)
    location_key = models.CharField(max_length=200, db_index=True, blank=True, default="")
    scraped_details = models.IntegerField(default=0, help_text="Details extracted")
    scraped_reviews = models.IntegerField(default=0, help_text="Reviews extracted")
    updated_at = models.DateTimeField(auto_now=True)

    objects = BatchQuerySet.as_manager()

    class Meta:
        db_table = "zomato_batches"
        ordering = ["-created_at"]
        verbose_name = "Batch"
        verbose_name_plural = "Batches"

    # Backward compatibility properties
    @property
    def batch_id(self):
        return self.requestid

    @batch_id.setter
    def batch_id(self, val):
        self.requestid = val

    @property
    def request_id(self):
        return str(self.requestid)

    @request_id.setter
    def request_id(self, val):
        self.requestid = val

    @property
    def fields_selected(self):
        return self.fields

    @fields_selected.setter
    def fields_selected(self, val):
        self.fields = val

    def __str__(self) -> str:
        cache_label = " [CACHED]" if self.is_cached else ""
        return f"{self.requestid} - {self.city} ({self.location}) - {self.status} ({self.progress}%){cache_label}"

    @classmethod
    def get_collection_stats(cls) -> Dict[str, int]:
        """
        Returns live statistics directly via PostgreSQL Django models.
        All database metrics handled by Django models only.
        """
        try:
            total_restaurants = RestaurantRecord.objects.count()
            total_batches = cls.objects.count()
            total_jobs = RestaurantJob.objects.count()
            reviews_sum = RestaurantRecord.objects.aggregate(total=models.Sum("total_reviews"))["total"] or 0
            return {
                "restaurants_urls": total_jobs,
                "restaurants_data": total_restaurants,
                "reviews": int(reviews_sum),
                "delivery_data": total_restaurants,
                "total_batches": total_batches,
                "total_jobs": total_jobs,
                "total_restaurants": total_restaurants,
            }
        except Exception:
            return {
                "restaurants_urls": 0,
                "restaurants_data": 0,
                "reviews": 0,
                "delivery_data": 0,
                "total_batches": 0,
                "total_jobs": 0,
                "total_restaurants": 0,
            }


# Backward compatibility alias for views and templates
ScrapeRequest = ScrapeBatch


class RestaurantJobQuerySet(models.QuerySet):
    """Custom QuerySet enabling transparent query aliases for RestaurantJob."""

    def _remap_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        new_kwargs = {}
        for k, v in kwargs.items():
            if k == "batch_id" or k.startswith("batch_id__"):
                new_key = "batch_id" + k[len("batch_id"):]
                new_kwargs[new_key] = v
            else:
                new_kwargs[k] = v
        return new_kwargs

    def filter(self, *args, **kwargs):
        return super().filter(*args, **self._remap_kwargs(kwargs))


class LatLongData(models.Model):
    """
    Stores location search results and lat/long coordinates.
    Populated whenever a user enters a location query (via location_extractor).
    All locations in a batch share the unified requestid (UUID) entered by the user.
    This exact requestid is used as the primary key for batch extraction (ScrapeBatch).
    """
    id = models.BigAutoField(primary_key=True)
    requestid = models.UUIDField(db_index=True, null=True, blank=True, default=uuid.uuid4, db_column="requestid")
    user = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="location_searches",
        help_text="User who initiated this location search",
    )
    search_query = models.CharField(max_length=255, db_index=True)
    city = models.CharField(max_length=120, db_index=True)
    entity_id = models.CharField(max_length=100, blank=True, default="")
    entity_type = models.CharField(max_length=50, blank=True, default="")
    entity_title = models.CharField(max_length=255)
    entity_name = models.CharField(max_length=255, blank=True, default="")
    latitude = models.DecimalField(max_digits=12, decimal_places=8, null=True, blank=True)
    longitude = models.DecimalField(max_digits=12, decimal_places=8, null=True, blank=True)
    place_id = models.CharField(max_length=100, blank=True, default="")
    cell_id = models.CharField(max_length=100, blank=True, default="")
    delivery_subzone_id = models.IntegerField(default=0)
    raw_data = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "latlong_data"
        ordering = ["-created_at"]
        verbose_name = "LatLong Data"
        verbose_name_plural = "LatLong Data"

    @property
    def request_id(self):
        return str(self.requestid) if self.requestid else None

    def __str__(self) -> str:
        return f"{self.entity_title} ({self.city}) [{self.latitude}, {self.longitude}]"


class RestaurantJob(models.Model):
    """
    Tracks the fine-grained state of an individual restaurant job in the single Redis queue.
    Maintains status, current stage (details vs reviews), and retry tracking.
    """
    STAGE_CHOICES = [
        ("pending", "Pending"),
        ("processing_details", "Processing Restaurant Details"),
        ("details_failed", "Details Failed"),
        ("details_done", "Details Extracted"),
        ("processing_reviews", "Processing Reviews"),
        ("reviews_failed", "Reviews Failed"),
        ("completed", "Completed"),
        ("failed_all_retries", "Failed All Retries"),
        ("terminated", "Terminated"),
    ]

    id = models.BigAutoField(primary_key=True)
    batch = models.ForeignKey(ScrapeBatch, on_delete=models.CASCADE, related_name="jobs", db_column="batch_id")
    rq_job_id = models.CharField(max_length=120, blank=True, default="", db_index=True)
    restaurant_id = models.BigIntegerField(null=True, blank=True, db_index=True)
    name = models.CharField(max_length=255, blank=True, default="")
    page_url = models.TextField()
    stage = models.CharField(max_length=50, choices=STAGE_CHOICES, default="pending", db_index=True)
    last_review_page = models.IntegerField(default=0, help_text="Last review page successfully scraped before failure")
    total_review_pages = models.IntegerField(default=0)
    retries_count = models.IntegerField(default=0)
    secondary_attempt_done = models.BooleanField(default=False, help_text="True if requeued from failed jobs")
    error_message = models.TextField(blank=True, default="")
    started_at = models.DateTimeField(null=True, blank=True, help_text="Timestamp when batch execution actually started")
    completed_at = models.DateTimeField(null=True, blank=True, help_text="Timestamp when batch execution finished or terminated")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = RestaurantJobQuerySet.as_manager()

    class Meta:
        db_table = "zomato_jobs"
        ordering = ["id"]
        verbose_name = "Input"
        verbose_name_plural = "Inputs"

    def __str__(self) -> str:
        return f"Job {self.id} (Res: {self.restaurant_id or self.name}) - Stage: {self.stage}"


class RestaurantRecordQuerySet(models.QuerySet):
    """Custom QuerySet enabling transparent query aliases for RestaurantRecord."""

    def _remap_kwargs(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        new_kwargs = {}
        for k, v in kwargs.items():
            if k == "batch_id" or k.startswith("batch_id__"):
                new_key = "requestid_id" + k[len("batch_id"):]
                new_kwargs[new_key] = v
            elif k == "batch" or k.startswith("batch__"):
                new_key = "requestid" + k[len("batch"):]
                new_kwargs[new_key] = v
            elif k == "page_url" or k.startswith("page_url__"):
                new_key = "pageurl" + k[len("page_url"):]
                new_kwargs[new_key] = v
            elif k == "phone_numbers" or k.startswith("phone_numbers__"):
                new_key = "phone_number" + k[len("phone_numbers"):]
                new_kwargs[new_key] = v
            elif k == "cost_for_two" or k.startswith("cost_for_two__"):
                new_key = "costof_two" + k[len("cost_for_two"):]
                new_kwargs[new_key] = v
            else:
                new_kwargs[k] = v
        return new_kwargs

    def filter(self, *args, **kwargs):
        return super().filter(*args, **self._remap_kwargs(kwargs))

    def exclude(self, *args, **kwargs):
        return super().exclude(*args, **self._remap_kwargs(kwargs))

    def get(self, *args, **kwargs):
        return super().get(*args, **self._remap_kwargs(kwargs))

    def update_or_create(self, defaults=None, **kwargs):
        remapped_defaults = self._remap_kwargs(defaults) if defaults else {}
        remapped_kwargs = self._remap_kwargs(kwargs)
        return super().update_or_create(defaults=remapped_defaults, **remapped_kwargs)


class RestaurantRecord(models.Model):
    """
    Combined PostgreSQL Data Collection Table in 'zomato.zomato_restaurants'.
    Combines restaurant metadata, details, customer reviews, and normalized delivery data.
    Linked to zomato_batches by foreign key requestid.
    """
    id = models.BigAutoField(primary_key=True)
    requestid = models.ForeignKey(
        ScrapeBatch,
        on_delete=models.CASCADE,
        db_column="requestid",
        to_field="requestid",
        related_name="restaurants",
        null=True,
        blank=True,
        db_index=True,
    )
    name = models.CharField(max_length=255, blank=True, default="")
    city = models.CharField(max_length=120, blank=True, default="")
    location = models.CharField(max_length=255, blank=True, default="")
    pageurl = models.TextField(db_column="pageurl", blank=True, default="")
    canonical_url = models.TextField(blank=True, default="")
    phone_number = models.TextField(db_column="phone_number", blank=True, default="")
    address = models.TextField(blank=True, default="")
    cuisines = models.TextField(blank=True, default="")
    timing = models.TextField(blank=True, default="")
    rating = models.CharField(max_length=50, blank=True, default="")
    votes = models.CharField(max_length=50, blank=True, default="")
    costof_two = models.CharField(max_length=100, db_column="costof_two", blank=True, default="")
    total_reviews = models.IntegerField(default=0)
    reviews = models.JSONField(default=list, blank=True, help_text="Consolidated customer reviews")
    status = models.CharField(max_length=50, default="completed")
    scraped_at = models.DateTimeField(default=timezone.now)

    # Additional structured metadata fields
    restaurant_id = models.BigIntegerField(null=True, blank=True, db_index=True)
    location_key = models.CharField(max_length=200, db_index=True, blank=True, default="")
    number_of_pages = models.IntegerField(default=0)
    pages_scraped = models.IntegerField(default=0)
    details_data = models.JSONField(default=dict, blank=True, help_text="Normalized sections")
    raw_data = models.JSONField(default=dict, blank=True, help_text="Combined raw API payloads")
    delivery_document = models.JSONField(default=dict, blank=True, help_text="Parsed 32 standard delivery fields")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    objects = RestaurantRecordQuerySet.as_manager()

    class Meta:
        db_table = "zomato_restaurants"
        ordering = ["-updated_at"]
        verbose_name = "Extracted Job"
        verbose_name_plural = "Jobs"
        constraints = [
            models.UniqueConstraint(
                fields=["requestid", "pageurl"],
                name="unique_batch_restaurant_pageurl",
            )
        ]

    # Backward compatibility properties
    @property
    def batch(self):
        return self.requestid

    @batch.setter
    def batch(self, val):
        self.requestid = val

    @property
    def batch_id(self):
        return self.requestid_id

    @batch_id.setter
    def batch_id(self, val):
        self.requestid_id = val

    @property
    def page_url(self):
        return self.pageurl

    @page_url.setter
    def page_url(self, val):
        self.pageurl = val

    @property
    def phone_numbers(self):
        return self.phone_number

    @phone_numbers.setter
    def phone_numbers(self, val):
        self.phone_number = val

    @property
    def cost_for_two(self):
        return self.costof_two

    @cost_for_two.setter
    def cost_for_two(self, val):
        self.costof_two = val

    def __str__(self) -> str:
        return f"{self.id} - {self.name} ({self.city}, {self.location}) [{self.total_reviews} reviews]"

    def to_dict(self) -> Dict[str, Any]:
        """Helper converting this model instance into a standard dictionary."""
        return {
            "id": self.id,
            "requestid": str(self.requestid.requestid) if self.requestid else None,
            "restaurant_id": self.restaurant_id or self.id,
            "_id": self.restaurant_id or self.id,
            "name": self.name,
            "city": self.city,
            "location": self.location,
            "location_key": self.location_key,
            "pageurl": self.pageurl,
            "page_url": self.pageurl,
            "canonical_url": self.canonical_url,
            "address": self.address,
            "phone_number": self.phone_number,
            "phone_numbers": self.phone_number,
            "timing": self.timing,
            "cuisines": self.cuisines,
            "rating": self.rating,
            "votes": self.votes,
            "costof_two": self.costof_two,
            "cost_for_two": self.costof_two,
            "total_reviews": self.total_reviews,
            "reviews": self.reviews,
            "details_data": self.details_data,
            "status": self.status,
            "scraped_at": self.scraped_at.isoformat() if self.scraped_at else None,
        }

    @classmethod
    def get_delivery_records(
        cls,
        query: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        location_key: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves records directly through the Django RestaurantRecord model.
        Projects specified fields if passed.
        Database handled by models only.
        """
        try:
            qs = cls.objects.all()
            if location_key:
                qs = qs.filter(location_key=location_key)
            elif query:
                if "location_key" in query:
                    qs = qs.filter(location_key=query["location_key"])
                if "batch_id" in query or "requestid" in query or "request_id" in query:
                    target_b = query.get("batch_id") or query.get("requestid") or query.get("request_id")
                    qs_direct = qs.filter(requestid_id=target_b)
                    if qs_direct.exists():
                        qs = qs_direct
                    else:
                        from scraper_app.models import RestaurantJob
                        job_res_ids = list(RestaurantJob.objects.filter(
                            batch_id=target_b
                        ).values_list("restaurant_id", flat=True))
                        if job_res_ids:
                            qs = qs.filter(restaurant_id__in=job_res_ids)
                        else:
                            qs = qs_direct
                if "city" in query:
                    city_val = query["city"]
                    if isinstance(city_val, dict) and "$regex" in city_val:
                        clean_city = re.sub(r"[\^\\$]", "", city_val["$regex"])
                        qs = qs.filter(city__iexact=clean_city)
                    elif isinstance(city_val, str):
                        qs = qs.filter(city__iexact=city_val)
                if "restaurant_id" in query:
                    qs = qs.filter(restaurant_id=query["restaurant_id"])

            if skip > 0:
                qs = qs[skip:]
            if limit and limit > 0:
                qs = qs[:limit]

            results = []
            for rec in qs:
                doc = dict(rec.delivery_document) if rec.delivery_document else rec.to_dict()
                doc["id"] = rec.id
                doc["requestid"] = str(rec.requestid_id) if rec.requestid_id else ""
                doc["restaurant_id"] = rec.restaurant_id or rec.id
                doc["_id"] = rec.restaurant_id or rec.id
                doc["name"] = rec.name or doc.get("name", "")
                doc["city"] = rec.city or doc.get("city", "")
                doc["location"] = rec.location or doc.get("location", "")
                doc["location_key"] = rec.location_key or doc.get("location_key", "")
                doc["total_reviews"] = rec.total_reviews if rec.total_reviews is not None else doc.get("total_reviews", 0)
                doc["reviews"] = rec.reviews if rec.reviews else doc.get("reviews", [])

                # Fallback to model fields if empty in delivery_document
                if not doc.get("cuisines") and rec.cuisines:
                    doc["cuisines"] = rec.cuisines
                if not doc.get("average_cost_for_two") and rec.costof_two:
                    doc["average_cost_for_two"] = rec.costof_two
                doc["cost_for_two"] = doc.get("average_cost_for_two") or rec.costof_two
                doc["costof_two"] = rec.costof_two
                if not doc.get("address") and rec.address:
                    doc["address"] = rec.address
                if not doc.get("phone_numbers") and rec.phone_number:
                    doc["phone_numbers"] = rec.phone_number
                doc["phone_number"] = rec.phone_number
                if not doc.get("timing") and rec.timing:
                    doc["timing"] = rec.timing
                if not doc.get("rating") and rec.rating:
                    doc["rating"] = rec.rating
                if not doc.get("votes") and rec.votes:
                    doc["votes"] = rec.votes
                if not doc.get("canonical_url") and rec.canonical_url:
                    doc["canonical_url"] = rec.canonical_url
                if not doc.get("page_url") and rec.pageurl:
                    doc["page_url"] = rec.pageurl
                doc["pageurl"] = rec.pageurl

                if fields and len(fields) > 0:
                    filtered = {f: doc.get(f, "") for f in fields}
                    filtered["requestid"] = doc["requestid"]
                    filtered["restaurant_id"] = rec.restaurant_id or rec.id
                    filtered["_id"] = rec.restaurant_id or rec.id
                    results.append(filtered)
                else:
                    results.append(doc)

            return results
        except Exception:
            return []


class UserProfile(models.Model):
    """
    Extends Django core User with Google OAuth 2.0 profile metadata.
    Stored in PostgreSQL schema 'zomato' table 'zomato_user_profiles'.
    """
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name="profile")
    google_id = models.CharField(max_length=255, unique=True, db_index=True, help_text="Google unique subject ID (sub)")
    avatar_url = models.URLField(max_length=1000, blank=True, default="", help_text="User Google avatar picture URL")
    email_verified = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "zomato_user_profiles"
        ordering = ["-created_at"]
        verbose_name = "User Profile"
        verbose_name_plural = "User Profiles"

    def __str__(self) -> str:
        return f"{self.user.email} ({self.google_id})"

