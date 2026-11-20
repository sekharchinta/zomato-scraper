"""
Django Management Command: reparse_restaurant_data
Parses all raw restaurant details and reviews stored in PostgreSQL 'zomato_restaurants'
and populates the complete 28+ standardized delivery_document fields for user queries and CSV export.
"""

import logging
from django.core.management.base import BaseCommand
from scraper_app.models import RestaurantRecord
from scraper_app.parser import build_delivery_document

logger = logging.getLogger("ReparseData")


class Command(BaseCommand):
    help = "Reparses raw JSON data from PostgreSQL into complete normalized delivery documents"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-id",
            type=str,
            default=None,
            help="Optional batch_id to only reparse restaurants for a specific batch",
        )

    def handle(self, *args, **options):
        batch_id = options.get("batch_id")
        qs = RestaurantRecord.objects.all()
        if batch_id:
            qs = qs.filter(batch_id=batch_id)

        total_count = qs.count()
        self.stdout.write(self.style.NOTICE(f"Starting reparsing for {total_count} restaurant record(s)..."))

        updated_count = 0
        skipped_count = 0

        for rec in qs.iterator(chunk_size=100):
            raw_details = None
            raw_reviews = None

            if rec.raw_data and isinstance(rec.raw_data, dict):
                raw_details = rec.raw_data.get("details")
                raw_reviews = rec.raw_data.get("reviews")

            if not raw_details and rec.details_data:
                raw_details = {"page_data": {"sections": rec.details_data}}

            if not raw_details:
                skipped_count += 1
                continue

            doc = build_delivery_document(
                detail_data=raw_details,
                reviews_data=raw_reviews if raw_reviews else rec.reviews,
                restaurant_id=rec.restaurant_id,
                city=rec.city,
                location=rec.location,
                location_key=rec.location_key,
            )

            if not doc:
                skipped_count += 1
                continue

            # Update model fields with newly parsed values
            rec.delivery_document = doc
            if doc.get("average_cost_for_two"):
                rec.cost_for_two = str(doc["average_cost_for_two"])
            if doc.get("cuisines"):
                rec.cuisines = doc["cuisines"]
            if doc.get("address"):
                rec.address = doc["address"]
            if doc.get("phone_numbers"):
                rec.phone_numbers = doc["phone_numbers"]
            if doc.get("timing"):
                rec.timing = doc["timing"]
            if doc.get("rating"):
                rec.rating = doc["rating"]
            if doc.get("votes"):
                rec.votes = str(doc["votes"])
            if doc.get("canonical_url"):
                rec.canonical_url = doc["canonical_url"]

            parsed_reviews = doc.get("reviews", [])
            if parsed_reviews and len(parsed_reviews) > len(rec.reviews or []):
                rec.reviews = parsed_reviews
                rec.total_reviews = len(parsed_reviews)

            rec.save(update_fields=[
                "delivery_document",
                "cost_for_two",
                "cuisines",
                "address",
                "phone_numbers",
                "timing",
                "rating",
                "votes",
                "canonical_url",
                "reviews",
                "total_reviews",
                "updated_at",
            ])
            updated_count += 1

            if updated_count % 50 == 0:
                self.stdout.write(f"  Processed {updated_count}/{total_count} records...")

        self.stdout.write(self.style.SUCCESS(
            f"✓ Successfully reparsed {updated_count}/{total_count} restaurants (skipped {skipped_count})."
        ))
