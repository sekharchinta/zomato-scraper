"""
Django Management Command: reparse_restaurant_data
Parses all raw restaurant details and reviews stored in PostgreSQL 'zomato_restaurants'
and populates the complete 28+ standardized delivery_document fields for user queries and CSV export.
"""

import concurrent.futures
import logging
from django.core.management.base import BaseCommand
from django.db import connection
from scraper_app.models import RestaurantRecord
from scraper_app.parsers.parser import build_delivery_document

logger = logging.getLogger("ReparseData")


def _reparse_single_record(record_id: int) -> bool:
    """Worker task to parse raw JSON data for a single RestaurantRecord."""
    connection.close_if_unusable_or_obsolete()
    try:
        rec = RestaurantRecord.objects.filter(id=record_id).first()
        if not rec:
            return False

        raw_details = None
        raw_reviews = None

        if rec.raw_data and isinstance(rec.raw_data, dict):
            raw_details = rec.raw_data.get("restaurant_details") or rec.raw_data.get("details")
            raw_reviews = rec.raw_data.get("reviews")

        if not raw_details and rec.details_data:
            raw_details = {"page_data": {"sections": rec.details_data}}

        if not raw_details:
            return False

        doc = build_delivery_document(
            detail_data=raw_details,
            reviews_data=raw_reviews if raw_reviews else rec.reviews,
            restaurant_id=rec.restaurant_id,
            city=rec.city,
            location=rec.location,
            location_key=rec.location_key,
        )

        if not doc:
            return False

        rec.delivery_document = doc
        if doc.get("name") and doc.get("name").strip().lower() != "restaurant":
            rec.name = doc["name"]
        if doc.get("average_cost_for_two"):
            rec.costof_two = str(doc["average_cost_for_two"])
        if doc.get("cuisines"):
            rec.cuisines = doc["cuisines"]
        if doc.get("address"):
            rec.address = doc["address"]
        if doc.get("phone_numbers"):
            rec.phone_number = doc["phone_numbers"]
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
            "name",
            "delivery_document",
            "costof_two",
            "cuisines",
            "address",
            "phone_number",
            "timing",
            "rating",
            "votes",
            "canonical_url",
            "reviews",
            "total_reviews",
            "updated_at",
        ])
        return True
    except Exception as exc:
        logger.warning("Error reparsing record %s: %s", record_id, exc)
        return False


class Command(BaseCommand):
    help = "Reparses raw JSON data from PostgreSQL into complete normalized delivery documents using concurrent workers"

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch-id",
            type=str,
            default=None,
            help="Optional batch_id to only reparse restaurants for a specific batch",
        )
        parser.add_argument(
            "--workers",
            "-w",
            type=int,
            default=6,
            help="Number of concurrent workers for parsing raw data (default: 6)",
        )

    def handle(self, *args, **options):
        batch_id = options.get("batch_id")
        num_workers = options.get("workers") or 6
        qs = RestaurantRecord.objects.all()
        if batch_id:
            qs = qs.filter(batch_id=batch_id)

        record_ids = list(qs.values_list("id", flat=True))
        total_count = len(record_ids)
        self.stdout.write(self.style.NOTICE(
            f"Starting raw data parsing for {total_count} restaurant record(s) across {num_workers} concurrent workers..."
        ))

        updated_count = 0
        skipped_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_id = {executor.submit(_reparse_single_record, rid): rid for rid in record_ids}
            for idx, future in enumerate(concurrent.futures.as_completed(future_to_id), 1):
                success = future.result()
                if success:
                    updated_count += 1
                else:
                    skipped_count += 1

                if idx % 50 == 0 or idx == total_count:
                    self.stdout.write(f"  Processed {idx}/{total_count} records (parsed: {updated_count}, skipped: {skipped_count})...")

        self.stdout.write(self.style.SUCCESS(
            f"✓ Successfully reparsed {updated_count}/{total_count} restaurants across {num_workers} workers (skipped {skipped_count})."
        ))
