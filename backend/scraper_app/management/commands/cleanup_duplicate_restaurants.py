"""
Management command to deduplicate 'zomato_restaurants' and 'zomato_jobs'.
Removes redundant duplicate rows so UniqueConstraint can be cleanly applied.
"""

from django.core.management.base import BaseCommand
from django.db import connection, transaction
from scraper_app.models import RestaurantRecord, RestaurantJob


class Command(BaseCommand):
    help = "Finds and deletes duplicate restaurant records and jobs in PostgreSQL"

    def handle(self, *args, **options):
        self.stdout.write("Starting deduplication of zomato_restaurants...")

        with connection.cursor() as cursor:
            # 1. Deduplicate zomato_restaurants
            cursor.execute("""
                SELECT requestid, pageurl, array_agg(id ORDER BY updated_at DESC, id DESC)
                FROM zomato.zomato_restaurants
                GROUP BY requestid, pageurl
                HAVING count(*) > 1;
            """)
            dup_records = cursor.fetchall()

        total_records_deleted = 0
        with transaction.atomic():
            for row in dup_records:
                batch_uuid, pageurl, ids = row
                primary_id = ids[0]
                redundant_ids = ids[1:]
                deleted_count, _ = RestaurantRecord.objects.filter(id__in=redundant_ids).delete()
                total_records_deleted += deleted_count

        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully cleaned up {total_records_deleted} redundant duplicate rows from zomato_restaurants."
            )
        )

        # 2. Deduplicate zomato_jobs
        self.stdout.write("Scanning for duplicate jobs in zomato_jobs...")
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT batch_id, page_url, array_agg(id ORDER BY (stage = 'completed') DESC, updated_at DESC, id DESC)
                FROM zomato.zomato_jobs
                GROUP BY batch_id, page_url
                HAVING count(*) > 1;
            """)
            dup_jobs = cursor.fetchall()

        total_jobs_deleted = 0
        with transaction.atomic():
            for row in dup_jobs:
                batch_uuid, page_url, ids = row
                primary_id = ids[0]
                redundant_ids = ids[1:]
                # Only delete redundant jobs that are not completed or if primary is already completed
                deleted_count, _ = RestaurantJob.objects.filter(id__in=redundant_ids).delete()
                total_jobs_deleted += deleted_count

        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully cleaned up {total_jobs_deleted} redundant duplicate jobs from zomato_jobs."
            )
        )
