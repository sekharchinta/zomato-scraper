"""
Management command to process failed jobs across the single queue:
- zomato

Implements the retry policy:
- Requeues failed jobs back into the same queue for a secondary 3-retry attempt.
- Resumes at failure point (restaurant details vs reviews).
- If all attempts are exhausted, persists partial fetched data to PostgreSQL and clears the job.
"""

from typing import Any
from django.core.management.base import BaseCommand
from scraper_app.workers.failed_jobs_handler import FailedJobsHandler


class Command(BaseCommand):
    help = "Process failed RQ jobs: requeue into same queue for secondary 3-retry attempt or save partial data to PostgreSQL"

    def handle(self, *args: Any, **options: Any) -> None:
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("      ZOMATO SCRAPER FAILED JOBS RECOVERY CONTROLLER"))
        self.stdout.write(self.style.SUCCESS("=" * 70))

        handler = FailedJobsHandler()
        counts_before = handler.get_failed_counts()
        self.stdout.write(f"Failed jobs before processing: {counts_before}")

        result = handler.process_failed_jobs()

        self.stdout.write(f"• Requeued for secondary 3 retries: {result['requeued_count']}")
        self.stdout.write(f"• Final fallbacks safely persisted in DB: {result['fallback_saved_count']}")

        counts_after = handler.get_failed_counts()
        self.stdout.write(f"Failed jobs remaining: {counts_after}")
        self.stdout.write(self.style.SUCCESS("=" * 70))
