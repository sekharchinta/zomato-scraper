"""
Management command to start RQ workers for Zomato Scraper single queue:
- zomato (3 workers default, or configurable via --workers)

Delegates to scraper_app.workers.WorkerManager for process lifecycle,
dead worker unregistration, and automated failed jobs recovery.
"""

import sys
from typing import Any
from django.core.management.base import BaseCommand
from django.conf import settings
from config import SINGLE_QUEUE_NAME
from scraper_app.workers.worker import WorkerManager
from scraper_app.workers.worker_lock import format_already_running_banner


class Command(BaseCommand):
    help = "Run RQ workers on the single queue ('zomato') with 3 concurrent workers and automatic failed job recovery"

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--workers",
            "-w",
            type=int,
            default=6,
            help="Number of concurrent workers on the single queue (default: 3)",
        )
        parser.add_argument(
            "--burst",
            "-b",
            action="store_true",
            help="Run in burst mode (process all available jobs then exit)",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        num_workers = options["workers"]
        burst = options["burst"]
        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)

        manager = WorkerManager(
            workers=num_workers,
            queue_name=queue_name,
            burst=burst,
        )

        # Enforce strict single-instance execution
        acquired, running_info = manager.acquire_lock()
        if not acquired and running_info:
            banner = format_already_running_banner(running_info, queue_name=queue_name)
            self.stderr.write(banner)
            sys.exit(1)

        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(self.style.SUCCESS("        ZOMATO SCRAPER SINGLE-QUEUE RQ WORKERS"))
        self.stdout.write(self.style.SUCCESS("=" * 70))
        self.stdout.write(f"• Queue Name:        {queue_name} (SINGLE QUEUE)")
        self.stdout.write(self.style.SUCCESS(f"• Concurrent Workers: {num_workers} worker processes"))
        self.stdout.write(f"• Burst mode:        {'Enabled' if burst else 'Disabled (continuous polling)'}")
        self.stdout.write(self.style.SUCCESS("=" * 70))

        manager.cleanup_stale_workers()
        manager.spawn_workers()
        manager.run_supervisor_loop()
