"""
Management command to start RQ Multi-User Worker Supervisor for Zomato Scraper:
- Automatically spawns dedicated isolated UserWorkerPool processes for every active user queue ('user_<id>')
- Maintains a standby fallback pool on 'zomato' for anonymous/legacy tasks
- Automatically shuts down user worker pools and cleans Redis queues upon completion

Delegates to scraper_app.workers.MultiUserWorkerSupervisor / WorkerManager.
"""

import sys
from typing import Any
from django.core.management.base import BaseCommand
from django.conf import settings
from config import SINGLE_QUEUE_NAME
from scraper_app.workers.worker import WorkerManager
from scraper_app.workers.worker_lock import format_already_running_banner


class Command(BaseCommand):
    help = "Run isolated per-user RQ worker pools with dynamic spawning, per-user URL extraction & auto-cleanup"

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--workers",
            "-w",
            type=int,
            default=6,
            help="Base worker count / fallback concurrency setting (default: 6)",
        )
        parser.add_argument(
            "--workers-per-user",
            type=int,
            default=3,
            help="Number of dedicated worker processes spawned per active user queue (default: 3)",
        )
        parser.add_argument(
            "--max-total-workers",
            type=int,
            default=24,
            help="Global safety ceiling for concurrent OS worker processes across all users (default: 24)",
        )
        parser.add_argument(
            "--queue",
            "-q",
            type=str,
            default=None,
            help="Optional specific queue name (e.g. 'user_110') to run an isolated standalone worker pool",
        )
        parser.add_argument(
            "--burst",
            "-b",
            action="store_true",
            help="Run in burst mode (process all available jobs then exit)",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        num_workers = options["workers"]
        workers_per_user = options["workers_per_user"]
        max_total_workers = options["max_total_workers"]
        explicit_queue = options["queue"]
        burst = options["burst"]
        default_queue = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        queue_name = explicit_queue or default_queue

        manager = WorkerManager(
            workers=num_workers,
            queue_name=queue_name,
            burst=burst,
            workers_per_user=workers_per_user,
            max_total_workers=max_total_workers,
            single_queue_only=bool(explicit_queue),
        )

        # Enforce strict single-instance execution
        acquired, running_info = manager.acquire_lock()
        if not acquired and running_info:
            banner = format_already_running_banner(running_info, queue_name=queue_name)
            self.stderr.write(banner)
            sys.exit(1)

        self.stdout.write(self.style.SUCCESS("=" * 72))
        self.stdout.write(self.style.SUCCESS("     ZOMATO SCRAPER ISOLATED PER-USER WORKER SUPERVISOR"))
        self.stdout.write(self.style.SUCCESS("=" * 72))
        if explicit_queue:
            self.stdout.write(f"• Mode:               Dedicated Single-Queue Pool ('{queue_name}')")
        else:
            self.stdout.write(f"• Mode:               Dynamic Per-User Isolated Pools + Fallback ('{queue_name}')")
            self.stdout.write(self.style.SUCCESS(f"• Workers per User:   {manager.workers_per_user} dedicated processes"))
            self.stdout.write(f"• Fallback Workers:   {manager.fallback_workers} on '{queue_name}'")
            self.stdout.write(f"• Max Total Workers:  {manager.max_total_workers}")
        self.stdout.write(f"• Burst mode:         {'Enabled' if burst else 'Disabled (continuous dynamic spawning)'}")
        self.stdout.write(self.style.SUCCESS("=" * 72))

        manager.cleanup_stale_workers()
        manager.spawn_workers()
        manager.run_supervisor_loop()
