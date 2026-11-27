"""
Django Management Command to launch the standalone RQ Dashboard configured
with this project's dedicated Redis database (DB 1) and queues.

Usage:
    python manage.py run_rq_dashboard
    python manage.py run_rq_dashboard --port 9181
"""

import os
import subprocess
import sys
from django.conf import settings
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = "Launch standalone RQ Dashboard on port 9181 connected to this project's Redis DB (DB 1)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--port",
            "-p",
            type=int,
            default=9181,
            help="HTTP port for RQ Dashboard (default: 9181)",
        )
        parser.add_argument(
            "--bind",
            "-b",
            type=str,
            default="0.0.0.0",
            help="Bind address (default: 0.0.0.0)",
        )

    def handle(self, *args, **options):
        port = options["port"]
        bind = options["bind"]

        redis_host = getattr(settings, "REDIS_HOST", "127.0.0.1")
        redis_port = getattr(settings, "REDIS_PORT", 6379)
        redis_db = getattr(settings, "REDIS_DB", 1)
        redis_password = getattr(settings, "REDIS_PASSWORD", None)

        if redis_password:
            redis_url = f"redis://:{redis_password}@{redis_host}:{redis_port}/{redis_db}"
        else:
            redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"

        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", "zomato")
        self.stdout.write(self.style.SUCCESS(f"Starting RQ Dashboard on http://{bind}:{port}"))
        self.stdout.write(self.style.NOTICE(f"Connecting to dedicated project Redis DB: {redis_url}"))
        self.stdout.write(self.style.NOTICE(f"Monitoring Single Queue: {queue_name}"))

        # Pre-register single queue 'zomato' and clean any invalid unprefixed keys from rq:queues
        try:
            import django_rq
            conn = django_rq.get_connection(queue_name)
            for qb in list(conn.smembers("rq:queues")):
                q_str = qb.decode("utf-8") if isinstance(qb, bytes) else str(qb)
                if not q_str.startswith("rq:queue:"):
                    conn.srem("rq:queues", qb)
            conn.sadd("rq:queues", f"rq:queue:{queue_name}")
        except Exception:
            pass

        import shutil

        env = os.environ.copy()
        env["RQ_DASHBOARD_REDIS_URL"] = redis_url

        rq_bin = shutil.which("rq-dashboard")
        if rq_bin:
            cmd = [
                rq_bin,
                "--bind",
                bind,
                "--port",
                str(port),
                "--redis-url",
                redis_url,
            ]
        else:
            cmd = [
                sys.executable,
                "-m",
                "rq_dashboard.cli",
                "--bind",
                bind,
                "--port",
                str(port),
                "--redis-url",
                redis_url,
            ]

        try:
            subprocess.run(cmd, env=env)
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\nRQ Dashboard stopped."))
