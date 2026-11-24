"""
worker.py
Main Worker Engine for Zomato Scraper:
1. Manages worker processes and assigns the single queue ('zomato') to every worker.
2. Manages Redis queue connections and process lifecycles.
3. Supervises worker processes and triggers failed jobs handler.

Note: Restaurant and review extraction logic has been relocated to the tasks package:
- tasks/restaurant_extractor.py: restaurant details extraction and pipeline execution
- tasks/review_extractor.py: customer reviews extraction and review URLs generation
"""

import logging
import multiprocessing
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import django_rq
from django.conf import settings
from rq import Worker

from config import SINGLE_QUEUE_NAME
from scraper_app.tasks.restaurant_extractor import (
    process_restaurant_pipeline_job,
    _handle_failed_urls_after_queue_completion,
)
from scraper_app.tasks.review_extractor import (
    generate_review_api_urls,
    extract_restaurant_reviews,
)
from scraper_app.workers.worker_lock import WorkerSingletonLock

logger = logging.getLogger("worker")


def is_pid_alive(pid: Optional[int]) -> bool:
    """Checks if a process ID is currently running on the OS."""
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _worker_process_entrypoint(queue_name: str, worker_index: int = 1, burst: bool = False) -> None:
    """Dedicated RQ worker process entrypoint listening to the single queue."""
    from django import db
    db.connections.close_all()

    pid = os.getpid()
    worker_name = f"zomato-worker-{worker_index}-{pid}"
    queue = django_rq.get_queue(queue_name)
    connection = django_rq.get_connection(queue_name)

    from scraper_app.workers.failed_jobs import rq_job_failure_handler

    worker = Worker([queue], connection=connection, name=worker_name, exception_handlers=[rq_job_failure_handler])
    try:
        worker.work(burst=burst, with_scheduler=(worker_index == 1))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            worker.register_death()
        except Exception:
            pass


class WorkerManager:
    """
    Manages worker processes, spawns dedicated RQ workers on the single queue ('zomato'),
    handles Redis connections, and supervises worker execution.
    """

    def __init__(self, workers: int = 6, queue_name: Optional[str] = None, burst: bool = False) -> None:
        self.num_workers = workers
        self.queue_name = queue_name or getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        self.burst = burst
        self.processes: List[multiprocessing.Process] = []
        self._shutdown_requested = False
        self.lock = WorkerSingletonLock(queue_name=self.queue_name, num_workers=self.num_workers)
        from scraper_app.workers.failed_jobs import FailedJobsHandler
        self.failed_handler = FailedJobsHandler(queues=[self.queue_name])

    def acquire_lock(self) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Acquires singleton lock, returning (acquired, running_info)."""
        return self.lock.acquire()

    def cleanup_stale_workers(self) -> None:
        """Removes dead worker registrations and purges legacy queues from Redis DB 1."""
        try:
            conn = django_rq.get_connection(self.queue_name)
            for w in Worker.all(connection=conn):
                if not is_pid_alive(w.pid):
                    try:
                        w.register_death()
                    except Exception:
                        pass

            expected_key = f"rq:queue:{self.queue_name}"
            existing_queues = conn.smembers("rq:queues")
            for qb in existing_queues:
                q_str = qb.decode("utf-8") if isinstance(qb, bytes) else str(qb)
                if q_str != expected_key and q_str != self.queue_name:
                    conn.srem("rq:queues", qb)
                    clean_name = q_str.replace("rq:queue:", "")
                    conn.delete(f"rq:queue:{clean_name}")

            conn.sadd("rq:queues", expected_key)
        except Exception as exc:
            logger.warning("[WORKER_MANAGER] Notice during stale worker cleanup: %s", exc)

    def spawn_workers(self) -> List[multiprocessing.Process]:
        """Spawns dedicated RQ worker processes listening on the assigned queue."""
        self.cleanup_stale_workers()
        procs: List[multiprocessing.Process] = []
        logger.info("[WORKER_MANAGER] Spawning %d workers assigned to queue '%s'...", self.num_workers, self.queue_name)

        for i in range(1, self.num_workers + 1):
            p = multiprocessing.Process(
                target=_worker_process_entrypoint,
                args=(self.queue_name, i, self.burst),
                name=f"zomato-worker-{i}",
            )
            p.daemon = False
            p.start()
            procs.append(p)
            logger.info("[WORKER_MANAGER] Worker process %d spawned (PID %d) on queue '%s'", i, p.pid, self.queue_name)

        self.processes = procs
        return procs

    start_workers = spawn_workers

    def run_supervisor_loop(self) -> None:
        """Supervises worker processes and periodically executes FailedJobsHandler."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        if self.burst:
            logger.info("[WORKER_MANAGER] Running in BURST mode (%d workers on '%s')...", self.num_workers, self.queue_name)
            for p in self.processes:
                p.join()
            self.failed_handler.process_failed_jobs()
            logger.info("[WORKER_MANAGER] Burst mode complete. All queue jobs processed.")
            self.shutdown()
            return

        logger.info("[WORKER_MANAGER] Worker Supervisor active (%d workers on '%s'). Monitoring...", self.num_workers, self.queue_name)
        idle_cycles = 0

        while not self._shutdown_requested:
            try:
                for idx, p in enumerate(self.processes):
                    if not p.is_alive():
                        logger.warning("[WORKER_MANAGER] Worker %d (PID %d) died. Respawning...", idx + 1, p.pid)
                        new_p = multiprocessing.Process(
                            target=_worker_process_entrypoint,
                            args=(self.queue_name, idx + 1, self.burst),
                            name=f"zomato-worker-{idx + 1}",
                        )
                        new_p.daemon = False
                        new_p.start()
                        self.processes[idx] = new_p
                        logger.info("[WORKER_MANAGER] Respawned worker %d (New PID: %d)", idx + 1, new_p.pid)

                if self.failed_handler.are_all_queues_empty():
                    idle_cycles += 1
                    if idle_cycles >= 3:
                        res = self.failed_handler.process_failed_jobs()
                        if res.get("requeued_count", 0) > 0 or res.get("fallback_saved_count", 0) > 0:
                            logger.info(
                                "[WORKER_MANAGER] Failed jobs processed: %d requeued, %d fallback saved.",
                                res["requeued_count"],
                                res["fallback_saved_count"],
                            )
                        idle_cycles = 0
                else:
                    idle_cycles = 0

                time.sleep(2.0)
            except KeyboardInterrupt:
                break

        self.shutdown()

    wait = run_supervisor_loop

    def _handle_signal(self, signum: int, frame: Any) -> None:
        self._shutdown_requested = True

    def shutdown(self) -> None:
        """Terminates all worker processes cleanly and releases the concurrency lock."""
        logger.info("[WORKER_MANAGER] Terminating worker processes...")
        for p in self.processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2.0)
        self.cleanup_stale_workers()
        self.lock.release()
        logger.info("[WORKER_MANAGER] All workers stopped cleanly.")

    stop = shutdown


__all__ = [
    "WorkerManager",
    "is_pid_alive",
    "_worker_process_entrypoint",
    "process_restaurant_pipeline_job",
    "generate_review_api_urls",
    "extract_restaurant_reviews",
]
