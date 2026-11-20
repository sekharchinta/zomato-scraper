"""
Worker Manager for Single Redis Queue ('zomato').
Orchestrates worker processes listening to the single queue and supervises
failed job secondary requeueing and final fallback saving.
"""

import multiprocessing
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import django_rq
from rq import Worker
from django.conf import settings

from config import SINGLE_QUEUE_NAME
from scraper_app.workers.failed_jobs_handler import FailedJobsHandler
from scraper_app.workers.worker_lock import WorkerSingletonLock


def is_pid_alive(pid: Optional[int]) -> bool:
    """Checks if a process ID is currently running on the operating system."""
    if not pid or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _worker_process_entrypoint(queue_name: str, worker_index: int = 1, burst: bool = False) -> None:
    """Entry point for each dedicated RQ worker process listening to the single queue."""
    from django import db
    db.connections.close_all()

    pid = os.getpid()
    worker_name = f"zomato-worker-{worker_index}-{pid}"

    queue = django_rq.get_queue(queue_name)
    connection = django_rq.get_connection(queue_name)

    worker = Worker([queue], connection=connection, name=worker_name)
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
    """Manages spawning, lifecycle, and supervision of RQ worker processes on the single queue."""

    def __init__(
        self,
        workers: int = 3,
        queue_name: Optional[str] = None,
        burst: bool = False,
    ) -> None:
        self.num_workers = workers
        self.queue_name = queue_name or getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        self.burst = burst
        self.processes: List[multiprocessing.Process] = []
        self.failed_handler = FailedJobsHandler(queues=[self.queue_name])
        self._shutdown_requested = False
        self.lock = WorkerSingletonLock(queue_name=self.queue_name, num_workers=self.num_workers)

    def acquire_lock(self) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Acquires singleton lock, returning (acquired, running_info)."""
        return self.lock.acquire()

    def cleanup_stale_workers(self) -> None:
        """Cleans any orphaned/dead worker keys and purges legacy queues from Redis DB 1."""
        try:
            conn = django_rq.get_connection(self.queue_name)
            for w in Worker.all(connection=conn):
                if not is_pid_alive(w.pid):
                    try:
                        w.register_death()
                    except Exception:
                        pass

            # Expected RQ queue key format in Redis is 'rq:queue:<queue_name>'
            expected_key = f"rq:queue:{self.queue_name}"

            # Remove any legacy queues that are NOT our single queue 'zomato'
            existing_queues = conn.smembers("rq:queues")
            for qb in existing_queues:
                q_str = qb.decode("utf-8") if isinstance(qb, bytes) else str(qb)
                if q_str != expected_key and q_str != self.queue_name:
                    conn.srem("rq:queues", qb)
                    clean_name = q_str.replace("rq:queue:", "")
                    conn.delete(f"rq:queue:{clean_name}")

            # Ensure our single queue 'zomato' is ALWAYS registered in rq:queues
            conn.sadd("rq:queues", expected_key)
        except Exception:
            pass

    def spawn_workers(self) -> List[multiprocessing.Process]:
        """Spawns dedicated worker processes for the single queue."""
        procs: List[multiprocessing.Process] = []

        print(f"[*] Spawning {self.num_workers} RQ worker process(es) on queue '{self.queue_name}'...")
        for i in range(1, self.num_workers + 1):
            p = multiprocessing.Process(
                target=_worker_process_entrypoint,
                args=(self.queue_name, i, self.burst),
                name=f"zomato-worker-{i}",
            )
            p.daemon = False
            p.start()
            procs.append(p)
            print(f"  [+] Spawned worker {i}/{self.num_workers} (PID: {p.pid}) -> listening on '{self.queue_name}'")

        self.processes = procs
        print(f"[*] All {self.num_workers} worker(s) active on Redis DB 1 (Queue: '{self.queue_name}').\n")
        return procs

    def run_supervisor_loop(self) -> None:
        """
        Supervises worker processes and periodically executes FailedJobsHandler
        to requeue failed jobs or save fallback data when the queue is idle.
        """
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        if self.burst:
            print(f"[*] Single Queue Workers running in BURST mode ({self.num_workers} workers on '{self.queue_name}')...")
            for p in self.processes:
                p.join()
            self.failed_handler.process_failed_jobs()
            print("[*] Burst mode complete. All queue jobs processed.")
            self.shutdown()
            return

        print(f"[*] Single Queue Worker Supervisor started ({self.num_workers} workers on '{self.queue_name}')")
        print("[*] Press Ctrl+C to terminate cleanly.")

        idle_cycles = 0

        while not self._shutdown_requested:
            try:
                # Check worker health
                for idx, p in enumerate(self.processes):
                    if not p.is_alive():
                        print(f"[!] Worker {idx + 1} (PID: {p.pid}) died. Respawning...")
                        new_p = multiprocessing.Process(
                            target=_worker_process_entrypoint,
                            args=(self.queue_name, idx + 1, self.burst),
                            name=f"zomato-worker-{idx + 1}",
                        )
                        new_p.daemon = False
                        new_p.start()
                        self.processes[idx] = new_p
                        print(f"  [+] Respawned worker {idx + 1} (New PID: {new_p.pid})")

                # Check queue status and failed jobs
                if self.failed_handler.are_all_queues_empty():
                    idle_cycles += 1
                    # After 3 consecutive idle checks, process any failed jobs
                    if idle_cycles >= 3:
                        res = self.failed_handler.process_failed_jobs()
                        if res["requeued_count"] > 0 or res["fallback_saved_count"] > 0:
                            print(
                                f"[+] Failed Jobs Processed: {res['requeued_count']} requeued, "
                                f"{res['fallback_saved_count']} fallback saved to PostgreSQL."
                            )
                        idle_cycles = 0
                else:
                    idle_cycles = 0

                time.sleep(2.0)
            except KeyboardInterrupt:
                break

        self.shutdown()

    def _handle_signal(self, signum: int, frame: Any) -> None:
        self._shutdown_requested = True

    def shutdown(self) -> None:
        """Terminates all worker processes cleanly."""
        print("\n[*] Terminating worker processes...")
        for p in self.processes:
            if p.is_alive():
                p.terminate()
                p.join(timeout=2.0)
        self.cleanup_stale_workers()
        self.lock.release()
        print("[*] All workers stopped cleanly.")
