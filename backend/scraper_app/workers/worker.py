"""
worker.py
Multi-User Isolated Worker Engine for Zomato Scraper:
1. UserWorkerPool: Spawns and supervises dedicated OS worker processes pinned strictly
   to a single user's Redis queue ('user_<id>' or fallback 'zomato') with `isolated=True`.
2. MultiUserWorkerSupervisor (and WorkerManager):
   - Watches 'rq:active_user_queues' and Redis PubSub 'rq:user_queue_events'.
   - Dynamically spawns an independent UserWorkerPool whenever a user starts a pipeline,
     so every user's URL extractor, restaurant scraping, and review extraction run concurrently
     in separate OS processes.
   - Executes per-user FailedJobsHandler recovery strictly within the user's queue.
   - Automatically shuts down a user's dedicated workers and deletes their Redis queue
     once all jobs and active batches for that user complete.
"""

import logging
import multiprocessing
import os
import signal
import time
from typing import Any, Dict, List, Optional, Tuple

import django_rq
from django.conf import settings
from redis import Redis
from rq import Worker
from rq.registry import FailedJobRegistry, StartedJobRegistry

from config import SINGLE_QUEUE_NAME
from scraper_app.tasks.restaurant_extractor import (
    _handle_failed_urls_after_queue_completion,
    process_restaurant_pipeline_job,
)
from scraper_app.tasks.review_extractor import (
    extract_restaurant_reviews,
    generate_review_api_urls,
)
from scraper_app.workers.failed_jobs import FailedJobsHandler, rq_job_failure_handler
from scraper_app.workers.fair_queue import (
    ACTIVE_USERS_KEY,
    GLOBAL_QUEUES_KEY,
    USER_QUEUE_EVENTS_CHANNEL,
    FairQueue,
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


def _worker_process_entrypoint(
    queue_name: str,
    worker_index: int = 1,
    burst: bool = False,
    isolated: bool = True,
) -> None:
    """Dedicated RQ worker process entrypoint pinned strictly to one user or fallback queue."""
    from django import db
    db.connections.close_all()

    pid = os.getpid()
    clean_label = str(queue_name).replace("_", "-")
    worker_name = f"{clean_label}-worker-{worker_index}-{pid}"
    connection = django_rq.get_connection("zomato")
    queue = FairQueue.resolve_queue(queue_name, connection=connection, isolated=isolated)

    worker = Worker(
        [queue],
        connection=connection,
        name=worker_name,
        queue_class=FairQueue,
        exception_handlers=[rq_job_failure_handler],
    )
    try:
        worker.work(burst=burst, with_scheduler=(worker_index == 1 and not burst))
    except KeyboardInterrupt:
        pass
    finally:
        try:
            worker.register_death()
        except Exception:
            pass


class UserWorkerPool:
    """
    Manages a dedicated pool of OS worker processes exclusively for a single queue
    (e.g. 'user_110' or fallback 'zomato'). Workers pop ONLY from this pool's queue.
    """

    def __init__(
        self,
        queue_name: str,
        num_workers: int = 3,
        burst: bool = False,
        isolated: bool = True,
    ) -> None:
        self.queue_name = queue_name
        self.num_workers = max(1, int(num_workers))
        self.burst = burst
        self.isolated = isolated
        self.processes: List[multiprocessing.Process] = []
        self.failed_handler = FailedJobsHandler(queues=[self.queue_name])
        self.idle_cycles: int = 0
        self.created_at: float = time.time()

    def _spawn_single_process(self, worker_index: int) -> multiprocessing.Process:
        # Close parent's DB connections before fork so children do not inherit/break SSL sockets
        try:
            from django import db
            db.connections.close_all()
        except Exception:
            pass
        clean_label = str(self.queue_name).replace("_", "-")
        proc = multiprocessing.Process(
            target=_worker_process_entrypoint,
            args=(self.queue_name, worker_index, self.burst, self.isolated),
            name=f"{clean_label}-worker-{worker_index}",
        )
        proc.daemon = False
        proc.start()
        return proc

    def start(self) -> List[multiprocessing.Process]:
        """Spawns all worker processes dedicated to this queue."""
        procs: List[multiprocessing.Process] = []
        logger.info(
            "[USER_WORKER_POOL] Spawning %d dedicated worker(s) exclusively for queue '%s' (isolated=%s)...",
            self.num_workers,
            self.queue_name,
            self.isolated,
        )
        for idx in range(1, self.num_workers + 1):
            proc = self._spawn_single_process(idx)
            procs.append(proc)
            logger.info(
                "[USER_WORKER_POOL] Spawned worker %d (PID %d) for queue '%s'",
                idx,
                proc.pid,
                self.queue_name,
            )
        self.processes = procs
        return procs

    spawn_workers = start

    def ensure_workers_alive(self) -> int:
        """Respawns any crashed worker processes while this pool is active."""
        if self.burst:
            return 0
        respawned = 0
        for idx, proc in enumerate(self.processes):
            if not proc.is_alive():
                logger.warning(
                    "[USER_WORKER_POOL] Worker %d (PID %s) on queue '%s' died. Respawning...",
                    idx + 1,
                    proc.pid,
                    self.queue_name,
                )
                new_proc = self._spawn_single_process(idx + 1)
                self.processes[idx] = new_proc
                respawned += 1
        return respawned

    def active_worker_count(self) -> int:
        return sum(1 for proc in self.processes if proc.is_alive())

    def join(self, timeout: Optional[float] = None) -> None:
        for proc in self.processes:
            proc.join(timeout=timeout)

    def _has_active_db_batches(self) -> bool:
        """Checks PostgreSQL if the queue's owner still has an active batch in flight."""
        if not self.queue_name.startswith("user_"):
            return False
        user_id_str = self.queue_name.replace("user_", "", 1)
        if not user_id_str.isdigit():
            return False
        try:
            from scraper_app.models import ScrapeBatch
            return ScrapeBatch.objects.filter(
                user_id=int(user_id_str),
                status__in=[
                    "extracting_urls",
                    "processing_restaurants",
                    "fetching_details",
                    "parsing",
                    "extracting_reviews",
                ],
            ).exists()
        except Exception:
            return False

    def is_queue_drained(self, connection: Redis, check_db_active: bool = True) -> bool:
        """
        Returns True only when this pool's queue has 0 pending jobs, 0 in-flight jobs,
        0 pending failed jobs, and (optionally) no active pipeline batches in PostgreSQL.
        """
        q = FairQueue.resolve_queue(self.queue_name, connection=connection, isolated=self.isolated)
        if q.count > 0:
            return False
        try:
            if StartedJobRegistry(queue=q).count > 0:
                return False
            if FailedJobRegistry(queue=q).count > 0:
                return False
        except Exception:
            pass
        if check_db_active and self._has_active_db_batches():
            return False
        return True

    def run_failed_recovery(self) -> Dict[str, Any]:
        """Processes failed jobs strictly scoped to this pool's queue."""
        try:
            q = FairQueue.resolve_queue(self.queue_name, isolated=self.isolated)
            if q.count == 0 and StartedJobRegistry(queue=q).count == 0:
                return self.failed_handler.process_failed_jobs()
        except Exception as exc:
            logger.warning("[USER_WORKER_POOL] Failed recovery error on '%s': %s", self.queue_name, exc)
        return {"requeued_count": 0, "fallback_saved_count": 0}

    def shutdown(self, cleanup_queue: bool = False, connection: Optional[Redis] = None) -> None:
        """Terminates all worker processes in this pool cleanly."""
        for proc in self.processes:
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2.0)
                if proc.is_alive():
                    try:
                        proc.kill()
                        proc.join(timeout=1.0)
                    except Exception:
                        pass

        conn = connection
        if conn is None:
            try:
                conn = django_rq.get_connection("zomato")
            except Exception:
                conn = None

        if conn is not None:
            try:
                for w in Worker.all(connection=conn):
                    if not is_pid_alive(w.pid):
                        try:
                            w.register_death()
                        except Exception:
                            pass
            except Exception:
                pass

            if cleanup_queue and self.queue_name != "zomato":
                try:
                    FairQueue.delete_user_queue(conn, self.queue_name)
                except Exception as del_err:
                    logger.warning("[USER_WORKER_POOL] Error cleaning queue '%s': %s", self.queue_name, del_err)

        logger.info("[USER_WORKER_POOL] Shutdown complete for queue '%s'.", self.queue_name)


class MultiUserWorkerSupervisor:
    """
    Dynamic Multi-User Worker Supervisor:
    - Maintains an optional standby fallback pool on the default 'zomato' queue.
    - Dynamically spawns an independent UserWorkerPool for each active user queue ('user_<id>').
    - Enforces resource safety caps (--workers-per-user and --max-total-workers).
    - Automatically shuts down idle user pools and deletes their Redis queues on completion.
    """

    def __init__(
        self,
        workers: int = 6,
        queue_name: Optional[str] = None,
        burst: bool = False,
        workers_per_user: Optional[int] = None,
        fallback_workers: Optional[int] = None,
        max_total_workers: int = 24,
        idle_grace_cycles: int = 2,
        single_queue_only: bool = False,
    ) -> None:
        self.num_workers = max(1, int(workers))
        self.queue_name = queue_name or getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        self.burst = burst
        self.max_total_workers = max(1, int(max_total_workers))
        self.idle_grace_cycles = max(0, int(idle_grace_cycles))

        default_queue = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        self.single_queue_only = bool(single_queue_only or (self.queue_name != default_queue))

        if workers_per_user is not None:
            self.workers_per_user = max(1, int(workers_per_user))
        else:
            self.workers_per_user = min(self.num_workers, 3) if not self.single_queue_only else self.num_workers

        if fallback_workers is not None:
            self.fallback_workers = max(0, int(fallback_workers))
        else:
            self.fallback_workers = min(2, self.num_workers) if not self.single_queue_only else self.num_workers

        self.fallback_pool: Optional[UserWorkerPool] = None
        self.user_pools: Dict[str, UserWorkerPool] = {}
        self._shutdown_requested = False
        self._pubsub = None
        self.lock = WorkerSingletonLock(queue_name=self.queue_name, num_workers=self.num_workers)
        self.failed_handler = FailedJobsHandler(queues=[self.queue_name])

    @property
    def processes(self) -> List[multiprocessing.Process]:
        """Returns all active worker processes across fallback and per-user pools."""
        all_procs: List[multiprocessing.Process] = []
        if self.fallback_pool:
            all_procs.extend(self.fallback_pool.processes)
        for pool in self.user_pools.values():
            all_procs.extend(pool.processes)
        return all_procs

    def acquire_lock(self) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Acquires singleton lock, returning (acquired, running_info)."""
        return self.lock.acquire()

    def cleanup_stale_workers(self) -> None:
        """Removes dead worker registrations and repairs rq:queues keys while preserving active user queues."""
        try:
            conn = django_rq.get_connection("zomato")
            for w in Worker.all(connection=conn):
                if not is_pid_alive(w.pid):
                    try:
                        w.register_death()
                    except Exception:
                        pass
            # Repair any invalid keys in rq:queues lacking 'rq:queue:' prefix (prevents rq-dashboard 500)
            for qb in conn.smembers(GLOBAL_QUEUES_KEY):
                q_str = qb.decode("utf-8") if isinstance(qb, bytes) else str(qb)
                if not q_str.startswith("rq:queue:"):
                    conn.srem(GLOBAL_QUEUES_KEY, qb)
            conn.sadd(GLOBAL_QUEUES_KEY, f"rq:queue:{self.queue_name}")
        except Exception as exc:
            logger.warning("[WORKER_SUPERVISOR] Notice during stale worker cleanup: %s", exc)

    def _discover_active_user_queues(self, conn: Redis) -> List[str]:
        """Discovers all active 'user_*' queues from Redis active list and rq:queues."""
        discovered: List[str] = []
        seen = set()

        for q_name in FairQueue.get_active_user_queues(conn):
            if q_name.startswith("user_") and q_name not in seen:
                seen.add(q_name)
                discovered.append(q_name)

        try:
            for qb in conn.smembers(GLOBAL_QUEUES_KEY):
                q_str = qb.decode("utf-8") if isinstance(qb, bytes) else str(qb)
                clean = q_str.replace("rq:queue:", "")
                if clean.startswith("user_") and clean not in seen:
                    q = FairQueue.resolve_queue(clean, connection=conn, isolated=True)
                    if q.count > 0 or StartedJobRegistry(queue=q).count > 0:
                        seen.add(clean)
                        discovered.append(clean)
        except Exception:
            pass

        return discovered

    def _current_total_workers(self) -> int:
        total = self.fallback_pool.active_worker_count() if self.fallback_pool else 0
        for pool in self.user_pools.values():
            total += pool.active_worker_count()
        return total

    def _init_pubsub(self, conn: Redis) -> None:
        if self._pubsub is not None or self.single_queue_only or self.burst:
            return
        try:
            ps = conn.pubsub(ignore_subscribe_messages=True)
            ps.subscribe(USER_QUEUE_EVENTS_CHANNEL)
            self._pubsub = ps
        except Exception as exc:
            logger.debug("[WORKER_SUPERVISOR] PubSub subscription optional notice: %s", exc)

    def _drain_pubsub_events(self) -> List[str]:
        events: List[str] = []
        if self._pubsub is None:
            return events
        try:
            while True:
                msg = self._pubsub.get_message(timeout=0.01)
                if not msg:
                    break
                data = msg.get("data")
                if data:
                    q_name = data.decode("utf-8") if isinstance(data, bytes) else str(data)
                    if q_name.startswith("user_") and q_name not in events:
                        events.append(q_name)
        except Exception:
            pass
        return events

    def reconcile_user_pools(self, check_db_active: bool = True) -> Dict[str, Any]:
        """
        Spawns dedicated UserWorkerPools for newly active user queues,
        executes per-user failed recovery, and shuts down & deletes drained user queues.
        """
        conn = django_rq.get_connection("zomato")
        spawned: List[str] = []
        cleaned: List[str] = []

        if not self.single_queue_only:
            event_queues = self._drain_pubsub_events()
            active_queues = self._discover_active_user_queues(conn)
            for eq in event_queues:
                if eq not in active_queues:
                    q = FairQueue.resolve_queue(eq, connection=conn, isolated=True)
                    if q.count > 0 or StartedJobRegistry(queue=q).count > 0:
                        active_queues.append(eq)

            # 1. Spawn dedicated isolated UserWorkerPools for active user queues
            for q_name in active_queues:
                if q_name not in self.user_pools:
                    q_obj = FairQueue.resolve_queue(q_name, connection=conn, isolated=True)
                    if q_obj.count == 0 and StartedJobRegistry(queue=q_obj).count == 0 and FailedJobRegistry(queue=q_obj).count == 0:
                        # Stale active entry with no jobs; clean it directly
                        FairQueue.delete_user_queue(conn, q_name)
                        cleaned.append(q_name)
                        continue

                    available = max(1, self.max_total_workers - self._current_total_workers())
                    allocated = min(self.workers_per_user, available)
                    pool = UserWorkerPool(
                        queue_name=q_name,
                        num_workers=allocated,
                        burst=self.burst,
                        isolated=True,
                    )
                    pool.start()
                    self.user_pools[q_name] = pool
                    spawned.append(q_name)

        # 2. Supervise fallback pool health & failed recovery
        if self.fallback_pool:
            self.fallback_pool.ensure_workers_alive()
            res = self.fallback_pool.run_failed_recovery()
            if res.get("requeued_count", 0) > 0 or res.get("fallback_saved_count", 0) > 0:
                logger.info(
                    "[WORKER_SUPERVISOR] Fallback '%s' failed jobs processed: %d requeued, %d fallback saved.",
                    self.queue_name,
                    res["requeued_count"],
                    res["fallback_saved_count"],
                )

        # 3. Supervise each active user pool & clean up completed/terminated ones
        for q_name, pool in list(self.user_pools.items()):
            pool.ensure_workers_alive()
            rec_res = pool.run_failed_recovery()
            if rec_res.get("requeued_count", 0) > 0:
                pool.idle_cycles = 0
                continue

            if pool.is_queue_drained(conn, check_db_active=check_db_active):
                pool.idle_cycles += 1
                if pool.idle_cycles >= self.idle_grace_cycles or self.burst:
                    logger.info(
                        "[WORKER_SUPERVISOR] Queue '%s' completed all tasks. Terminating dedicated workers and deleting queue...",
                        q_name,
                    )
                    pool.shutdown(cleanup_queue=True, connection=conn)
                    del self.user_pools[q_name]
                    cleaned.append(q_name)
            else:
                pool.idle_cycles = 0

        return {
            "spawned": spawned,
            "cleaned": cleaned,
            "active_user_pools": list(self.user_pools.keys()),
        }

    def spawn_workers(self) -> List[multiprocessing.Process]:
        """Starts the standby queue pool and spawns pools for any active user queues."""
        self.cleanup_stale_workers()
        conn = django_rq.get_connection("zomato")
        self._init_pubsub(conn)

        if self.fallback_workers > 0:
            self.fallback_pool = UserWorkerPool(
                queue_name=self.queue_name,
                num_workers=self.fallback_workers,
                burst=self.burst,
                isolated=True,
            )
            self.fallback_pool.start()

        self.reconcile_user_pools()
        return self.processes

    start_workers = spawn_workers

    def run_supervisor_loop(self) -> None:
        """Supervises per-user worker pools, failed job recovery, and dynamic scaling."""
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        if self.burst:
            logger.info("[WORKER_SUPERVISOR] Running in BURST mode...")
            # Drain fallback and any initially discovered user pools
            while True:
                if self.fallback_pool:
                    self.fallback_pool.join()
                    self.fallback_pool.run_failed_recovery()

                for pool in list(self.user_pools.values()):
                    pool.join()

                summary = self.reconcile_user_pools(check_db_active=False)
                # Keep looping in burst mode only if new jobs/pools spawned from URL extractors
                conn = django_rq.get_connection("zomato")
                has_more = False
                if self.fallback_pool and not self.fallback_pool.is_queue_drained(conn, check_db_active=False):
                    self.fallback_pool.start()
                    has_more = True
                for pool in list(self.user_pools.values()):
                    if not pool.is_queue_drained(conn, check_db_active=False):
                        if pool.active_worker_count() == 0:
                            pool.start()
                        has_more = True
                if not has_more and not summary["spawned"] and not self.user_pools:
                    break
                time.sleep(0.2)

            logger.info("[WORKER_SUPERVISOR] Burst mode complete. All queue jobs processed.")
            self.shutdown()
            return

        logger.info(
            "[WORKER_SUPERVISOR] Multi-User Supervisor active (workers_per_user=%d, max_total=%d, fallback=%d on '%s'). Monitoring...",
            self.workers_per_user,
            self.max_total_workers,
            self.fallback_workers,
            self.queue_name,
        )

        while not self._shutdown_requested:
            try:
                self.reconcile_user_pools(check_db_active=True)
                time.sleep(1.0)
            except KeyboardInterrupt:
                break
            except Exception as loop_err:
                logger.warning("[WORKER_SUPERVISOR] Supervisor tick notice: %s", loop_err)
                time.sleep(1.0)

        self.shutdown()

    wait = run_supervisor_loop

    def _handle_signal(self, signum: int, frame: Any) -> None:
        self._shutdown_requested = True

    def shutdown(self) -> None:
        """Terminates all per-user pools and fallback pool cleanly and releases lock."""
        logger.info("[WORKER_SUPERVISOR] Terminating all worker pools...")
        if self._pubsub is not None:
            try:
                self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None

        conn = None
        try:
            conn = django_rq.get_connection("zomato")
        except Exception:
            pass

        for q_name, pool in list(self.user_pools.items()):
            pool.shutdown(cleanup_queue=False, connection=conn)
        self.user_pools.clear()

        if self.fallback_pool:
            self.fallback_pool.shutdown(cleanup_queue=False, connection=conn)
            self.fallback_pool = None

        self.cleanup_stale_workers()
        self.lock.release()
        logger.info("[WORKER_SUPERVISOR] All workers stopped cleanly.")

    stop = shutdown


class WorkerManager(MultiUserWorkerSupervisor):
    """Backward-compatible WorkerManager facade backed by MultiUserWorkerSupervisor."""
    pass


__all__ = [
    "UserWorkerPool",
    "MultiUserWorkerSupervisor",
    "WorkerManager",
    "is_pid_alive",
    "_worker_process_entrypoint",
    "process_restaurant_pipeline_job",
    "generate_review_api_urls",
    "extract_restaurant_reviews",
]
