"""
backend/scraper_app/workers/fair_queue.py

Dynamic Per-User Dedicated Queues using Native Standard RQ Lists ('rq:queue:user_<id>').
Subclasses DjangoRQ to provide:
1. Full Live Visibility in Redis & RQ Dashboard (:9181):
   - All pending priority (URL discovery) and batch (restaurant) jobs are stored directly
     in the standard RQ list key 'rq:queue:user_<id>' (or fallback 'rq:queue:zomato').
   - Priority tasks (without 'batch_id') are placed at the front via LPUSH, while
     restaurant jobs (with 'batch_id') are appended via RPUSH.
   - Standard `rq.Queue.count`, `LRANGE rq:queue:user_<id> 0 -1`, and `rq-dashboard`
     Queued Counts & Job Lists reflect all queued jobs natively in real time.
2. Strict Per-User Isolated Worker Dequeue (`isolated=True`):
   - Dedicated UserWorkerPool processes pop strictly from their assigned queue key ONLY,
     raising `DequeueTimeout` when idle in continuous (`burst=False`) worker mode.
3. Shared Fair Round-Robin Multi-User Dispatch (`isolated=False`):
   - Rotates across 'rq:active_user_queues' via RPOPLPUSH and pops 1 job per user
     directly from 'rq:queue:<name>', automatically deleting empty user queues.
"""

import logging
from typing import Any, Iterable, List, Optional, Tuple, Type

from django.conf import settings
from django_rq.queues import DjangoRQ
from redis import Redis
from rq.exceptions import DequeueTimeout
from rq.job import Job, JobStatus
from rq.utils import as_text

logger = logging.getLogger("fair_queue")

ACTIVE_USERS_KEY = "rq:active_user_queues"
GLOBAL_QUEUES_KEY = "rq:queues"
FALLBACK_QUEUE_KEY = "rq:queue:zomato"
USER_QUEUE_EVENTS_CHANNEL = "rq:user_queue_events"

# Global fair round-robin Lua script across active user standard RQ lists (used when isolated=False).
LUA_DYNAMIC_PER_USER_FAIR_DEQUEUE = """
-- KEYS[1] = Active users cursor list ('rq:active_user_queues')
-- KEYS[2] = RQ global queues set ('rq:queues')
-- KEYS[3] = Fallback primary queue ('rq:queue:zomato')

-- 1. Service dynamic active user queues
local num_users = redis.call('LLEN', KEYS[1])
if num_users > 0 then
    for i = 1, num_users do
        -- Rotate active users cursor atomically: Head -> Tail
        local q_name = redis.call('RPOPLPUSH', KEYS[1], KEYS[1])
        local q_key = "rq:queue:" .. q_name
        local job_id = redis.call('LPOP', q_key)

        if job_id then
            if redis.call('LLEN', q_key) == 0 then
                redis.call('LREM', KEYS[1], 0, q_name)
                if q_name ~= "zomato" then
                    redis.call('SREM', KEYS[2], q_key, q_name)
                    redis.call('DEL', q_key)
                end
            end
            return {q_key, job_id}
        else
            -- User queue has no jobs left: clean up immediately
            redis.call('LREM', KEYS[1], 0, q_name)
            if q_name ~= "zomato" then
                redis.call('SREM', KEYS[2], q_key, q_name)
                redis.call('DEL', q_key)
            end
        end
    end
end

-- 2. Fallback check on primary 'zomato' queue if any jobs were pushed there directly
local fallback_job = redis.call('LPOP', KEYS[3])
if fallback_job then
    return {KEYS[3], fallback_job}
end

return nil
"""


class FairQueue(DjangoRQ):
    """
    Subclass of DjangoRQ (and RQ Queue) storing jobs natively in standard RQ queue lists
    ('rq:queue:user_<id>') for full Redis / RQ Dashboard queued visibility while supporting
    strict per-user worker isolation (`isolated=True`) and multi-user round-robin (`isolated=False`).
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.isolated: bool = bool(kwargs.pop("isolated", False))
        super().__init__(*args, **kwargs)

    @classmethod
    def get_user_queue_name(cls, user_id_or_user: Any = None) -> str:
        """Returns normalized queue name for a given user: 'user_<id>' or fallback 'zomato'."""
        if user_id_or_user is None:
            return "zomato"
        if hasattr(user_id_or_user, "is_authenticated"):
            if not user_id_or_user.is_authenticated:
                return "zomato"
            u_id = getattr(user_id_or_user, "id", None)
            return f"user_{u_id}" if u_id else "zomato"
        u_str = str(user_id_or_user).strip()
        if not u_str or u_str == "None" or u_str == "0":
            return "zomato"
        if u_str == "zomato" or u_str == "rq:queue:zomato":
            return "zomato"
        if u_str.startswith("rq:queue:"):
            u_str = u_str.replace("rq:queue:", "", 1)
        if u_str.startswith("user_"):
            return u_str
        return f"user_{u_str}"

    @classmethod
    def resolve_queue(
        cls,
        queue_name: str,
        connection: Optional[Redis] = None,
        isolated: bool = False,
    ) -> "FairQueue":
        """Safely resolves any queue name ('user_<id>' or 'zomato') without DjangoRQ KeyError."""
        clean_name = str(queue_name or "zomato").replace("rq:queue:", "").strip() or "zomato"
        if connection is None:
            import django_rq
            connection = django_rq.get_connection("zomato")
        return cls(name=clean_name, connection=connection, isolated=isolated)

    @classmethod
    def for_user(
        cls,
        user_id_or_user: Any = None,
        connection: Optional[Redis] = None,
        isolated: bool = False,
    ) -> "FairQueue":
        """Factory method returning a FairQueue instance configured for the specified user."""
        q_name = cls.get_user_queue_name(user_id_or_user)
        return cls.resolve_queue(q_name, connection=connection, isolated=isolated)

    @classmethod
    def get_active_user_queues(cls, connection: Redis) -> List[str]:
        """Returns list of currently active user queues in Redis."""
        return [as_text(x) for x in connection.lrange(ACTIVE_USERS_KEY, 0, -1)]

    @classmethod
    def delete_user_queue(cls, connection: Redis, user_id_or_user: Any) -> None:
        """Immediately flushes and completely deletes a user's dedicated queue from Redis."""
        q_name = cls.get_user_queue_name(user_id_or_user)
        q_key = f"rq:queue:{q_name}"
        batches_key = f"{q_key}:batches"
        keys_to_del = [q_key, batches_key]
        try:
            active_batches = connection.lrange(batches_key, 0, -1)
            for b in active_batches:
                b_str = b.decode("utf-8") if isinstance(b, bytes) else str(b)
                keys_to_del.append(f"{q_key}:batch:{b_str}")
        except Exception:
            pass

        pipe = connection.pipeline()
        pipe.lrem(ACTIVE_USERS_KEY, 0, q_name)
        if q_name != "zomato":
            pipe.srem(GLOBAL_QUEUES_KEY, q_key, q_name)
        if keys_to_del:
            pipe.delete(*keys_to_del)
        pipe.execute()
        logger.info("[FAIR_QUEUE] Deleted user queue '%s' from Redis.", q_name)

    @classmethod
    def migrate_legacy_subqueues(cls, connection: Redis, queue_name: Optional[str] = None) -> int:
        """
        Promotes any legacy ':batch:<batch_id>' subqueue jobs into their parent
        standard RQ list 'rq:queue:<name>' so restarting workers preserves in-flight jobs
        and exposes them immediately in Redis & RQ Dashboard.
        """
        pattern = f"rq:queue:{queue_name}:batch:*" if queue_name else "rq:queue:*:batch:*"
        migrated_jobs = 0
        for raw_key in list(connection.keys(pattern)):
            subq_key = as_text(raw_key)
            parts = subq_key.split(":batch:", 1)
            if len(parts) != 2:
                continue
            parent_key = parts[0]
            q_name = parent_key.replace("rq:queue:", "", 1)
            job_ids = connection.lrange(subq_key, 0, -1)
            pipe = connection.pipeline()
            if job_ids:
                pipe.rpush(parent_key, *job_ids)
                migrated_jobs += len(job_ids)
                pipe.lrem(ACTIVE_USERS_KEY, 0, q_name)
                pipe.rpush(ACTIVE_USERS_KEY, q_name)
                pipe.sadd(GLOBAL_QUEUES_KEY, parent_key)
            pipe.delete(subq_key, f"{parent_key}:batches")
            pipe.execute()
        if migrated_jobs > 0:
            logger.info("[FAIR_QUEUE] Migrated %d legacy :batch subqueue job(s) into standard RQ lists.", migrated_jobs)
        return migrated_jobs

    @classmethod
    def remove_batch(cls, connection: Redis, queue_name: str, batch_id: str, user_id_or_user: Any = None) -> None:
        """Removes all queued jobs belonging to `batch_id` from the queue and auto-cleans if empty."""
        if user_id_or_user is not None:
            q_name = cls.get_user_queue_name(user_id_or_user)
        else:
            q_name = str(queue_name).replace("rq:queue:", "")
        q_key = f"rq:queue:{q_name}"

        raw_job_ids = [as_text(jid) for jid in connection.lrange(q_key, 0, -1)]
        if raw_job_ids:
            try:
                fetched_jobs = Job.fetch_many(raw_job_ids, connection=connection)
                pipe = connection.pipeline()
                for jid, job_obj in zip(raw_job_ids, fetched_jobs):
                    if job_obj and str((job_obj.kwargs or {}).get("batch_id") or "") == str(batch_id):
                        pipe.lrem(q_key, 0, jid)
                pipe.execute()
            except Exception:
                pass

        # Clean up any legacy subqueue keys if present
        batches_key = f"{q_key}:batches"
        subq_key = f"{q_key}:batch:{batch_id}"
        pipe_legacy = connection.pipeline()
        pipe_legacy.lrem(batches_key, 0, batch_id)
        pipe_legacy.delete(subq_key)
        pipe_legacy.execute()

        # If user queue is now completely empty, unregister and clean up
        if q_name != "zomato" and connection.llen(q_key) == 0:
            pipe2 = connection.pipeline()
            pipe2.lrem(ACTIVE_USERS_KEY, 0, q_name)
            pipe2.srem(GLOBAL_QUEUES_KEY, q_key, q_name)
            pipe2.delete(q_key, batches_key)
            pipe2.execute()
            logger.info("[FAIR_QUEUE] User queue '%s' auto-cleaned after batch removal.", q_name)

    @classmethod
    def dequeue_any(
        cls,
        queues: Iterable["FairQueue"],
        timeout: Optional[int],
        connection: Redis,
        job_class: Optional[Type[Job]] = None,
        serializer: Any = None,
        death_penalty_class: Any = None,
    ) -> Optional[Tuple[Job, "FairQueue"]]:
        """
        Dequeues a job directly from standard RQ queue lists:
        - When `isolated=True` (dedicated UserWorkerPool), pops strictly from `queues`.
        - When `isolated=False`, rotates active user queues in round-robin order via Lua.
        - Raises `DequeueTimeout` when `timeout > 0` and idle so continuous workers stay alive.
        """
        queue_list = list(queues) if queues else []
        if queue_list and any(getattr(q, "isolated", False) for q in queue_list):
            keys_to_wait = [q.key for q in queue_list]
            for q_key in keys_to_wait:
                raw_jid = connection.lpop(q_key)
                if raw_jid is not None:
                    res = cls._fetch_dequeued_job(
                        connection,
                        q_key,
                        as_text(raw_jid),
                        job_class=job_class,
                        serializer=serializer,
                        death_penalty_class=death_penalty_class,
                    )
                    if res:
                        job_obj, fetched_q = res
                        return job_obj, cls.resolve_queue(fetched_q.name, connection=connection, isolated=True)

            if timeout is not None and timeout > 0:
                blpop_res = connection.blpop(keys_to_wait, timeout=1)
                if blpop_res:
                    q_k, j_id = map(as_text, blpop_res)
                    res = cls._fetch_dequeued_job(
                        connection,
                        q_k,
                        j_id,
                        job_class=job_class,
                        serializer=serializer,
                        death_penalty_class=death_penalty_class,
                    )
                    if res:
                        job_obj, fetched_q = res
                        return job_obj, cls.resolve_queue(fetched_q.name, connection=connection, isolated=True)
                raise DequeueTimeout(timeout, keys_to_wait)
            return None

        raw_result = connection.eval(
            LUA_DYNAMIC_PER_USER_FAIR_DEQUEUE,
            3,
            ACTIVE_USERS_KEY,
            GLOBAL_QUEUES_KEY,
            FALLBACK_QUEUE_KEY,
        )

        if raw_result:
            q_k, j_id = map(as_text, raw_result)
            return cls._fetch_dequeued_job(
                connection,
                q_k,
                j_id,
                job_class=job_class,
                serializer=serializer,
                death_penalty_class=death_penalty_class,
            )

        # When queue is completely empty in continuous mode, wait briefly then raise DequeueTimeout
        if timeout is not None and timeout > 0:
            blpop_res = connection.blpop([FALLBACK_QUEUE_KEY], timeout=1)
            if blpop_res:
                q_k, j_id = map(as_text, blpop_res)
                return cls._fetch_dequeued_job(
                    connection,
                    q_k,
                    j_id,
                    job_class=job_class,
                    serializer=serializer,
                    death_penalty_class=death_penalty_class,
                )
            raise DequeueTimeout(timeout, [FALLBACK_QUEUE_KEY])

        return None

    def enqueue_call(self, *args: Any, **kwargs: Any) -> Job:
        """Bypasses DjangoRQ's QUEUES[name] dictionary lookup to support dynamic user queues."""
        if "result_ttl" not in kwargs or kwargs["result_ttl"] is None:
            kwargs["result_ttl"] = getattr(settings, "RQ_RESULT_TTL", 500)
        if "timeout" not in kwargs or kwargs["timeout"] is None:
            kwargs["timeout"] = getattr(settings, "RQ_DEFAULT_TIMEOUT", 360)
        return super(DjangoRQ, self).enqueue_call(*args, **kwargs)

    def push_job_id(self, job_id: str, pipeline: Optional[Any] = None, at_front: bool = False) -> None:
        """Pushes a job ID directly into `self.key` and notifies MultiUserWorkerSupervisor."""
        pipe = pipeline if pipeline is not None else self.connection.pipeline()
        pipe.lrem(ACTIVE_USERS_KEY, 0, self.name)
        pipe.rpush(ACTIVE_USERS_KEY, self.name)
        pipe.sadd(self.redis_queues_keys, self.key)

        if at_front:
            pipe.lpush(self.key, job_id)
        else:
            pipe.rpush(self.key, job_id)

        pipe.publish(USER_QUEUE_EVENTS_CHANNEL, self.name)

        if pipeline is None:
            pipe.execute()

    def enqueue_many(
        self,
        job_datas: Iterable[Any],
        pipeline: Optional[Any] = None,
        group_id: Optional[str] = None,
    ) -> List[Job]:
        """
        Enqueues multiple jobs directly into standard RQ queue list `self.key`:
        - Priority jobs (without 'batch_id') are placed at the front via LPUSH.
        - Restaurant jobs (with 'batch_id') are appended cleanly via RPUSH.
        """
        if not job_datas:
            return []

        job_datas_list = list(job_datas)
        pipe = pipeline if pipeline is not None else self.connection.pipeline()

        # Register queue in active list and RQ global queues set (using self.key 'rq:queue:<name>')
        pipe.lrem(ACTIVE_USERS_KEY, 0, self.name)
        pipe.rpush(ACTIVE_USERS_KEY, self.name)
        pipe.sadd(self.redis_queues_keys, self.key)

        created_jobs: List[Job] = []
        batch_job_ids: List[str] = []
        priority_job_ids: List[str] = []

        for job_data in job_datas_list:
            b_id = ""
            if hasattr(job_data, "kwargs") and isinstance(job_data.kwargs, dict):
                b_id = str(job_data.kwargs.get("batch_id") or "").strip()

            job_kwargs = {
                "func": job_data.func,
                "args": job_data.args,
                "kwargs": job_data.kwargs,
                "result_ttl": job_data.result_ttl,
                "ttl": job_data.ttl,
                "failure_ttl": job_data.failure_ttl,
                "description": job_data.description,
                "depends_on": job_data.depends_on,
                "job_id": job_data.job_id,
                "meta": job_data.meta,
                "status": JobStatus.QUEUED,
                "timeout": job_data.timeout,
                "retry": job_data.retry,
                "on_success": job_data.on_success,
                "on_failure": job_data.on_failure,
                "on_stopped": job_data.on_stopped,
                "webhooks": job_data.webhooks,
                "group_id": group_id,
                "repeat": job_data.repeat,
            }
            job = self.create_job(**job_kwargs)
            self._prepare_for_queue(job)
            self._persist_job(job, pipe)
            created_jobs.append(job)

            if b_id:
                batch_job_ids.append(job.id)
            else:
                priority_job_ids.append(job.id)

        # Append restaurant batch jobs cleanly to standard RQ list `self.key`
        if batch_job_ids:
            pipe.rpush(self.key, *batch_job_ids)

        # Prepend priority discovery jobs at front of `self.key` while preserving relative order
        if priority_job_ids:
            pipe.lpush(self.key, *reversed(priority_job_ids))

        pipe.publish(USER_QUEUE_EVENTS_CHANNEL, self.name)

        if pipeline is None:
            pipe.execute()

        return created_jobs

    @property
    def count(self) -> int:
        """Returns total count of all pending jobs in this queue's standard RQ list."""
        return self.connection.llen(self.key)

    def __len__(self) -> int:
        return self.count

    def empty(self) -> None:
        """Empties this queue and unregisters non-default queues from active lists."""
        super().empty()
        pipe = self.connection.pipeline()
        pipe.delete(f"{self.key}:batches")
        pipe.lrem(ACTIVE_USERS_KEY, 0, self.name)
        if self.name != "zomato":
            pipe.srem(GLOBAL_QUEUES_KEY, self.key, self.name)
        pipe.execute()
