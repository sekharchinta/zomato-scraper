"""
backend/scraper_app/workers/fair_queue.py

Dynamic Per-User Dedicated Queues & Fair Round-Robin Multi-Request Queue for Redis RQ.
Subclasses DjangoRQ to provide fair, interleaved job dispatch across concurrent users and requests,
with automatic lifecycle creation and deletion of user queues.

How it works:
1. Dynamic Per-User Queue Creation:
   When an authenticated user triggers a scrape or jobs are pushed, a dedicated queue
   'rq:queue:user_<user_id>' is instantiated and registered in 'rq:active_user_queues'
   and 'rq:queues' (RQ Dashboard).
2. Priority & Batch Subqueues:
   - Priority URL discovery tasks go into 'rq:queue:user_<user_id>'.
   - Restaurant jobs for specific batches go into 'rq:queue:user_<user_id>:batch:<batch_id>',
     with batch UUIDs registered in 'rq:queue:user_<user_id>:batches'.
3. Fair Interleaved Worker Dispatch:
   Workers execute an atomic Lua script (LUA_DYNAMIC_PER_USER_FAIR_DEQUEUE) that rotates
   the active user cursor (RPOPLPUSH) across users in O(1) time (<0.1ms), popping 1 job
   per user so no user blocks or starves another.
4. Automatic Lifecycle Deletion:
   Immediately when all jobs for a user are completed (or upon cancellation/termination),
   the Lua script automatically deletes the user queue keys from Redis RAM and unregisters
   them from 'rq:active_user_queues' and 'rq:queues' (RQ Dashboard).
"""

import logging
from typing import Any, Iterable, List, Optional, Tuple, Type, Union

from django.conf import settings
from django_rq.queues import DjangoRQ
from redis import Redis
from rq.job import Job, JobStatus
from rq.serializers import resolve_serializer
from rq.utils import as_text

logger = logging.getLogger("fair_queue")

ACTIVE_USERS_KEY = "rq:active_user_queues"
GLOBAL_QUEUES_KEY = "rq:queues"
FALLBACK_QUEUE_KEY = "rq:queue:zomato"
FALLBACK_BATCHES_KEY = "rq:queue:zomato:batches"

LUA_DYNAMIC_PER_USER_FAIR_DEQUEUE = """
-- KEYS[1] = Active users cursor list ('rq:active_user_queues')
-- KEYS[2] = RQ global queues set ('rq:queues')
-- KEYS[3] = Fallback primary queue ('rq:queue:zomato')
-- KEYS[4] = Fallback primary batches ('rq:queue:zomato:batches')

-- 1. Service dynamic active user queues
local num_users = redis.call('LLEN', KEYS[1])
if num_users > 0 then
    for i = 1, num_users do
        -- Rotate active users cursor atomically: Head -> Tail
        local q_name = redis.call('RPOPLPUSH', KEYS[1], KEYS[1])
        local q_key = "rq:queue:" .. q_name
        local batches_key = q_key .. ":batches"
        local batch_prefix = q_key .. ":batch:"

        -- Step A: Check priority queue first (e.g. URL extractor task)
        local prio_job = redis.call('LPOP', q_key)
        if prio_job then
            local rem_prio = redis.call('LLEN', q_key)
            local rem_batches = redis.call('LLEN', batches_key)
            if rem_prio == 0 and rem_batches == 0 then
                redis.call('LREM', KEYS[1], 0, q_name)
                if q_name ~= "zomato" then
                    redis.call('SREM', KEYS[2], q_name)
                    redis.call('DEL', q_key, batches_key)
                end
            end
            return {q_key, prio_job}
        end

        -- Step B: Check active batch subqueues (restaurant jobs)
        local num_batches = redis.call('LLEN', batches_key)
        if num_batches > 0 then
            for b = 1, num_batches do
                local batch_id = redis.call('RPOPLPUSH', batches_key, batches_key)
                local subq_key = batch_prefix .. batch_id
                local job_id = redis.call('LPOP', subq_key)
                if job_id then
                    local rem_in_subq = redis.call('LLEN', subq_key)
                    if rem_in_subq == 0 then
                        redis.call('LREM', batches_key, 0, batch_id)
                        redis.call('DEL', subq_key)
                    end
                    -- Check if user queue is now completely drained
                    local rem_prio = redis.call('LLEN', q_key)
                    local rem_b = redis.call('LLEN', batches_key)
                    if rem_prio == 0 and rem_b == 0 then
                        redis.call('LREM', KEYS[1], 0, q_name)
                        if q_name ~= "zomato" then
                            redis.call('SREM', KEYS[2], q_name)
                            redis.call('DEL', q_key, batches_key)
                        end
                    end
                    return {q_key, job_id}
                else
                    -- Subqueue empty: clean it up
                    redis.call('LREM', batches_key, 0, batch_id)
                    redis.call('DEL', subq_key)
                end
            end
        end

        -- Step C: Check if this user queue is completely empty
        local rem_prio = redis.call('LLEN', q_key)
        local rem_batches = redis.call('LLEN', batches_key)
        if rem_prio == 0 and rem_batches == 0 then
            -- Auto-cleanup user queue when all jobs are completed!
            redis.call('LREM', KEYS[1], 0, q_name)
            if q_name ~= "zomato" then
                redis.call('SREM', KEYS[2], q_name)
                redis.call('DEL', q_key, batches_key)
            end
        end
    end
end

-- 2. Fallback check on primary 'zomato' queue if any jobs were pushed there directly
local fallback_prio = redis.call('LPOP', KEYS[3])
if fallback_prio then
    return {KEYS[3], fallback_prio}
end

local num_fb_batches = redis.call('LLEN', KEYS[4])
if num_fb_batches > 0 then
    for fb = 1, num_fb_batches do
        local fb_batch_id = redis.call('RPOPLPUSH', KEYS[4], KEYS[4])
        local fb_subq_key = "rq:queue:zomato:batch:" .. fb_batch_id
        local fb_job_id = redis.call('LPOP', fb_subq_key)
        if fb_job_id then
            local rem_in_fb = redis.call('LLEN', fb_subq_key)
            if rem_in_fb == 0 then
                redis.call('LREM', KEYS[4], 0, fb_batch_id)
                redis.call('DEL', fb_subq_key)
            end
            return {KEYS[3], fb_job_id}
        else
            redis.call('LREM', KEYS[4], 0, fb_batch_id)
            redis.call('DEL', fb_subq_key)
        end
    end
end

return nil
"""


class FairQueue(DjangoRQ):
    """
    Subclass of DjangoRQ (and RQ Queue) providing dynamic per-user queues and
    fair round-robin scheduling across concurrent users and scrape requests.
    """

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
        if u_str.startswith("user_"):
            return u_str
        return f"user_{u_str}"

    @classmethod
    def for_user(cls, user_id_or_user: Any = None, connection: Optional[Redis] = None) -> "FairQueue":
        """Factory method returning a FairQueue instance configured for the specified user."""
        q_name = cls.get_user_queue_name(user_id_or_user)
        if connection is None:
            try:
                import django_rq
                connection = django_rq.get_connection("zomato")
            except Exception:
                pass
        return cls(name=q_name, connection=connection)

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
        active_batches = connection.lrange(batches_key, 0, -1)
        keys_to_del = [q_key, batches_key]
        if active_batches:
            for b in active_batches:
                b_str = b.decode("utf-8") if isinstance(b, bytes) else str(b)
                keys_to_del.append(f"{q_key}:batch:{b_str}")

        pipe = connection.pipeline()
        pipe.lrem(ACTIVE_USERS_KEY, 0, q_name)
        if q_name != "zomato":
            pipe.srem(GLOBAL_QUEUES_KEY, q_name)
        if keys_to_del:
            pipe.delete(*keys_to_del)
        pipe.execute()
        logger.info("[FAIR_QUEUE] Deleted user queue '%s' and all subkeys from Redis.", q_name)

    @classmethod
    def remove_batch(cls, connection: Redis, queue_name: str, batch_id: str, user_id_or_user: Any = None) -> None:
        """Removes a batch subqueue and unregisters it from the active rotation list."""
        if user_id_or_user is not None:
            q_name = cls.get_user_queue_name(user_id_or_user)
        else:
            q_name = queue_name.replace("rq:queue:", "")
        q_key = f"rq:queue:{q_name}"
        batches_key = f"{q_key}:batches"
        subq_key = f"{q_key}:batch:{batch_id}"

        pipe = connection.pipeline()
        pipe.lrem(batches_key, 0, batch_id)
        pipe.delete(subq_key)
        pipe.execute()

        # Check if user queue is now completely empty
        if q_name != "zomato":
            rem_prio = connection.llen(q_key)
            rem_batches = connection.llen(batches_key)
            if rem_prio == 0 and rem_batches == 0:
                pipe2 = connection.pipeline()
                pipe2.lrem(ACTIVE_USERS_KEY, 0, q_name)
                pipe2.srem(GLOBAL_QUEUES_KEY, q_name)
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
        Dequeues a job using the atomic Lua round-robin script across active per-user queues.
        Falls back to BLPOP on the primary queue when idle to avoid busy-polling.
        """
        raw_result = connection.eval(
            LUA_DYNAMIC_PER_USER_FAIR_DEQUEUE,
            4,
            ACTIVE_USERS_KEY,
            GLOBAL_QUEUES_KEY,
            FALLBACK_QUEUE_KEY,
            FALLBACK_BATCHES_KEY,
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

        # When queue is completely empty, use BLPOP with timeout on fallback queue to prevent CPU spinning
        if timeout and timeout > 0:
            blpop_res = connection.blpop([FALLBACK_QUEUE_KEY], timeout=min(int(timeout), 1))
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

        return None

    def enqueue_call(self, *args, **kwargs):
        """Bypasses DjangoRQ's QUEUES[name] dictionary lookup to support dynamic user queues."""
        if "result_ttl" not in kwargs or kwargs["result_ttl"] is None:
            kwargs["result_ttl"] = getattr(settings, "RQ_RESULT_TTL", 500)
        if "timeout" not in kwargs or kwargs["timeout"] is None:
            kwargs["timeout"] = getattr(settings, "RQ_DEFAULT_TIMEOUT", 360)
        return super(DjangoRQ, self).enqueue_call(*args, **kwargs)

    def push_job_id(self, job_id: str, pipeline: Optional[Any] = None, at_front: bool = False) -> None:
        """Pushes a job ID on the corresponding Redis queue and registers queue in active list."""
        pipe = pipeline if pipeline is not None else self.connection.pipeline()
        pipe.lrem(ACTIVE_USERS_KEY, 0, self.name)
        pipe.rpush(ACTIVE_USERS_KEY, self.name)
        pipe.sadd(self.redis_queues_keys, self.name)

        if at_front:
            pipe.lpush(self.key, job_id)
        else:
            pipe.rpush(self.key, job_id)

        if pipeline is None:
            pipe.execute()

    def enqueue_many(
        self,
        job_datas: Iterable[Any],
        pipeline: Optional[Any] = None,
        group_id: Optional[str] = None,
    ) -> List[Job]:
        """
        Enqueues multiple jobs. If jobs contain 'batch_id' in their kwargs,
        they are partitioned into that batch's fair round-robin subqueue.
        Otherwise, they are enqueued into the primary queue list as normal.
        """
        if not job_datas:
            return []

        job_datas_list = list(job_datas)
        pipe = pipeline if pipeline is not None else self.connection.pipeline()

        # Register queue in active list and RQ global queues set
        pipe.lrem(ACTIVE_USERS_KEY, 0, self.name)
        pipe.rpush(ACTIVE_USERS_KEY, self.name)
        pipe.sadd(self.redis_queues_keys, self.name)

        batches_key = f"{self.key}:batches"
        created_jobs: List[Job] = []
        batch_job_ids_map: dict = {}
        standard_job_ids: List[str] = []

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
                if b_id not in batch_job_ids_map:
                    batch_job_ids_map[b_id] = []
                batch_job_ids_map[b_id].append(job.id)
            else:
                standard_job_ids.append(job.id)

        # Push batch-specific jobs into their respective subqueues
        for b_id, j_ids in batch_job_ids_map.items():
            subq_key = f"{self.key}:batch:{b_id}"
            pipe.rpush(subq_key, *j_ids)
            pipe.lrem(batches_key, 0, b_id)
            pipe.rpush(batches_key, b_id)

        # Standard jobs go to the primary queue list
        if standard_job_ids:
            pipe.rpush(self.key, *standard_job_ids)

        if pipeline is None:
            pipe.execute()

        return created_jobs

    @property
    def count(self) -> int:
        """Returns total count of all pending jobs across primary queue and all active batch subqueues."""
        total = self.connection.llen(self.key)
        batches_key = f"{self.key}:batches"
        active_batches = self.connection.lrange(batches_key, 0, -1)
        if active_batches:
            pipe = self.connection.pipeline()
            for b in active_batches:
                b_str = b.decode("utf-8") if isinstance(b, bytes) else str(b)
                pipe.llen(f"{self.key}:batch:{b_str}")
            lens = pipe.execute()
            total += sum(lens)
        return total

    def __len__(self) -> int:
        return self.count

    def empty(self) -> None:
        """Cleans primary queue list and all batch subqueues, unregistering from active queues."""
        super().empty()
        batches_key = f"{self.key}:batches"
        active_batches = self.connection.lrange(batches_key, 0, -1)
        pipe = self.connection.pipeline()
        for b in active_batches:
            b_str = b.decode("utf-8") if isinstance(b, bytes) else str(b)
            pipe.delete(f"{self.key}:batch:{b_str}")
        pipe.delete(batches_key)
        pipe.lrem(ACTIVE_USERS_KEY, 0, self.name)
        if self.name != "zomato":
            pipe.srem(GLOBAL_QUEUES_KEY, self.name)
        pipe.execute()
