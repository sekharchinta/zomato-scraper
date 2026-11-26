"""
backend/scraper_app/workers/fair_queue.py

Fair Round-Robin Multi-Request Queue for Redis RQ.
Subclasses DjangoRQ to provide fair, interleaved job dispatch across concurrent scrape requests.

How it works:
1. Priority Jobs (e.g. URL extractor tasks) go to the primary queue list 'rq:queue:zomato'
   and are picked first.
2. Restaurant processing jobs (tagged with 'batch_id' in kwargs) are partitioned into
   request-specific Redis subqueues: 'rq:queue:zomato:batch:<batch_id>'.
3. Active batch UUIDs are registered in 'rq:queue:zomato:batches'.
4. Workers execute an atomic Lua script (LUA_FAIR_ROUND_ROBIN_DEQUEUE) that rotates
   the active batch cursor (RPOPLPUSH) in O(1) time (<0.1ms) and pops exactly 1 job
   per batch in round-robin order.
5. Mid-stream incoming requests are appended to the rotation cursor immediately.
6. When a batch is exhausted, it is automatically removed from the active rotation list.
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

LUA_FAIR_ROUND_ROBIN_DEQUEUE = """
-- KEYS[1] = Primary queue key (e.g. 'rq:queue:zomato')
-- KEYS[2] = Active batches list (e.g. 'rq:queue:zomato:batches')
-- ARGV[1] = Subqueue prefix (e.g. 'rq:queue:zomato:batch:')

-- 1. Priority: check primary queue first for URL discovery tasks
local prio_job = redis.call('LPOP', KEYS[1])
if prio_job then
    return {KEYS[1], prio_job}
end

-- 2. Fair Round-Robin across active request subqueues
local num_batches = redis.call('LLEN', KEYS[2])
if num_batches == 0 then
    return nil
end

for i = 1, num_batches do
    -- Rotate the active batch list atomically (Head -> Tail)
    local batch_id = redis.call('RPOPLPUSH', KEYS[2], KEYS[2])
    local subq_key = ARGV[1] .. batch_id
    local job_id = redis.call('LPOP', subq_key)
    if job_id then
        return {KEYS[1], job_id}
    else
        -- Batch has no more jobs remaining: clean it up from active list
        redis.call('LREM', KEYS[2], 0, batch_id)
    end
end

return nil
"""


class FairQueue(DjangoRQ):
    """
    Subclass of DjangoRQ (and RQ Queue) providing fair round-robin scheduling
    across concurrent scrape requests.
    """

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
        Dequeues a job using the atomic Lua round-robin script across active request subqueues.
        Falls back to BLPOP on the primary queue when idle to avoid busy-polling.
        """
        queue_list = list(queues) if queues else []
        if not queue_list:
            return None

        primary_queue = queue_list[0]
        queue_key = primary_queue.key
        batches_key = f"{queue_key}:batches"
        subq_prefix = f"{queue_key}:batch:"

        # Execute atomic Lua Round-Robin pop
        raw_result = connection.eval(
            LUA_FAIR_ROUND_ROBIN_DEQUEUE,
            2,
            queue_key,
            batches_key,
            subq_prefix,
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

        # When queue is completely empty, use BLPOP with timeout on primary queue to prevent CPU spinning
        if timeout and timeout > 0:
            blpop_res = connection.blpop([queue_key], timeout=min(int(timeout), 1))
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
        pipe.sadd(self.redis_queues_keys, self.key)

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
            # Ensure b_id is registered in the active rotation list
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
        """Cleans primary queue list and all batch subqueues."""
        super().empty()
        batches_key = f"{self.key}:batches"
        active_batches = self.connection.lrange(batches_key, 0, -1)
        pipe = self.connection.pipeline()
        for b in active_batches:
            b_str = b.decode("utf-8") if isinstance(b, bytes) else str(b)
            pipe.delete(f"{self.key}:batch:{b_str}")
        pipe.delete(batches_key)
        pipe.execute()

    @classmethod
    def remove_batch(cls, connection: Redis, queue_name: str, batch_id: str) -> None:
        """Removes a batch subqueue and unregisters it from the active rotation list."""
        clean_name = queue_name.replace("rq:queue:", "")
        queue_key = f"rq:queue:{clean_name}"
        batches_key = f"{queue_key}:batches"
        subq_key = f"{queue_key}:batch:{batch_id}"
        pipe = connection.pipeline()
        pipe.lrem(batches_key, 0, batch_id)
        pipe.delete(subq_key)
        pipe.execute()
