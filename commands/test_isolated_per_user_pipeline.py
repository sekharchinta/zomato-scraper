"""
commands/test_isolated_per_user_pipeline.py

End-to-End Verification for Isolated Per-User Concurrent Pipelines:
1. UserProducer: Scoped batch dispatch, URL pushing, and user-isolated batch advancement.
2. FairQueue (isolated=True): Dedicated workers never pop another user's jobs.
3. UserURLExtractor: Concurrent per-user URL discovery with separate ZomatoScraper sessions.
4. FailedJobsHandler: Dynamic 'user_<id>' queue resolution and user-scoped recovery.
5. MultiUserWorkerSupervisor + UserWorkerPool: Real OS worker process pools spawned
   concurrently per user, executing exclusively their user's jobs with disjoint PIDs,
   followed by automatic worker shutdown and Redis queue deletion.
"""

import os
import threading
import time
from unittest.mock import MagicMock, patch

import django_rq
from django.contrib.auth.models import User
from django.test import TransactionTestCase
from redis import Redis
from rq.registry import FailedJobRegistry

from scraper_app.models import RestaurantJob, ScrapeBatch, UserProfile
from scraper_app.producers.producer import UserProducer
from scraper_app.tasks.url_extractor import UserURLExtractor
from scraper_app.workers.failed_jobs import FailedJobsHandler
from scraper_app.workers.fair_queue import (
    ACTIVE_USERS_KEY,
    GLOBAL_QUEUES_KEY,
    FairQueue,
)
from scraper_app.workers.worker import MultiUserWorkerSupervisor


def concurrent_worker_pid_task(redis_result_key: str, label: str) -> str:
    """Real picklable RQ task executed by spawned UserWorkerPool child OS processes."""
    conn = django_rq.get_connection("zomato")
    pid = os.getpid()
    conn.rpush(redis_result_key, f"{label}:{pid}")
    time.sleep(0.05)
    return f"{label}:{pid}"


def failing_sample_task(batch_id: str = "", job_record_id: int = 0) -> None:
    raise RuntimeError("Simulated scrape failure for retry test")


class TestIsolatedPerUserPipeline(TransactionTestCase):
    def setUp(self):
        self.conn: Redis = django_rq.get_connection("zomato")
        self._cleanup_redis()

        User.objects.filter(username__in=["iso_alice", "iso_bob"]).delete()
        self.user_a = User.objects.create_user(username="iso_alice", email="iso_alice@example.com", password="pw")
        UserProfile.objects.create(user=self.user_a, google_id="g_iso_alice")

        self.user_b = User.objects.create_user(username="iso_bob", email="iso_bob@example.com", password="pw")
        UserProfile.objects.create(user=self.user_b, google_id="g_iso_bob")

    def tearDown(self):
        self._cleanup_redis()

    def _cleanup_redis(self):
        for q_name in FairQueue.get_active_user_queues(self.conn):
            if q_name.startswith("user_"):
                FairQueue.delete_user_queue(self.conn, q_name)
        for uid in [getattr(getattr(self, "user_a", None), "id", None), getattr(getattr(self, "user_b", None), "id", None)]:
            if uid:
                FairQueue.delete_user_queue(self.conn, uid)
        self.conn.delete("test:iso:results_a", "test:iso:results_b")

    def test_01_user_producer_isolation_and_scoped_batch_advancement(self):
        """UserProducer completing User A's batch must advance ONLY User A's queued batch, never User B's."""
        prod_a = UserProducer(user=self.user_a)
        batch_a_active = ScrapeBatch.objects.create(
            user=self.user_a,
            city="Hyderabad",
            location="Madhapur",
            status="processing_restaurants",
        )
        batch_a_next = ScrapeBatch.objects.create(
            user=self.user_a,
            city="Hyderabad",
            location="Gachibowli",
            status="queued",
        )
        batch_b_queued = ScrapeBatch.objects.create(
            user=self.user_b,
            city="Bengaluru",
            location="Koramangala",
            status="queued",
        )

        # Even if User B's queued batch was created earlier or concurrently, User A advancing must skip User B
        ScrapeBatch.objects.filter(pk=batch_b_queued.pk).update(created_at=batch_a_next.created_at)

        next_started = prod_a.complete_or_terminate_batch_and_advance(batch_a_active, status="completed")
        self.assertIsNotNone(next_started)
        self.assertEqual(next_started.requestid, batch_a_next.requestid)

        batch_a_next.refresh_from_db()
        batch_b_queued.refresh_from_db()
        self.assertEqual(batch_a_next.status, "extracting_urls")
        self.assertEqual(batch_b_queued.status, "queued")

        # Verify the job was pushed strictly into User A's queue
        q_a = FairQueue.for_user(self.user_a, connection=self.conn)
        q_b = FairQueue.for_user(self.user_b, connection=self.conn)
        self.assertEqual(q_a.count, 1)
        self.assertEqual(q_b.count, 0)

    def test_02_isolated_dequeue_prevents_cross_user_job_stealing(self):
        """Workers with isolated=True pop ONLY from their assigned user queue, never from another user."""
        q_a_iso = FairQueue.for_user(self.user_a, connection=self.conn, isolated=True)
        q_b_iso = FairQueue.for_user(self.user_b, connection=self.conn, isolated=True)

        batch_a = ScrapeBatch.objects.create(user=self.user_a, city="Hyderabad", location="A")
        batch_b = ScrapeBatch.objects.create(user=self.user_b, city="Mumbai", location="B")

        q_a_iso.enqueue_many([
            q_a_iso.prepare_data(concurrent_worker_pid_task, args=("test:iso:results_a", "A"), kwargs={"batch_id": str(batch_a.requestid)})
        ])
        q_b_iso.enqueue_many([
            q_b_iso.prepare_data(concurrent_worker_pid_task, args=("test:iso:results_b", "B"), kwargs={"batch_id": str(batch_b.requestid)}),
            q_b_iso.prepare_data(concurrent_worker_pid_task, args=("test:iso:results_b", "B"), kwargs={"batch_id": str(batch_b.requestid)}),
        ])

        # User A's isolated worker pops User A's 1 job
        pop1 = FairQueue.dequeue_any([q_a_iso], timeout=0, connection=self.conn)
        self.assertIsNotNone(pop1)
        self.assertEqual(pop1[1].name, q_a_iso.name)

        # Next pop by User A's isolated worker MUST return None (even though User B still has 2 jobs!)
        pop2 = FairQueue.dequeue_any([q_a_iso], timeout=0, connection=self.conn)
        self.assertIsNone(pop2)
        self.assertEqual(q_b_iso.count, 2)

        # User B's isolated worker pops both of User B's jobs cleanly
        pop_b1 = FairQueue.dequeue_any([q_b_iso], timeout=0, connection=self.conn)
        pop_b2 = FairQueue.dequeue_any([q_b_iso], timeout=0, connection=self.conn)
        self.assertIsNotNone(pop_b1)
        self.assertIsNotNone(pop_b2)
        self.assertEqual(pop_b1[1].name, q_b_iso.name)
        self.assertEqual(pop_b2[1].name, q_b_iso.name)
        self.assertEqual(q_b_iso.count, 0)

    def test_03_concurrent_user_url_extractors_isolated_sessions_and_queues(self):
        """User A and User B UserURLExtractors run concurrently in separate threads with isolated scrapers."""
        batch_a = ScrapeBatch.objects.create(user=self.user_a, city="Hyderabad", location="Banjara Hills", location_key="hyd_bh")
        batch_b = ScrapeBatch.objects.create(user=self.user_b, city="Bengaluru", location="Indiranagar", location_key="blr_ind")

        def make_fake_scraper(city_label, rest_slug):
            scraper = MagicMock()
            scraper.config.request_timeout = 5
            scraper.ensure_csrf_token.return_value = f"csrf_{city_label}"
            scraper.build_restaurants_payload.return_value = {"context": "delivery"}
            scraper.session.cookies = MagicMock()
            scraper.resolve_location.return_value = {
                "city_id": "6" if city_label == "Hyderabad" else "4",
                "entity_id": "6" if city_label == "Hyderabad" else "4",
                "entity_type": "city",
                "delivery_subzone_id": 100,
                "latitude": 17.41,
                "longitude": 78.44,
            }
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {
                "sections": {
                    "SECTION_SEARCH_RESULT": [
                        {"info": {"resId": 101 if city_label == "Hyderabad" else 202, "name": f"{city_label} Kitchen", "url": f"https://www.zomato.com{rest_slug}"}}
                    ],
                    "SECTION_SEARCH_META_INFO": {"searchMetaData": {"hasMore": False, "totalResults": 1}},
                }
            }
            scraper.session.post.return_value = resp
            return scraper

        scraper_map = {
            "Hyderabad": make_fake_scraper("Hyderabad", "/hyderabad/hyd-kitchen/info"),
            "Bengaluru": make_fake_scraper("Bengaluru", "/bangalore/blr-kitchen/info"),
        }
        lock = threading.Lock()
        call_order = []

        def fake_get_active_scraper():
            with lock:
                key = "Hyderabad" if "Hyderabad" not in call_order else "Bengaluru"
                call_order.append(key)
                return scraper_map[key]

        results = {}
        errors = []

        def run_extractor(user_obj, batch_obj, city_name, loc_name):
            try:
                extractor = UserURLExtractor(batch_id=str(batch_obj.requestid), user_id=user_obj.id)
                results[user_obj.id] = extractor.run(
                    city=city_name,
                    locations_list=[{"entity_title": loc_name, "latitude": 17.41, "longitude": 78.44}],
                    location_raw=loc_name,
                    location_key=batch_obj.location_key,
                    location_title=loc_name,
                    max_pages=1,
                )
            except Exception as exc:
                errors.append(exc)

        with patch("scraper_app.tasks.url_extractor.get_active_scraper", side_effect=fake_get_active_scraper), \
             patch("scraper_app.tasks.url_extractor.fetch_city_html_fallback", return_value=[]):
            t_a = threading.Thread(target=run_extractor, args=(self.user_a, batch_a, "Hyderabad", "Banjara Hills"))
            t_b = threading.Thread(target=run_extractor, args=(self.user_b, batch_b, "Bengaluru", "Indiranagar"))
            t_a.start()
            t_b.start()
            t_a.join(timeout=15)
            t_b.join(timeout=15)

        self.assertEqual(errors, [])
        self.assertEqual(results[self.user_a.id]["status"], "success")
        self.assertEqual(results[self.user_b.id]["status"], "success")
        self.assertEqual(results[self.user_a.id]["dispatch"]["queue"], f"user_{self.user_a.id}")
        self.assertEqual(results[self.user_b.id]["dispatch"]["queue"], f"user_{self.user_b.id}")
        self.assertEqual(RestaurantJob.objects.filter(batch=batch_a).count(), 1)
        self.assertEqual(RestaurantJob.objects.filter(batch=batch_b).count(), 1)

    def test_04_failed_jobs_handler_scoped_to_dynamic_user_queues(self):
        """FailedJobsHandler resolves 'user_<id>' without KeyError and isolates recovery per user."""
        q_a = FairQueue.for_user(self.user_a, connection=self.conn)
        q_b = FairQueue.for_user(self.user_b, connection=self.conn)

        batch_a = ScrapeBatch.objects.create(user=self.user_a, city="Hyderabad", location="A")
        batch_b = ScrapeBatch.objects.create(user=self.user_b, city="Mumbai", location="B")
        job_rec_a = RestaurantJob.objects.create(batch=batch_a, name="Rest A", page_url="/a/info", stage="details_failed")
        job_rec_b = RestaurantJob.objects.create(batch=batch_b, name="Rest B", page_url="/b/info", stage="details_failed")

        rq_job_a = q_a.enqueue(failing_sample_task, batch_id=str(batch_a.requestid), job_record_id=job_rec_a.id)
        rq_job_b = q_b.enqueue(failing_sample_task, batch_id=str(batch_b.requestid), job_record_id=job_rec_b.id)

        # Pop and place directly into each queue's FailedJobRegistry
        q_a.remove(rq_job_a)
        q_b.remove(rq_job_b)
        reg_a = FailedJobRegistry(queue=q_a)
        reg_b = FailedJobRegistry(queue=q_b)
        reg_a.add(rq_job_a, ttl=500, exc_string="boom A")
        reg_b.add(rq_job_b, ttl=500, exc_string="boom B")

        handler_a = FailedJobsHandler(queues=[q_a.name])
        self.assertEqual(handler_a.get_failed_counts()[q_a.name], 1)
        res = handler_a.process_failed_jobs()

        self.assertEqual(res["requeued_count"], 1)
        self.assertEqual(reg_a.count, 0)
        self.assertEqual(reg_b.count, 1)  # User B's failed registry remains untouched
        reg_b.remove(rq_job_b)

    def test_05_multi_user_worker_supervisor_spawns_separate_os_pools_concurrently(self):
        """
        Real OS multiprocessing test:
        Spawns separate UserWorkerPool OS processes for User A and User B simultaneously,
        executes jobs in parallel with disjoint worker PIDs, and auto-deletes both user queues.
        """
        res_key_a = "test:iso:results_a"
        res_key_b = "test:iso:results_b"
        q_a = FairQueue.for_user(self.user_a, connection=self.conn)
        q_b = FairQueue.for_user(self.user_b, connection=self.conn)

        for i in range(4):
            q_a.enqueue(concurrent_worker_pid_task, res_key_a, f"A{i}")
            q_b.enqueue(concurrent_worker_pid_task, res_key_b, f"B{i}")

        supervisor = MultiUserWorkerSupervisor(
            workers=4,
            workers_per_user=2,
            fallback_workers=0,
            max_total_workers=10,
            burst=True,
            idle_grace_cycles=0,
        )

        try:
            supervisor.spawn_workers()
            # Separate dedicated UserWorkerPools must exist for BOTH users simultaneously
            self.assertIn(q_a.name, supervisor.user_pools)
            self.assertIn(q_b.name, supervisor.user_pools)
            self.assertEqual(len(supervisor.user_pools[q_a.name].processes), 2)
            self.assertEqual(len(supervisor.user_pools[q_b.name].processes), 2)

            supervisor.run_supervisor_loop()
        finally:
            try:
                supervisor.shutdown()
            except Exception:
                pass

        # Verify all 4 jobs completed for each user
        raw_a = [x.decode("utf-8") if isinstance(x, bytes) else str(x) for x in self.conn.lrange(res_key_a, 0, -1)]
        raw_b = [x.decode("utf-8") if isinstance(x, bytes) else str(x) for x in self.conn.lrange(res_key_b, 0, -1)]
        self.assertEqual(len(raw_a), 4)
        self.assertEqual(len(raw_b), 4)

        # Verify strictly separate OS PIDs processed User A vs User B!
        pids_a = {entry.split(":")[1] for entry in raw_a}
        pids_b = {entry.split(":")[1] for entry in raw_b}
        self.assertTrue(pids_a.isdisjoint(pids_b), f"Expected disjoint worker PIDs, got A={pids_a}, B={pids_b}")

        # Verify automatic lifecycle deletion of both user queues from Redis
        self.assertFalse(self.conn.exists(q_a.key))
        self.assertFalse(self.conn.exists(q_b.key))
        self.assertNotIn(q_a.name, FairQueue.get_active_user_queues(self.conn))
        self.assertNotIn(q_b.name, FairQueue.get_active_user_queues(self.conn))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, q_a.key))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, q_b.key))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, q_a.name))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, q_b.name))

    def test_06_rq_dashboard_keys_live_worker_survival_and_no_double_enqueue(self):
        """
        Verifies:
        1. rq:queues stores valid 'rq:queue:<name>' keys so rq.Queue.all() / rq-dashboard never crash with 500.
        2. FairQueue.dequeue_any(..., timeout=5) raises DequeueTimeout so burst=False live workers stay alive >2s.
        3. start_queued_batch does not double-enqueue extract_urls_from_latlongs after dispatch_scrape_request.
        """
        from rq import Queue
        from rq.exceptions import DequeueTimeout
        from scraper_app.producers.producer import dispatch_scrape_request, start_queued_batch
        from scraper_app.workers.worker import UserWorkerPool

        # 1. Dispatch request and verify start_queued_batch does not double-enqueue
        res = dispatch_scrape_request(
            city="Hyderabad",
            location_raw="Banjara Hills",
            user=self.user_a,
        )
        batch = ScrapeBatch.objects.get(batch_id=res["batch_id"])
        q_a = FairQueue.for_user(self.user_a, connection=self.conn, isolated=True)
        self.assertEqual(q_a.count, 1)

        dup_res = start_queued_batch(batch)
        self.assertEqual(dup_res["status"], "success")
        self.assertEqual(q_a.count, 1)  # Must still be 1 job, NOT 2!

        # 2. Verify rq:queues has valid 'rq:queue:...' key so rq.Queue.all() succeeds (no ValueError/500)
        self.assertTrue(self.conn.sismember(GLOBAL_QUEUES_KEY, q_a.key))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, q_a.name))
        # Clean any stale non-prefixed keys and verify Queue.all()
        for qb in self.conn.smembers(GLOBAL_QUEUES_KEY):
            s = qb.decode("utf-8") if isinstance(qb, bytes) else str(qb)
            if not s.startswith("rq:queue:"):
                self.conn.srem(GLOBAL_QUEUES_KEY, qb)
        all_queues = Queue.all(connection=self.conn)
        self.assertTrue(any(q.name == q_a.name for q in all_queues))

        # 3. Verify DequeueTimeout raised when isolated queue is empty with timeout > 0
        q_b_empty = FairQueue.for_user(self.user_b, connection=self.conn, isolated=True)
        with self.assertRaises(DequeueTimeout):
            FairQueue.dequeue_any([q_b_empty], timeout=5, connection=self.conn)

        # 4. Verify live burst=False worker stays alive for >2s when idle and executes jobs arriving later
        pool_b = UserWorkerPool(queue_name=q_b_empty.name, num_workers=1, burst=False, isolated=True)
        try:
            pool_b.start()
            time.sleep(2.2)
            self.assertEqual(pool_b.active_worker_count(), 1, "Live burst=False worker exited prematurely!")

            q_b_empty.enqueue(concurrent_worker_pid_task, "test:iso:results_b", "LIVE")
            deadline = time.time() + 5.0
            while time.time() < deadline and self.conn.llen("test:iso:results_b") == 0:
                time.sleep(0.1)
            self.assertEqual(self.conn.llen("test:iso:results_b"), 1)
        finally:
            pool_b.shutdown(cleanup_queue=True, connection=self.conn)

