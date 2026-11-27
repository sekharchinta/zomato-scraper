"""
tests/test_dynamic_per_user_queues.py

Comprehensive Test Suite for Dynamic Per-User Dedicated Redis Queues
with Automatic Lifecycle Deletion.

Verifies:
1. Dynamic Queue Creation: Dedicated queue 'rq:queue:user_<id>' created for each user.
2. Multi-User Registration: Active users tracked in 'rq:active_user_queues' and 'rq:queues'.
3. Fair Interleaved Worker Dispatch: Jobs strictly alternate round-robin across users (A1 -> B1 -> A2 -> B2).
4. Automatic Lifecycle Deletion: User queue automatically deleted from Redis RAM and RQ Dashboard
   immediately when all jobs are completed.
5. Independent Processing: When one user completes, other users continue processing without interruption.
6. Termination Cleanup: Terminating/deleting a batch immediately purges the user's queue.
"""

import time
import uuid
from django.contrib.auth.models import User
from django.test import TestCase
import django_rq
from redis import Redis
from rq.job import Job, JobStatus

from scraper_app.models import ScrapeBatch, RestaurantJob, UserProfile
from scraper_app.workers.fair_queue import (
    FairQueue,
    ACTIVE_USERS_KEY,
    GLOBAL_QUEUES_KEY,
)
from scraper_app.producers.producer import push_urls_to_queue


def dummy_job_func(x):
    return x * 2


class DynamicPerUserQueueLifecycleTests(TestCase):
    def setUp(self):
        self.conn: Redis = django_rq.get_connection("zomato")
        # Clear any queues left by earlier test classes on isolated test Redis DB (never touch live DB 1)
        if int(self.conn.connection_pool.connection_kwargs.get("db", 1)) != 1:
            self.conn.flushdb()

        # Create two distinct test users
        self.user_a = User.objects.create_user(username="alice", email="alice@example.com", password="password123")
        self.profile_a = UserProfile.objects.create(user=self.user_a, google_id="google_alice_101")

        self.user_b = User.objects.create_user(username="bob", email="bob@example.com", password="password123")
        self.profile_b = UserProfile.objects.create(user=self.user_b, google_id="google_bob_202")

        FairQueue.delete_user_queue(self.conn, self.user_a.id)
        FairQueue.delete_user_queue(self.conn, self.user_b.id)

        self.batch_a = ScrapeBatch.objects.create(
            user=self.user_a,
            city="Hyderabad",
            location="Banjara Hills",
            status="processing_restaurants",
            location_key="banjara_hills_hyderabad",
        )

        self.batch_b = ScrapeBatch.objects.create(
            user=self.user_b,
            city="Bangalore",
            location="Indiranagar",
            status="processing_restaurants",
            location_key="indiranagar_bangalore",
        )

    def tearDown(self):
        FairQueue.delete_user_queue(self.conn, self.user_a.id)
        FairQueue.delete_user_queue(self.conn, self.user_b.id)

    def test_dynamic_per_user_queue_creation_and_registration(self):
        """Phase 1 & 2: Creating and enqueuing jobs into dedicated user queues."""
        queue_a = FairQueue.for_user(self.user_a, connection=self.conn)
        self.assertEqual(queue_a.name, f"user_{self.user_a.id}")
        self.assertEqual(queue_a.key, f"rq:queue:user_{self.user_a.id}")

        # Enqueue priority job for User A
        job_a1 = queue_a.enqueue(dummy_job_func, 10)
        self.assertIsNotNone(job_a1)

        # Verify User A is registered in active user queues list and global queues set (with rq:queue: key prefix)
        active_users = FairQueue.get_active_user_queues(self.conn)
        self.assertIn(queue_a.name, active_users)
        self.assertTrue(self.conn.sismember(GLOBAL_QUEUES_KEY, queue_a.key))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, queue_a.name))

        # Enqueue priority job for User B
        queue_b = FairQueue.for_user(self.user_b, connection=self.conn)
        job_b1 = queue_b.enqueue(dummy_job_func, 20)
        self.assertIsNotNone(job_b1)

        # Verify both User A and User B are active
        active_users = FairQueue.get_active_user_queues(self.conn)
        self.assertIn(queue_a.name, active_users)
        self.assertIn(queue_b.name, active_users)
        self.assertEqual(len(active_users), 2)

    def test_fair_interleaved_multi_user_round_robin_dispatch(self):
        """Phase 3: Worker dispatch strictly alternates 1-by-1 between User A and User B."""
        queue_a = FairQueue.for_user(self.user_a, connection=self.conn)
        queue_b = FairQueue.for_user(self.user_b, connection=self.conn)

        # Enqueue 3 batch jobs for User A
        datas_a = [
            queue_a.prepare_data(dummy_job_func, args=(i,), kwargs={"batch_id": str(self.batch_a.requestid)})
            for i in range(3)
        ]
        queue_a.enqueue_many(datas_a)

        # Enqueue 3 batch jobs for User B
        datas_b = [
            queue_b.prepare_data(dummy_job_func, args=(i + 10,), kwargs={"batch_id": str(self.batch_b.requestid)})
            for i in range(3)
        ]
        queue_b.enqueue_many(datas_b)

        # Dequeue 6 jobs and verify alternating pattern (A -> B -> A -> B -> A -> B)
        dequeued_queues = []
        for _ in range(6):
            res = FairQueue.dequeue_any([queue_a], timeout=0, connection=self.conn)
            self.assertIsNotNone(res)
            job, popped_queue = res
            dequeued_queues.append(popped_queue.name)

        # Count 3 for each user and verify interleaving
        self.assertEqual(dequeued_queues.count(queue_a.name), 3)
        self.assertEqual(dequeued_queues.count(queue_b.name), 3)

        # Verify no two consecutive jobs belong to the same user (unless other user is empty)
        for i in range(len(dequeued_queues) - 1):
            self.assertNotEqual(dequeued_queues[i], dequeued_queues[i + 1])

    def test_automatic_lifecycle_queue_deletion_on_completion(self):
        """Phase 4 & 5: When User A finishes, queue is deleted from Redis, while User B continues."""
        queue_a = FairQueue.for_user(self.user_a, connection=self.conn)
        queue_b = FairQueue.for_user(self.user_b, connection=self.conn)

        # User A has 1 job; User B has 2 jobs
        datas_a = [
            queue_a.prepare_data(dummy_job_func, args=(1,), kwargs={"batch_id": str(self.batch_a.requestid)})
        ]
        queue_a.enqueue_many(datas_a)

        datas_b = [
            queue_b.prepare_data(dummy_job_func, args=(i,), kwargs={"batch_id": str(self.batch_b.requestid)})
            for i in range(2)
        ]
        queue_b.enqueue_many(datas_b)

        # Pop 1st job -> User A
        res1 = FairQueue.dequeue_any([queue_a], timeout=0, connection=self.conn)
        self.assertIsNotNone(res1)

        # Pop 2nd job -> User B
        res2 = FairQueue.dequeue_any([queue_a], timeout=0, connection=self.conn)
        self.assertIsNotNone(res2)

        # At this point, User A has 0 jobs remaining!
        # The next dequeue cycle triggers the automatic cleanup in Lua
        res3 = FairQueue.dequeue_any([queue_a], timeout=0, connection=self.conn)
        self.assertIsNotNone(res3)
        self.assertEqual(res3[1].name, queue_b.name)  # Must be User B's 2nd job

        # Verify User A's queue key is completely deleted from Redis RAM
        self.assertFalse(self.conn.exists(queue_a.key))
        self.assertFalse(self.conn.exists(f"{queue_a.key}:batches"))

        # Verify User A is removed from active user queues and RQ dashboard
        active_users = FairQueue.get_active_user_queues(self.conn)
        self.assertNotIn(queue_a.name, active_users)
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, queue_a.name))

        # But User B is still active or finishing cleanly
        # Now dequeue when empty
        res_empty = FairQueue.dequeue_any([queue_a], timeout=0, connection=self.conn)
        self.assertIsNone(res_empty)

        # After empty pop, User B is also automatically pruned!
        self.assertFalse(self.conn.exists(queue_b.key))
        self.assertFalse(self.conn.exists(f"{queue_b.key}:batches"))
        self.assertNotIn(queue_b.name, FairQueue.get_active_user_queues(self.conn))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, queue_b.name))

    def test_instant_queue_deletion_on_batch_removal(self):
        """Phase 6: Removing/terminating a batch flushes and deletes the user queue."""
        queue_a = FairQueue.for_user(self.user_a, connection=self.conn)
        datas_a = [
            queue_a.prepare_data(dummy_job_func, args=(i,), kwargs={"batch_id": str(self.batch_a.requestid)})
            for i in range(5)
        ]
        queue_a.enqueue_many(datas_a)

        self.assertTrue(self.conn.exists(queue_a.key))
        self.assertEqual(self.conn.llen(queue_a.key), 5)
        self.assertIn(queue_a.name, FairQueue.get_active_user_queues(self.conn))

        # Remove the batch jobs
        FairQueue.remove_batch(
            self.conn,
            queue_a.name,
            str(self.batch_a.requestid),
            user_id_or_user=self.user_a.id,
        )

        # Queue should be purged and unregistered immediately
        self.assertEqual(self.conn.llen(queue_a.key), 0)
        self.assertFalse(self.conn.exists(queue_a.key))
        self.assertNotIn(queue_a.name, FairQueue.get_active_user_queues(self.conn))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, queue_a.name))

    def test_producer_routes_to_dedicated_user_queue_and_auto_cleans(self):
        """Phase 7: End-to-end Producer dispatch and URL push to user's dedicated queue."""
        from scraper_app.producers.producer import dispatch_scrape_request

        res = dispatch_scrape_request(
            city="Hyderabad",
            location_raw="Banjara Hills",
            user=self.user_a,
        )

        expected_queue_name = f"user_{self.user_a.id}"
        self.assertEqual(res["status"], "queued")
        self.assertEqual(res["queue"], expected_queue_name)

        # Verify User A's dedicated queue is registered in Redis and RQ Dashboard (with rq:queue: prefix)
        self.assertIn(expected_queue_name, FairQueue.get_active_user_queues(self.conn))
        self.assertTrue(self.conn.sismember(GLOBAL_QUEUES_KEY, f"rq:queue:{expected_queue_name}"))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, expected_queue_name))

        batch = ScrapeBatch.objects.get(batch_id=res["batch_id"])
        self.assertEqual(batch.user, self.user_a)

        # Create 2 restaurant jobs and push them
        job1 = RestaurantJob.objects.create(batch=batch, name="Rest 1", page_url="http://zomato.com/rest1", stage="pending")
        job2 = RestaurantJob.objects.create(batch=batch, name="Rest 2", page_url="http://zomato.com/rest2", stage="pending")

        push_res = push_urls_to_queue(str(batch.requestid))
        self.assertEqual(push_res["enqueued_jobs"], 2)

        queue_a = FairQueue.for_user(self.user_a, connection=self.conn)
        self.assertEqual(queue_a.count, 3)  # 1 url_extractor job + 2 restaurant jobs

        # Dequeue all 3 jobs
        for _ in range(3):
            popped = FairQueue.dequeue_any([queue_a], timeout=0, connection=self.conn)
            self.assertIsNotNone(popped)
            self.assertEqual(popped[1].name, expected_queue_name)

        # Now queue is completely empty -> verify auto-cleanup
        self.assertFalse(self.conn.exists(queue_a.key))
        self.assertFalse(self.conn.exists(f"{queue_a.key}:batches"))
        self.assertNotIn(expected_queue_name, FairQueue.get_active_user_queues(self.conn))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, f"rq:queue:{expected_queue_name}"))
        self.assertFalse(self.conn.sismember(GLOBAL_QUEUES_KEY, expected_queue_name))

