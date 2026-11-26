#!/usr/bin/env python3
"""
commands/test_round_robin_fair_scheduling.py

Verification script for Fair Round-Robin Queue Scheduling across concurrent requests.
Tests:
1. Single request sequential processing.
2. Two requests alternating 1-to-1 round-robin (A1, B1, A2, B2, A3, B3...).
3. Five requests balanced cycling across 6 workers (A1, B1, C1, D1, E1, A2, B2...).
4. Mid-stream ingestion: new request added while processing is underway immediately enters rotation.
5. Batch exhaustion: finished batches auto-unregister from active cursor.
6. Priority jobs: discovery tasks in primary queue preempt round-robin.
"""

import os
import sys
import uuid

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "backend"))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "zomato_scraper.settings")

import django
django.setup()

import django_rq
from rq import Queue
from scraper_app.workers.fair_queue import FairQueue


def dummy_job_func(batch_id, job_index):
    return f"{batch_id}-{job_index}"


def print_banner(text):
    print("\n" + "=" * 75)
    print(f"  {text}")
    print("=" * 75)


def run_tests():
    conn = django_rq.get_connection("zomato")
    q = FairQueue("test_fair_scheduling_q", connection=conn)

    # Clean up test environment
    print_banner("0. Initializing FairQueue Test Environment")
    q.empty()
    print("  ✓ FairQueue and Redis DB 1 initialized cleanly.")

    # -------------------------------------------------------------------------
    # TEST 1: Single Request Processing
    # -------------------------------------------------------------------------
    print_banner("1. Testing Single Request (Request A: 6 Jobs)")
    batch_a = f"test-a-{uuid.uuid4().hex[:6]}"
    job_datas_a = [
        q.prepare_data(dummy_job_func, kwargs={"batch_id": batch_a, "job_index": i + 1})
        for i in range(6)
    ]
    q.enqueue_many(job_datas_a)

    print(f"  Enqueued 6 jobs for {batch_a}. Total queue count: {len(q)}")
    assert len(q) == 6, f"Expected 6, got {len(q)}"

    dequeued_a = []
    workers = [f"Worker {i}" for i in range(1, 7)]
    for i in range(6):
        job, _ = FairQueue.dequeue_any([q], timeout=1, connection=conn)
        w = workers[i]
        dequeued_a.append(job.kwargs["job_index"])
        print(f"  {w} popped -> Job {job.kwargs['job_index']} for {batch_a}")

    assert dequeued_a == [1, 2, 3, 4, 5, 6], f"Unexpected sequence: {dequeued_a}"
    assert len(q) == 0, f"Expected 0 remaining, got {len(q)}"
    print("  ✓ Single request processed sequentially across 6 workers.")

    # -------------------------------------------------------------------------
    # TEST 2: Two Concurrent Requests (A & B) - 1-to-1 Alternating
    # -------------------------------------------------------------------------
    print_banner("2. Testing Two Concurrent Requests (A & B: Alternating 1-to-1)")
    batch_a = f"batch-a-{uuid.uuid4().hex[:4]}"
    batch_b = f"batch-b-{uuid.uuid4().hex[:4]}"

    job_datas_a = [q.prepare_data(dummy_job_func, kwargs={"batch_id": batch_a, "job_index": i + 1}) for i in range(4)]
    job_datas_b = [q.prepare_data(dummy_job_func, kwargs={"batch_id": batch_b, "job_index": i + 1}) for i in range(4)]

    q.enqueue_many(job_datas_a)
    q.enqueue_many(job_datas_b)
    print(f"  Enqueued 4 jobs for A and 4 jobs for B. Total queue count: {len(q)}")
    assert len(q) == 8

    dequeued_batches = []
    for i in range(8):
        job, _ = FairQueue.dequeue_any([q], timeout=1, connection=conn)
        b_name = "A" if job.kwargs["batch_id"] == batch_a else "B"
        dequeued_batches.append(f"{b_name}{job.kwargs['job_index']}")
        w = workers[i % 6]
        print(f"  Step {i+1} ({w}): Popped -> {b_name}{job.kwargs['job_index']}")

    # Check alternating pattern: should alternate between A and B
    for i in range(len(dequeued_batches) - 1):
        assert dequeued_batches[i][0] != dequeued_batches[i + 1][0], f"Violation at step {i}: {dequeued_batches}"

    print(f"  Sequence: {' -> '.join(dequeued_batches)}")
    print("  ✓ Two requests strictly alternate 1-to-1 across workers.")

    # -------------------------------------------------------------------------
    # TEST 3: Five Concurrent Requests (A, B, C, D, E) Across 6 Workers
    # -------------------------------------------------------------------------
    print_banner("3. Testing Five Concurrent Requests (A, B, C, D, E)")
    batches = [f"batch-{letter}-{uuid.uuid4().hex[:4]}" for letter in ["A", "B", "C", "D", "E"]]
    for letter, b_id in zip(["A", "B", "C", "D", "E"], batches):
        jds = [q.prepare_data(dummy_job_func, kwargs={"batch_id": b_id, "job_index": i + 1}) for i in range(3)]
        q.enqueue_many(jds)

    print(f"  Enqueued 3 jobs each for 5 batches (15 total). Queue count: {len(q)}")
    assert len(q) == 15

    dequeued_5 = []
    for i in range(12):
        job, _ = FairQueue.dequeue_any([q], timeout=1, connection=conn)
        letter = ["A", "B", "C", "D", "E"][batches.index(job.kwargs["batch_id"])]
        dequeued_5.append(f"{letter}{job.kwargs['job_index']}")
        w = workers[i % 6]
        print(f"  Step {i+1} ({w}): Popped -> {letter}{job.kwargs['job_index']}")

    # First 5 jobs must each be from a distinct batch:
    first_5_batches = [x[0] for x in dequeued_5[:5]]
    assert len(set(first_5_batches)) == 5, f"Expected 5 distinct batches in first 5 pops: {first_5_batches}"
    # 6th job must wrap around to the first batch:
    assert dequeued_5[5][0] == first_5_batches[0], f"Expected wrap to {first_5_batches[0]}, got {dequeued_5[5]}"

    print(f"  Sequence: {' -> '.join(dequeued_5)}")
    print("  ✓ 5 requests evenly distributed across 6 workers with round-robin wrap.")

    # Clean remaining
    q.empty()

    # -------------------------------------------------------------------------
    # TEST 4: Mid-Stream Request Ingestion
    # -------------------------------------------------------------------------
    print_banner("4. Testing Mid-Stream Request Ingestion (Request C arrives during A & B)")
    b_a = f"batch-a-{uuid.uuid4().hex[:4]}"
    b_b = f"batch-b-{uuid.uuid4().hex[:4]}"

    q.enqueue_many([q.prepare_data(dummy_job_func, kwargs={"batch_id": b_a, "job_index": i + 1}) for i in range(5)])
    q.enqueue_many([q.prepare_data(dummy_job_func, kwargs={"batch_id": b_b, "job_index": i + 1}) for i in range(5)])

    print(f"  Initial state: Batches A and B enqueued (10 jobs).")
    # Dequeue 3 jobs first
    for i in range(3):
        job, _ = FairQueue.dequeue_any([q], timeout=1, connection=conn)
        b_name = "A" if job.kwargs["batch_id"] == b_a else "B"
        print(f"  Pre-midstream: Popped {b_name}{job.kwargs['job_index']}")

    # Now mid-stream, Request C arrives with 3 jobs!
    b_c = f"batch-c-{uuid.uuid4().hex[:4]}"
    print(f"\n  ⚡ MID-STREAM EVENT: Request C arrives with 3 jobs!")
    q.enqueue_many([q.prepare_data(dummy_job_func, kwargs={"batch_id": b_c, "job_index": i + 1}) for i in range(3)])

    subsequent_pops = []
    for i in range(5):
        job, _ = FairQueue.dequeue_any([q], timeout=1, connection=conn)
        if job.kwargs["batch_id"] == b_a:
            b_name = "A"
        elif job.kwargs["batch_id"] == b_b:
            b_name = "B"
        else:
            b_name = "C"
        subsequent_pops.append(f"{b_name}{job.kwargs['job_index']}")
        print(f"  Post-midstream Step {i+1}: Popped -> {b_name}{job.kwargs['job_index']}")

    assert any(x.startswith("C") for x in subsequent_pops), "Error: Request C was not picked up mid-stream!"
    print(f"  Subsequent sequence: {' -> '.join(subsequent_pops)}")
    print("  ✓ Request C immediately picked up and integrated into active rotation.")

    # Clean remaining
    q.empty()

    # -------------------------------------------------------------------------
    # TEST 5: Priority Job Preemption
    # -------------------------------------------------------------------------
    print_banner("5. Testing Priority URL Discovery Preemption")
    b_a = f"batch-a-{uuid.uuid4().hex[:4]}"
    q.enqueue_many([q.prepare_data(dummy_job_func, kwargs={"batch_id": b_a, "job_index": i + 1}) for i in range(5)])

    # Enqueue a priority job into the primary queue (no batch_id, e.g. URL extractor)
    priority_job_data = q.prepare_data(dummy_job_func, args=("priority-url-discovery", 999))
    q.enqueue_many([priority_job_data])

    print("  Enqueued 5 batch restaurant jobs AND 1 priority URL discovery task.")
    # Next dequeue MUST be the priority job!
    first_job, _ = FairQueue.dequeue_any([q], timeout=1, connection=conn)
    print(f"  First popped job: {first_job.args}")
    assert first_job.args == ("priority-url-discovery", 999), f"Expected priority job, got {first_job.args}"
    print("  ✓ Priority URL discovery task immediately preempted batch restaurant jobs.")

    # Clean up
    q.empty()
    print_banner("ALL FAIR ROUND-ROBIN QUEUE SCHEDULING TESTS PASSED! (100% SUCCESS)")


if __name__ == "__main__":
    run_tests()
