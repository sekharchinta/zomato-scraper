"""
failed_jobs.py
Dedicated Failed Jobs Handler:
Monitors, diagnoses, and recovers failed jobs in the single Redis queue ('zomato').
Triggered whenever failed jobs are queued, or when jobs fail during pipeline execution.
Supports secondary requeueing with exponential backoff and permanent failure logging.
"""

import logging
import time
from typing import Any, Dict, List, Optional
import django_rq
from django.conf import settings
from django.utils import timezone
from rq.job import Job
from rq.registry import FailedJobRegistry

from config import SINGLE_QUEUE_NAME
from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch

logger = logging.getLogger("failed_jobs")

MAX_JOB_RETRIES = 3


class FailedJobsHandler:
    """Manages detection, secondary requeue into same queue, and PostgreSQL fallback persistence."""

    def __init__(self, queues: Optional[List[str]] = None) -> None:
        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
        self.queues = queues or [queue_name]

    def get_failed_counts(self) -> Dict[str, int]:
        """Returns the number of failed jobs in FailedJobRegistry for the single queue."""
        counts = {}
        for q_name in self.queues:
            try:
                q = django_rq.get_queue(q_name)
                reg = FailedJobRegistry(queue=q)
                counts[q_name] = reg.count
            except Exception:
                counts[q_name] = 0
        return counts

    def are_all_queues_empty(self) -> bool:
        """Checks if the project queue is currently empty of waiting jobs."""
        for q_name in self.queues:
            try:
                q = django_rq.get_queue(q_name)
                if not q.is_empty():
                    return False
            except Exception:
                pass
        return True

    def process_failed_jobs(self) -> Dict[str, Any]:
        """
        Processes failed jobs in FailedJobRegistry for 'zomato':
        1. If a job has not yet completed a secondary attempt:
           Inspects whether restaurant details or review failed from RestaurantJob.
           Re-enqueues it into the SAME single queue with resume_stage and fresh 3 retries.
        2. If a job fails again after the secondary attempt:
           Persists partial/fallback data in PostgreSQL, marks failed_all_retries,
           dismisses the job from registry, and moves to the next job.
        """
        requeued_jobs: List[str] = []
        fallback_saved_jobs: List[str] = []

        for q_name in self.queues:
            try:
                queue = django_rq.get_queue(q_name)
                registry = FailedJobRegistry(queue=queue)
                job_ids = registry.get_job_ids()

                if not job_ids:
                    continue

                logger.info("Found %d failed job(s) in registry for queue '%s'", len(job_ids), q_name)

                for jid in job_ids:
                    try:
                        job = Job.fetch(jid, connection=queue.connection)
                    except Exception as fetch_err:
                        logger.warning("Could not fetch failed job %s: %s. Removing from registry.", jid, fetch_err)
                        try:
                            registry.remove(jid)
                        except Exception:
                            pass
                        continue

                    # Lookup PostgreSQL job tracking record
                    job_record_id = job.kwargs.get("job_record_id")
                    job_record = None
                    if job_record_id:
                        job_record = RestaurantJob.objects.filter(id=job_record_id).first()
                    else:
                        # Handle URL discovery / extraction job failures
                        batch_id = job.kwargs.get("batch_id")
                        secondary_done = job.meta.get("secondary_attempt_done", False)
                        if not secondary_done:
                            logger.info("[RECOVERY] Re-enqueuing failed URL extraction job %s back to '%s'", job.id, q_name)
                            job.meta["secondary_attempt_done"] = True
                            job.retries_left = 3
                            job.save()
                            try:
                                registry.requeue(job.id)
                                requeued_jobs.append(job.id)
                            except Exception as req_err:
                                logger.error("[RECOVERY] Failed to requeue job %s: %s", job.id, req_err)
                        else:
                            logger.warning("[RECOVERY] URL extraction job %s exhausted all retries. Marking batch as failed.", job.id)
                            if batch_id:
                                ScrapeBatch.objects.filter(requestid=batch_id).update(
                                    status="failed",
                                    error_message="URL extraction job failed after all retries.",
                                    updated_at=timezone.now(),
                                )
                            try:
                                registry.remove(job.id)
                            except Exception:
                                pass
                            fallback_saved_jobs.append(job.id)
                        continue

                    # Determine resume stage
                    resume_stage = "details_failed"
                    secondary_done = job.meta.get("secondary_attempt_done", False)

                    if job_record:
                        if job_record.stage == "reviews_failed":
                            resume_stage = "reviews_failed"
                        elif job_record.stage == "details_failed":
                            resume_stage = "details_failed"
                        secondary_done = secondary_done or job_record.secondary_attempt_done

                    if not secondary_done:
                        # Stage 2: Requeue back into the SAME queue with appropriate resume_stage
                        logger.info(
                            "[RECOVERY] Re-enqueuing failed job %s back to '%s' (resume_stage=%s) for secondary 3-retry cycle",
                            job.id,
                            q_name,
                            resume_stage,
                        )
                        job.meta["secondary_attempt_done"] = True
                        job.kwargs["resume_stage"] = resume_stage
                        job.retries_left = 3
                        job.save()

                        if job_record:
                            job_record.secondary_attempt_done = True
                            job_record.save(update_fields=["secondary_attempt_done", "updated_at"])

                        try:
                            registry.requeue(job.id)
                            requeued_jobs.append(job.id)
                            logger.info("[RECOVERY] Successfully requeued job %s into '%s'", job.id, q_name)
                        except Exception as req_err:
                            logger.error("[RECOVERY] Failed to requeue job %s: %s", job.id, req_err)

                    else:
                        # Stage 3: Job failed again after secondary attempt
                        logger.warning(
                            "[RECOVERY] Job %s exhausted all retries (primary + secondary). "
                            "Persisting extracted/fallback data into PostgreSQL 'zomato_restaurants' and moving to next job.",
                            job.id,
                        )
                        self.handle_final_failure(job, registry, job_record)
                        fallback_saved_jobs.append(job.id)

            except Exception as q_err:
                logger.error("Error inspecting failed registry for queue '%s': %s", q_name, q_err)

        return {
            "requeued_count": len(requeued_jobs),
            "requeued_job_ids": requeued_jobs,
            "fallback_saved_count": len(fallback_saved_jobs),
            "fallback_saved_job_ids": fallback_saved_jobs,
        }

    def process_failed_jobs_for_batch(self, batch_id: str) -> Dict[str, Any]:
        """
        Runs AFTER queue completion of all pushed jobs for a batch:
        Inspects if any jobs failed during the initial run (secondary_attempt_done=False).
        Pushes ONLY the failed URLs into the single queue 'zomato' for workers to process based on status.
        """
        batch = ScrapeBatch.objects.filter(requestid=batch_id).first()
        if not batch:
            return {"status": "error", "message": f"Batch {batch_id} not found", "requeued_count": 0}

        # Check if any pushed jobs are still pending or in progress
        pending_count = RestaurantJob.objects.filter(
            batch=batch,
            stage__in=["pending", "in_progress", "processing_details", "processing_reviews"]
        ).count()

        if pending_count > 0:
            logger.info("[FAILED_JOBS] Batch %s has %d jobs still in flight. Waiting for queue completion.", batch_id, pending_count)
            return {"status": "in_progress", "pending_jobs": pending_count, "requeued_count": 0}

        # Find failed jobs that have not completed secondary attempt
        failed_jobs = RestaurantJob.objects.filter(
            batch=batch,
            stage__in=["details_failed", "reviews_failed", "failed"],
            secondary_attempt_done=False,
        ).order_by("id")

        if not failed_jobs.exists():
            # If all jobs finished (completed or failed_all_retries)
            completed_count = RestaurantJob.objects.filter(batch=batch, stage="completed").count()
            permanently_failed = RestaurantJob.objects.filter(batch=batch, stage="failed_all_retries").count()
            tot = batch.total_restaurants or 1
            if completed_count + permanently_failed >= tot and batch.status != "completed":
                batch.status = "completed"
                batch.progress = 100
                batch.completed_at = timezone.now()
                batch.save(update_fields=["status", "progress", "completed_jobs", "failed_jobs", "completed_at", "updated_at"])
                from scraper_app.producers.producer import complete_or_terminate_batch_and_advance
                complete_or_terminate_batch_and_advance(batch, status="completed", delay_seconds=1.0)
            return {"status": "completed", "requeued_count": 0}

        queue_name = self.queues[0]
        queue = django_rq.get_queue(queue_name)
        from scraper_app.tasks.restaurant_extractor import process_restaurant_pipeline_job
        from scraper_app.tasks.base import DEFAULT_RETRY

        jobs_list = list(failed_jobs)
        prepared_data_list = []
        for job_record in jobs_list:
            resume_stage = "reviews_failed" if job_record.stage == "reviews_failed" else "details_failed"
            job_record.secondary_attempt_done = True
            job_record.stage = "pending"

            job_data = queue.prepare_data(
                process_restaurant_pipeline_job,
                kwargs={
                    "batch_id": str(batch.requestid),
                    "job_record_id": job_record.id,
                    "restaurant_url": job_record.page_url,
                    "name": job_record.name,
                    "city": batch.city,
                    "location": batch.location,
                    "location_key": batch.location_key,
                    "max_review_pages": 10,
                    "resume_stage": resume_stage,
                    "retry": DEFAULT_RETRY,
                },
                timeout=getattr(settings, "RQ_DEFAULT_TIMEOUT", 360),
                result_ttl=getattr(settings, "RQ_RESULT_TTL", 500),
            )
            prepared_data_list.append(job_data)

        # Push all failed jobs at once in a single atomic batch
        enqueued_rq_jobs = queue.enqueue_many(prepared_data_list)
        now_ts = timezone.now()
        for job_rec, rq_job in zip(jobs_list, enqueued_rq_jobs):
            job_rec.rq_job_id = rq_job.id
            job_rec.updated_at = now_ts

        RestaurantJob.objects.bulk_update(jobs_list, ["secondary_attempt_done", "stage", "rq_job_id", "updated_at"], batch_size=500)
        requeued = [j.id for j in jobs_list]

        logger.info(
            "[FAILED_JOBS] Initial queue run complete for batch %s. Pushed %d failed jobs into '%s' based on status.",
            batch_id,
            len(requeued),
            queue_name,
        )

        return {"status": "requeued", "requeued_count": len(requeued), "requeued_job_ids": requeued}

    def save_fallback_record(
        self,
        batch: ScrapeBatch,
        job_record: RestaurantJob,
        error: str = "",
    ) -> None:
        """Persists partial/fallback restaurant record into PostgreSQL 'zomato_restaurants' table."""
        res_id = job_record.restaurant_id or (abs(hash(job_record.page_url or job_record.name)) % (10**9))
        try:
            RestaurantRecord.objects.update_or_create(
                restaurant_id=res_id,
                defaults={
                    "requestid": batch,
                    "name": job_record.name or "Restaurant",
                    "city": batch.city,
                    "location": batch.location,
                    "location_key": batch.location_key,
                    "pageurl": job_record.page_url,
                    "reviews": [],
                    "total_reviews": 0,
                    "raw_data": {"error": error, "status": "failed_all_retries"},
                    "delivery_document": {"error": error, "status": "failed_all_retries"},
                },
            )
        except Exception as db_err:
            logger.error("[FAILED_JOBS] Error persisting fallback record to PostgreSQL: %s", db_err)

    def handle_final_failure(
        self,
        job: Job,
        registry: Optional[FailedJobRegistry] = None,
        job_record: Optional[RestaurantJob] = None,
    ) -> None:
        """
        Gracefully handles final job failure:
        1. Persists partial / fallback data into PostgreSQL 'zomato_restaurants'.
        2. Updates ScrapeBatch metrics in PostgreSQL.
        3. Removes job from FailedJobRegistry.
        """
        kwargs = job.kwargs or {}
        batch_id = kwargs.get("batch_id")
        restaurant_id = kwargs.get("restaurant_id") or (job_record.restaurant_id if job_record else None)
        name = kwargs.get("name") or (job_record.name if job_record else "Unknown Restaurant")
        city = kwargs.get("city", "")
        location = kwargs.get("location", "")
        location_key = kwargs.get("location_key", "")
        page_url = kwargs.get("restaurant_url") or (job_record.page_url if job_record else "")

        if not restaurant_id:
            restaurant_id = abs(hash(page_url or name)) % (10**9)

        # Save partial/fallback data in PostgreSQL
        try:
            RestaurantRecord.objects.update_or_create(
                restaurant_id=restaurant_id,
                defaults={
                    "batch_id": batch_id,
                    "name": name,
                    "city": city,
                    "location": location,
                    "location_key": location_key,
                    "page_url": page_url,
                    "reviews": [],
                    "total_reviews": 0,
                    "status": "failed_all_retries",
                    "details_data": {"error": str(job.exc_info)},
                    "raw_data": {"error": str(job.exc_info), "status": "failed_all_retries"},
                    "scraped_at": timezone.now(),
                },
            )
        except Exception as db_err:
            logger.error("Error saving fallback record to PostgreSQL: %s", db_err)

        if job_record:
            job_record.stage = "failed_all_retries"
            job_record.error_message = str(job.exc_info or "Failed all primary and secondary retries")
            job_record.save(update_fields=["stage", "error_message", "updated_at"])

        if batch_id:
            try:
                batch = ScrapeBatch.objects.filter(requestid=batch_id).first()
                if batch:
                    batch.failed_jobs += 1
                    done = batch.completed_jobs + batch.failed_jobs
                    total = max(batch.total_restaurants, 1)
                    batch.progress = min(100, int((done / total) * 100))
                    batch.save(update_fields=["failed_jobs", "progress", "updated_at"])
                    if done >= total:
                        if batch.status != "completed":
                            from scraper_app.producers.producer import complete_or_terminate_batch_and_advance
                            complete_or_terminate_batch_and_advance(batch, status="completed", delay_seconds=5.0)
                        else:
                            try:
                                from scraper_app.alerts import check_and_alert_batch_status
                                check_and_alert_batch_status(batch)
                            except Exception as alert_err:
                                logger.warning("[SLACK_ALERT] Error checking alerts on batch completion: %s", alert_err)
            except Exception:
                pass

        if registry:
            try:
                registry.remove(job.id)
                logger.info("Dismissed final failed job %s from FailedJobRegistry.", job.id)
            except Exception as rm_err:
                logger.warning("Error removing final failed job %s from registry: %s", job.id, rm_err)


def handle_failed_jobs(
    queue_name: Optional[str] = None,
    batch_id: Optional[str] = None,
    max_retries: int = MAX_JOB_RETRIES,
) -> Dict[str, Any]:
    """
    Scans the FailedJobRegistry and 'zomato_jobs' table for failed restaurant jobs.
    Uses FailedJobsHandler to requeue recoverable jobs or persist fallbacks.
    """
    target_queue = queue_name or getattr(settings, "SINGLE_QUEUE_NAME", SINGLE_QUEUE_NAME)
    handler = FailedJobsHandler(queues=[target_queue])
    return handler.process_failed_jobs()


def rq_job_failure_handler(job: Job, *exc_info) -> bool:
    """Custom RQ failure handler hooked into worker error lifecycle."""
    logger.warning("[FAILED_JOBS] Custom RQ failure handler triggered for job %s: %s", job.id, exc_info[1] if exc_info else "")
    return True
