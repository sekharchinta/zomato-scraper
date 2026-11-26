"""
Integration & Unit Tests for Zomato-Scraper Django Application.
Tests Redis Single Queue, Streamlined Worker Tasks, PostgreSQL Persistence,
UUID Batch Tracking, Retry & Requeue Mechanisms, Caching, and Export APIs.
Zero MongoDB, Zero SQLite - All tests run with PostgreSQL schema 'zomato'.
"""

import json
import uuid
from unittest.mock import MagicMock, patch
from django.conf import settings
from django.contrib.auth.models import User
from django.test import Client, SimpleTestCase, TestCase
from django.utils import timezone
import django_rq

from config import SINGLE_QUEUE_NAME
from scraper_app.models import (
    RestaurantJob,
    RestaurantRecord,
    ScrapeBatch,
    generate_location_key,
)
from scraper_app.parsers.parser import (
    DEFAULT_FIELDS,
    build_delivery_document,
    extract_total_reviews_from_restaurant_data,
    parse_numeric_metric,
)
from scraper_app.workers.worker import (
    generate_review_api_urls,
    process_restaurant_pipeline_job,
)


class TestMetricsAndParser(TestCase):
    def test_parse_numeric_metric(self):
        self.assertEqual(parse_numeric_metric("16.2K"), 16200)
        self.assertEqual(parse_numeric_metric("639.8K"), 639800)
        self.assertEqual(parse_numeric_metric("1.5M"), 1500000)
        self.assertEqual(parse_numeric_metric("500"), 500)
        self.assertEqual(parse_numeric_metric(1234), 1234)
        self.assertEqual(parse_numeric_metric(None), 0)

    def test_extract_total_reviews_from_ratings(self):
        detail_data = {
            "page_data": {
                "sections": {
                    "SECTION_BASIC_INFO": {
                        "name": "Test Biryani Palace",
                        "rating_new": {
                            "ratings": {
                                "DINING": {"reviewCount": "10K", "votes": 10000},
                                "DELIVERY": {"reviewCount": "25.5K"},
                            }
                        }
                    }
                }
            }
        }
        total = extract_total_reviews_from_restaurant_data(detail_data)
        self.assertEqual(total, 35500)

    def test_build_delivery_document_includes_total_reviews(self):
        detail_data = {
            "restaurant_id": 99901,
            "name": "Paradise Cafe",
            "city": "Hyderabad",
            "location": "Secunderabad",
            "address": "MG Road",
            "phone_numbers": "040-12345678",
            "timing": "10am - 11pm",
            "rating": "4.3",
            "page_data": {
                "sections": {
                    "SECTION_BASIC_INFO": {
                        "name": "Paradise Cafe",
                        "rating_new": {
                            "ratings": {
                                "DINING": {"reviewCount": "5K"},
                                "DELIVERY": {"reviewCount": "15K"},
                            }
                        }
                    }
                }
            }
        }
        reviews_data = {
            "restaurant_id": 99901,
            "reviews": [
                {"user_name": "Alice", "rating": "5", "review_text": "Great place", "like_count": 3}
            ]
        }
        doc = build_delivery_document(
            detail_data=detail_data,
            reviews_data=reviews_data,
            restaurant_id=99901,
            request_id="req_test_001",
        )
        self.assertIsNotNone(doc)
        self.assertEqual(doc["restaurant_id"], 99901)
        self.assertIn("total_reviews", doc)
        self.assertEqual(doc["total_reviews"], 1)
        self.assertEqual(len(doc["reviews"]), 1)
        self.assertEqual(doc["reviews"][0]["user_name"], "Alice")


class TestReviewApiGenerationAndPostgresPipeline(TestCase):
    def test_tasks_extractor_imports(self):
        """Verifies restaurant and review extraction logic is located in the tasks package."""
        from scraper_app.tasks.restaurant_extractor import (
            extract_restaurant_details,
            process_restaurant_pipeline_job as pipeline_job_from_task,
        )
        from scraper_app.tasks.review_extractor import (
            extract_restaurant_reviews,
            generate_review_api_urls as rev_urls_from_task,
        )
        from scraper_app.tasks import (
            extract_restaurant_details as exp_res_details,
            extract_restaurant_reviews as exp_res_reviews,
            process_restaurant_pipeline_job as exp_pipeline_job,
            generate_review_api_urls as exp_rev_urls,
        )
        self.assertIsNotNone(extract_restaurant_details)
        self.assertIsNotNone(pipeline_job_from_task)
        self.assertIsNotNone(extract_restaurant_reviews)
        self.assertIsNotNone(rev_urls_from_task)
        self.assertEqual(pipeline_job_from_task, exp_pipeline_job)
        self.assertEqual(rev_urls_from_task, exp_rev_urls)

    def test_generate_review_api_urls(self):
        """Verifies review API URLs generation from restaurant ID and clean URL path."""
        res_info = generate_review_api_urls(
            restaurant_id=18438922,
            page_url="https://www.zomato.com/hyderabad/bawarchi-rtc-x-roads/order",
            page=2,
            sort="dd",
            filter_type="reviews-dining",
        )
        self.assertEqual(res_info["res_id"], 18438922)
        self.assertEqual(res_info["clean_base"], "/hyderabad/bawarchi-rtc-x-roads")
        self.assertEqual(res_info["reviews_path"], "/hyderabad/bawarchi-rtc-x-roads/reviews")
        self.assertIn("webroutes/reviews/loadMore", res_info["load_more_url"])
        self.assertEqual(res_info["load_more_params"]["res_id"], 18438922)
        self.assertEqual(res_info["load_more_params"]["page"], 2)
        self.assertIn("webroutes/getPage", res_info["get_page_url"])
        self.assertIn("/hyderabad/bawarchi-rtc-x-roads/reviews?page=2", res_info["get_page_params"]["page_url"])

    def test_combined_postgres_persistence(self):
        """Verifies RestaurantRecord stores combined hotel details, reviews, and 32 delivery fields."""
        b_uuid = uuid.uuid4()
        batch = ScrapeBatch.objects.create(
            batch_id=b_uuid,
            city="Hyderabad",
            location="Madhapur",
            location_key="loc_hyderabad_madhapur",
            status="in_progress",
        )
        rec, created = RestaurantRecord.objects.update_or_create(
            restaurant_id=888123,
            defaults={
                "batch": batch,
                "name": "Combined Biryani Point",
                "city": "Hyderabad",
                "location": "Madhapur",
                "location_key": "loc_hyderabad_madhapur",
                "page_url": "/hyderabad/combined-biryani-point",
                "total_reviews": 2,
                "reviews": [
                    {"user_name": "Bob", "rating": "4.5", "review_text": "Spicy and fresh!"},
                    {"user_name": "Carol", "rating": "5.0", "review_text": "Super tasty!"},
                ],
                "delivery_document": {
                    "restaurant_id": 888123,
                    "name": "Combined Biryani Point",
                    "city": "Hyderabad",
                    "location": "Madhapur",
                    "rating": "4.5",
                    "total_reviews": 2,
                },
                "status": "completed",
            }
        )
        self.assertTrue(created)
        self.assertEqual(rec.restaurant_id, 888123)
        self.assertEqual(rec.total_reviews, 2)
        self.assertEqual(len(rec.reviews), 2)

        # Query using Django model directly
        records = RestaurantRecord.get_delivery_records(query={"restaurant_id": 888123})
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "Combined Biryani Point")
        self.assertEqual(records[0]["total_reviews"], 2)


class TestReactSPAView(SimpleTestCase):
    def setUp(self):
        self.client = Client()

    def test_react_spa_view(self):
        """Verifies that the root URL serves the compiled React Single Page Application."""
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(b"root", resp.content)
        self.assertIn(b"assets/index", resp.content)


class TestAPIsAndModels(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = User.objects.create_user(username="api_test_user", email="api_test_user@example.com")
        self.client.force_login(self.user)

    def test_schema_fields_api(self):
        resp = self.client.get("/api/get-schema-fields/")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "success")
        self.assertIn("total_reviews", data["default_fields"])
        self.assertIn("reviews", data["all_fields"])
        self.assertEqual(data["example_format"]["restaurant_id"], 18438922)

    def test_live_scraping_and_model_access(self):
        """Verifies that start-scrape performs live scraping and records are accessed via models."""
        test_res_id = 777001
        b_uuid = uuid.uuid4()
        batch = ScrapeBatch.objects.create(
            batch_id=b_uuid,
            city="Hyderabad",
            location="Banjara Hills",
            location_key="loc_hyderabad_banjara_hills",
            status="completed",
            progress=100,
            total_restaurants=1,
            completed_jobs=1,
        )
        RestaurantRecord.objects.update_or_create(
            restaurant_id=test_res_id,
            defaults={
                "batch": batch,
                "name": "Live Kebab Hub",
                "city": "Hyderabad",
                "location": "Banjara Hills",
                "location_key": "loc_hyderabad_banjara_hills",
                "page_url": "/hyderabad/live-kebab-hub",
                "total_reviews": 54,
                "rating": "4.4",
                "delivery_document": {
                    "restaurant_id": test_res_id,
                    "name": "Live Kebab Hub",
                    "city": "Hyderabad",
                    "location": "Banjara Hills",
                    "rating": "4.4",
                    "total_reviews": 54,
                },
            }
        )

        req_id = str(b_uuid)
        data_resp = self.client.get(f"/api/scrape-data/{req_id}/?fields=name,rating,total_reviews")
        self.assertEqual(data_resp.status_code, 200)
        records = data_resp.json()["data"]
        self.assertTrue(any(r.get("name") == "Live Kebab Hub" for r in records))

        # Test CSV export
        csv_resp = self.client.get(f"/api/export-csv/{req_id}/?fields=name,rating,total_reviews")
        self.assertEqual(csv_resp.status_code, 200)
        self.assertEqual(csv_resp["Content-Type"], "text/csv")
        content = csv_resp.content.decode("utf-8")
        self.assertIn("total_reviews", content)
        self.assertIn("Live Kebab Hub", content)

    def test_collection_stats_api(self):
        resp = self.client.get("/api/collection-stats/")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "success")
        self.assertIn("restaurants_urls", data["stats"])
        self.assertIn("delivery_data", data["stats"])
        self.assertIn("total_batches", data["stats"])

    def test_isolated_queues_and_db_configuration(self):
        """Verifies strictly ONE Redis queue ('zomato') is configured on Redis DB 1."""
        expected_queues = {"zomato"}
        self.assertEqual(set(settings.RQ_QUEUES.keys()), expected_queues)
        self.assertNotIn("default", settings.RQ_QUEUES)
        self.assertNotIn("zomato_single_queue", settings.RQ_QUEUES)
        self.assertNotIn("locations_queue", settings.RQ_QUEUES)
        self.assertNotIn("restaurants_queue", settings.RQ_QUEUES)
        self.assertNotIn("reviews_queue", settings.RQ_QUEUES)
        for q_name, q_cfg in settings.RQ_QUEUES.items():
            self.assertEqual(q_cfg["DB"], 1)

    def test_generate_location_key(self):
        """Verifies deterministic unique key generation for PostgreSQL caching."""
        k1 = generate_location_key("Hyderabad", "Banjara Hills")
        self.assertEqual(k1, "loc_hyderabad_banjara_hills")

        k2 = generate_location_key("Hyderabad", "ALL")
        self.assertEqual(k2, "loc_hyderabad_all")

        k3 = generate_location_key("Bengaluru", {"entity_id": 304, "entity_title": "Indiranagar"})
        self.assertEqual(k3, "loc_bengaluru_indiranagar_304")

        k4 = generate_location_key("Mumbai", {"entity_type": "all", "entity_title": "All Locations"})
        self.assertEqual(k4, "loc_mumbai_all")

    def test_scrape_status_api_not_found_handling(self):
        """Verifies querying a non-existent batch returns 404 with active/queued recovery info."""
        non_existent_uuid = str(uuid.uuid4())
        resp = self.client.get(f"/api/scrape-status/{non_existent_uuid}/")
        self.assertEqual(resp.status_code, 404)
        data = resp.json()
        self.assertEqual(data["status"], "not_found")
        self.assertIn("active_running_batch", data)
        self.assertIn("queued_batches", data)

        # Create a running batch and test that not_found response provides active running batch info
        running_uuid = uuid.uuid4()
        ScrapeBatch.objects.create(
            batch_id=running_uuid,
            city="Hyderabad",
            location="Jubilee Hills",
            status="processing_restaurants",
            progress=50,
            total_restaurants=10,
            completed_jobs=5,
        )
        resp2 = self.client.get(f"/api/scrape-status/{non_existent_uuid}/")
        self.assertEqual(resp2.status_code, 404)
        data2 = resp2.json()
        self.assertEqual(data2["status"], "not_found")
        self.assertIsNotNone(data2["active_running_batch"])
        self.assertEqual(data2["active_running_batch"]["batch_id"], str(running_uuid))
        self.assertEqual(data2["active_running_batch"]["status"], "processing_restaurants")


class TestRetryAndFailedJobsRecovery(TestCase):
    def setUp(self):
        q = django_rq.get_queue(SINGLE_QUEUE_NAME)
        q._commit_mode = "auto"
        q.empty()

    def tearDown(self):
        django_rq.get_queue(SINGLE_QUEUE_NAME).empty()

    def test_producer_enqueues_tasks_with_3_retries(self):
        from scraper_app.producers.producer import enqueue_pipeline_restaurant_job

        b_id = str(uuid.uuid4())
        job = enqueue_pipeline_restaurant_job(
            batch_id=b_id,
            job_record_id=1,
            restaurant_url="/hyderabad/test-rest",
            name="Test Rest",
        )
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.retry_intervals, [1, 2, 4])

    def test_failed_jobs_handler_secondary_requeue_and_fallback(self):
        from rq.job import Job
        from rq.registry import FailedJobRegistry
        from scraper_app.tasks.base import DEFAULT_RETRY
        from scraper_app.workers.failed_jobs import FailedJobsHandler

        test_res_id = 999333
        b_uuid = uuid.uuid4()
        batch = ScrapeBatch.objects.create(
            batch_id=b_uuid,
            city="Hyderabad",
            location="Ameerpet",
            location_key="loc_hyderabad_ameerpet",
            total_restaurants=1,
            status="in_progress",
        )

        job_rec = RestaurantJob.objects.create(
            batch=batch,
            restaurant_id=test_res_id,
            name="Ameerpet Cafe",
            page_url="/hyderabad/ameerpet-cafe",
            stage="reviews_failed",
            last_review_page=1,
        )

        q = django_rq.get_queue(SINGLE_QUEUE_NAME)
        reg = FailedJobRegistry(queue=q)
        for old_jid in reg.get_job_ids():
            reg.remove(old_jid)

        # 1. Create Job directly and add to FailedJobRegistry
        job = Job.create(
            process_restaurant_pipeline_job,
            kwargs={
                "batch_id": str(b_uuid),
                "job_record_id": job_rec.id,
                "restaurant_url": "/hyderabad/ameerpet-cafe",
                "name": "Ameerpet Cafe",
                "city": "Hyderabad",
                "location": "Ameerpet",
                "location_key": "loc_hyderabad_ameerpet",
                "resume_stage": "reviews_failed",
            },
            connection=q.connection,
            retry=DEFAULT_RETRY,
        )
        job.origin = SINGLE_QUEUE_NAME
        job.save()

        reg.add(job, exc_string="Simulated initial 3-retry failure on review page 2")
        self.assertIn(job.id, reg.get_job_ids())

        handler = FailedJobsHandler(queues=[SINGLE_QUEUE_NAME])

        # Stage 2: Process failed jobs -> should requeue into same queue with resume_stage
        res1 = handler.process_failed_jobs()
        self.assertEqual(res1["requeued_count"], 1)
        self.assertIn(job.id, res1["requeued_job_ids"])
        job.refresh()
        self.assertTrue(job.meta.get("secondary_attempt_done"))
        self.assertEqual(job.retries_left, 3)
        self.assertEqual(job.kwargs.get("resume_stage"), "reviews_failed")

        # Stage 3: Job fails again on secondary attempt -> lands in FailedJobRegistry again
        reg.add(job, exc_string="Simulated secondary 3-retry failure")

        # Process failed jobs again -> should recognize secondary_attempt_done, save fallback into PostgreSQL, and dismiss
        res2 = handler.process_failed_jobs()
        self.assertEqual(res2["fallback_saved_count"], 1)
        self.assertIn(job.id, res2["fallback_saved_job_ids"])

        # Job is dismissed from registry
        self.assertNotIn(job.id, reg.get_job_ids())

        # Verify fallback data persisted in PostgreSQL 'zomato_restaurants'
        pg_rec = RestaurantRecord.objects.filter(restaurant_id=test_res_id).first()
        self.assertIsNotNone(pg_rec)
        self.assertEqual(pg_rec.status, "failed_all_retries")
        self.assertEqual(pg_rec.reviews, [])

        # Batch progress updated
        batch.refresh_from_db()
        self.assertEqual(batch.failed_jobs, 1)


class TestLocationSearchLive(TestCase):
    """Verifies live location search 401 resolution, session warming, and preloaded state fallback (No caching)."""

    def setUp(self):
        self.user = User.objects.create_user(username="loc_user", email="loc@example.com")

    @patch("scraper.fetch_city_html_fallback")
    @patch("requests.Session.get")
    def test_location_search_api_401_falls_back_to_html_preloaded(self, mock_get, mock_html_fallback):
        """When Search API returns 401, system falls back to HTML preloaded state and extracts real coordinates."""
        mock_resp_401 = MagicMock()
        mock_resp_401.status_code = 401
        mock_get.return_value = mock_resp_401

        mock_html_fallback.return_value = [
            {
                "entity_id": 12131,
                "entity_type": "city",
                "entity_title": "Amalapuram",
                "entity_name": "Amalapuram, India",
                "latitude": 16.57917,
                "longitude": 82.007288,
            }
        ]

        from django.test import RequestFactory
        from scraper_app.views import search_locations_api

        rf = RequestFactory()
        req = rf.get("/api/search-locations/?query=amalapuram")
        req.user = self.user
        resp = search_locations_api(req)

        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.content.decode("utf-8"))
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["city"], "amalapuram")
        self.assertEqual(len(data["suggestions"]), 1)
        self.assertEqual(data["suggestions"][0]["entity_title"], "Amalapuram")
        self.assertEqual(data["suggestions"][0]["latitude"], 16.57917)
        self.assertEqual(data["suggestions"][0]["longitude"], 82.007288)

    @patch("scraper.fetch_city_html_fallback")
    @patch("requests.Session.get")
    def test_location_search_fallback_returns_fallback_record(self, mock_get, mock_html_fallback):
        """If a search query fails completely, returns safe fallback object."""
        mock_resp_404 = MagicMock()
        mock_resp_404.status_code = 404
        mock_get.return_value = mock_resp_404
        mock_html_fallback.return_value = []

        from django.test import RequestFactory
        from scraper_app.views import search_locations_api

        rf = RequestFactory()
        req = rf.get("/api/search-locations/?query=completely_unknown_place")
        req.user = self.user
        resp = search_locations_api(req)

        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.content.decode("utf-8"))
        self.assertEqual(data["status"], "success")
        self.assertIsNone(data["suggestions"][0]["latitude"])


class TestUrlExtractionAndSingleQueuePipeline(TestCase):
    """Verifies that URL extraction runs entirely through the single queue 'zomato'."""

    def setUp(self):
        self.client = Client()
        self.user = User.objects.create_user(username="queue_test_user", email="queue_test_user@example.com")
        self.client.force_login(self.user)
        q = django_rq.get_queue(SINGLE_QUEUE_NAME)
        q._commit_mode = "auto"
        q.empty()

    def tearDown(self):
        django_rq.get_queue(SINGLE_QUEUE_NAME).empty()

    def test_dispatch_scrape_enqueues_url_extraction_task(self):
        from scraper_app.producers.producer import dispatch_scrape_request
        from rq.job import Job

        result = dispatch_scrape_request(
            city="Hyderabad",
            location_raw="Banjara Hills",
        )
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["queue"], "zomato")
        self.assertIn("batch_id", result)
        self.assertIn("rq_job_id", result)

        q = django_rq.get_queue("zomato")
        job = Job.fetch(result["rq_job_id"], connection=q.connection)
        self.assertEqual(job.func_name, "scraper_app.tasks.url_extractor.extract_urls_from_latlongs")
        self.assertEqual(job.kwargs["city"], "Hyderabad")

        # Verify batch created in PostgreSQL
        batch = ScrapeBatch.objects.get(batch_id=result["batch_id"])
        self.assertEqual(batch.status, "queued")
        self.assertEqual(batch.city, "Hyderabad")

    def test_direct_restaurant_url_enqueues_pipeline_job(self):
        from scraper_app.producers.producer import dispatch_scrape_request
        from rq.job import Job

        result = dispatch_scrape_request(
            city="Hyderabad",
            restaurant_url="/hyderabad/bawarchi-rtc-x-roads/info",
        )
        self.assertEqual(result["status"], "queued")
        self.assertEqual(result["total_restaurants"], 1)
        self.assertIn("rq_job_id", result)

        q = django_rq.get_queue("zomato")
        job = Job.fetch(result["rq_job_id"], connection=q.connection)
        self.assertIn(job.func_name, [
            "scraper_app.tasks.restaurant_extractor.process_restaurant_pipeline_job",
            "scraper_app.workers.worker.process_restaurant_pipeline_job",
        ])
        self.assertEqual(job.kwargs["restaurant_url"], "/hyderabad/bawarchi-rtc-x-roads/info")

    @patch("scraper_app.tasks.url_extractor.get_active_scraper")
    def test_extract_location_urls_task_enqueues_restaurants_into_zomato(self, mock_get_scraper):
        from scraper_app.tasks.url_extractor import extract_urls_from_latlongs as extract_location_urls_task
        from rq.job import Job

        # Mock scraper
        mock_scraper = MagicMock()
        mock_scraper.resolve_location.return_value = {
            "entity_id": "123",
            "entity_type": "subzone",
            "city_name": "Hyderabad",
            "location_name": "Banjara Hills",
            "exact_location_url": "https://www.zomato.com/hyderabad/restaurants",
        }
        mock_scraper.ensure_csrf_token.return_value = "fake_csrf"
        mock_scraper.build_restaurants_payload.return_value = {}

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "sections": {
                "SECTION_SEARCH_RESULT": [
                    {
                        "restaurant": {"id": 101, "name": "Biryani Point"},
                        "page_url": "/hyderabad/biryani-point/info",
                    },
                    {
                        "restaurant": {"id": 102, "name": "Kebab Corner"},
                        "page_url": "/hyderabad/kebab-corner/info",
                    },
                ],
                "SECTION_SEARCH_META_INFO": {"hasMore": False},
            }
        }
        mock_scraper.session.post.return_value = mock_resp
        mock_get_scraper.return_value = mock_scraper

        # Create batch
        b_uuid = uuid.uuid4()
        batch = ScrapeBatch.objects.create(
            batch_id=b_uuid,
            city="Hyderabad",
            location="Banjara Hills",
            status="queued",
        )

        res = extract_location_urls_task(
            batch_id=str(b_uuid),
            city="Hyderabad",
            location_title="Banjara Hills",
            locations_list=[{"entity_title": "Banjara Hills"}],
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["total_urls"], 2)

        # Verify RestaurantJob rows created in PostgreSQL
        jobs = RestaurantJob.objects.filter(batch=batch)
        self.assertEqual(jobs.count(), 2)
        q = django_rq.get_queue("zomato")
        for j in jobs:
            self.assertTrue(j.rq_job_id)
            job = Job.fetch(j.rq_job_id, connection=q.connection)
            self.assertIn(job.func_name, [
            "scraper_app.tasks.restaurant_extractor.process_restaurant_pipeline_job",
            "scraper_app.workers.worker.process_restaurant_pipeline_job",
        ])

    @patch("scraper_app.tasks.url_extractor.get_active_scraper")
    def test_extract_location_urls_task_multi_page_pagination(self, mock_get_scraper):
        """Verifies extract_location_urls_task paginates beyond page 1 when hasMore is True in searchMetaData."""
        from scraper_app.tasks.url_extractor import extract_urls_from_latlongs as extract_location_urls_task

        mock_scraper = MagicMock()
        mock_scraper.resolve_location.return_value = {
            "entity_id": "123",
            "entity_type": "subzone",
            "city_name": "Hyderabad",
            "location_name": "Banjara Hills",
            "exact_location_url": "https://www.zomato.com/hyderabad/restaurants",
        }
        mock_scraper.ensure_csrf_token.return_value = "fake_csrf"
        mock_scraper.build_restaurants_payload.return_value = {}

        # Page 1 response: 2 restaurants, searchMetaData with hasMore=True
        mock_resp1 = MagicMock()
        mock_resp1.status_code = 200
        mock_resp1.json.return_value = {
            "sections": {
                "SECTION_SEARCH_RESULT": [
                    {"restaurant": {"id": 201, "name": "Bawarchi"}, "page_url": "/hyderabad/bawarchi/info"},
                    {"restaurant": {"id": 202, "name": "Paradise"}, "page_url": "/hyderabad/paradise/info"},
                ],
                "SECTION_SEARCH_META_INFO": {
                    "searchMetaData": {
                        "hasMore": True,
                        "totalResults": 50,
                        "postbackParams": '{"solr_offset": 12, "page": 1}',
                    }
                },
            }
        }

        # Page 2 response: 2 more restaurants, searchMetaData with hasMore=False
        mock_resp2 = MagicMock()
        mock_resp2.status_code = 200
        mock_resp2.json.return_value = {
            "sections": {
                "SECTION_SEARCH_RESULT": [
                    {"restaurant": {"id": 203, "name": "Shah Ghouse"}, "page_url": "/hyderabad/shah-ghouse/info"},
                    {"restaurant": {"id": 204, "name": "Pista House"}, "page_url": "/hyderabad/pista-house/info"},
                ],
                "SECTION_SEARCH_META_INFO": {
                    "searchMetaData": {
                        "hasMore": False,
                        "totalResults": 50,
                        "postbackParams": '{"solr_offset": 24, "page": 2}',
                    }
                },
            }
        }

        mock_scraper.session.post.side_effect = [mock_resp1, mock_resp2]
        mock_get_scraper.return_value = mock_scraper

        b_uuid = uuid.uuid4()
        batch = ScrapeBatch.objects.create(
            batch_id=b_uuid,
            city="Hyderabad",
            location="Banjara Hills",
            status="queued",
        )

        res = extract_location_urls_task(
            batch_id=str(b_uuid),
            city="Hyderabad",
            location_title="Banjara Hills",
            locations_list=[{"entity_title": "Banjara Hills"}],
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["total_urls"], 4)

        jobs = RestaurantJob.objects.filter(batch=batch)
        self.assertEqual(jobs.count(), 4)
        job_names = set(jobs.values_list("name", flat=True))
        self.assertEqual(job_names, {"Bawarchi", "Paradise", "Shah Ghouse", "Pista House"})

    @patch("scraper_app.tasks.url_extractor.get_active_scraper")
    def test_extract_location_urls_strict_locality_filtering(self, mock_get_scraper):
        """Verifies strict locality filtering drops delivery-radius restaurants from other areas and does not fallback."""
        from scraper_app.tasks.url_extractor import extract_urls_from_latlongs as extract_location_urls_task

        mock_scraper = MagicMock()
        mock_scraper.resolve_location.return_value = {
            "entity_id": "999",
            "entity_type": "subzone",
            "city_name": "Palakollu",
            "location_name": "Someswara Agraharam",
            "exact_location_url": "https://www.zomato.com/palakollu/restaurants",
        }
        mock_scraper.ensure_csrf_token.return_value = "fake_csrf"
        mock_scraper.build_restaurants_payload.return_value = {}

        # Mock API returning restaurants belonging strictly to 'Palakollu Locality'
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "sections": {
                "SECTION_SEARCH_RESULT": [
                    {
                        "restaurant": {"id": 1001, "name": "Central Tiffins"},
                        "info": {
                            "resId": 1001,
                            "name": "Central Tiffins",
                            "locality": {
                                "name": "Palakollu Locality, Palakollu",
                                "address": "Palakol Town, Palakollu Locality",
                                "localityUrl": "palakollu/palakollu-locality-restaurants",
                            },
                        },
                        "page_url": "/palakollu/central-tiffins-palakollu-locality/info",
                    },
                    {
                        "restaurant": {"id": 1002, "name": "Brodipeta Sweets"},
                        "info": {
                            "resId": 1002,
                            "name": "Brodipeta Sweets",
                            "locality": {
                                "name": "Brodipeta, Palakollu",
                                "address": "Brodipeta Main Road, Palakollu",
                                "localityUrl": "palakollu/brodipeta-restaurants",
                            },
                        },
                        "page_url": "/palakollu/brodipeta-sweets-brodipeta/info",
                    },
                ],
                "SECTION_SEARCH_META_INFO": {"hasMore": False},
            }
        }
        mock_scraper.session.post.return_value = mock_resp
        mock_get_scraper.return_value = mock_scraper

        b_uuid = uuid.uuid4()
        batch = ScrapeBatch.objects.create(
            batch_id=b_uuid,
            city="Palakollu",
            location="Someswara Agraharam",
            status="queued",
        )

        res = extract_location_urls_task(
            batch_id=str(b_uuid),
            city="Palakollu",
            location_title="Someswara Agraharam",
            locations_list=[{"entity_title": "Someswara Agraharam"}],
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["total_urls"], 2)

        batch.refresh_from_db()
        self.assertEqual(batch.total_restaurants, 2)
        self.assertEqual(RestaurantJob.objects.filter(batch=batch).count(), 2)

    @patch("scraper_app.tasks.url_extractor.get_active_scraper")
    def test_extract_location_urls_matching_locality_and_all_locations(self, mock_get_scraper):
        """Verifies that matching localities and 'All Locations' allow restaurants through."""
        from scraper_app.tasks.url_extractor import extract_urls_from_latlongs as extract_location_urls_task

        mock_scraper = MagicMock()
        mock_scraper.resolve_location.return_value = {
            "entity_id": "999",
            "entity_type": "subzone",
            "city_name": "Palakollu",
            "location_name": "Palakollu Locality",
            "exact_location_url": "https://www.zomato.com/palakollu/restaurants",
        }
        mock_scraper.ensure_csrf_token.return_value = "fake_csrf"
        mock_scraper.build_restaurants_payload.return_value = {}

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "sections": {
                "SECTION_SEARCH_RESULT": [
                    {
                        "restaurant": {"id": 1001, "name": "Central Tiffins"},
                        "info": {
                            "resId": 1001,
                            "name": "Central Tiffins",
                            "locality": {
                                "name": "Palakollu Locality, Palakollu",
                                "address": "Palakol Town, Palakollu Locality",
                            },
                        },
                        "page_url": "/palakollu/central-tiffins-palakollu-locality/info",
                    },
                ],
                "SECTION_SEARCH_META_INFO": {"hasMore": False},
            }
        }
        mock_scraper.session.post.return_value = mock_resp
        mock_get_scraper.return_value = mock_scraper

        # Case 1: Specific matching locality
        b1_uuid = uuid.uuid4()
        batch1 = ScrapeBatch.objects.create(
            batch_id=b1_uuid,
            city="Palakollu",
            location="Palakollu Locality",
            status="queued",
        )
        res1 = extract_location_urls_task(
            batch_id=str(b1_uuid),
            city="Palakollu",
            location_title="Palakollu Locality",
            locations_list=[{"entity_title": "Palakollu Locality"}],
        )
        self.assertEqual(res1["total_urls"], 1)

        # Case 2: City-wide ALL
        b2_uuid = uuid.uuid4()
        batch2 = ScrapeBatch.objects.create(
            batch_id=b2_uuid,
            city="Palakollu",
            location="All Locations in Palakollu",
            status="queued",
        )
        res2 = extract_location_urls_task(
            batch_id=str(b2_uuid),
            city="Palakollu",
            location_title="All Locations in Palakollu",
            locations_list=[{"entity_title": "All Locations in Palakollu"}],
        )
        self.assertEqual(res2["total_urls"], 1)

    @patch("scraper_app.tasks.url_extractor.get_active_scraper")
    def test_city_boundary_guard_rejects_foreign_town_restaurants(self, mock_get_scraper):
        """Verifies extraction captures all delivery restaurants returned in SECTION_SEARCH_RESULT without filters."""
        from scraper_app.tasks.url_extractor import extract_urls_from_latlongs as extract_location_urls_task

        mock_scraper = MagicMock()
        mock_scraper.resolve_location.return_value = {
            "entity_id": "888",
            "entity_type": "subzone",
            "city_name": "Palakollu",
            "location_name": "Palakollu - Razole Road",
            "exact_location_url": "https://www.zomato.com/palakollu/restaurants",
        }
        mock_scraper.ensure_csrf_token.return_value = "fake_csrf"
        mock_scraper.build_restaurants_payload.return_value = {}

        # API returns 2 restaurants in SECTION_SEARCH_RESULT
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "sections": {
                "SECTION_SEARCH_RESULT": [
                    {
                        "restaurant": {"id": 1001, "name": "Palakollu Tiffins"},
                        "info": {
                            "resId": 1001,
                            "name": "Palakollu Tiffins",
                            "locality": {
                                "name": "Palakollu Locality, Palakollu",
                                "address": "Main Road, Palakollu",
                            },
                        },
                        "page_url": "/palakollu/palakollu-tiffins-palakollu-locality/info",
                    },
                    {
                        "restaurant": {"id": 9001, "name": "Candy Krush Narsapur"},
                        "info": {
                            "resId": 9001,
                            "name": "Candy Krush Narsapur",
                            "locality": {
                                "name": "Narsapur Locality, Narsapur",
                                "address": "Main Road, Narsapur",
                            },
                        },
                        "page_url": "/narsapur/candy-krush-narsapur-locality/info",
                    },
                ],
                "SECTION_SEARCH_META_INFO": {"hasMore": False},
            }
        }
        mock_scraper.session.post.return_value = mock_resp
        mock_get_scraper.return_value = mock_scraper

        b_uuid = uuid.uuid4()
        batch = ScrapeBatch.objects.create(
            batch_id=b_uuid,
            city="Palakollu",
            location="All Locations in Palakollu",
            status="queued",
        )

        res = extract_location_urls_task(
            batch_id=str(b_uuid),
            city="Palakollu",
            location_title="All Locations in Palakollu",
            locations_list=[{"entity_title": "Palakollu - Razole Road"}],
        )

        # All restaurants in SECTION_SEARCH_RESULT must be captured without filtering
        self.assertEqual(res["total_urls"], 2)
        jobs = RestaurantJob.objects.filter(batch=batch)
        self.assertEqual(jobs.count(), 2)
        self.assertEqual(jobs.first().restaurant_id, 1001)

    @patch("scraper_app.producers.producer.ZomatoScraper")
    def test_resolve_locations_list_filters_highway_connector_routes(self, mock_scraper_cls):
        """Verifies resolve_locations_list filters out inter-city highway connector routes with ' - '."""
        from scraper_app.producers.producer import resolve_locations_list

        mock_inst = MagicMock()
        mock_inst.get_location_suggestions.return_value = [
            {"entity_title": "Palakollu", "latitude": 16.518, "longitude": 81.725},
            {"entity_title": "Palakollu Bypass Road", "latitude": 16.520, "longitude": 81.726},
            {"entity_title": "Palakollu - Razole Road", "latitude": 16.462, "longitude": 81.817},
            {"entity_title": "Palakollu - Valluru Road", "latitude": 16.515, "longitude": 81.736},
            {"entity_title": "Brodipet", "latitude": 16.525, "longitude": 81.725},
        ]
        mock_scraper_cls.return_value = mock_inst

        locs = resolve_locations_list(city="Palakollu", location_raw=None, is_all_locations=True)
        titles = [l["entity_title"] for l in locs]
        self.assertIn("Palakollu", titles)
        self.assertIn("Palakollu Bypass Road", titles)
        self.assertIn("Brodipet", titles)
        self.assertNotIn("Palakollu - Razole Road", titles)
        self.assertNotIn("Palakollu - Valluru Road", titles)

    def test_worker_singleton_lock_prevents_duplicate_instances(self):
        """Verifies WorkerSingletonLock blocks duplicate instances and provides running process info."""
        import os
        import tempfile
        from pathlib import Path
        from scraper_app.workers.worker_lock import (
            WorkerSingletonLock,
            format_already_running_banner,
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            lock1 = WorkerSingletonLock(queue_name="zomato", num_workers=3)
            lock1.lock_path = Path(tmp_dir) / ".worker_manager.lock"

            lock2 = WorkerSingletonLock(queue_name="zomato", num_workers=3)
            lock2.lock_path = Path(tmp_dir) / ".worker_manager.lock"

            # Instance 1 acquires lock
            with patch("scraper_app.workers.worker_lock.find_running_worker_supervisor", return_value=None):
                ok1, info1 = lock1.acquire()
                self.assertTrue(ok1)
                self.assertIsNone(info1)
                self.assertTrue(lock1.is_locked)

                # Instance 2 attempts to acquire lock on the same file -> must fail!
                ok2, info2 = lock2.acquire()
                self.assertFalse(ok2)
                self.assertIsNotNone(info2)
                self.assertEqual(info2.get("pid"), os.getpid())

                # Release instance 1
                lock1.release()
                self.assertFalse(lock1.is_locked)

                # Now instance 2 can acquire lock cleanly
                ok2_after, info2_after = lock2.acquire()
                self.assertTrue(ok2_after)
                self.assertIsNone(info2_after)
                lock2.release()

    def test_format_already_running_banner(self):
        """Verifies the already-running error banner contains PID, TTY, user, and instructions."""
        from scraper_app.workers.worker_lock import format_already_running_banner

        sample_info = {
            "pid": 831545,
            "tty": "pts/2",
            "tty_display": "pts/2 (/dev/pts/2)",
            "user": "sekhar",
            "etime": "07:45",
            "cmd": "python manage.py run_workers",
            "child_workers_count": 3,
        }
        banner = format_already_running_banner(sample_info, queue_name="zomato")
        self.assertIn("831545", banner)
        self.assertIn("pts/2", banner)
        self.assertIn("sekhar", banner)
        self.assertIn("zomato", banner)
        self.assertIn("kill -TERM 831545", banner)

    def test_delete_batch_api_deletes_db_and_redis_jobs(self):
        """Verifies delete_batch_api deletes ScrapeBatch, RestaurantJobs, and Redis queue jobs."""
        from django.test import Client
        import django_rq
        from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch
        from scraper_app.workers.worker import process_restaurant_pipeline_job

        q = django_rq.get_queue("zomato")

        # Create batch in DB
        batch = ScrapeBatch.objects.create(
            city="Visakhapatnam",
            location="Beach Road",
            status="queued",
            total_restaurants=1,
        )

        # Create restaurant job
        j1 = RestaurantJob.objects.create(
            batch=batch,
            name="Sea Pearl",
            page_url="/vizag/sea-pearl/info",
            stage="pending",
        )
        rq1 = q.enqueue(process_restaurant_pipeline_job, batch_id=str(batch.batch_id), job_record_id=j1.id, restaurant_url="/vizag/sea-pearl/info")
        j1.rq_job_id = rq1.id
        j1.save(update_fields=["rq_job_id"])

        # Also create a partial RestaurantRecord for this batch
        RestaurantRecord.objects.create(
            restaurant_id=98765432,
            batch=batch,
            name="Sea Pearl",
            city="Visakhapatnam",
            location="Beach Road",
            location_key="loc_visakhapatnam_beach_road",
        )

        self.assertTrue(ScrapeBatch.objects.filter(batch_id=batch.batch_id).exists())
        self.assertEqual(RestaurantJob.objects.filter(batch=batch).count(), 1)
        self.assertEqual(RestaurantRecord.objects.filter(batch=batch).count(), 1)

        # Call delete_batch_api
        resp = self.client.post(f"/api/delete-batch/{batch.batch_id}/")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "success")

        # Verify completely deleted from PostgreSQL
        self.assertFalse(ScrapeBatch.objects.filter(batch_id=batch.batch_id).exists())
        self.assertFalse(RestaurantJob.objects.filter(batch_id=batch.batch_id).exists())
        self.assertFalse(RestaurantRecord.objects.filter(batch_id=batch.batch_id).exists())

    def test_delete_batch_api_not_found(self):
        """Verifies delete_batch_api returns 404 for nonexistent batch."""
        resp = self.client.post("/api/delete-batch/00000000-0000-0000-0000-000000000000/")
        self.assertEqual(resp.status_code, 404)
        data = resp.json()
        self.assertEqual(data["status"], "error")

    def test_fresh_restaurant_url_clears_queue_before_starting_pipeline(self):
        """Verifies providing a fresh restaurant URL checks and clears existing jobs from the queue and waits 5s before starting."""
        import django_rq
        from unittest.mock import MagicMock, patch
        from scraper_app.producers.producer import dispatch_scrape_request

        real_q = django_rq.get_queue("zomato")
        mock_q = MagicMock(wraps=real_q)
        mock_q.__len__.return_value = 5

        with patch("scraper_app.producers.producer.django_rq.get_queue", return_value=mock_q), \
             patch("scraper_app.producers.producer.time.sleep") as mock_sleep:
            result = dispatch_scrape_request(
                city="Hyderabad",
                restaurant_url="/hyderabad/paradise-biryani-secunderabad/info",
            )
            self.assertEqual(result["status"], "queued")
            mock_q.empty.assert_called_once()
            mock_sleep.assert_called_with(5.0)

    def test_normal_location_enqueueing_does_not_clear_queue(self):
        """Verifies normal location scraping preserves existing queue jobs without arbitrary clearing."""
        import django_rq
        from unittest.mock import patch
        from scraper_app.producers.producer import dispatch_scrape_request

        q = django_rq.get_queue("zomato")
        with patch.object(q, "empty") as mock_empty:
            result = dispatch_scrape_request(
                city="Hyderabad",
                location_raw="Jubilee Hills",
            )
            self.assertEqual(result["status"], "queued")
            mock_empty.assert_not_called()

    def test_clear_queue_with_delay_waits_5_seconds(self):
        """Verifies clear_queue_with_delay clears queue and FailedJobRegistry, then pauses for 5 seconds."""
        from unittest.mock import MagicMock, patch
        from scraper_app.producers.producer import clear_queue_with_delay

        mock_q = MagicMock()
        mock_q.__len__.return_value = 10

        with patch("scraper_app.producers.producer.time.sleep") as mock_sleep, \
             patch("scraper_app.producers.producer.FailedJobRegistry") as mock_registry_cls:
            mock_registry = MagicMock()
            mock_registry.get_job_ids.return_value = ["failed-job-1"]
            mock_registry_cls.return_value = mock_registry

            cleared = clear_queue_with_delay(mock_q, "zomato", delay_seconds=5.0)

            self.assertTrue(cleared)
            mock_q.empty.assert_called_once()
            mock_registry.remove.assert_called_once_with("failed-job-1")
            mock_sleep.assert_called_once_with(5.0)

    def test_viewing_queued_request_data_is_disabled(self):
        """Verifies get_scrape_data_api disables viewing data for queued batches."""
        from django.test import Client
        from scraper_app.models import ScrapeBatch

        queued_b = ScrapeBatch.objects.create(
            city="Guntur",
            location="Arundelpet",
            status="queued",
        )

        resp = self.client.get(f"/api/scrape-data/{queued_b.batch_id}/")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "queued")
        self.assertIn("Viewing queued requests is disabled", data["message"])
        self.assertEqual(data["data"], [])

    def test_batch_id_is_primary_key(self):
        """Verifies that requestid is strictly configured as the primary key of ScrapeBatch."""
        from scraper_app.models import ScrapeBatch
        pk_field = ScrapeBatch._meta.pk
        self.assertEqual(pk_field.name, "requestid")
        self.assertTrue(pk_field.primary_key)
        batch = ScrapeBatch(city="Hyderabad", location="Test")
        self.assertEqual(batch.batch_id, batch.requestid)

    def test_process_restaurant_pipeline_job_auto_heals_missing_record(self):
        """Verifies process_restaurant_pipeline_job auto-creates RestaurantJob if passed restaurant_id directly."""
        from scraper_app.models import RestaurantJob, ScrapeBatch
        from scraper_app.workers.worker import process_restaurant_pipeline_job
        from unittest.mock import MagicMock, patch

        batch = ScrapeBatch.objects.create(
            city="Hyderabad",
            location="Jubilee Hills",
            status="processing_restaurants",
        )

        with patch("scraper_app.tasks.restaurant_extractor.get_active_scraper") as mock_scraper_cls:
            mock_scraper = MagicMock()
            mock_scraper.session.get.return_value.status_code = 200
            mock_scraper.session.get.return_value.json.return_value = {
                "page_data": {"sections": {}}
            }
            mock_scraper.session.headers = {}
            mock_scraper_cls.return_value = mock_scraper

            res = process_restaurant_pipeline_job(
                batch_id=str(batch.batch_id),
                job_record_id=22318800,
                restaurant_url="/hyderabad/dokii-dokii-jubilee-hills/info",
                name="Dokii Dokii",
                city="Hyderabad",
                location="Jubilee Hills",
            )
            self.assertEqual(res["status"], "success")

            # Verify RestaurantJob was auto-healed in PostgreSQL
            created_job = RestaurantJob.objects.filter(batch=batch).first()
            self.assertIsNotNone(created_job)
            self.assertEqual(created_job.name, "Dokii Dokii")

    def test_process_restaurant_pipeline_job_skips_cancelled_batch(self):
        """Verifies process_restaurant_pipeline_job cleanly skips orphaned jobs from deleted batches."""
        from scraper_app.workers.worker import process_restaurant_pipeline_job

        res = process_restaurant_pipeline_job(
            batch_id="00000000-0000-0000-0000-000000000000",
            job_record_id=999999,
            restaurant_url="/test/restaurant",
            name="Orphaned Place",
        )
        self.assertEqual(res["status"], "cancelled")

    def test_pause_and_resume_endpoints_removed(self):
        """Verifies pause and resume endpoints have been cleanly removed."""
        from scraper_app.models import ScrapeBatch

        batch = ScrapeBatch.objects.create(
            city="Hyderabad",
            location="Banjara Hills",
            status="processing_restaurants",
            total_restaurants=10,
        )

        pause_resp = self.client.post(f"/api/pause-batch/{batch.batch_id}/")
        self.assertEqual(pause_resp.status_code, 404)

        resume_resp = self.client.post(f"/api/resume-batch/{batch.batch_id}/")
        self.assertEqual(resume_resp.status_code, 404)

    def test_terminate_batch_api_cancels_jobs_and_retains_data(self):
        """Verifies terminate_batch_api halts workers, marks terminated, and retains scraped records in PostgreSQL."""
        from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch

        batch = ScrapeBatch.objects.create(
            city="Hyderabad",
            location="Madhapur",
            status="processing_restaurants",
            total_restaurants=5,
        )
        job = RestaurantJob.objects.create(
            batch=batch,
            name="Biryani House",
            page_url="/hyderabad/biryani-house/info",
            stage="processing_details",
        )
        record = RestaurantRecord.objects.create(
            batch=batch,
            restaurant_id=123456,
            name="Biryani House",
            city="Hyderabad",
            location="Madhapur",
            location_key="loc_hyderabad_madhapur",
            delivery_document={"name": "Biryani House", "rating": 4.5},
        )

        # Clean up any residual batches from previous tests so next_batch isolates cleanly
        RestaurantRecord.objects.exclude(batch=batch).delete()
        RestaurantJob.objects.exclude(batch=batch).delete()
        ScrapeBatch.objects.exclude(batch_id=batch.batch_id).delete()

        # Also create a queued batch to test next_batch resolution
        next_batch = ScrapeBatch.objects.create(
            city="Hyderabad",
            location="Gachibowli",
            status="queued",
            total_restaurants=3,
        )

        terminate_resp = self.client.post(f"/api/terminate-batch/{batch.batch_id}/")
        self.assertEqual(terminate_resp.status_code, 200)
        term_data = terminate_resp.json()
        self.assertEqual(term_data["status"], "success")
        self.assertEqual(term_data["batch_status"], "terminated")
        self.assertIsNotNone(term_data.get("next_batch"))
        self.assertEqual(term_data["next_batch"]["batch_id"], str(next_batch.batch_id))

        # Check DB states
        batch.refresh_from_db()
        self.assertEqual(batch.status, "terminated")

        job.refresh_from_db()
        self.assertEqual(job.stage, "terminated")

        # CRITICAL: Extracted records MUST remain in PostgreSQL!
        self.assertTrue(RestaurantRecord.objects.filter(batch=batch, restaurant_id=123456).exists())

        # CRITICAL: Next queued batch MUST be triggered and start execution!
        next_batch.refresh_from_db()
        self.assertIn(next_batch.status, ["extracting_urls", "processing_restaurants"])

    def test_start_queued_batch_api(self):
        """Verifies start_queued_batch_api enqueues jobs and moves queued batch to active status."""
        from scraper_app.models import ScrapeBatch

        batch = ScrapeBatch.objects.create(
            city="Hyderabad",
            location="Kondapur",
            status="queued",
        )
        resp = self.client.post(f"/api/start-queued-batch/{batch.batch_id}/")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["status"], "success")

        batch.refresh_from_db()
        self.assertEqual(batch.status, "extracting_urls")

