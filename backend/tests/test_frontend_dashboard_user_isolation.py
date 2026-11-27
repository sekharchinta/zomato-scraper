"""
tests/test_frontend_dashboard_user_isolation.py

Automated Verification Suite for Frontend Dashboard User Authorization & Multi-Tenant Data Isolation.
Verifies that:
1. /api/collection-stats/ returns strictly user-scoped metrics (PAR-01 to PAR-04).
2. /api/scrape-status/active/ resolves strictly to the authenticated user's running/active batch.
3. /api/scrape-status/<id>/ strictly rejects cross-user access and unassigned legacy batches with 403 Forbidden.
4. /api/scrape-data/<id>/ strictly isolates data preview to the authorized owner.
5. /api/completed-batches/ returns only the authenticated user's completed history.
6. /api/export-csv/<id>/ forbids unauthorized CSV exports.
"""

import uuid
from typing import Tuple

from django.contrib.auth.models import User
from django.test import Client, TestCase
from django.utils import timezone
from scraper_app.models import RestaurantJob, RestaurantRecord, ScrapeBatch, UserProfile


class TestFrontendDashboardUserIsolation(TestCase):
    """Rigorous multi-tenant isolation tests for frontend dashboard endpoints."""

    def setUp(self):
        # 1. Create User A and User B
        self.user_a = User.objects.create_user(
            username="user_alpha_test",
            email="alpha@test.com",
            password="securepassword123",
            first_name="Alpha",
            last_name="Tester",
        )
        UserProfile.objects.create(
            google_id="google_id_alpha_111",
            user=self.user_a,
        )

        self.user_b = User.objects.create_user(
            username="user_beta_test",
            email="beta@test.com",
            password="securepassword123",
            first_name="Beta",
            last_name="Tester",
        )
        UserProfile.objects.create(
            google_id="google_id_beta_222",
            user=self.user_b,
        )

        # 2. Create User A's Batch (Hyderabad)
        self.batch_a_id = uuid.uuid4()
        self.batch_a = ScrapeBatch.objects.create(
            requestid=self.batch_a_id,
            user=self.user_a,
            city="Hyderabad",
            location="Banjara Hills",
            status="completed",
            progress=100,
            total_restaurants=10,
            completed_jobs=10,
            completed_at=timezone.now(),
        )
        self.job_a = RestaurantJob.objects.create(
            batch=self.batch_a,
            restaurant_id=101,
            name="Alpha Grand Biryani",
            page_url="https://www.zomato.com/hyderabad/alpha-biryani",
            stage="completed",
        )
        self.record_a = RestaurantRecord.objects.create(
            requestid=self.batch_a,
            restaurant_id=101,
            name="Alpha Grand Biryani",
            city="Hyderabad",
            location="Banjara Hills",
            pageurl="https://www.zomato.com/hyderabad/alpha-biryani",
            rating=4.5,
            total_reviews=100,
        )

        # 3. Create User B's Batch (Bengaluru)
        self.batch_b_id = uuid.uuid4()
        self.batch_b = ScrapeBatch.objects.create(
            requestid=self.batch_b_id,
            user=self.user_b,
            city="Bengaluru",
            location="Koramangala",
            status="completed",
            progress=100,
            total_restaurants=5,
            completed_jobs=5,
            completed_at=timezone.now(),
        )
        self.job_b = RestaurantJob.objects.create(
            batch=self.batch_b,
            restaurant_id=202,
            name="Beta South Dosa",
            page_url="https://www.zomato.com/bengaluru/beta-dosa",
            stage="completed",
        )
        self.record_b = RestaurantRecord.objects.create(
            requestid=self.batch_b,
            restaurant_id=202,
            name="Beta South Dosa",
            city="Bengaluru",
            location="Koramangala",
            pageurl="https://www.zomato.com/bengaluru/beta-dosa",
            rating=4.2,
            total_reviews=50,
        )

        # 4. Create an Unassigned Legacy Batch (user=None)
        self.legacy_batch_id = uuid.uuid4()
        self.legacy_batch = ScrapeBatch.objects.create(
            requestid=self.legacy_batch_id,
            user=None,
            city="Palakollu",
            location="Solasavari St",
            status="completed",
            progress=100,
            total_restaurants=20,
            completed_jobs=20,
            completed_at=timezone.now(),
        )
        self.legacy_record = RestaurantRecord.objects.create(
            requestid=self.legacy_batch,
            restaurant_id=999,
            name="Legacy Diner",
            city="Palakollu",
            location="Solasavari St",
            pageurl="https://www.zomato.com/palakollu/legacy-diner",
        )

        # Setup Test Clients
        self.client_a = Client()
        self.client_a.force_login(self.user_a)

        self.client_b = Client()
        self.client_b.force_login(self.user_b)

    def test_01_collection_stats_scoped_strictly_to_user(self):
        """PAR-01 to PAR-04 metric cards must count ONLY the authenticated user's data."""
        # User A's stats
        res_a = self.client_a.get("/api/collection-stats/")
        self.assertEqual(res_a.status_code, 200)
        stats_a = res_a.json()["stats"]
        self.assertEqual(stats_a["total_batches"], 1, "User A must see exactly 1 batch")
        self.assertEqual(stats_a["restaurants_urls"], 1, "User A must see exactly 1 job URL")
        self.assertEqual(stats_a["restaurants_data"], 1, "User A must see exactly 1 restaurant record")
        self.assertEqual(stats_a["delivery_data"], 1)

        # User B's stats
        res_b = self.client_b.get("/api/collection-stats/")
        self.assertEqual(res_b.status_code, 200)
        stats_b = res_b.json()["stats"]
        self.assertEqual(stats_b["total_batches"], 1, "User B must see exactly 1 batch")
        self.assertEqual(stats_b["restaurants_urls"], 1, "User B must see exactly 1 job URL")
        self.assertEqual(stats_b["restaurants_data"], 1, "User B must see exactly 1 restaurant record")
        self.assertEqual(stats_b["delivery_data"], 1)

    def test_02_scrape_status_active_resolves_strictly_per_user(self):
        """/api/scrape-status/active/ must resolve the user's batch and ignore other users and legacy."""
        res_a = self.client_a.get("/api/scrape-status/active/")
        self.assertEqual(res_a.status_code, 200)
        data_a = res_a.json()
        self.assertEqual(data_a["batch_id"], str(self.batch_a_id))
        self.assertEqual(data_a["city"], "Hyderabad")

        res_b = self.client_b.get("/api/scrape-status/active/")
        self.assertEqual(res_b.status_code, 200)
        data_b = res_b.json()
        self.assertEqual(data_b["batch_id"], str(self.batch_b_id))
        self.assertEqual(data_b["city"], "Bengaluru")

    def test_03_cross_user_and_legacy_batch_status_returns_403(self):
        """Querying someone else's batch or unassigned legacy batch returns 403 Forbidden."""
        # User B queries User A's batch -> 403 Forbidden
        res = self.client_b.get(f"/api/scrape-status/{self.batch_a_id}/")
        self.assertEqual(res.status_code, 403)
        self.assertEqual(res.json()["code"], "permission_denied")

        # User A queries User B's batch -> 403 Forbidden
        res = self.client_a.get(f"/api/scrape-status/{self.batch_b_id}/")
        self.assertEqual(res.status_code, 403)
        self.assertEqual(res.json()["code"], "permission_denied")

        # User A queries legacy unassigned batch -> 403 Forbidden
        res = self.client_a.get(f"/api/scrape-status/{self.legacy_batch_id}/")
        self.assertEqual(res.status_code, 403)
        self.assertEqual(res.json()["code"], "permission_denied")

    def test_04_scrape_data_preview_strictly_isolated(self):
        """Previewing extracted records returns 200 for owner, 403 for other users and legacy."""
        # User A views their own data -> 200 OK
        res_a = self.client_a.get(f"/api/scrape-data/{self.batch_a_id}/")
        self.assertEqual(res_a.status_code, 200)
        data = res_a.json()["data"]
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["name"], "Alpha Grand Biryani")

        # User B attempts to view User A's data -> 403 Forbidden
        res_b = self.client_b.get(f"/api/scrape-data/{self.batch_a_id}/")
        self.assertEqual(res_b.status_code, 403)
        self.assertEqual(res_b.json()["code"], "permission_denied")

        # User A attempts to view unassigned legacy batch -> 403 Forbidden
        res_legacy = self.client_a.get(f"/api/scrape-data/{self.legacy_batch_id}/")
        self.assertEqual(res_legacy.status_code, 403)
        self.assertEqual(res_legacy.json()["code"], "permission_denied")

    def test_05_completed_batches_list_returns_only_user_history(self):
        """Completed history lists only the user's batches; zero leakage of other users or legacy."""
        res_a = self.client_a.get("/api/completed-batches/")
        self.assertEqual(res_a.status_code, 200)
        batches_a = res_a.json()["batches"]
        self.assertEqual(len(batches_a), 1)
        self.assertEqual(batches_a[0]["batch_id"], str(self.batch_a_id))
        self.assertEqual(batches_a[0]["city"], "Hyderabad")

        res_b = self.client_b.get("/api/completed-batches/")
        self.assertEqual(res_b.status_code, 200)
        batches_b = res_b.json()["batches"]
        self.assertEqual(len(batches_b), 1)
        self.assertEqual(batches_b[0]["batch_id"], str(self.batch_b_id))
        self.assertEqual(batches_b[0]["city"], "Bengaluru")

    def test_06_csv_export_isolated(self):
        """CSV download succeeds for owner (200), forbidden for other users and legacy (403)."""
        # Owner download -> 200 OK
        res = self.client_a.get(f"/api/export-csv/{self.batch_a_id}/")
        self.assertEqual(res.status_code, 200)
        self.assertIn("text/csv", res.headers.get("Content-Type", ""))

        # Cross-user download -> 403 Forbidden
        res_bad = self.client_b.get(f"/api/export-csv/{self.batch_a_id}/")
        self.assertEqual(res_bad.status_code, 403)

        # Legacy batch download -> 403 Forbidden
        res_legacy = self.client_a.get(f"/api/export-csv/{self.legacy_batch_id}/")
        self.assertEqual(res_legacy.status_code, 403)

