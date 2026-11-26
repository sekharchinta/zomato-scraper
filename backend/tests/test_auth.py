"""
test_auth.py
Unit tests for Google OAuth 2.0 authentication endpoints.
"""

from unittest.mock import patch
from django.contrib.auth.models import User
from django.test import TestCase, Client
from scraper_app.models import LatLongData, UserProfile


class GoogleAuthTestCase(TestCase):
    def setUp(self):
        self.client = Client()

    def test_google_auth_config_api(self):
        """Tests that /api/auth/google/config/ returns client_id."""
        response = self.client.get("/api/auth/google/config/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data.get("status"), "success")
        self.assertTrue("client_id" in data)

    def test_get_current_user_unauthenticated(self):
        """Tests that /api/auth/me/ returns authenticated: False when not logged in."""
        response = self.client.get("/api/auth/me/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertFalse(data.get("authenticated"))
        self.assertIsNone(data.get("user"))

    def test_google_auth_callback_missing_credential(self):
        """Tests that /api/auth/google/ returns 400 when credential is missing."""
        response = self.client.post("/api/auth/google/", data="{}", content_type="application/json")
        self.assertEqual(response.status_code, 400)
        data = response.json()
        self.assertEqual(data.get("status"), "error")

    @patch("scraper_app.auth_views.id_token.verify_oauth2_token")
    def test_google_auth_callback_success(self, mock_verify):
        """Tests successful Google authentication, user creation, and session login."""
        mock_verify.return_value = {
            "sub": "google-test-sub-12345",
            "email": "testuser@example.com",
            "name": "Test Scraper User",
            "picture": "https://lh3.googleusercontent.com/test-pic.jpg",
            "email_verified": True,
        }

        response = self.client.post(
            "/api/auth/google/",
            data='{"credential": "mocked-valid-jwt-token"}',
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data.get("status"), "success")
        user_info = data.get("user", {})
        self.assertEqual(user_info.get("email"), "testuser@example.com")
        self.assertEqual(user_info.get("google_id"), "google-test-sub-12345")
        self.assertEqual(user_info.get("name"), "Test Scraper User")
        self.assertEqual(user_info.get("avatar"), "https://lh3.googleusercontent.com/test-pic.jpg")

        # Verify PostgreSQL database entries
        user = User.objects.get(email="testuser@example.com")
        self.assertEqual(user.first_name, "Test")
        self.assertEqual(user.last_name, "Scraper User")
        self.assertTrue(hasattr(user, "profile"))
        self.assertEqual(user.profile.google_id, "google-test-sub-12345")
        self.assertEqual(user.profile.avatar_url, "https://lh3.googleusercontent.com/test-pic.jpg")

        # Verify /api/auth/me/ now returns authenticated: True
        me_resp = self.client.get("/api/auth/me/")
        self.assertEqual(me_resp.status_code, 200)
        me_data = me_resp.json()
        self.assertTrue(me_data.get("authenticated"))
        self.assertEqual(me_data["user"]["email"], "testuser@example.com")
        self.assertEqual(me_data["user"]["google_id"], "google-test-sub-12345")

        # Verify logout
        logout_resp = self.client.post("/api/auth/logout/")
        self.assertEqual(logout_resp.status_code, 200)
        self.assertEqual(logout_resp.json().get("status"), "success")

        # Verify /api/auth/me/ is now unauthenticated
        me_after_logout = self.client.get("/api/auth/me/")
        self.assertFalse(me_after_logout.json().get("authenticated"))

    @patch("scraper_app.tasks.location_extractor.ZomatoScraper")
    def test_authenticated_search_locations_binds_user(self, mock_scraper_cls):
        """Tests that location searches performed by an authenticated user attach user to LatLongData."""
        mock_scraper = mock_scraper_cls.return_value
        mock_scraper.get_location_suggestions.return_value = [
            {
                "entity_id": "111",
                "entity_type": "subzone",
                "entity_title": "Indiranagar",
                "entity_name": "Indiranagar, Bengaluru",
                "latitude": 12.9716,
                "longitude": 77.5946,
            }
        ]

        # Create and log in a user
        user = User.objects.create(username="geo_user", email="geo_user@example.com")
        self.client.force_login(user)

        resp = self.client.get("/api/search-locations/?query=Indiranagar")
        self.assertEqual(resp.status_code, 200)

        # Verify latlong_data records have user assigned
        stored_records = LatLongData.objects.filter(search_query="Indiranagar")
        self.assertTrue(stored_records.exists())
        for rec in stored_records:
            self.assertEqual(rec.user, user)

    def test_unauthenticated_api_endpoints_return_401(self):
        """Verifies that protected API endpoints reject unauthenticated requests with HTTP 401."""
        endpoints = [
            "/api/collection-stats/",
            "/api/completed-batches/",
            "/api/get-schema-fields/",
            "/api/search-locations/?query=Hyderabad",
        ]
        for url in endpoints:
            resp = self.client.get(url)
            self.assertEqual(resp.status_code, 401, f"Expected 401 for {url}, got {resp.status_code}")
            data = resp.json()
            self.assertEqual(data.get("code"), "unauthenticated")
            self.assertEqual(data.get("status"), "error")

    def test_authenticated_api_endpoints_return_200(self):
        """Verifies that authenticated users can access protected API endpoints."""
        user = User.objects.create(username="auth_tester", email="auth_tester@example.com")
        self.client.force_login(user)

        endpoints = [
            "/api/collection-stats/",
            "/api/completed-batches/",
            "/api/get-schema-fields/",
        ]
        for url in endpoints:
            resp = self.client.get(url)
            self.assertEqual(resp.status_code, 200, f"Expected 200 for {url}, got {resp.status_code}")
            data = resp.json()
            self.assertEqual(data.get("status"), "success")

    @patch("scraper_app.auth_views.id_token.verify_oauth2_token")
    def test_relogin_with_same_google_id_reuses_same_user_id(self, mock_verify):
        """Verifies that subsequent logins with the exact same google_id reuse the existing User record and user.id."""
        mock_verify.return_value = {
            "sub": "105697140295126583646",
            "email": "somasekhar.chinta@gamyam.co",
            "name": "Somasekhar Chinta",
            "picture": "https://lh3.googleusercontent.com/pic.jpg",
            "email_verified": True,
        }

        # First login (registration / initial creation)
        resp1 = self.client.post(
            "/api/auth/google/",
            data='{"credential": "mocked-token-1"}',
            content_type="application/json",
        )
        self.assertEqual(resp1.status_code, 200)
        user_1 = resp1.json()["user"]
        self.assertEqual(user_1["google_id"], "105697140295126583646")
        user_id_1 = user_1["id"]

        # Logout
        self.client.post("/api/auth/logout/")

        # Second login (re-login with same google_id)
        resp2 = self.client.post(
            "/api/auth/google/",
            data='{"credential": "mocked-token-2"}',
            content_type="application/json",
        )
        self.assertEqual(resp2.status_code, 200)
        user_2 = resp2.json()["user"]
        self.assertEqual(user_2["google_id"], "105697140295126583646")
        user_id_2 = user_2["id"]

        # Assert User ID is 100% identical and no duplicate records were created
        self.assertEqual(user_id_1, user_id_2)
        self.assertEqual(User.objects.filter(email="somasekhar.chinta@gamyam.co").count(), 1)
        self.assertEqual(UserProfile.objects.filter(google_id="105697140295126583646").count(), 1)



