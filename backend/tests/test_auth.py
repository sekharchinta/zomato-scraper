"""
test_auth.py
Unit tests for Google OAuth 2.0 authentication endpoints.
"""

from unittest.mock import patch
from django.contrib.auth.models import User
from django.test import TestCase, Client
from scraper_app.models import UserProfile


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

        # Verify logout
        logout_resp = self.client.post("/api/auth/logout/")
        self.assertEqual(logout_resp.status_code, 200)
        self.assertEqual(logout_resp.json().get("status"), "success")

        # Verify /api/auth/me/ is now unauthenticated
        me_after_logout = self.client.get("/api/auth/me/")
        self.assertFalse(me_after_logout.json().get("authenticated"))
