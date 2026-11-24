"""
Tests for Slack Alerting Service.
Validates:
1. Request Completed Alert triggers when a batch finishes (status == 'completed').
2. Low Success Rate Alert triggers when (completed / total) < 99.0%.
3. No low success rate alert is triggered when success rate >= 99.0%.
4. Atomic Redis deduplication prevents duplicate alerts for both types.
5. Fail-safe non-blocking execution when Slack webhook fails or is unconfigured.
6. test_slack_alert management command execution for both alert types.
"""

import uuid
from unittest.mock import MagicMock, patch
from django.core.management import call_command
from django.test import TestCase

from scraper_app.alerts import (
    build_low_success_rate_payload,
    build_request_completed_payload,
    check_and_alert_batch_status,
    get_min_success_rate,
    is_slack_alert_enabled,
    send_slack_message,
)
from scraper_app.models import ScrapeBatch


class SlackAlertServiceTests(TestCase):
    def setUp(self):
        self.batch_id = uuid.uuid4()
        self.batch = ScrapeBatch.objects.create(
            requestid=self.batch_id,
            city="Chennai",
            location="T. Nagar",
            status="completed",
            total_restaurants=100,
            completed_jobs=100,
            failed_jobs=0,
            progress=100,
        )

    def test_min_success_rate_default(self):
        """Default threshold should be 99.0%."""
        self.assertEqual(get_min_success_rate(), 99.0)

    def test_slack_alerts_enabled_by_default(self):
        """Slack alerts should be enabled by default."""
        self.assertTrue(is_slack_alert_enabled())

    def test_build_request_completed_payload(self):
        """Verify request completed payload structure."""
        text, blocks, color = build_request_completed_payload(self.batch, success_rate=100.0)
        self.assertIn("Request Completed", text)
        self.assertEqual(color, "#059669")
        self.assertTrue(len(blocks) >= 2)
        self.assertIn("COMPLETED", str(blocks))
        self.assertIn("100.00%", str(blocks))

    def test_build_low_success_rate_payload(self):
        """Verify low success rate payload structure."""
        self.batch.completed_jobs = 95
        self.batch.failed_jobs = 5
        text, blocks, color = build_low_success_rate_payload(self.batch, success_rate=95.0, threshold=99.0)
        self.assertIn("Low Success Rate", text)
        self.assertEqual(color, "#ea580c")
        self.assertIn("95.00%", str(blocks))
        self.assertIn("99.0%", str(blocks))

    @patch("requests.post")
    def test_send_slack_message_via_webhook(self, mock_post):
        """Verify webhook POST payload and success return."""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "ok"
        mock_post.return_value = mock_resp

        with self.settings(SLACK_WEBHOOK_URL="https://hooks.slack.com/services/test/test/test"):
            success, msg = send_slack_message(text="Test Alert", color="#059669")
            self.assertTrue(success)
            self.assertIn("Incoming Webhook", msg)
            mock_post.assert_called_once()
            args, kwargs = mock_post.call_args
            self.assertEqual(args[0], "https://hooks.slack.com/services/test/test/test")
            self.assertIn("Test Alert", kwargs["json"]["text"])

    @patch("requests.post")
    def test_send_slack_message_handles_failure_gracefully(self, mock_post):
        """Verify non-blocking error handling when Slack webhook fails."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_post.return_value = mock_resp

        with self.settings(SLACK_WEBHOOK_URL="https://hooks.slack.com/services/test/test/test"):
            success, msg = send_slack_message(text="Test Alert")
            self.assertFalse(success)
            self.assertIn("status 500", msg)

    @patch("scraper_app.alerts.send_slack_message")
    @patch("scraper_app.alerts._is_alert_already_sent", return_value=False)
    def test_alert_triggers_on_completed_batch(self, mock_dedup, mock_send):
        """check_and_alert_batch_status should trigger Request Completed alert when batch completes with 100%."""
        mock_send.return_value = (True, "Delivered")
        self.batch.status = "completed"
        self.batch.total_restaurants = 100
        self.batch.completed_jobs = 100
        self.batch.failed_jobs = 0

        dispatched = check_and_alert_batch_status(self.batch)
        self.assertTrue(dispatched)
        mock_send.assert_called_once()
        args, kwargs = mock_send.call_args
        self.assertIn("Request Completed", kwargs["text"])

    @patch("scraper_app.alerts.send_slack_message")
    @patch("scraper_app.alerts._is_alert_already_sent", return_value=False)
    def test_alert_triggers_both_completed_and_low_success_when_below_99(self, mock_dedup, mock_send):
        """When batch completes with < 99% success rate, both completed and low_success alerts should fire."""
        mock_send.return_value = (True, "Delivered")
        self.batch.status = "completed"
        self.batch.total_restaurants = 100
        self.batch.completed_jobs = 95
        self.batch.failed_jobs = 5  # 95% < 99%

        dispatched = check_and_alert_batch_status(self.batch)
        self.assertTrue(dispatched)
        self.assertEqual(mock_send.call_count, 2)
        first_call = mock_send.call_args_list[0]
        second_call = mock_send.call_args_list[1]
        self.assertIn("Request Completed", first_call[1]["text"])
        self.assertIn("Low Success Rate", second_call[1]["text"])

    @patch("scraper_app.alerts.send_slack_message")
    @patch("scraper_app.alerts._is_alert_already_sent", return_value=True)
    def test_deduplication_prevents_duplicate_alert(self, mock_dedup, mock_send):
        """Deduplication should prevent sending duplicate alerts."""
        self.batch.status = "completed"
        dispatched = check_and_alert_batch_status(self.batch)
        self.assertFalse(dispatched)
        mock_send.assert_not_called()

    def test_test_slack_alert_command_runs(self):
        """test_slack_alert command should run cleanly for both types."""
        with patch("scraper_app.alerts.send_slack_message", return_value=(True, "Delivered")):
            call_command("test_slack_alert", "--type=completed")
            call_command("test_slack_alert", "--type=low_success")
