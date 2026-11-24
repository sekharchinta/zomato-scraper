"""
Django Management Command: test_slack_alert
Sends a test alert to verify Slack connectivity and webhook / bot token setup.
Usage:
    python manage.py test_slack_alert --type=completed
    python manage.py test_slack_alert --type=low_success
    python manage.py test_slack_alert --webhook=https://hooks.slack.com/services/...
"""

import uuid
from typing import Any
from django.core.management.base import BaseCommand
from scraper_app.alerts import (
    build_low_success_rate_payload,
    build_request_completed_payload,
    get_min_success_rate,
    is_slack_alert_enabled,
    send_slack_message,
)


class MockBatch:
    """Mock ScrapeBatch instance for testing alerts without touching the database."""
    def __init__(self, alert_type: str = "completed"):
        self.requestid = uuid.uuid4()
        self.batch_id = self.requestid
        self.city = "Chennai"
        self.location = "Hotel Chariot, T. Nagar"
        self.status = "completed"
        if alert_type == "low_success":
            self.total_restaurants = 100
            self.completed_jobs = 94
            self.failed_jobs = 6
        else:
            self.total_restaurants = 50
            self.completed_jobs = 50
            self.failed_jobs = 0


class Command(BaseCommand):
    help = "Tests Slack Alert connectivity by sending a test alert."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--type",
            type=str,
            default="completed",
            choices=["completed", "low_success"],
            help="Type of alert to test: 'completed' (Request Completed) or 'low_success' (Success Rate < 99%)",
        )
        parser.add_argument(
            "--webhook",
            type=str,
            default="",
            help="Optional Slack Incoming Webhook URL to test directly",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        alert_type = options["type"]
        override_webhook = options.get("webhook", "")

        self.stdout.write(self.style.MIGRATE_HEADING("================================================================="))
        self.stdout.write(self.style.MIGRATE_HEADING("             Zomato Scraper: Slack Alert Test Utility            "))
        self.stdout.write(self.style.MIGRATE_HEADING("================================================================="))

        from django.conf import settings
        if override_webhook:
            settings.SLACK_WEBHOOK_URL = override_webhook

        enabled = is_slack_alert_enabled()
        webhook_configured = bool(getattr(settings, "SLACK_WEBHOOK_URL", ""))
        bot_configured = bool(getattr(settings, "SLACK_BOT_TOKEN", ""))

        self.stdout.write(f"• Slack Alerts Enabled: {enabled}")
        self.stdout.write(f"• Webhook Configured:   {webhook_configured}")
        self.stdout.write(f"• Bot Token Configured: {bot_configured}")
        self.stdout.write(f"• Alert Type to Test:   {alert_type}")
        self.stdout.write(f"• Min Success Rate:     {get_min_success_rate()}%\n")

        mock = MockBatch(alert_type=alert_type)
        threshold = get_min_success_rate()
        rate = (mock.completed_jobs / mock.total_restaurants) * 100.0

        if alert_type == "low_success":
            text, blocks, color = build_low_success_rate_payload(mock, success_rate=rate, threshold=threshold)
            self.stdout.write(f"Generated Payload: Low Job Success Rate ({rate:.1f}% < {threshold:.1f}%)")
        else:
            text, blocks, color = build_request_completed_payload(mock, success_rate=rate)
            self.stdout.write(f"Generated Payload: Request Completed ({mock.completed_jobs}/{mock.total_restaurants} restaurants scraped)")

        self.stdout.write("\nAttempting delivery to Slack...")
        success, message = send_slack_message(text=text, blocks=blocks, color=color)

        if success:
            self.stdout.write(self.style.SUCCESS(f"\n✓ SUCCESS: {message}"))
            self.stdout.write(self.style.SUCCESS("Check your Slack channel to view the delivered alert card!"))
        else:
            self.stdout.write(self.style.ERROR(f"\n✗ NOT DELIVERED: {message}"))
            self.stdout.write("\nTip: To receive alerts, ensure SLACK_WEBHOOK_URL is set in your .env file:")
            self.stdout.write("  SLACK_WEBHOOK_URL=https://hooks.slack.com/services/T.../B.../X...")
