"""
Slack Alerting Service for Zomato Scraper.
Monitors batch requests and job completion metrics.
Sends real-time Slack alerts for:
1. Request Completion: When a scrape request/batch finishes successfully (status == 'completed').
2. Low Job Success Rate: When the overall job success rate of a batch is below the required threshold (< 99.0%).

Supports both:
- Slack Incoming Webhooks (SLACK_WEBHOOK_URL)
- Slack Bot / User OAuth Tokens (SLACK_BOT_TOKEN + SLACK_CHANNEL)

Non-blocking, fail-safe, and deduplicated via Redis.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import requests
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger("scraper.slack_alerts")

DEFAULT_MIN_SUCCESS_RATE = 99.0


def is_slack_alert_enabled() -> bool:
    """Checks whether Slack alerts are enabled in settings / environment."""
    return getattr(settings, "SLACK_ALERT_ENABLED", True)


def get_min_success_rate() -> float:
    """Returns minimum acceptable job success rate percentage (default 99.0%)."""
    val = getattr(settings, "SLACK_MIN_SUCCESS_RATE", None)
    if val is None:
        val = os.getenv("SLACK_MIN_SUCCESS_RATE", DEFAULT_MIN_SUCCESS_RATE)
    try:
        return float(val)
    except (ValueError, TypeError):
        return DEFAULT_MIN_SUCCESS_RATE


def _is_alert_already_sent(batch_id: str, alert_type: str) -> bool:
    """
    Checks and sets an atomic deduplication key in Redis.
    Returns True if an alert of this type was already dispatched for this batch.
    """
    try:
        import django_rq
        queue_name = getattr(settings, "SINGLE_QUEUE_NAME", "zomato")
        redis_conn = django_rq.get_connection(queue_name)
        dedup_key = f"zomato:alert:sent:{batch_id}:{alert_type}"
        # Set if not exists, 24-hour expiration (86400 seconds)
        was_set = redis_conn.set(dedup_key, "1", nx=True, ex=86400)
        return not bool(was_set)
    except Exception as exc:
        logger.debug("Redis dedup check error (falling back to send): %s", exc)
        return False


def send_slack_message(
    text: str,
    blocks: Optional[List[Dict[str, Any]]] = None,
    color: Optional[str] = None,
) -> Tuple[bool, str]:
    """
    Dispatches a formatted message to Slack via Incoming Webhook or Bot Token.
    Returns (success: bool, status_message: str).
    """
    if not is_slack_alert_enabled():
        return False, "Slack alerts are disabled (SLACK_ALERT_ENABLED=false)"

    webhook_url = getattr(settings, "SLACK_WEBHOOK_URL", "") or os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        try:
            from config import load_env
            load_env()
            webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
        except Exception:
            pass
    bot_token = getattr(settings, "SLACK_BOT_TOKEN", "") or os.getenv("SLACK_BOT_TOKEN", "")
    channel = getattr(settings, "SLACK_CHANNEL", "") or os.getenv("SLACK_CHANNEL", "")

    if not webhook_url and not bot_token:
        msg = "Neither SLACK_WEBHOOK_URL nor SLACK_BOT_TOKEN is configured in environment"
        logger.debug(msg)
        return False, msg

    payload: Dict[str, Any] = {"text": text}

    if blocks:
        if color:
            payload["attachments"] = [{"color": color, "blocks": blocks}]
        else:
            payload["blocks"] = blocks

    # 1. Preferred: Slack Incoming Webhook
    if webhook_url:
        try:
            resp = requests.post(webhook_url, json=payload, timeout=8.0)
            if resp.status_code == 200 and resp.text.strip().lower() == "ok":
                logger.info("[SLACK_ALERT] Alert sent successfully via incoming webhook.")
                return True, "Alert sent successfully via Incoming Webhook"
            else:
                err_msg = f"Slack webhook returned status {resp.status_code}: {resp.text}"
                logger.warning("[SLACK_ALERT] %s", err_msg)
                return False, err_msg
        except requests.RequestException as req_err:
            err_msg = f"Network error sending Slack alert via webhook: {req_err}"
            logger.warning("[SLACK_ALERT] %s", err_msg)
            return False, err_msg

    # 2. Alternative: Slack Bot Token via chat.postMessage
    if bot_token:
        target_channel = channel or "#general"
        api_url = "https://slack.com/api/chat.postMessage"
        headers = {
            "Authorization": f"Bearer {bot_token}",
            "Content-Type": "application/json; charset=utf-8",
        }
        api_payload = dict(payload)
        api_payload["channel"] = target_channel
        try:
            resp = requests.post(api_url, headers=headers, json=api_payload, timeout=8.0)
            data = resp.json()
            if data.get("ok"):
                logger.info("[SLACK_ALERT] Alert sent successfully to %s via chat.postMessage.", target_channel)
                return True, f"Alert sent successfully to {target_channel}"
            else:
                err_msg = f"Slack chat.postMessage returned error: {data.get('error')}"
                logger.warning("[SLACK_ALERT] %s", err_msg)
                return False, err_msg
        except Exception as bot_err:
            err_msg = f"Error calling Slack chat.postMessage: {bot_err}"
            logger.warning("[SLACK_ALERT] %s", err_msg)
            return False, err_msg

    return False, "No valid dispatch method"


def build_request_completed_payload(
    batch: Any,
    success_rate: float,
) -> Tuple[str, List[Dict[str, Any]], str]:
    """Builds Slack Block Kit payload when a scrape request/batch finishes successfully."""
    batch_id = str(getattr(batch, "requestid", getattr(batch, "batch_id", "Unknown")))
    city = getattr(batch, "city", "Unknown")
    loc = getattr(batch, "location", "Unknown")
    completed = getattr(batch, "completed_jobs", 0)
    failed = getattr(batch, "failed_jobs", 0)
    total = getattr(batch, "total_restaurants", 0) or (completed + failed)
    now_str = timezone.now().strftime("%Y-%m-%d %H:%M:%S %Z")

    fallback_text = (
        f"✅ [Zomato Alert] Request Completed: Batch {batch_id} ({city}, {loc}) "
        f"- {completed}/{total} restaurants scraped ({success_rate:.1f}%)"
    )
    color = "#059669"  # Green

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "✅ Zomato Scraper: Request Completed",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Batch ID:*\n`{batch_id}`"},
                {"type": "mrkdwn", "text": f"*City / Location:*\n{city} ({loc})"},
                {"type": "mrkdwn", "text": "*Status:*\n`COMPLETED`"},
                {"type": "mrkdwn", "text": f"*Success Rate:*\n*{success_rate:.2f}%*"},
                {"type": "mrkdwn", "text": f"*Completed Jobs:*\n{completed} / {total}"},
                {"type": "mrkdwn", "text": f"*Failed Jobs:*\n{failed}"},
                {"type": "mrkdwn", "text": f"*Completed At:*\n{now_str}"},
            ],
        },
    ]

    return fallback_text, blocks, color


def build_low_success_rate_payload(
    batch: Any,
    success_rate: float,
    threshold: float,
) -> Tuple[str, List[Dict[str, Any]], str]:
    """Builds Slack Block Kit payload when job success rate falls below threshold."""
    batch_id = str(getattr(batch, "requestid", getattr(batch, "batch_id", "Unknown")))
    city = getattr(batch, "city", "Unknown")
    loc = getattr(batch, "location", "Unknown")
    completed = getattr(batch, "completed_jobs", 0)
    failed = getattr(batch, "failed_jobs", 0)
    total = getattr(batch, "total_restaurants", 0) or (completed + failed)
    now_str = timezone.now().strftime("%Y-%m-%d %H:%M:%S %Z")

    fallback_text = (
        f"⚠️ [Zomato Alert] Low Success Rate: {success_rate:.1f}% (< {threshold:.1f}%) "
        f"for Batch {batch_id} ({city}, {loc})"
    )
    color = "#ea580c"  # Amber / Warning Orange

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "⚠️ Zomato Scraper: Low Job Success Rate",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Batch ID:*\n`{batch_id}`"},
                {"type": "mrkdwn", "text": f"*City / Location:*\n{city} ({loc})"},
                {
                    "type": "mrkdwn",
                    "text": f"*Success Rate:*\n*{success_rate:.2f}%* (Required: `{threshold:.1f}%`)",
                },
                {"type": "mrkdwn", "text": f"*Completed Jobs:*\n{completed} / {total}"},
                {"type": "mrkdwn", "text": f"*Failed Jobs:*\n*{failed}*"},
                {"type": "mrkdwn", "text": f"*Timestamp:*\n{now_str}"},
            ],
        },
    ]

    return fallback_text, blocks, color


def check_and_alert_batch_status(
    batch: Any,
    trigger_reason: Optional[str] = None,
    error_message: str = "",
) -> bool:
    """
    Main evaluation function called upon batch completion.
    Checks:
    1. Request Completed: When batch finishes successfully (status == 'completed').
    2. Low Success Rate: When job success rate < 99.0%.
    Returns True if at least one alert was dispatched.
    """
    if not batch:
        return False

    batch_id = str(getattr(batch, "requestid", getattr(batch, "batch_id", "")))
    status = getattr(batch, "status", "")

    total = getattr(batch, "total_restaurants", 0)
    completed = getattr(batch, "completed_jobs", 0)
    failed = getattr(batch, "failed_jobs", 0)
    done_jobs = completed + failed

    # Calculate success rate against total expected or total attempted
    effective_total = total if total > 0 else done_jobs
    if effective_total > 0:
        success_rate = (completed / effective_total) * 100.0
    else:
        success_rate = 100.0 if status == "completed" else 0.0

    min_threshold = get_min_success_rate()
    dispatched_any = False

    # --------------------------------------------------------------------------
    # 1. Alert for Request Completion (status == 'completed')
    # --------------------------------------------------------------------------
    is_completed = (status == "completed" or trigger_reason == "request_completed")
    if is_completed:
        if not _is_alert_already_sent(batch_id, "request_completed"):
            text, blocks, color = build_request_completed_payload(batch, success_rate=success_rate)
            sent, info = send_slack_message(text=text, blocks=blocks, color=color)
            if sent:
                logger.info("[SLACK_ALERT] Dispatched 'request_completed' alert for batch %s", batch_id)
                dispatched_any = True
            else:
                logger.debug("[SLACK_ALERT] Failed to send 'request_completed' alert: %s", info)

    # --------------------------------------------------------------------------
    # 2. Alert for Low Job Success Rate (< 99.0%)
    # --------------------------------------------------------------------------
    should_alert_low_success = (
        (status in ("completed", "terminated", "failed_all_retries") or trigger_reason == "low_success_rate")
        and effective_total > 0
        and success_rate < min_threshold
    )

    if should_alert_low_success:
        if not _is_alert_already_sent(batch_id, "low_success_rate"):
            text, blocks, color = build_low_success_rate_payload(
                batch, success_rate=success_rate, threshold=min_threshold
            )
            sent, info = send_slack_message(text=text, blocks=blocks, color=color)
            if sent:
                logger.info(
                    "[SLACK_ALERT] Dispatched 'low_success_rate' alert (%.2f%% < %.1f%%) for batch %s",
                    success_rate,
                    min_threshold,
                    batch_id,
                )
                dispatched_any = True
            else:
                logger.debug("[SLACK_ALERT] Failed to send 'low_success_rate' alert: %s", info)

    return dispatched_any
