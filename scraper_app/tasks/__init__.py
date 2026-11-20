"""
Tasks package for Zomato Scraper.
Exposes modular RQ worker tasks:
- location_task: extract_location_urls_task (locations_queue)
- restaurant_task: extract_restaurant_data_task (restaurants_queue)
- review_task: extract_restaurant_reviews_task (reviews_queue)
"""

from .base import (
    DEFAULT_RETRY,
    extract_zomato_preloaded_state,
    get_active_scraper,
    save_task_fallback_data,
    update_request_progress,
)
from .single_pipeline_task import (
    process_restaurant_pipeline_job,
    generate_review_api_urls,
)
from .location_task import (
    extract_location_urls_task,
    discover_and_enqueue_restaurant_urls_job,
)
from .restaurant_task import extract_restaurant_data_task
from .review_task import extract_restaurant_reviews_task

__all__ = [
    "DEFAULT_RETRY",
    "extract_zomato_preloaded_state",
    "get_active_scraper",
    "save_task_fallback_data",
    "update_request_progress",
    "process_restaurant_pipeline_job",
    "generate_review_api_urls",
    "extract_location_urls_task",
    "discover_and_enqueue_restaurant_urls_job",
    "extract_restaurant_data_task",
    "extract_restaurant_reviews_task",
]
