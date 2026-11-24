"""
tasks package exports:
- location_extractor: Location suggestion extraction and PostgreSQL latlong_data persistence.
- url_extractor: Restaurant URL discovery from coordinates and PostgreSQL zomato_jobs persistence.
- restaurant_extractor: Restaurant raw details extraction and pipeline execution in PostgreSQL zomato_restaurants.
- review_extractor: Review URLs generator and paginated customer reviews extraction.
- base: Scraper session warming, preloaded state extraction, and pause/resume checks.
"""

from scraper_app.tasks.base import DEFAULT_RETRY, extract_zomato_preloaded_state, get_active_scraper
from scraper_app.tasks.location_extractor import (
    extract_locations_and_store,
    get_stored_locations,
)
from scraper_app.tasks.url_extractor import (
    extract_urls_from_latlongs,
    extract_location_urls_task,
)
from scraper_app.tasks.restaurant_extractor import (
    extract_restaurant_details,
    process_restaurant_pipeline_job,
)
from scraper_app.tasks.review_extractor import (
    extract_restaurant_reviews,
    generate_review_api_urls,
)

__all__ = [
    "DEFAULT_RETRY",
    "extract_zomato_preloaded_state",
    "get_active_scraper",
    "extract_locations_and_store",
    "get_stored_locations",
    "extract_urls_from_latlongs",
    "extract_location_urls_task",
    "extract_restaurant_details",
    "process_restaurant_pipeline_job",
    "extract_restaurant_reviews",
    "generate_review_api_urls",
]
