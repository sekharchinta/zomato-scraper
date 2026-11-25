"""
URL Configuration for Zomato-Scraper.
"""

from django.conf import settings
from django.contrib import admin
from django.urls import include, path, re_path
from django.views.static import serve

from scraper_app.views import (
    react_app_view,
    delete_batch_api,
    export_csv_api,
    get_collection_stats_api,
    get_completed_batches_api,
    get_schema_fields_api,
    get_scrape_data_api,
    scrape_status_api,
    search_locations_api,
    start_queued_batch_api,
    start_scrape_api,
    terminate_batch_api,
    test_slack_alert_api,
)
from scraper_app.auth_views import (
    google_auth_callback,
    google_auth_config,
    get_current_user,
    logout_view,
)

# Customize Admin Site Branding
admin.site.site_header = "Zomato-Scraper Admin Dashboard"
admin.site.site_title = "Zomato-Scraper Admin"
admin.site.index_title = "Zomato Scraping & Queue Management"

urlpatterns = [
    # Main React Single Page Application (SPA)
    path("", react_app_view, name="react_app"),

    # REST APIs
    path("api/search-locations/", search_locations_api, name="api_search_locations"),
    path("api/collection-stats/", get_collection_stats_api, name="api_collection_stats"),
    path("api/get-schema-fields/", get_schema_fields_api, name="api_schema_fields"),
    path("api/start-scrape/", start_scrape_api, name="api_start_scrape"),
    path("api/scrape-status/<str:request_id>/", scrape_status_api, name="api_scrape_status"),
    path("api/scrape-data/<str:request_id>/", get_scrape_data_api, name="api_scrape_data"),
    path("api/delete-batch/<str:request_id>/", delete_batch_api, name="api_delete_batch"),
    path("api/cancel-batch/<str:request_id>/", delete_batch_api, name="api_cancel_batch"),
    path("api/terminate-batch/<str:request_id>/", terminate_batch_api, name="api_terminate_batch"),
    path("api/start-queued-batch/<str:request_id>/", start_queued_batch_api, name="api_start_queued_batch"),
    path("api/export-csv/<str:request_id>/", export_csv_api, name="api_export_csv"),
    path("api/completed-batches/", get_completed_batches_api, name="api_completed_batches"),
    path("api/test-slack-alert/", test_slack_alert_api, name="api_test_slack_alert"),

    # Google OAuth 2.0 Authentication
    path("api/auth/google/config/", google_auth_config, name="api_google_auth_config"),
    path("api/auth/google/", google_auth_callback, name="api_google_auth_callback"),
    path("api/auth/me/", get_current_user, name="api_auth_me"),
    path("api/auth/logout/", logout_view, name="api_auth_logout"),

    # Direct static assets serving for compiled React bundle in production
    re_path(r"^assets/(?P<path>.*)$", serve, {"document_root": getattr(settings, "FRONTEND_DIST_DIR", settings.ROOT_DIR / "frontend" / "dist") / "assets"}),

    # Django Default Admin & RQ Monitoring
    path("admin/django-rq/", include("django_rq.urls")),
    path("admin/", admin.site.urls),
]
