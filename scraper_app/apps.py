from django.apps import AppConfig


class ScraperAppConfig(AppConfig):
    name = 'scraper_app'

    def ready(self):
        """Ensure single queue 'zomato' is pre-registered in Redis DB 1 so RQ Dashboard displays it immediately."""
        try:
            import django_rq
            from config import SINGLE_QUEUE_NAME
            conn = django_rq.get_connection(SINGLE_QUEUE_NAME)
            conn.sadd("rq:queues", f"rq:queue:{SINGLE_QUEUE_NAME}")
        except Exception:
            pass
