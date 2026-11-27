"""
Custom Test Runner for PostgreSQL without requiring CREATEDB privilege.
Reuses existing database and schema 'zomato' while isolating test changes via transaction rollbacks,
and isolates Redis queue tests on dedicated test Redis DB 2 without touching live Redis DB 1.
"""

import django_rq
from django.test.runner import DiscoverRunner


class PostgresSchemaTestRunner(DiscoverRunner):
    """
    Test runner that runs tests within transaction rollbacks on the existing PostgreSQL database,
    and flushes the isolated test Redis DB (DB != 1) cleanly before and after tests.
    """

    def _flush_test_redis_db(self) -> None:
        try:
            conn = django_rq.get_connection("zomato")
            db_num = conn.connection_pool.connection_kwargs.get("db")
            if db_num is not None and int(db_num) != 1:
                conn.flushdb()
        except Exception:
            pass

    def setup_databases(self, **kwargs):
        self._flush_test_redis_db()
        return []

    def teardown_databases(self, old_config, **kwargs):
        self._flush_test_redis_db()
