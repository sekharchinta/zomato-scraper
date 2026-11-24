"""
Custom Test Runner for PostgreSQL without requiring CREATEDB privilege.
Reuses existing database and schema 'zomato' while isolating test changes via transaction rollbacks.
"""

from django.test.runner import DiscoverRunner


class PostgresSchemaTestRunner(DiscoverRunner):
    """
    Test runner that runs tests within transaction rollbacks on the existing PostgreSQL database,
    avoiding the requirement for CREATEDB permissions.
    """

    def setup_databases(self, **kwargs):
        return []

    def teardown_databases(self, old_config, **kwargs):
        pass
