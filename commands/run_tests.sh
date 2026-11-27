#!/usr/bin/env bash
# Execute Full Test Suites (Django PostgreSQL, Multi-User Queues, Isolation, Slack Alerts & Scraper Engine)
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
cd "$DIR/backend"

PYTHON="$DIR/venv/bin/python"
if [ ! -f "$PYTHON" ]; then
    PYTHON="python3"
fi

if [ -d "venv" ]; then
    source venv/bin/activate
fi

echo "================================================================="
echo "  [1/2] Running Full Django PostgreSQL, Auth, Queue & Isolation Tests"
echo "================================================================="
"$PYTHON" manage.py test \
    tests.test_django_scraper \
    tests.test_auth \
    tests.test_slack_alerts \
    tests.test_frontend_dashboard_user_isolation \
    tests.test_dynamic_per_user_queues \
    tests.test_isolated_per_user_pipeline

echo ""
echo "================================================================="
echo "  [2/2] Running Core Scraper Engine Unit Test Suite"
echo "================================================================="
"$PYTHON" -m unittest tests/test_scraper.py
