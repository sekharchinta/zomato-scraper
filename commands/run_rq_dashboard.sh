#!/usr/bin/env bash
# Start Standalone RQ Dashboard on Port 9181 pointing to Redis DB 1
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

export RQ_DASHBOARD_REDIS_URL="${RQ_DASHBOARD_REDIS_URL:-redis://127.0.0.1:6379/1}"

# Ensure queue 'zomato' is pre-registered in Redis DB 1 so dashboard displays it immediately
"$PYTHON" -c "
import os, django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'zomato_scraper.settings')
django.setup()
import django_rq
from config import SINGLE_QUEUE_NAME
conn = django_rq.get_connection(SINGLE_QUEUE_NAME)
conn.sadd('rq:queues', f'rq:queue:{SINGLE_QUEUE_NAME}')
" 2>/dev/null || true

echo "================================================================="
echo "  Starting RQ Dashboard on http://0.0.0.0:9181"
echo "  Connected to Dedicated Redis DB 1: $RQ_DASHBOARD_REDIS_URL"
echo "  Monitoring Single Queue: 'zomato'"
echo "================================================================="

exec rq-dashboard --port 9181 -b 0.0.0.0 --redis-url "$RQ_DASHBOARD_REDIS_URL" "$@"
