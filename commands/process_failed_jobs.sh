#!/usr/bin/env bash
# Trigger Failed Jobs Requeue & PostgreSQL Fallback Persistence for Queue 'zomato'
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
cd "$DIR"

PYTHON="$DIR/venv/bin/python"
if [ ! -f "$PYTHON" ]; then
    PYTHON="python3"
fi

if [ -d "venv" ]; then
    source venv/bin/activate
fi

echo "================================================================="
echo "  Processing Failed Jobs for Single Queue 'zomato' (Redis DB 1)"
echo "================================================================="
exec "$PYTHON" manage.py process_failed_jobs "$@"
