#!/usr/bin/env bash
# Start the Django Web Application Server (Port 8000)
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
echo "  Starting Zomato Scraper Django Web Server on http://0.0.0.0:8000"
echo "================================================================="
exec "$PYTHON" manage.py runserver 0.0.0.0:8000 "$@"
