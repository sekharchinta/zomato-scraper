#!/usr/bin/env bash
# Execute Full Test Suites (Django PostgreSQL & Core Scraper Engine)
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
echo "  [1/2] Running Django PostgreSQL & Single Queue Test Suite"
echo "================================================================="
"$PYTHON" manage.py test

echo ""
echo "================================================================="
echo "  [2/2] Running Core Scraper Engine Unit Test Suite (34 Tests)"
echo "================================================================="
"$PYTHON" -m unittest tests/test_scraper.py
