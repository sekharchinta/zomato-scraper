#!/usr/bin/env bash
# Apply PostgreSQL Migrations for Schema 'zomato'
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
echo "  Applying PostgreSQL Migrations (Schema: 'zomato')"
echo "================================================================="
exec "$PYTHON" manage.py migrate "$@"
