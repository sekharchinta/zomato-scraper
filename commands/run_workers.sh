#!/usr/bin/env bash
# Start Concurrent RQ Workers on the Single Queue ('zomato')
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

# Intelligent argument handling:
# If first argument is an integer, treat it as --workers <int>
# Otherwise pass all arguments directly to manage.py run_workers
if [[ "$1" =~ ^[0-9]+$ ]]; then
    WORKERS="$1"
    shift
    exec "$PYTHON" manage.py run_workers --workers "$WORKERS" "$@"
else
    exec "$PYTHON" manage.py run_workers "$@"
fi
