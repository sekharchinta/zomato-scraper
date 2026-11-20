#!/usr/bin/env bash
# Root shortcut to commands/run_rq_dashboard.sh
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
exec "$DIR/commands/run_rq_dashboard.sh" "$@"
