#!/usr/bin/env bash
# Start the Vite React Frontend Development Server (Port 5173) with HMR
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
cd "$DIR/frontend"

echo "================================================================="
echo "  Starting Zomato Scraper React Frontend on http://localhost:5173"
echo "  Proxying API calls -> http://127.0.0.1:8000"
echo "================================================================="
exec npm run dev -- --host 0.0.0.0
