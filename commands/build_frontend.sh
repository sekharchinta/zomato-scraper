#!/usr/bin/env bash
# Build the Production React Bundle into frontend/dist/
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
cd "$DIR/frontend"

echo "================================================================="
echo "  Building Zomato Scraper React Frontend Production Assets"
echo "================================================================="
npm run build
echo "Build complete! Assets generated in frontend/dist/."
