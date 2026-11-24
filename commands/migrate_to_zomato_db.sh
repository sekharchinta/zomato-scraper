#!/usr/bin/env bash
# ==============================================================================
# Migrate all PostgreSQL tables and data from 'eazydiner' to 'zomato' database
# Manages connections, schema restoration, migrations, and environment configuration
# ==============================================================================
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
cd "$DIR/backend"

PYTHON="$DIR/venv/bin/python"
if [ ! -f "$PYTHON" ]; then
    PYTHON="python3"
fi

POSTGRES_HOST="${POSTGRES_HOST:-127.0.0.1}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
TARGET_DB="${POSTGRES_DB:-zomato-scraper}"
SOURCE_DB="${SOURCE_DB:-zomato}"

echo "================================================================="
echo "  [1/5] Checking PostgreSQL Target Database: '${TARGET_DB}'"
echo "================================================================="

# Check if target database exists
DB_EXISTS=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${TARGET_DB}';" 2>/dev/null || true)

if [ "$DB_EXISTS" != "1" ]; then
    echo "Attempting to create database '${TARGET_DB}' as user '${POSTGRES_USER}'..."
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE ${TARGET_DB} OWNER ${POSTGRES_USER};" 2>/dev/null || true
    DB_EXISTS=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${TARGET_DB}';" 2>/dev/null || true)
fi

if [ "$DB_EXISTS" != "1" ]; then
    echo ""
    echo "⚠️  Database '${TARGET_DB}' does not exist yet, and role '${POSTGRES_USER}' requires CREATEDB privilege."
    echo "👉 Please run this single command in your terminal with sudo to create '${TARGET_DB}':"
    echo ""
    echo "   sudo -u postgres psql -c \"CREATE DATABASE ${TARGET_DB} OWNER ${POSTGRES_USER}; GRANT ALL PRIVILEGES ON DATABASE ${TARGET_DB} TO ${POSTGRES_USER};\""
    echo ""
    if [ -t 0 ]; then
        read -p "Press [Enter] once you have executed the command above..."
        DB_EXISTS=$(PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${TARGET_DB}';" 2>/dev/null || true)
    fi
fi

if [ "$DB_EXISTS" != "1" ]; then
    echo "❌ Database '${TARGET_DB}' could not be accessed. Migration aborted."
    exit 1
fi

echo "✓ Database '${TARGET_DB}' exists and is accessible by '${POSTGRES_USER}'."

echo ""
echo "================================================================="
echo "  [2/5] Transferring Tables and Data from '${SOURCE_DB}' to '${TARGET_DB}'"
echo "================================================================="

# Create schema 'zomato' in target db if not present
PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$TARGET_DB" -c "CREATE SCHEMA IF NOT EXISTS zomato AUTHORIZATION ${POSTGRES_USER};"

# Dump schema zomato from source db and pipe into target db
echo "Dumping and restoring schema 'zomato' tables (batches, jobs, restaurants)..."
PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$SOURCE_DB" -n zomato --clean --if-exists | \
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$TARGET_DB" >/dev/null

# Dump schema public (django_migrations, django_content_type)
echo "Dumping and restoring public schema (Django system tables)..."
PGPASSWORD="$POSTGRES_PASSWORD" pg_dump -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$SOURCE_DB" -n public --clean --if-exists | \
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$TARGET_DB" >/dev/null || true

echo "✓ Data transfer complete."

echo ""
echo "================================================================="
echo "  [3/5] Updating Environment and Connection Configuration"
echo "================================================================="

# Update .env
if [ -f ".env" ]; then
    sed -i 's/^POSTGRES_DB=.*/POSTGRES_DB=zomato/' .env
    echo "✓ Updated .env -> POSTGRES_DB=zomato"
fi

# Update config.py default
sed -i 's/DEFAULT_POSTGRES_DB = "eazydiner"/DEFAULT_POSTGRES_DB = "zomato"/' config.py
echo "✓ Updated config.py -> DEFAULT_POSTGRES_DB = \"zomato\""

export POSTGRES_DB="zomato"

echo ""
echo "================================================================="
echo "  [4/5] Running Django Migrations on Database '${TARGET_DB}'"
echo "================================================================="
"$PYTHON" manage.py migrate

echo ""
echo "================================================================="
echo "  [5/5] Reparsing Raw Restaurant & Review Data in '${TARGET_DB}'"
echo "================================================================="
"$PYTHON" manage.py reparse_restaurant_data

echo ""
echo "================================================================="
echo "🎉 SUCCESS: All tables moved to '${TARGET_DB}' database!"
echo "   Connections are now actively using PostgreSQL '${TARGET_DB}'."
echo "================================================================="
