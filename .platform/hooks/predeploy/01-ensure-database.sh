

#!/bin/bash

# Ensure required environment variables are set
if [[ -z "$DB_HOST" || -z "$DB_USER" || -z "$DB_PASSWORD" || -z "$DB_NAME" ]]; then
  echo "❌ One or more required environment variables (DB_HOST, DB_USER, DB_PASSWORD, DB_NAME) are not set."
  exit 1
fi

echo "🔍 Checking if database '${DB_NAME}' exists..."

# Default to 'postgres' database for initial connection
PGPASSWORD="${DB_PASSWORD}" psql "host=${DB_HOST} port=${DB_PORT:-5432} user=${DB_USER} dbname=postgres sslmode=require" <<EOF
DO \$\$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_database WHERE datname = '${DB_NAME}'
  ) THEN
    RAISE NOTICE 'Creating database ${DB_NAME}...';
    EXECUTE 'CREATE DATABASE ${DB_NAME}';
  ELSE
    RAISE NOTICE 'Database ${DB_NAME} already exists.';
  END IF;
END
\$\$;
EOF