#!/usr/bin/env bash

set -Eeuo pipefail

: "${METABASE_APP_DB_NAME:?Missing METABASE_APP_DB_NAME}"
: "${METABASE_APP_DB_USER:?Missing METABASE_APP_DB_USER}"
: "${METABASE_APP_DB_PASSWORD:?Missing METABASE_APP_DB_PASSWORD}"

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=metabase_db="$METABASE_APP_DB_NAME" \
  --set=metabase_user="$METABASE_APP_DB_USER" \
  --set=metabase_password="$METABASE_APP_DB_PASSWORD" <<'EOSQL'

SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L',
    :'metabase_user',
    :'metabase_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = :'metabase_user'
)
\gexec

ALTER ROLE :"metabase_user"
    LOGIN
    PASSWORD :'metabase_password';

SELECT format(
    'CREATE DATABASE %I OWNER %I',
    :'metabase_db',
    :'metabase_user'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = :'metabase_db'
)
\gexec

ALTER DATABASE :"metabase_db"
    OWNER TO :"metabase_user";

EOSQL