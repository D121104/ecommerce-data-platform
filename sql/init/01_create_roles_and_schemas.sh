#!/usr/bin/env bash

set -Eeuo pipefail

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=db_name="$POSTGRES_DB" \
  --set=ingestion_user="$INGESTION_DB_USER" \
  --set=ingestion_password="$INGESTION_DB_PASSWORD" \
  --set=dbt_user="$DBT_DB_USER" \
  --set=dbt_password="$DBT_DB_PASSWORD" \
  --set=bi_user="$BI_DB_USER" \
  --set=bi_password="$BI_DB_PASSWORD" <<'EOSQL'

CREATE ROLE :"ingestion_user"
  LOGIN
  PASSWORD :'ingestion_password';

CREATE ROLE :"dbt_user"
  LOGIN
  PASSWORD :'dbt_password';

CREATE ROLE :"bi_user"
  LOGIN
  PASSWORD :'bi_password';

REVOKE CONNECT ON DATABASE :"db_name" FROM PUBLIC;

GRANT CONNECT ON DATABASE :"db_name"
  TO :"ingestion_user", :"dbt_user", :"bi_user";

GRANT CREATE ON DATABASE :"db_name"
  TO :"dbt_user";

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA raw
  AUTHORIZATION :"ingestion_user";

CREATE SCHEMA ops
  AUTHORIZATION :"ingestion_user";

CREATE SCHEMA staging
  AUTHORIZATION :"dbt_user";

CREATE SCHEMA snapshots
  AUTHORIZATION :"dbt_user";

CREATE SCHEMA warehouse
  AUTHORIZATION :"dbt_user";

CREATE SCHEMA marts
  AUTHORIZATION :"dbt_user";

GRANT USAGE ON SCHEMA raw, ops
  TO :"dbt_user";

GRANT SELECT ON ALL TABLES IN SCHEMA raw, ops
  TO :"dbt_user";

ALTER DEFAULT PRIVILEGES
  FOR ROLE :"ingestion_user"
  IN SCHEMA raw
  GRANT SELECT ON TABLES TO :"dbt_user";

ALTER DEFAULT PRIVILEGES
  FOR ROLE :"ingestion_user"
  IN SCHEMA ops
  GRANT SELECT ON TABLES TO :"dbt_user";

GRANT USAGE ON SCHEMA marts
  TO :"bi_user";

GRANT SELECT ON ALL TABLES IN SCHEMA marts
  TO :"bi_user";

ALTER DEFAULT PRIVILEGES
  FOR ROLE :"dbt_user"
  IN SCHEMA marts
  GRANT SELECT ON TABLES TO :"bi_user";

EOSQL
