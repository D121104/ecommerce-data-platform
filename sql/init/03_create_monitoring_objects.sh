#!/usr/bin/env bash

set -Eeuo pipefail

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=ingestion_user="$INGESTION_DB_USER" \
  --set=dbt_user="$DBT_DB_USER" \
  --set=bi_user="$BI_DB_USER" <<'EOSQL'

BEGIN;

SET ROLE :"ingestion_user";

CREATE TABLE IF NOT EXISTS ops.pipeline_alerts (
    alert_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    alert_key text NOT NULL UNIQUE,
    pipeline_name text NOT NULL,
    batch_id uuid,

    alert_type text NOT NULL,
    severity text NOT NULL,
    status text NOT NULL DEFAULT 'open',

    message text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,

    first_detected_at timestamptz NOT NULL
        DEFAULT clock_timestamp(),

    last_detected_at timestamptz NOT NULL
        DEFAULT clock_timestamp(),

    occurrence_count integer NOT NULL DEFAULT 1,
    resolved_at timestamptz,

    CONSTRAINT pipeline_alerts_severity_check
        CHECK (
            severity IN ('info', 'warning', 'critical')
        ),

    CONSTRAINT pipeline_alerts_status_check
        CHECK (
            status IN ('open', 'resolved')
        ),

    CONSTRAINT pipeline_alerts_occurrence_check
        CHECK (occurrence_count > 0),

    CONSTRAINT pipeline_alerts_details_check
        CHECK (jsonb_typeof(details) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_pipeline_alerts_open
    ON ops.pipeline_alerts (
        pipeline_name,
        status,
        last_detected_at DESC
    );

RESET ROLE;

CREATE OR REPLACE VIEW ops.v_pipeline_run_health AS
WITH latest_runs AS (
    SELECT DISTINCT ON (pipeline_name)
        batch_id,
        pipeline_name,
        source_name,
        status,
        started_at,
        finished_at,
        error_message,
        records_inserted,
        records_updated,
        records_rejected
    FROM ops.pipeline_runs
    ORDER BY
        pipeline_name,
        started_at DESC,
        batch_id DESC
)
SELECT
    batch_id,
    pipeline_name,
    source_name,
    status,
    started_at,
    finished_at,

    EXTRACT(
        EPOCH FROM (
            COALESCE(finished_at, clock_timestamp())
            - started_at
        )
    )::bigint AS duration_seconds,

    EXTRACT(
        EPOCH FROM (
            clock_timestamp()
            - COALESCE(finished_at, started_at)
        )
    )::bigint AS age_seconds,

    records_inserted,
    records_updated,
    records_rejected,
    error_message,

    CASE
        WHEN status = 'failed'
            THEN 'critical'

        WHEN status = 'running'
             AND started_at < clock_timestamp()
                 - INTERVAL '120 minutes'
            THEN 'critical'

        WHEN status = 'partial'
            THEN 'warning'

        WHEN status = 'success'
             AND finished_at < clock_timestamp()
                 - INTERVAL '26 hours'
            THEN 'warning'

        WHEN status IN ('success', 'running')
            THEN 'healthy'

        ELSE 'unknown'
    END AS health_status

FROM latest_runs;

ALTER TABLE ops.pipeline_alerts
    OWNER TO :"ingestion_user";

ALTER VIEW ops.v_pipeline_run_health
    OWNER TO :"ingestion_user";

GRANT USAGE ON SCHEMA ops
    TO :"dbt_user", :"bi_user";

GRANT INSERT, SELECT, UPDATE
    ON TABLE ops.pipeline_alerts
    TO :"ingestion_user";

GRANT SELECT
    ON TABLE ops.pipeline_alerts
    TO :"dbt_user", :"bi_user";

GRANT SELECT
    ON ops.v_pipeline_run_health
    TO :"dbt_user", :"bi_user";

GRANT USAGE, SELECT
    ON SEQUENCE ops.pipeline_alerts_alert_id_seq
    TO :"ingestion_user";

COMMIT;

EOSQL