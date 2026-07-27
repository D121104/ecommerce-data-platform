#!/usr/bin/env bash

set -Eeuo pipefail

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=ingestion_user="$INGESTION_DB_USER" <<'EOSQL'

BEGIN;

SET ROLE :"ingestion_user";

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    batch_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    pipeline_name text NOT NULL,
    source_name text NOT NULL,

    status text NOT NULL DEFAULT 'running',

    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,

    records_extracted integer NOT NULL DEFAULT 0,
    records_inserted integer NOT NULL DEFAULT 0,
    records_updated integer NOT NULL DEFAULT 0,
    records_rejected integer NOT NULL DEFAULT 0,

    error_message text,

    run_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT pipeline_runs_status_check
        CHECK (
            status IN ('running', 'success', 'partial', 'failed')
        ),

    CONSTRAINT pipeline_runs_record_counts_check
        CHECK (
            records_extracted >= 0
            AND records_inserted >= 0
            AND records_updated >= 0
            AND records_rejected >= 0
        ),

    CONSTRAINT pipeline_runs_finished_at_check
        CHECK (
            finished_at IS NULL
            OR finished_at >= started_at
        ),

    CONSTRAINT pipeline_runs_status_time_check
        CHECK (
            (status = 'running' AND finished_at IS NULL)
            OR
            (
                status IN ('success', 'partial', 'failed')
                AND finished_at IS NOT NULL
            )
        ),

    CONSTRAINT pipeline_runs_metadata_check
        CHECK (
            jsonb_typeof(run_metadata) = 'object'
        )
);


CREATE TABLE IF NOT EXISTS ops.rejected_records (
    rejected_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    batch_id uuid NOT NULL,
    entity_name text NOT NULL,
    source_record_id text,

    error_code text NOT NULL,
    error_message text NOT NULL,

    payload jsonb NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT rejected_records_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES ops.pipeline_runs (batch_id),

    CONSTRAINT rejected_records_payload_check
        CHECK (
            jsonb_typeof(payload) = 'object'
        )
);


CREATE TABLE IF NOT EXISTS raw.categories (
    category_id bigint PRIMARY KEY,

    name text NOT NULL,
    slug text,
    image_url text,

    source_created_at timestamptz,
    source_updated_at timestamptz,

    payload jsonb NOT NULL,
    record_hash char(64) NOT NULL,

    batch_id uuid NOT NULL,

    is_active boolean NOT NULL DEFAULT true,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT categories_id_check
        CHECK (category_id > 0),

    CONSTRAINT categories_name_check
        CHECK (btrim(name) <> ''),

    CONSTRAINT categories_payload_check
        CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT categories_hash_check
        CHECK (record_hash ~ '^[0-9a-f]{64}$'),

    CONSTRAINT categories_seen_time_check
        CHECK (last_seen_at >= first_seen_at),

    CONSTRAINT categories_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES ops.pipeline_runs (batch_id)
);


CREATE TABLE IF NOT EXISTS raw.products (
    product_id bigint PRIMARY KEY,

    title text NOT NULL,
    slug text,
    price numeric(12, 2) NOT NULL,
    description text,

    category_id bigint,
    images text[] NOT NULL DEFAULT ARRAY[]::text[],

    source_created_at timestamptz,
    source_updated_at timestamptz,

    payload jsonb NOT NULL,
    record_hash char(64) NOT NULL,

    batch_id uuid NOT NULL,

    is_active boolean NOT NULL DEFAULT true,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT products_id_check
        CHECK (product_id > 0),

    CONSTRAINT products_title_check
        CHECK (btrim(title) <> ''),

    CONSTRAINT products_price_check
        CHECK (price >= 0),

    CONSTRAINT products_category_id_check
        CHECK (category_id IS NULL OR category_id > 0),

    CONSTRAINT products_images_check
        CHECK (array_position(images, NULL) IS NULL),

    CONSTRAINT products_payload_check
        CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT products_hash_check
        CHECK (record_hash ~ '^[0-9a-f]{64}$'),

    CONSTRAINT products_seen_time_check
        CHECK (last_seen_at >= first_seen_at),

    CONSTRAINT products_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES ops.pipeline_runs (batch_id)
);


CREATE TABLE IF NOT EXISTS raw.users (
    user_id bigint PRIMARY KEY,

    email text NOT NULL,
    name text NOT NULL,
    role text,
    avatar_url text,

    source_created_at timestamptz,
    source_updated_at timestamptz,

    payload jsonb NOT NULL,
    record_hash char(64) NOT NULL,

    batch_id uuid NOT NULL,

    is_active boolean NOT NULL DEFAULT true,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT users_id_check
        CHECK (user_id > 0),

    CONSTRAINT users_email_check
        CHECK (btrim(email) <> ''),

    CONSTRAINT users_name_check
        CHECK (btrim(name) <> ''),

    CONSTRAINT users_payload_check
        CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT users_password_check
        CHECK (NOT (payload ? 'password')),

    CONSTRAINT users_hash_check
        CHECK (record_hash ~ '^[0-9a-f]{64}$'),

    CONSTRAINT users_seen_time_check
        CHECK (last_seen_at >= first_seen_at),

    CONSTRAINT users_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES ops.pipeline_runs (batch_id)
);


CREATE TABLE IF NOT EXISTS raw.orders (
    order_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),

    customer_id bigint NOT NULL,
    order_status text NOT NULL,
    ordered_at timestamptz NOT NULL,

    currency text NOT NULL DEFAULT 'USD',
    payment_method text NOT NULL,
    shipping_country text,

    payload jsonb NOT NULL,
    batch_id uuid NOT NULL,

    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT orders_customer_check
        CHECK (customer_id > 0),

    CONSTRAINT orders_status_check
        CHECK (
            order_status IN (
                'pending',
                'paid',
                'shipped',
                'delivered',
                'cancelled'
            )
        ),

    CONSTRAINT orders_currency_check
        CHECK (currency ~ '^[A-Z]{3}$'),

    CONSTRAINT orders_country_check
        CHECK (
            shipping_country IS NULL
            OR shipping_country ~ '^[A-Z]{2}$'
        ),

    CONSTRAINT orders_payload_check
        CHECK (jsonb_typeof(payload) = 'object'),

    CONSTRAINT orders_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES ops.pipeline_runs (batch_id)
);


CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id uuid NOT NULL,
    line_number smallint NOT NULL,

    product_id bigint NOT NULL,
    quantity smallint NOT NULL,
    unit_price numeric(12, 2) NOT NULL,

    batch_id uuid NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),

    PRIMARY KEY (order_id, line_number),

    CONSTRAINT order_items_line_check
        CHECK (line_number > 0),

    CONSTRAINT order_items_product_check
        CHECK (product_id > 0),

    CONSTRAINT order_items_quantity_check
        CHECK (quantity > 0),

    CONSTRAINT order_items_price_check
        CHECK (unit_price >= 0),

    CONSTRAINT order_items_order_fk
        FOREIGN KEY (order_id)
        REFERENCES raw.orders (order_id)
        ON DELETE CASCADE,

    CONSTRAINT order_items_batch_fk
        FOREIGN KEY (batch_id)
        REFERENCES ops.pipeline_runs (batch_id)
);


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_started
    ON ops.pipeline_runs (status, started_at);


CREATE INDEX IF NOT EXISTS idx_rejected_records_batch
    ON ops.rejected_records (batch_id);


CREATE INDEX IF NOT EXISTS idx_categories_batch
    ON raw.categories (batch_id);


CREATE INDEX IF NOT EXISTS idx_products_category
    ON raw.products (category_id);


CREATE INDEX IF NOT EXISTS idx_products_batch
    ON raw.products (batch_id);


CREATE INDEX IF NOT EXISTS idx_products_active
    ON raw.products (is_active);


CREATE INDEX IF NOT EXISTS idx_users_email
    ON raw.users (email);


CREATE INDEX IF NOT EXISTS idx_users_batch
    ON raw.users (batch_id);


CREATE INDEX IF NOT EXISTS idx_orders_customer_time
    ON raw.orders (customer_id, ordered_at);


CREATE INDEX IF NOT EXISTS idx_orders_status_time
    ON raw.orders (order_status, ordered_at);


CREATE INDEX IF NOT EXISTS idx_orders_batch
    ON raw.orders (batch_id);


CREATE INDEX IF NOT EXISTS idx_order_items_product
    ON raw.order_items (product_id);


CREATE INDEX IF NOT EXISTS idx_order_items_batch
    ON raw.order_items (batch_id);


COMMENT ON TABLE ops.pipeline_runs IS
    'One row per pipeline execution for lineage and monitoring.';

COMMENT ON TABLE ops.rejected_records IS
    'Records rejected during extraction, validation, or loading.';

COMMENT ON TABLE raw.products IS
    'Current state of products extracted from the Platzi Fake Store API.';

COMMENT ON COLUMN raw.products.record_hash IS
    'SHA-256 hash of normalized business fields used for change detection.';

COMMENT ON TABLE raw.users IS
    'Current state of API users; password fields must never be stored.';

COMMENT ON TABLE raw.orders IS
    'Append-only synthetic customer orders.';

COMMENT ON COLUMN raw.order_items.unit_price IS
    'Product price captured at purchase time, independent of current catalog price.';

COMMIT;

EOSQL
