# Data Model and Contracts

## 1. Layer overview

| Layer | Schemas | Materialization | Owner | Consumer |
|---|---|---|---|---|
| Source/raw | `raw` | PostgreSQL tables | `ingestion_user` | dbt staging |
| Operations | `ops` | PostgreSQL tables/view | `ingestion_user` | Airflow, monitor, dbt/BI read |
| Staging | `staging` or target-prefixed | dbt views | `dbt_user` | warehouse |
| Snapshot extension | `snapshots` or target-prefixed | dbt snapshots when configured | `dbt_user` | warehouse |
| Warehouse | `warehouse` or target-prefixed | dbt tables | `dbt_user` | marts |
| BI | `marts` or target-prefixed | dbt tables | `dbt_user` | `bi_reader` / Metabase |

Production BI contract is the unprefixed `staging`, `warehouse`, and `marts` schemas.

## 2. Raw and operations tables

### `ops.pipeline_runs`

**Grain:** one row per ingestion or orchestration batch.

| Column group | Meaning |
|---|---|
| `batch_id` | UUID primary key and lineage identifier |
| `pipeline_name`, `source_name` | execution identity |
| `status` | `running`, `success`, `partial` or `failed` |
| `started_at`, `finished_at` | execution timestamps in UTC |
| `records_*` | extracted/inserted/updated/rejected counters |
| `error_message` | bounded failure detail |
| `run_metadata` | JSON object, including Airflow run ID for orchestration rows |

`batch_id` is the idempotency boundary. The orchestration DAG uses deterministic UUID5 values derived from `run_id`.

### `ops.rejected_records`

**Grain:** one rejected source record/event.

Stores `batch_id`, entity, source ID, error code/message, JSON object payload and rejection timestamp. It is the quarantine path rather than a silent drop.

### `ops.pipeline_alerts`

**Grain:** one open/resolved alert key.

`alert_key` is unique, so repeated monitor observations update `occurrence_count` instead of creating alert spam. `details` is a JSON object and may contain expected run timestamps and latest audit status.

### `raw.categories`

**Grain:** one current row per source category ID.

Important fields: `category_id`, `name`, `slug`, `image_url`, source timestamps, JSON `payload`, SHA-256 `record_hash`, `batch_id`, active/seen timestamps.

### `raw.products`

**Grain:** one current row per source product ID.

Important fields: `product_id`, `title`, non-negative `price`, optional `category_id`, `images`, JSON `payload`, `record_hash`, `batch_id` and active/seen timestamps.

### `raw.users`

**Grain:** one current row per source user ID.

Important fields: `user_id`, `email`, `name`, `role`, `avatar_url`, JSON `payload`, `record_hash`, `batch_id` and active/seen timestamps. A database check rejects payloads containing the `password` key.

### `raw.orders`

**Grain:** one synthetic order.

Important fields: `order_id`, `customer_id`, `order_status`, `ordered_at`, three-letter `currency`, `payment_method`, optional country, JSON payload, `batch_id` and `ingested_at`.

### `raw.order_items`

**Grain:** one order + positive `line_number`.

Primary key: `(order_id, line_number)`. Quantity and unit price are non-negative/positive according to the constraints. `unit_price` captures the purchase-time price, not the current catalog price.

## 3. Staging models

| Model | Grain | Main responsibility |
|---|---|---|
| `stg_categories` | category ID | normalize category fields, timestamps and batch metadata |
| `stg_products` | product ID | normalize title/price/category/images |
| `stg_users` | user ID | normalize customer attributes and remove unsafe payload fields upstream |
| `stg_orders` | order ID | standardize status, timestamp, currency and country names |
| `stg_order_items` | order ID + line number | calculate `line_amount` and standardize numeric fields |

Staging models are views. They are the semantic boundary between source-shaped raw tables and warehouse models.

## 4. Warehouse models

| Model | Grain | Key measures/keys |
|---|---|---|
| `dim_customers` | customer ID | `customer_key`, `customer_id`, name, email, role, active state |
| `dim_categories` | category ID | `category_key`, `category_id`, name, active state |
| `dim_products` | product ID | `product_key`, `product_id`, name, price, category key |
| `fct_orders` | order ID | order/customer keys, status, ordered time, line count, quantity, order amount |
| `fct_order_items` | order ID + line number | order/product/category/customer keys, quantity, unit price, line amount |

Surrogate keys are deterministic hashes. Natural IDs remain available for reconciliation and debugging.

## 5. Marts and BI metric contract

### `marts.mart_daily_sales`

**Grain:** `ordered_date + currency_code`.

Metrics: `order_count`, `order_line_count`, `total_quantity`, `gross_order_amount`, `delivered_order_amount`.

### `marts.mart_customer_sales`

**Grain:** `customer_key + currency_code`.

Dimensions/attributes: customer ID/name/email/role and first/last order timestamps. Metrics: order count, line count, quantity, gross and delivered amount.

A customer name is not a unique identifier. Dashboard queries must group by `customer_id` and `customer_name`; if display clarity matters, build a label such as `name (#id)`.

### `marts.mart_product_sales`

**Grain:** `product_key + currency_code`.

Dimensions/attributes: product ID/name/category and first/last order timestamps. Metrics: order count, units sold, gross sales and delivered sales.

### Currency rule

All revenue charts must filter or segment by `currency_code`. The current deterministic fixture and production reference use `USD`, but the model retains currency as part of the grain for future extension.

### Top customer query pattern

```sql
SELECT
    customer_id,
    customer_name,
    customer_name || ' (#' || customer_id::text || ')' AS customer_label,
    SUM(gross_order_amount)::numeric(18, 2) AS gross_order_amount
FROM marts.mart_customer_sales
WHERE currency_code = 'USD'
GROUP BY customer_id, customer_name
ORDER BY gross_order_amount DESC, customer_name ASC, customer_id ASC
LIMIT 10;
```

This query aggregates before limiting. Sorting raw mart rows before a visualization groups duplicate display names can produce an apparently unsorted chart.

## 6. Data quality contracts

Current dbt tests cover combinations of:

- not-null and unique keys;
- accepted status, payment and currency values;
- positive numeric values;
- relationships between staging and dimensions;
- unique order line grains;
- fact/mart reconciliation;
- valid seen timestamps;
- mart grain uniqueness.

The database layer additionally enforces JSON object payloads, positive IDs, non-negative prices, valid status/currency/country patterns, foreign keys for batch/order relationships and no raw password payload.
