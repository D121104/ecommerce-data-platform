# Executive Overview Dashboard Specification

## Purpose

Dashboard này cung cấp góc nhìn điều hành cho doanh thu, daily pipeline health, customer performance và product performance. Nó đọc các bảng trong schema `marts` qua role read-only `bi_reader`.

> Metabase application metadata không được commit. Tài liệu này là source-controlled contract để recreate dashboard trên một instance Metabase mới.

## Data source

| Setting | Value |
|---|---|
| Database | `ecommerce` |
| Host inside Compose | `postgres` |
| Reader role | `bi_reader` |
| Primary schema | `marts` |
| Currency example | `USD` |

Không ghi password, session token, API key hoặc Metabase export chưa sanitize vào repository.

## Dashboard layout

| Section | Question | Source | Grain/behavior |
|---|---|---|---|
| KPI | Daily orders | `marts.mart_daily_sales` | Sum `order_count` theo filter date/currency |
| KPI | Gross revenue | `marts.mart_daily_sales` | Sum `gross_order_amount` theo currency |
| Trend | Daily revenue | `marts.mart_daily_sales` | X=`ordered_date`, Y=`gross_order_amount` |
| Ranking | Top customers | `marts.mart_customer_sales` | Aggregate by customer, sort desc, limit 10 |
| Ranking | Top products | `marts.mart_product_sales` | Aggregate by product, sort desc, limit 10 |
| Operations | Latest pipeline state | `ops.v_pipeline_run_health` | Latest audit status and health |

## Required filters

- `currency_code`: default `USD` for the current fixture.
- date range: apply to `ordered_date`, `first_ordered_at` or `last_ordered_at` according to question semantics.
- optional order status: use warehouse/fact status only when the question explicitly needs operational segmentation.

## Top customers query

Use native SQL or an equivalent summarized query. The aggregate must happen before `LIMIT`:

```sql
SELECT
    customer_id,
    customer_name,
    customer_name || ' (#' || customer_id::text || ')' AS customer_label,
    SUM(gross_order_amount)::numeric(18, 2) AS gross_order_amount,
    SUM(order_count)::bigint AS order_count,
    SUM(total_quantity)::bigint AS total_quantity
FROM marts.mart_customer_sales
WHERE currency_code = 'USD'
GROUP BY customer_id, customer_name
ORDER BY gross_order_amount DESC, customer_name ASC, customer_id ASC
LIMIT 10;
```

Visualization mapping:

- category/dimension: `customer_label`;
- measure: `gross_order_amount`;
- sort: query result order or measure descending;
- row limit: 10;
- formatting: currency with two decimal places.

Grouping by `customer_name` alone is invalid because multiple source customer IDs may share a display name such as `Daniel`.

## Daily sales query

```sql
SELECT
    ordered_date,
    currency_code,
    order_count,
    order_line_count,
    total_quantity,
    gross_order_amount,
    delivered_order_amount
FROM marts.mart_daily_sales
WHERE currency_code = 'USD'
ORDER BY ordered_date;
```

## Product ranking query

```sql
SELECT
    product_id,
    product_name,
    SUM(gross_sales_amount)::numeric(18, 2) AS gross_sales_amount,
    SUM(units_sold)::bigint AS units_sold
FROM marts.mart_product_sales
WHERE currency_code = 'USD'
GROUP BY product_id, product_name
ORDER BY gross_sales_amount DESC, product_name ASC, product_id ASC
LIMIT 10;
```

## Pipeline health query

```sql
SELECT
    pipeline_name,
    status,
    health_status,
    started_at,
    finished_at,
    duration_seconds,
    age_seconds,
    records_inserted,
    records_rejected,
    error_message
FROM ops.v_pipeline_run_health
WHERE pipeline_name = 'ecommerce_daily_pipeline';
```

## Current local reference

The current local Metabase instance uses an Executive Overview dashboard and a Top Customers card whose local metadata IDs are not portable. The query contract above is portable; recreate the data source and cards rather than copying a database dump.

A reference run produced:

- daily sales: `90` orders and `180,024.00` USD gross revenue;
- customer sales: `56` customer/currency rows;
- product sales: `163` product/currency rows;
- Top Customers: `10` rows, sorted descending with `0` ordering violations.

## Validation checklist

Before publishing a screenshot or portfolio link:

- verify the data source uses `bi_reader`;
- verify production models are in `marts`, not only a `dbt_dev_marts` schema;
- filter revenue by currency;
- confirm Top Customers aggregates by ID before limiting;
- refresh questions after the latest dbt build;
- remove credentials, cookies, internal URLs and raw customer payloads from evidence.
