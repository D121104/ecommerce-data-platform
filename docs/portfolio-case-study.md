# CV and Portfolio Content

## Project title

**E-commerce API-to-Warehouse Data Platform**

## One-line portfolio summary

Built an end-to-end batch analytics platform that ingests catalog data from a public API, generates deterministic synthetic orders, models a PostgreSQL warehouse with dbt, orchestrates daily processing with Airflow, and serves BI-ready marts to Metabase.

## CV bullets — English

- Designed and implemented a Dockerized API-to-warehouse data platform using Python, PostgreSQL 17, dbt Core, Apache Airflow and Metabase.
- Built resilient ingestion for Platzi Fake Store API data with pagination, timeout controls, retry/backoff, Pydantic validation, rejected-record handling and raw JSON lineage.
- Implemented deterministic synthetic order generation keyed by business date and seed, with idempotent order-line loading so retries do not create duplicates.
- Developed dbt staging views, warehouse dimensions/facts (`fct_orders`, `fct_order_items`) and BI marts for daily, customer and product sales, with relationships, grain, reconciliation and numeric data tests.
- Hardened Airflow orchestration with UUID5 run-level audit IDs, explicit row-count validation, failure-callback fallback upserts and a 15-minute monitor for failed, stalled, stale and missing daily runs.
- Applied least-privilege PostgreSQL roles separating ingestion writes, dbt transformations and read-only BI access; prevented password fields from entering raw user payloads.
- Repaired the BI target contract by promoting the completed local dashboard runtime to `prod` schemas (`marts`, `warehouse`, `staging`) and validated end-to-end output of 90 orders, 312 order lines and USD 180,024 revenue.
- Designed a Top Customers visualization contract that aggregates by `customer_id` and `customer_name` before sorting/limiting, producing 10 correctly descending rows with zero ordering violations.
- Added pytest/Ruff/GitHub Actions checks, deterministic CI fixtures, Compose validation, repository validation and high-confidence secret scanning without committing credentials.

## CV bullets — Vietnamese

- Thiết kế và triển khai nền tảng Data Engineering batch end-to-end bằng Python, PostgreSQL 17, dbt Core, Apache Airflow và Metabase.
- Xây dựng ingestion có pagination, timeout, retry/backoff, Pydantic validation, rejected records và raw JSON lineage cho dữ liệu từ Platzi Fake Store API.
- Xây dựng synthetic order generator có business date/seed quyết định và loader idempotent, bảo đảm retry không tạo duplicate order hoặc order line.
- Phát triển dbt staging, warehouse dimensions/facts (`fct_orders`, `fct_order_items`) và marts cho daily/customer/product sales, kèm data tests về grain, relationship, reconciliation và numeric values.
- Hardening Airflow bằng UUID5 audit ID, row-count validation, failure callback fallback upsert và monitor 15 phút phát hiện failed, stalled, stale hoặc missing daily run.
- Áp dụng least-privilege PostgreSQL roles, tách quyền ingestion, dbt và BI read-only; ngăn password đi vào raw user payload.
- Chuẩn hóa target BI sang production schemas sau khi dashboard hoàn thiện và kiểm thử E2E với 90 orders, 312 order lines và doanh thu USD 180.024.
- Sửa Top Customers theo đúng grain nghiệp vụ: aggregate theo `customer_id` + `customer_name` trước khi sort/limit, đạt 10 rows và 0 lỗi thứ tự giảm dần.
- Thiết lập pytest, Ruff, GitHub Actions, deterministic fixtures, Compose validation và secret scanning không phát hành credential.

## Role and ownership

This is an individual portfolio project. The implementation role covered:

1. **Architecture:** selected the API → raw/ops → staging → warehouse → marts → BI flow and documented contracts.
2. **Ingestion:** implemented API client, pagination/retry behavior, validation, loader and rejected records.
3. **Data modeling:** designed PostgreSQL schemas, role grants, dbt staging/warehouse/marts and tests.
4. **Orchestration:** built the daily Airflow DAG, deterministic run IDs, audit finalization and monitor DAG.
5. **BI:** built the Metabase data-source contract/dashboard layout and corrected customer ranking grain/sort behavior.
6. **Engineering quality:** added unit/integration tests, CI, repository checks, secret hygiene, runbook and clone instructions.

## Portfolio case-study structure

1. **Problem:** make a public API plus synthetic transactions behave like a traceable analytics pipeline.
2. **Design:** separate raw, ops, staging, warehouse and marts; keep BI role read-only.
3. **Reliability challenge:** a task could be killed before the audit insert; solve with stable UUID5 IDs and failure callback fallback.
4. **BI challenge:** Metabase read old data because dbt dev schemas differed from the dashboard's production schema; standardize `prod` target for the completed dashboard.
5. **Visualization challenge:** sorting raw rows before grouping duplicate customer names produced an apparently unsorted chart; aggregate at customer ID grain before limiting.
6. **Evidence:** show Airflow success, dbt build/test and Metabase dashboard using sanitized assets and reproducible queries.
7. **Lessons:** schema contracts, idempotency boundaries, least privilege and operational documentation are as important as the happy-path DAG.

## Suggested LinkedIn / portfolio paragraph

I built an individual E-commerce API-to-Warehouse Data Platform to demonstrate production-oriented data engineering. The project ingests Platzi Fake Store API data, generates deterministic synthetic orders, loads PostgreSQL raw and operations layers, transforms the data with dbt into warehouse facts and BI marts, orchestrates the workflow with Airflow, and exposes metrics through Metabase. I focused on reliability and operability: idempotent retries, run-level audit rows, failure callbacks, monitoring alerts, least-privilege roles, data-quality tests, CI and secret-safe clone instructions. A validated reference run produced 90 orders, 312 order lines and USD 180,024 of gross revenue.

## How to present metrics responsibly

- Call the numbers a **validated reference run**, not a guaranteed SLA.
- State that orders are synthetic and catalog data comes from a public demo API.
- Link to architecture, data model, dashboard specification and sanitized evidence.
- Do not publish local credentials, Metabase application dumps, raw user payloads or screenshots containing tokens.
