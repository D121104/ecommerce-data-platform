# Evidence assets

This directory contains public-safe evidence for the project. Do not place `.env`, database dumps, Metabase application exports, screenshots with credentials, bearer tokens, cookies or private webhook URLs here.

## Recommended screenshots

Capture these from a local run after removing sensitive details:

1. `airflow-dag-success.png`
   - Airflow Graph/Grid view for `ecommerce_daily_pipeline`.
   - Show successful task states and UTC run timestamp.
   - Hide browser address tokens, user email and unrelated tabs.

2. `dbt-build-success.png`
   - Terminal or dbt output showing `dbt build` and tests passed.
   - Crop out connection strings and environment values.

3. `metabase-executive-overview.png`
   - Executive Overview dashboard with daily revenue and Top Customers.
   - Keep only aggregate synthetic data; hide account identity and internal hostnames if publishing publicly.

## Reproducible capture workflow

```bash
make db-up
make airflow-list
make airflow-trigger
make dbt-build
```

Then verify the dashboard contract in [`dashboards/executive-overview.md`](../../dashboards/executive-overview.md). The repository deliberately stores the specification and sanitized evidence guidance, not Metabase's application database.

## Naming convention

Use lowercase kebab-case and describe the tool/state:

```text
airflow-dag-success.png
dbt-build-success.png
metabase-executive-overview.png
```

Before commit, inspect every image manually for secrets, session IDs, private URLs and personal data.
