# Operations Runbook

## 1. Scope and safety

This runbook is for local Docker Compose or disposable CI environments. It is not a high-availability production deployment guide. Keep a copy of `.env` outside the repository if the local environment must be restored.

Never paste passwords, API keys or webhook URLs into issue comments, screenshots or Git commits.

## 2. Start/stop

```bash
python scripts/bootstrap.py
docker compose up -d --build --wait
docker compose ps
```

Stop while retaining named volumes:

```bash
docker compose down
```

Stop and delete all local state:

```bash
docker compose down -v
```

The destructive command deletes PostgreSQL business data, Airflow metadata and Metabase application metadata. SQL init scripts run automatically only when the PostgreSQL data volume is initialized.

## 3. Health checks

```bash
docker compose ps
docker compose exec postgres pg_isready -U "$POSTGRES_ADMIN_USER" -d "$POSTGRES_DB"
docker compose exec airflow-api-server curl --fail http://localhost:8080/api/v2/monitor/health
docker compose exec metabase curl --fail -I http://localhost:3000/api/health
```

When using `cmd.exe` or PowerShell, environment variables inside the container command may not expand as expected. Prefer the concrete local role/database values from `.env`, or use the Make targets that do not require host shell expansion.

## 4. Daily execution

1. Ensure the data and Airflow databases are healthy.
2. Open Airflow at `http://localhost:8080`.
3. Unpause `ecommerce_daily_pipeline`.
4. Trigger it manually for a smoke test, or wait for `02:00 UTC`.
5. Inspect task logs in this order: ingestion, raw validation, order generation, dbt snapshot/run/test, audit finalization.
6. Check `ops.pipeline_runs` and `ops.pipeline_alerts`.
7. Open Metabase and refresh the affected questions/dashboard.

CLI:

```bash
docker compose exec airflow-api-server airflow dags unpause ecommerce_daily_pipeline
docker compose exec airflow-api-server airflow dags trigger ecommerce_daily_pipeline
docker compose exec airflow-api-server airflow dags list-runs ecommerce_daily_pipeline
```

## 5. Audit verification

```sql
SELECT
    batch_id,
    pipeline_name,
    source_name,
    status,
    started_at,
    finished_at,
    records_extracted,
    records_inserted,
    records_updated,
    records_rejected,
    run_metadata
FROM ops.pipeline_runs
ORDER BY started_at DESC
LIMIT 20;
```

Expected behavior:

- one orchestration row per Airflow `run_id`;
- retry of the same run updates the same `batch_id`;
- successful run has non-null `finished_at` and `status='success'`;
- failed callback creates/finalizes a row even when the initial task was killed;
- ingestion batch and orchestration batch are separate rows and can be linked through metadata/logs.

## 6. Monitor verification

```sql
SELECT
    alert_key,
    alert_type,
    severity,
    status,
    occurrence_count,
    first_detected_at,
    last_detected_at,
    resolved_at,
    message
FROM ops.pipeline_alerts
ORDER BY last_detected_at DESC
LIMIT 20;
```

The monitor does not use an old successful row as proof that a new daily run happened. It calculates the expected daily start from `MONITOR_EXPECTED_RUN_HOUR_UTC` and waits `MONITOR_EXPECTED_RUN_GRACE_MINUTES` before raising a missing-run alert.

## 7. dbt recovery

Run from the Airflow image so host/container networking and credentials match the DAG:

```bash
make dbt-debug
make dbt-snapshot
make dbt-build
```

If a model fails:

1. capture the dbt node name and error from logs;
2. run `dbt debug`;
3. check `DBT_TARGET`, `DBT_SCHEMA`, current database role and schema permissions;
4. rerun the smallest affected selector if appropriate;
5. rerun the complete `dbt build` and `dbt test` before marking the pipeline healthy.

Do not manually write to marts to hide a failed model. Fix the model/permissions and rebuild.

## 8. Metabase recovery

### No new daily data

Check that:

- Airflow environment has `DBT_TARGET=prod` for dashboard runtime;
- production models exist in `marts`, not only `dbt_dev_marts`;
- Metabase data source uses host `postgres`, database `ecommerce`, role `bi_reader`;
- the question filters the intended `currency_code`;
- the question was refreshed after dbt completed.

### Dashboard metadata lost

Metabase cards/dashboards live in its application database. If the named/anonymous volume was removed, recreate the data source and cards from [`dashboards/executive-overview.md`](../dashboards/executive-overview.md). Do not restore an unsanitized application database dump into a public repository.

## 9. Common failures

| Symptom | Likely cause | Action |
|---|---|---|
| bind mount error for Airflow password file | old Compose config or stale checkout | use current Compose named `airflow_auth` volume and recreate init/service |
| `could not translate host name airflow-db` | metadata database stopped/unhealthy | `docker compose ps`, inspect `airflow-db` logs, restart with `--wait` |
| no audit row | task killed before insert or DB unavailable | inspect failure callback logs and DB health; check `ops.pipeline_runs` by run metadata |
| monitor reports healthy old run | outdated monitor code/config | run current monitor DAG and verify expected-run env settings |
| dbt writes to wrong schema | dev target while BI reads prod | align `DBT_TARGET`/`DBT_SCHEMA`, rebuild and refresh Metabase |
| SQL init changes not applied | existing PostgreSQL volume | apply a migration or recreate disposable volume; init scripts are not rerun on restart |
| `py_compile` cannot write `__pycache__` | DAG mount is read-only | use `ast.parse` or `python scripts/validate_repository.py` |
| top customer chart not sorted | raw rows limited before aggregate/grouping | aggregate by customer ID/name, sort aggregate descending, then limit |

## 10. Incident evidence checklist

For a failure or portfolio demo, collect only sanitized evidence:

- Airflow run ID, DAG/task state and UTC timestamps;
- dbt command summary and test result;
- row counts and aggregate totals, not raw user payloads;
- Metabase question/dashboard name and filter state;
- container/service status;
- no passwords, cookies, bearer tokens, webhook URLs or private connection strings.

Evidence files belong under `docs/assets/` and should be reviewed before commit.
