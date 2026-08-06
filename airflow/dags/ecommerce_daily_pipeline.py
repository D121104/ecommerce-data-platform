from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pendulum
import psycopg
from airflow.sdk import DAG, get_current_context, task


LOGGER = logging.getLogger(__name__)

DAG_ID = "ecommerce_daily_pipeline"

PROJECT_ROOT = Path(
    os.getenv(
        "AIRFLOW_PROJECT_ROOT",
        "/opt/airflow/project",
    )
)

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "ecommerce_dw"
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

ORCHESTRATION_NAMESPACE = uuid.UUID(
    "91536752-2208-4c55-8a99-6f29e558ab8d"
)


def _db_connection() -> psycopg.Connection[Any]:
    """Tạo kết nối đến database ecommerce."""

    return psycopg.connect(
        host=os.environ["INGESTION_DB_HOST"],
        port=int(os.environ["INGESTION_DB_PORT"]),
        dbname=os.environ["INGESTION_DB_NAME"],
        user=os.environ["INGESTION_DB_USER"],
        password=os.environ["INGESTION_DB_PASSWORD"],
        connect_timeout=int(
            os.getenv(
                "INGESTION_DB_CONNECT_TIMEOUT_SECONDS",
                "10",
            )
        ),
    )


def _orchestration_batch_id(run_id: str) -> uuid.UUID:
    """Sinh batch_id ổn định từ Airflow run_id."""

    return uuid.uuid5(
        ORCHESTRATION_NAMESPACE,
        f"{DAG_ID}:{run_id}",
    )


def _ingestion_batch_id(run_id: str) -> uuid.UUID:
    """Sinh ingestion batch_id ổn định từ Airflow run_id."""

    return uuid.uuid5(
        ORCHESTRATION_NAMESPACE,
        f"{DAG_ID}:catalog_ingestion:{run_id}",
    )


def _run_command(
    command: list[str],
    cwd: Path,
) -> subprocess.CompletedProcess[str]:
    """Chạy CLI và ghi stdout/stderr vào log Airflow."""

    LOGGER.info(
        "Running command: %s",
        " ".join(command),
    )

    completed = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )

    if completed.stdout:
        LOGGER.info(
            "stdout:\n%s",
            completed.stdout.rstrip(),
        )

    if completed.stderr:
        LOGGER.info(
            "stderr:\n%s",
            completed.stderr.rstrip(),
        )

    return completed


def _update_orchestration_run(
    *,
    run_id: str,
    status: str,
    error_message: str | None = None,
    allow_missing: bool = False,
) -> None:
    """Cập nhật trạng thái cuối của toàn bộ DAG."""

    batch_id = _orchestration_batch_id(run_id)
    final_metadata = json.dumps(
        {
            "airflow_finalized": True,
            "airflow_run_id": run_id,
        }
    )

    with _db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ops.pipeline_runs
                SET
                    status = %s,
                    finished_at = clock_timestamp(),
                    error_message = %s,
                    run_metadata =
                        run_metadata
                        || %s::jsonb
                WHERE batch_id = %s
                """,
                (
                    status,
                    error_message,
                    final_metadata,
                    batch_id,
                ),
            )

            updated_rows = cursor.rowcount

            LOGGER.info(
                "Finalized orchestration audit: run_id=%s "
                "batch_id=%s status=%s updated_rows=%s",
                run_id,
                batch_id,
                status,
                updated_rows,
            )

            if updated_rows == 1:
                return

            if not allow_missing:
                raise RuntimeError(
                    "No orchestration audit row exists for "
                    f"run_id={run_id}, batch_id={batch_id}"
                )

            cursor.execute(
                """
                INSERT INTO ops.pipeline_runs (
                    batch_id,
                    pipeline_name,
                    source_name,
                    status,
                    finished_at,
                    error_message,
                    run_metadata
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    clock_timestamp(),
                    %s,
                    %s::jsonb
                )
                ON CONFLICT (batch_id)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    finished_at = EXCLUDED.finished_at,
                    error_message = EXCLUDED.error_message,
                    run_metadata =
                        ops.pipeline_runs.run_metadata
                        || EXCLUDED.run_metadata
                """,
                (
                    batch_id,
                    DAG_ID,
                    "airflow",
                    status,
                    error_message,
                    final_metadata,
                ),
            )

            LOGGER.warning(
                "Created fallback orchestration audit row: "
                "run_id=%s batch_id=%s status=%s",
                run_id,
                batch_id,
                status,
            )


def _mark_dag_failed(context: dict[str, Any]) -> None:
    """Callback ghi trạng thái failed khi DAG thất bại."""

    dag_run = context.get("dag_run")

    if dag_run is None:
        LOGGER.error(
            "Cannot record DAG failure: "
            "dag_run is missing from callback context"
        )
        return

    exception = context.get("exception")

    try:
        _update_orchestration_run(
            run_id=dag_run.run_id,
            status="failed",
            error_message=str(
                exception
                or "One or more Airflow tasks failed"
            )[:4000],
            allow_missing=True,
        )
    except Exception:
        LOGGER.exception(
            "Failed to update orchestration audit row"
        )


with DAG(
    dag_id=DAG_ID,
    description=(
        "Daily ecommerce ingestion, synthetic orders, "
        "and dbt transformations"
    ),
    schedule="0 2 * * *",
    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=5),
    },
    on_failure_callback=_mark_dag_failed,
    tags=[
        "ecommerce",
        "ingestion",
        "dbt",
    ],
) as dag:

    @task
    def create_pipeline_run() -> str:
        """Tạo audit row đại diện cho toàn bộ DAG run."""

        context = get_current_context()
        run_id = context["run_id"]
        batch_id = _orchestration_batch_id(run_id)

        with _db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO ops.pipeline_runs (
                        batch_id,
                        pipeline_name,
                        source_name,
                        status,
                        run_metadata
                    )
                    VALUES (
                        %s,
                        %s,
                        %s,
                        'running',
                        %s::jsonb
                    )
                    ON CONFLICT (batch_id)
                    DO UPDATE SET
                        status = 'running',
                        started_at = clock_timestamp(),
                        finished_at = NULL,
                        records_extracted = 0,
                        records_inserted = 0,
                        records_updated = 0,
                        records_rejected = 0,
                        error_message = NULL,
                        run_metadata = EXCLUDED.run_metadata
                    RETURNING batch_id
                    """,
                    (
                        batch_id,
                        DAG_ID,
                        "airflow",
                        json.dumps(
                            {
                                "orchestrator": "apache_airflow",
                                "timezone": "UTC",
                                "airflow_run_id": run_id,
                            }
                        ),
                    ),
                )

                result = cursor.fetchone()

        if result is None or result[0] != batch_id:
            raise RuntimeError(
                "Database did not return the expected orchestration "
                f"batch_id for run_id={run_id}"
            )

        LOGGER.info(
            "Created or resumed orchestration audit: run_id=%s "
            "batch_id=%s",
            run_id,
            batch_id,
        )

        return str(batch_id)

    @task
    def ingest_source_data(
        _: str,
    ) -> str:
        """Chạy ingestion CLI với batch ổn định theo Airflow run."""

        context = get_current_context()
        ingestion_batch_id = _ingestion_batch_id(
            context["run_id"],
        )

        completed = _run_command(
            ["ingest-platzi", "--batch-id", str(ingestion_batch_id)],
            PROJECT_ROOT,
        )

        output = (
            f"{completed.stdout}\n"
            f"{completed.stderr}"
        )

        match = re.search(
            (
                r"batch_id="
                r"([0-9a-fA-F]{8}-"
                r"[0-9a-fA-F-]{27,})"
            ),
            output,
        )

        if match is None:
            raise RuntimeError(
                "ingest-platzi did not report a batch_id; "
                f"process exit code was "
                f"{completed.returncode}"
            )

        reported_batch_id = uuid.UUID(match.group(1))

        if reported_batch_id != ingestion_batch_id:
            raise RuntimeError(
                "ingest-platzi reported an unexpected batch_id "
                f"{reported_batch_id}; expected {ingestion_batch_id}"
            )

        with _db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        status,
                        records_inserted,
                        records_updated,
                        records_rejected
                    FROM ops.pipeline_runs
                    WHERE batch_id = %s
                    """,
                    (ingestion_batch_id,),
                )

                result = cursor.fetchone()

        if result is None:
            raise RuntimeError(
                "No audit row found for ingestion batch "
                f"{ingestion_batch_id}"
            )

        status, inserted, updated, rejected = result

        if status not in {"success", "partial"}:
            raise RuntimeError(
                f"Ingestion batch {ingestion_batch_id} "
                f"ended with status={status}"
            )

        if status == "partial":
            LOGGER.warning(
                "Ingestion completed partially: "
                "batch_id=%s inserted=%s "
                "updated=%s rejected=%s",
                ingestion_batch_id,
                inserted,
                updated,
                rejected,
            )

        elif completed.returncode != 0:
            raise RuntimeError(
                "ingest-platzi returned a non-zero "
                "exit code despite status=success"
            )

        return str(ingestion_batch_id)

    @task
    def validate_raw_data(
        _: str,
    ) -> None:
        """Kiểm tra raw layer có dữ liệu active."""

        with _db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE is_active
                        ) AS active_categories,
                        (
                            SELECT COUNT(*)
                            FROM raw.products
                            WHERE is_active
                        ),
                        (
                            SELECT COUNT(*)
                            FROM raw.users
                            WHERE is_active
                        )
                    FROM raw.categories
                    """
                )

                result = cursor.fetchone()

        if result is None:
            raise RuntimeError(
                "Raw validation query returned no result"
            )

        (
            active_categories,
            active_products,
            active_users,
        ) = result

        counts = {
            "categories": active_categories,
            "products": active_products,
            "users": active_users,
        }

        LOGGER.info(
            "Active raw record counts: %s",
            counts,
        )

        empty_entities = [
            name
            for name, count in counts.items()
            if count == 0
        ]

        if empty_entities:
            raise ValueError(
                "Raw validation failed; "
                f"no active rows for {empty_entities}"
            )

    @task
    def generate_orders() -> None:
        """Sinh orders cho business date của DAG run."""

        context = get_current_context()
        run_id = context.get("run_id", "<unknown>")
        run_start = (
            context.get("data_interval_start")
            or context.get("logical_date")
        )

        if run_start is None:
            run_start = pendulum.now("UTC")
            LOGGER.warning(
                "Run %s has no data interval or logical date; "
                "using current UTC time %s for business date",
                run_id,
                run_start,
            )
        else:
            LOGGER.info(
                "Using Airflow run timestamp %s for run %s "
                "to derive business date",
                run_start,
                run_id,
            )

        business_date = run_start.date().isoformat()

        order_count = os.getenv(
            "AIRFLOW_ORDERS_PER_RUN",
            "30",
        )

        seed = os.getenv(
            "AIRFLOW_ORDER_SEED",
            "42",
        )

        completed = _run_command(
            [
                "generate-orders",
                "--business-date",
                business_date,
                "--orders",
                order_count,
                "--seed",
                seed,
            ],
            PROJECT_ROOT,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                "generate-orders failed with "
                f"exit code {completed.returncode}"
            )

    @task
    def dbt_snapshot() -> None:
        """Cập nhật SCD Type 2 snapshots."""

        completed = _run_command(
            [
                "dbt",
                "snapshot",
                "--profiles-dir",
                ".",
                "--target",
                DBT_TARGET,
            ],
            DBT_PROJECT_DIR,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                "dbt snapshot failed with "
                f"exit code {completed.returncode}"
            )

    @task
    def dbt_run() -> None:
        """Build staging, warehouse và marts."""

        completed = _run_command(
            [
                "dbt",
                "run",
                "--profiles-dir",
                ".",
                "--target",
                DBT_TARGET,
            ],
            DBT_PROJECT_DIR,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                "dbt run failed with "
                f"exit code {completed.returncode}"
            )

    @task
    def dbt_test() -> None:
        """Chạy toàn bộ dbt data tests."""

        completed = _run_command(
            [
                "dbt",
                "test",
                "--profiles-dir",
                ".",
                "--target",
                DBT_TARGET,
            ],
            DBT_PROJECT_DIR,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                "dbt test failed with "
                f"exit code {completed.returncode}"
            )

    @task
    def record_pipeline_success(
        batch_id: str,
    ) -> None:
        """Đánh dấu orchestration run thành công."""

        context = get_current_context()

        expected_batch_id = (
            _orchestration_batch_id(
                context["run_id"]
            )
        )

        if uuid.UUID(batch_id) != expected_batch_id:
            raise ValueError(
                "Orchestration batch_id does not "
                "match the Airflow run"
            )

        _update_orchestration_run(
            run_id=context["run_id"],
            status="success",
        )

    orchestration_batch = create_pipeline_run()

    ingestion_batch = ingest_source_data(
        orchestration_batch
    )

    raw_validated = validate_raw_data(
        ingestion_batch
    )

    orders_generated = generate_orders()
    snapshot_finished = dbt_snapshot()
    models_built = dbt_run()
    tests_passed = dbt_test()

    pipeline_recorded = record_pipeline_success(
        orchestration_batch
    )

    raw_validated >> orders_generated
    orders_generated >> snapshot_finished
    snapshot_finished >> models_built
    models_built >> tests_passed
    tests_passed >> pipeline_recorded