from __future__ import annotations

import json
import logging
import os
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import pendulum
import psycopg
from airflow.sdk import DAG, task


LOGGER = logging.getLogger(__name__)

MONITOR_DAG_ID = "ecommerce_pipeline_monitor"
PIPELINE_NAME = "ecommerce_daily_pipeline"

STALE_AFTER_HOURS = int(
    os.getenv(
        "MONITOR_STALE_AFTER_HOURS",
        "26",
    )
)

RUNNING_TIMEOUT_MINUTES = int(
    os.getenv(
        "MONITOR_RUNNING_TIMEOUT_MINUTES",
        "120",
    )
)

EXPECTED_RUN_HOUR_UTC = int(
    os.getenv(
        "MONITOR_EXPECTED_RUN_HOUR_UTC",
        "2",
    )
)

EXPECTED_RUN_GRACE_MINUTES = int(
    os.getenv(
        "MONITOR_EXPECTED_RUN_GRACE_MINUTES",
        "60",
    )
)


def _expected_run_start(now: datetime) -> datetime:
    """Tính thời điểm bắt đầu run hằng ngày cần có gần nhất."""

    expected_start = now.replace(
        hour=EXPECTED_RUN_HOUR_UTC,
        minute=0,
        second=0,
        microsecond=0,
    )

    if now < expected_start:
        expected_start -= timedelta(days=1)

    return expected_start


def _db_connection() -> psycopg.Connection[Any]:
    """Tạo kết nối tới ecommerce database."""

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


def _send_webhook_alert(
    *,
    severity: str,
    message: str,
) -> None:
    """Gửi cảnh báo đến Slack hoặc Discord."""

    webhook_url = os.getenv("ALERT_WEBHOOK_URL")

    if not webhook_url:
        LOGGER.warning(
            "ALERT_WEBHOOK_URL is not configured. "
            "Alert was saved to PostgreSQL only: %s",
            message,
        )
        return

    webhook_kind = os.getenv(
        "ALERT_WEBHOOK_KIND",
        "slack",
    ).lower()

    formatted_message = (
        f"[{severity.upper()}] "
        f"{PIPELINE_NAME}: {message}"
    )

    if webhook_kind == "discord":
        payload = {
            "content": formatted_message,
        }
    else:
        payload = {
            "text": formatted_message,
        }

    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=10,
        ) as response:
            LOGGER.info(
                "Webhook response status: %s",
                response.status,
            )
    except Exception as exc:
        raise RuntimeError(
            "Failed to deliver pipeline alert"
        ) from exc


def _record_alert(
    *,
    alert_key: str,
    batch_id: uuid.UUID | None,
    alert_type: str,
    severity: str,
    message: str,
    details: dict[str, Any],
) -> bool:
    """
    Lưu hoặc cập nhật cảnh báo.

    Trả về True khi cần gửi notification.
    """

    with _db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ops.pipeline_alerts (
                    alert_key,
                    pipeline_name,
                    batch_id,
                    alert_type,
                    severity,
                    status,
                    message,
                    details
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    'open',
                    %s,
                    %s::jsonb
                )
                ON CONFLICT (alert_key)
                DO UPDATE SET
                    batch_id = EXCLUDED.batch_id,
                    severity = EXCLUDED.severity,
                    status = 'open',
                    message = EXCLUDED.message,
                    details = EXCLUDED.details,

                    occurrence_count =
                        CASE
                            WHEN ops.pipeline_alerts.status
                                = 'resolved'
                                THEN 1
                            ELSE
                                ops.pipeline_alerts
                                    .occurrence_count + 1
                        END,

                    last_detected_at =
                        clock_timestamp(),

                    resolved_at = NULL

                RETURNING occurrence_count
                """,
                (
                    alert_key,
                    PIPELINE_NAME,
                    batch_id,
                    alert_type,
                    severity,
                    message,
                    json.dumps(
                        details,
                        default=str,
                    ),
                ),
            )

            result = cursor.fetchone()

    if result is None:
        raise RuntimeError(
            "Alert upsert returned no result"
        )

    occurrence_count = result[0]

    return occurrence_count == 1


def _resolve_open_alerts() -> int:
    """Đóng các cảnh báo khi pipeline đã khỏe lại."""

    with _db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ops.pipeline_alerts
                SET
                    status = 'resolved',
                    resolved_at = clock_timestamp(),
                    last_detected_at =
                        clock_timestamp()
                WHERE pipeline_name = %s
                  AND status = 'open'
                """,
                (PIPELINE_NAME,),
            )

            return cursor.rowcount


with DAG(
    dag_id=MONITOR_DAG_ID,
    description=(
        "Detect failed, stalled, or stale "
        "ecommerce pipeline runs"
    ),
    schedule="*/15 * * * *",
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
        "retries": 1,
        "retry_delay": pendulum.duration(
            minutes=2
        ),
    },
    tags=[
        "ecommerce",
        "monitoring",
        "alerting",
    ],
) as dag:

    @task
    def check_pipeline_health() -> None:
        """Kiểm tra trạng thái orchestration gần nhất."""

        with _db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        batch_id,
                        status,
                        started_at,
                        finished_at,
                        error_message
                    FROM ops.pipeline_runs
                    WHERE pipeline_name = %s
                    ORDER BY started_at DESC
                    LIMIT 1
                    """,
                    (PIPELINE_NAME,),
                )

                run = cursor.fetchone()

        now = datetime.now(timezone.utc)
        expected_run_start = _expected_run_start(now)
        grace_deadline = (
            expected_run_start
            + timedelta(minutes=EXPECTED_RUN_GRACE_MINUTES)
        )

        LOGGER.info(
            "Pipeline audit health input: latest_run=%s "
            "expected_run_start=%s grace_deadline=%s",
            run,
            expected_run_start,
            grace_deadline,
        )

        if run is None:
            alert_key = (
                f"{PIPELINE_NAME}:no_runs"
            )

            message = (
                "No orchestration run was found"
            )

            should_notify = _record_alert(
                alert_key=alert_key,
                batch_id=None,
                alert_type="no_runs",
                severity="critical",
                message=message,
                details={},
            )

            if should_notify:
                _send_webhook_alert(
                    severity="critical",
                    message=message,
                )

            return

        (
            batch_id,
            status,
            started_at,
            finished_at,
            error_message,
        ) = run

        details = {
            "batch_id": str(batch_id),
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "error_message": error_message,
        }

        if status == "failed":
            alert_key = (
                f"{PIPELINE_NAME}:failed:"
                f"{batch_id}"
            )

            message = (
                f"Pipeline run {batch_id} failed. "
                f"Error: {error_message or 'unknown'}"
            )

            should_notify = _record_alert(
                alert_key=alert_key,
                batch_id=batch_id,
                alert_type="run_failed",
                severity="critical",
                message=message,
                details=details,
            )

            if should_notify:
                _send_webhook_alert(
                    severity="critical",
                    message=message,
                )

            return

        if status == "running":
            running_minutes = (
                now - started_at
            ).total_seconds() / 60

            if (
                running_minutes
                > RUNNING_TIMEOUT_MINUTES
            ):
                alert_key = (
                    f"{PIPELINE_NAME}:stalled:"
                    f"{batch_id}"
                )

                message = (
                    f"Pipeline run {batch_id} has "
                    f"been running for "
                    f"{running_minutes:.1f} minutes"
                )

                should_notify = _record_alert(
                    alert_key=alert_key,
                    batch_id=batch_id,
                    alert_type="run_stalled",
                    severity="critical",
                    message=message,
                    details=details,
                )

                if should_notify:
                    _send_webhook_alert(
                        severity="critical",
                        message=message,
                    )

                return

        if (
            status in {"success", "partial"}
            and now >= grace_deadline
            and started_at < expected_run_start
        ):
            alert_key = (
                f"{PIPELINE_NAME}:missing:"
                f"{expected_run_start.isoformat()}"
            )

            message = (
                "No orchestration row has started for the expected "
                f"daily run at {expected_run_start.isoformat()}; "
                f"latest run started at {started_at.isoformat()}"
            )

            missing_details = {
                **details,
                "expected_run_start": expected_run_start,
                "grace_deadline": grace_deadline,
            }

            should_notify = _record_alert(
                alert_key=alert_key,
                batch_id=batch_id,
                alert_type="run_missing",
                severity="critical",
                message=message,
                details=missing_details,
            )

            if should_notify:
                _send_webhook_alert(
                    severity="critical",
                    message=message,
                )

            LOGGER.error(
                "Pipeline run is missing: expected_start=%s "
                "latest_started_at=%s latest_status=%s",
                expected_run_start,
                started_at,
                status,
            )
            return

        reference_time = (
            finished_at or started_at
        )

        age_hours = (
            now - reference_time
        ).total_seconds() / 3600

        if (
            status in {"success", "partial"}
            and age_hours > STALE_AFTER_HOURS
        ):
            alert_key = (
                f"{PIPELINE_NAME}:stale:"
                f"{batch_id}"
            )

            message = (
                "Pipeline has not completed a new "
                f"run for {age_hours:.1f} hours"
            )

            should_notify = _record_alert(
                alert_key=alert_key,
                batch_id=batch_id,
                alert_type="pipeline_stale",
                severity="warning",
                message=message,
                details=details,
            )

            if should_notify:
                _send_webhook_alert(
                    severity="warning",
                    message=message,
                )

            return

        resolved_count = _resolve_open_alerts()

        LOGGER.info(
            "Pipeline is healthy. "
            "Resolved alerts: %s",
            resolved_count,
        )

    check_pipeline_health()