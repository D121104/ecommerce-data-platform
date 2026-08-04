from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseSettings):
    host: str = Field(
        default="127.0.0.1",
        validation_alias="INGESTION_DB_HOST",
    )

    port: int = Field(
        default=5432,
        ge=1,
        le=65535,
        validation_alias="INGESTION_DB_PORT",
    )

    database: str = Field(
        validation_alias=AliasChoices(
            "INGESTION_DB_NAME",
            "POSTGRES_DB",
        )
    )

    user: str = Field(
        validation_alias="INGESTION_DB_USER",
    )

    password: SecretStr = Field(
        validation_alias="INGESTION_DB_PASSWORD",
    )

    connect_timeout_seconds: int = Field(
        default=10,
        ge=1,
        le=60,
        validation_alias="INGESTION_DB_CONNECT_TIMEOUT_SECONDS",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


def connect_database(
    settings: DatabaseSettings,
) -> psycopg.Connection[dict[str, Any]]:
    return psycopg.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password.get_secret_value(),
        connect_timeout=settings.connect_timeout_seconds,
        application_name="ecommerce-data-platform",
        row_factory=dict_row,
    )


def create_pipeline_run(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    pipeline_name: str,
    source_name: str,
    metadata: dict[str, Any],
) -> UUID:
    with connection.transaction():
        row = connection.execute(
            """
            INSERT INTO ops.pipeline_runs (
                pipeline_name,
                source_name,
                status,
                run_metadata
            )
            VALUES (%s, %s, 'running', %s)
            RETURNING batch_id
            """,
            (
                pipeline_name,
                source_name,
                Jsonb(metadata),
            ),
        ).fetchone()

    if row is None:
        raise RuntimeError("Database did not return a batch_id.")

    return row["batch_id"]


def finish_pipeline_run(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    batch_id: UUID,
    status: str,
    records_extracted: int,
    records_inserted: int,
    records_updated: int,
    records_rejected: int,
    error_message: str | None,
    metadata: dict[str, Any],
) -> None:
    with connection.transaction():
        connection.execute(
            """
            UPDATE ops.pipeline_runs
            SET
                status = %s,
                finished_at = clock_timestamp(),
                records_extracted = %s,
                records_inserted = %s,
                records_updated = %s,
                records_rejected = %s,
                error_message = %s,
                run_metadata = run_metadata || %s
            WHERE batch_id = %s
            """,
            (
                status,
                records_extracted,
                records_inserted,
                records_updated,
                records_rejected,
                error_message,
                Jsonb(metadata),
                batch_id,
            ),
        )
