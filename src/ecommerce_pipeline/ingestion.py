import logging
from dataclasses import asdict, dataclass
from typing import Literal
from uuid import UUID

import httpx
import psycopg

from ecommerce_pipeline.api_client import (
    ApiClientError,
    EntityName,
    PlatziApiClient,
)
from ecommerce_pipeline.config import get_settings
from ecommerce_pipeline.database import (
    DatabaseSettings,
    connect_database,
    create_pipeline_run,
    finish_pipeline_run,
)
from ecommerce_pipeline.loader import load_entity
from ecommerce_pipeline.validation import validate_records

logger = logging.getLogger(__name__)


class EmptySourceError(RuntimeError):
    """The source returned no records for an expected entity."""


PipelineStatus = Literal["success", "partial", "failed"]


@dataclass(frozen=True)
class PipelineSummary:
    batch_id: UUID
    status: PipelineStatus
    records_extracted: int
    records_inserted: int
    records_updated: int
    records_rejected: int
    error_message: str | None


def run_ingestion() -> PipelineSummary:
    api_settings = get_settings()
    database_settings = DatabaseSettings()

    entities: tuple[EntityName, ...] = (
        "categories",
        "products",
        "users",
    )

    connection = connect_database(database_settings)
    batch_id: UUID | None = None

    extracted = 0
    inserted = 0
    updated = 0
    rejected = 0

    successful_entities = 0
    entity_metadata: dict[str, object] = {}
    errors: list[str] = []

    try:
        batch_id = create_pipeline_run(connection)

        logger.info("Created pipeline batch %s", batch_id)

        with PlatziApiClient(api_settings) as api_client:
            for entity in entities:
                try:
                    logger.info("Extracting entity=%s", entity)

                    records = list(api_client.iter_all(entity))
                    extracted += len(records)

                    if not records:
                        raise EmptySourceError(f"Entity {entity!r} returned zero records.")

                    validation_result = validate_records(
                        entity,
                        records,
                    )

                    load_stats = load_entity(
                        connection,
                        entity=entity,
                        batch_id=batch_id,
                        result=validation_result,
                    )

                    inserted += load_stats.inserted
                    updated += load_stats.updated
                    rejected += load_stats.rejected
                    successful_entities += 1

                    entity_metadata[entity] = {
                        "extracted": len(records),
                        "valid": len(validation_result.valid),
                        "validation_rejected": len(validation_result.rejected),
                        **asdict(load_stats),
                    }

                    logger.info(
                        "%s: extracted=%s inserted=%s "
                        "updated=%s unchanged=%s rejected=%s "
                        "deactivated=%s",
                        entity,
                        len(records),
                        load_stats.inserted,
                        load_stats.updated,
                        load_stats.unchanged,
                        load_stats.rejected,
                        load_stats.deactivated,
                    )

                except (
                    httpx.HTTPError,
                    ApiClientError,
                    psycopg.Error,
                    EmptySourceError,
                ) as exc:
                    message = f"{entity}: {type(exc).__name__}: {exc}"

                    errors.append(message)

                    entity_metadata[entity] = {
                        "status": "failed",
                        "error": str(exc),
                    }

                    logger.exception(
                        "Entity ingestion failed: %s",
                        entity,
                    )

        if successful_entities == len(entities) and rejected == 0:
            status: PipelineStatus = "success"
        elif successful_entities == 0:
            status = "failed"
        else:
            status = "partial"

        error_message = " | ".join(errors) or None

        finish_pipeline_run(
            connection,
            batch_id=batch_id,
            status=status,
            records_extracted=extracted,
            records_inserted=inserted,
            records_updated=updated,
            records_rejected=rejected,
            error_message=error_message,
            metadata={
                "successful_entities": successful_entities,
                "total_entities": len(entities),
                "entity_results": entity_metadata,
            },
        )

        return PipelineSummary(
            batch_id=batch_id,
            status=status,
            records_extracted=extracted,
            records_inserted=inserted,
            records_updated=updated,
            records_rejected=rejected,
            error_message=error_message,
        )

    except Exception as exc:
        if batch_id is not None:
            try:
                finish_pipeline_run(
                    connection,
                    batch_id=batch_id,
                    status="failed",
                    records_extracted=extracted,
                    records_inserted=inserted,
                    records_updated=updated,
                    records_rejected=rejected,
                    error_message=(f"{type(exc).__name__}: {exc}"),
                    metadata={
                        "successful_entities": successful_entities,
                        "entity_results": entity_metadata,
                    },
                )
            except Exception:
                logger.exception(
                    "Could not mark batch %s as failed.",
                    batch_id,
                )

        raise

    finally:
        connection.close()
