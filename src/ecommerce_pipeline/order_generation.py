import logging
from dataclasses import dataclass
from datetime import date
from uuid import UUID

from ecommerce_pipeline.database import (
    DatabaseSettings,
    connect_database,
    create_pipeline_run,
    finish_pipeline_run,
)
from ecommerce_pipeline.synthetic_orders import (
    generate_orders,
    load_generation_inputs,
    load_orders,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OrderGenerationSummary:
    batch_id: UUID
    business_date: date
    orders_generated: int
    items_generated: int
    orders_inserted: int
    items_inserted: int


def run_order_generation(
    *,
    business_date: date,
    order_count: int,
    seed: int,
) -> OrderGenerationSummary:
    database_settings = DatabaseSettings()
    connection = connect_database(database_settings)

    batch_id: UUID | None = None

    try:
        batch_id = create_pipeline_run(
            connection,
            pipeline_name="synthetic_order_generation",
            source_name="internal_generator",
            metadata={
                "business_date": (business_date.isoformat()),
                "requested_order_count": order_count,
                "seed": seed,
                "generator_version": "0.1.0",
            },
        )

        logger.info(
            "Created order generation batch %s",
            batch_id,
        )

        with connection.transaction():
            customer_ids, products = load_generation_inputs(connection)

            orders = generate_orders(
                business_date=business_date,
                order_count=order_count,
                seed=seed,
                customer_ids=customer_ids,
                products=products,
            )

            stats = load_orders(
                connection,
                batch_id=batch_id,
                orders=orders,
            )

        inserted_records = stats.orders_inserted + stats.items_inserted

        finish_pipeline_run(
            connection,
            batch_id=batch_id,
            status="success",
            records_extracted=0,
            records_inserted=inserted_records,
            records_updated=0,
            records_rejected=0,
            error_message=None,
            metadata={
                "orders_generated": (stats.orders_generated),
                "items_generated": (stats.items_generated),
                "orders_inserted": (stats.orders_inserted),
                "items_inserted": (stats.items_inserted),
            },
        )

        return OrderGenerationSummary(
            batch_id=batch_id,
            business_date=business_date,
            orders_generated=stats.orders_generated,
            items_generated=stats.items_generated,
            orders_inserted=stats.orders_inserted,
            items_inserted=stats.items_inserted,
        )

    except Exception as exc:
        if batch_id is not None:
            try:
                finish_pipeline_run(
                    connection,
                    batch_id=batch_id,
                    status="failed",
                    records_extracted=0,
                    records_inserted=0,
                    records_updated=0,
                    records_rejected=0,
                    error_message=(f"{type(exc).__name__}: {exc}"),
                    metadata={
                        "business_date": (business_date.isoformat()),
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
