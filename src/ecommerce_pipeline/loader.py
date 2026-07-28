import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from ecommerce_pipeline.api_client import EntityName
from ecommerce_pipeline.models import Category, Product, User
from ecommerce_pipeline.validation import (
    ValidatedRecord,
    ValidationResult,
)


@dataclass
class LoadStats:
    inserted: int = 0
    updated: int = 0
    rejected: int = 0
    unchanged: int = 0
    deactivated: int = 0


@dataclass(frozen=True)
class PreparedRecord:
    source_id: int
    record_hash: str
    parameters: dict[str, Any]


SELECT_EXISTING_SQL: dict[EntityName, str] = {
    "categories": """
        SELECT
            record_hash::text AS record_hash,
            is_active
        FROM raw.categories
        WHERE category_id = %s
    """,
    "products": """
        SELECT
            record_hash::text AS record_hash,
            is_active
        FROM raw.products
        WHERE product_id = %s
    """,
    "users": """
        SELECT
            record_hash::text AS record_hash,
            is_active
        FROM raw.users
        WHERE user_id = %s
    """,
}


UPSERT_SQL: dict[EntityName, str] = {
    "categories": """
        INSERT INTO raw.categories (
            category_id,
            name,
            slug,
            image_url,
            source_created_at,
            source_updated_at,
            payload,
            record_hash,
            batch_id,
            is_active
        )
        VALUES (
            %(category_id)s,
            %(name)s,
            %(slug)s,
            %(image_url)s,
            %(source_created_at)s,
            %(source_updated_at)s,
            %(payload)s,
            %(record_hash)s,
            %(batch_id)s,
            true
        )
        ON CONFLICT (category_id)
        DO UPDATE SET
            name = EXCLUDED.name,
            slug = EXCLUDED.slug,
            image_url = EXCLUDED.image_url,
            source_created_at = EXCLUDED.source_created_at,
            source_updated_at = EXCLUDED.source_updated_at,
            payload = EXCLUDED.payload,
            record_hash = EXCLUDED.record_hash,
            batch_id = EXCLUDED.batch_id,
            is_active = true,
            last_seen_at = clock_timestamp()
    """,
    "products": """
        INSERT INTO raw.products (
            product_id,
            title,
            slug,
            price,
            description,
            category_id,
            images,
            source_created_at,
            source_updated_at,
            payload,
            record_hash,
            batch_id,
            is_active
        )
        VALUES (
            %(product_id)s,
            %(title)s,
            %(slug)s,
            %(price)s,
            %(description)s,
            %(category_id)s,
            %(images)s,
            %(source_created_at)s,
            %(source_updated_at)s,
            %(payload)s,
            %(record_hash)s,
            %(batch_id)s,
            true
        )
        ON CONFLICT (product_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            slug = EXCLUDED.slug,
            price = EXCLUDED.price,
            description = EXCLUDED.description,
            category_id = EXCLUDED.category_id,
            images = EXCLUDED.images,
            source_created_at = EXCLUDED.source_created_at,
            source_updated_at = EXCLUDED.source_updated_at,
            payload = EXCLUDED.payload,
            record_hash = EXCLUDED.record_hash,
            batch_id = EXCLUDED.batch_id,
            is_active = true,
            last_seen_at = clock_timestamp()
    """,
    "users": """
        INSERT INTO raw.users (
            user_id,
            email,
            name,
            role,
            avatar_url,
            source_created_at,
            source_updated_at,
            payload,
            record_hash,
            batch_id,
            is_active
        )
        VALUES (
            %(user_id)s,
            %(email)s,
            %(name)s,
            %(role)s,
            %(avatar_url)s,
            %(source_created_at)s,
            %(source_updated_at)s,
            %(payload)s,
            %(record_hash)s,
            %(batch_id)s,
            true
        )
        ON CONFLICT (user_id)
        DO UPDATE SET
            email = EXCLUDED.email,
            name = EXCLUDED.name,
            role = EXCLUDED.role,
            avatar_url = EXCLUDED.avatar_url,
            source_created_at = EXCLUDED.source_created_at,
            source_updated_at = EXCLUDED.source_updated_at,
            payload = EXCLUDED.payload,
            record_hash = EXCLUDED.record_hash,
            batch_id = EXCLUDED.batch_id,
            is_active = true,
            last_seen_at = clock_timestamp()
    """,
}


DEACTIVATE_SQL: dict[EntityName, str] = {
    "categories": """
        UPDATE raw.categories
        SET is_active = false
        WHERE
            is_active = true
            AND NOT (category_id = ANY(%s))
    """,
    "products": """
        UPDATE raw.products
        SET is_active = false
        WHERE
            is_active = true
            AND NOT (product_id = ANY(%s))
    """,
    "users": """
        UPDATE raw.users
        SET is_active = false
        WHERE
            is_active = true
            AND NOT (user_id = ANY(%s))
    """,
}


def json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, Decimal):
        normalized = value.normalize()
        return format(normalized, "f")

    raise TypeError(f"Value of type {type(value).__name__} is not JSON serializable.")


def calculate_record_hash(
    business_values: dict[str, Any],
) -> str:
    canonical_json = json.dumps(
        business_values,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=json_default,
    )

    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def prepare_record(
    entity: EntityName,
    record: ValidatedRecord,
    batch_id: UUID,
) -> PreparedRecord:
    model = record.model

    if entity == "categories":
        if not isinstance(model, Category):
            raise TypeError("Expected Category model.")

        business_values = {
            "name": model.name,
            "slug": model.slug,
            "image_url": model.image_url,
            "source_created_at": model.creation_at,
            "source_updated_at": model.updated_at,
        }

        record_hash = calculate_record_hash(business_values)

        return PreparedRecord(
            source_id=model.id,
            record_hash=record_hash,
            parameters={
                "category_id": model.id,
                **business_values,
                "payload": Jsonb(record.payload),
                "record_hash": record_hash,
                "batch_id": batch_id,
            },
        )

    if entity == "products":
        if not isinstance(model, Product):
            raise TypeError("Expected Product model.")

        category_id = model.category.id if model.category is not None else None

        business_values = {
            "title": model.title,
            "slug": model.slug,
            "price": model.price,
            "description": model.description,
            "category_id": category_id,
            "images": model.images,
            "source_created_at": model.creation_at,
            "source_updated_at": model.updated_at,
        }

        record_hash = calculate_record_hash(business_values)

        return PreparedRecord(
            source_id=model.id,
            record_hash=record_hash,
            parameters={
                "product_id": model.id,
                **business_values,
                "payload": Jsonb(record.payload),
                "record_hash": record_hash,
                "batch_id": batch_id,
            },
        )

    if entity == "users":
        if not isinstance(model, User):
            raise TypeError("Expected User model.")

        business_values = {
            "email": str(model.email),
            "name": model.name,
            "role": model.role,
            "avatar_url": model.avatar_url,
            "source_created_at": model.creation_at,
            "source_updated_at": model.updated_at,
        }

        record_hash = calculate_record_hash(business_values)

        return PreparedRecord(
            source_id=model.id,
            record_hash=record_hash,
            parameters={
                "user_id": model.id,
                **business_values,
                "payload": Jsonb(record.payload),
                "record_hash": record_hash,
                "batch_id": batch_id,
            },
        )

    raise ValueError(f"Unsupported entity: {entity}")


def insert_rejected_record(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    batch_id: UUID,
    entity: EntityName,
    source_record_id: str | None,
    error_code: str,
    error_message: str,
    payload: dict[str, Any],
) -> None:
    connection.execute(
        """
        INSERT INTO ops.rejected_records (
            batch_id,
            entity_name,
            source_record_id,
            error_code,
            error_message,
            payload
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            batch_id,
            entity,
            source_record_id,
            error_code,
            error_message,
            Jsonb(payload),
        ),
    )


def record_observed_ids(
    result: ValidationResult,
) -> list[int]:
    observed_ids: set[int] = set()

    for record in result.valid:
        source_id = getattr(record.model, "id", None)

        if isinstance(source_id, int) and source_id > 0:
            observed_ids.add(source_id)

    for issue in result.rejected:
        if issue.source_record_id is None:
            continue

        try:
            source_id = int(issue.source_record_id)
        except ValueError:
            continue

        if source_id > 0:
            observed_ids.add(source_id)

    return sorted(observed_ids)


def load_entity(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    entity: EntityName,
    batch_id: UUID,
    result: ValidationResult,
) -> LoadStats:
    stats = LoadStats()
    observed_ids = record_observed_ids(result)

    with connection.transaction():
        for issue in result.rejected:
            insert_rejected_record(
                connection,
                batch_id=batch_id,
                entity=entity,
                source_record_id=issue.source_record_id,
                error_code="validation_error",
                error_message=issue.error_message,
                payload=issue.payload,
            )

            stats.rejected += 1

        for validated_record in result.valid:
            prepared = prepare_record(
                entity,
                validated_record,
                batch_id,
            )

            existing = connection.execute(
                SELECT_EXISTING_SQL[entity],
                (prepared.source_id,),
            ).fetchone()

            try:
                # Vì transaction bên ngoài đã tồn tại, block này
                # trở thành một PostgreSQL savepoint.
                with connection.transaction():
                    connection.execute(
                        UPSERT_SQL[entity],
                        prepared.parameters,
                    )

            except psycopg.Error as exc:
                insert_rejected_record(
                    connection,
                    batch_id=batch_id,
                    entity=entity,
                    source_record_id=str(prepared.source_id),
                    error_code=exc.sqlstate or "database_error",
                    error_message=str(exc).splitlines()[0],
                    payload=validated_record.payload,
                )

                stats.rejected += 1
                continue

            if existing is None:
                stats.inserted += 1
                continue

            changed = existing["record_hash"] != prepared.record_hash or not existing["is_active"]

            if changed:
                stats.updated += 1
            else:
                stats.unchanged += 1

        # Chỉ đánh dấu inactive nếu đã nạp được ít nhất một record.
        # Điều này ngăn việc API trả rỗng/lỗi làm inactive toàn bộ bảng.
        successfully_loaded = stats.inserted + stats.updated + stats.unchanged

        if successfully_loaded > 0 and observed_ids:
            cursor = connection.execute(
                DEACTIVATE_SQL[entity],
                (observed_ids,),
            )

            stats.deactivated = cursor.rowcount

    return stats
