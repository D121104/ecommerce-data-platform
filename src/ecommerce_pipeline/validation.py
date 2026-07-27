from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ValidationError

from ecommerce_pipeline.api_client import EntityName
from ecommerce_pipeline.models import Category, Product, User

SENSITIVE_KEYS = {"password"}

MODEL_BY_ENTITY: dict[EntityName, type[BaseModel]] = {
    "categories": Category,
    "products": Product,
    "users": User,
}


@dataclass(frozen=True)
class ValidatedRecord:
    model: BaseModel
    payload: dict[str, Any]


@dataclass(frozen=True)
class ValidationIssue:
    source_record_id: str | None
    error_message: str
    payload: dict[str, Any]


@dataclass
class ValidationResult:
    valid: list[ValidatedRecord] = field(default_factory=list)
    rejected: list[ValidationIssue] = field(default_factory=list)


def scrub_sensitive_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: scrub_sensitive_fields(item)
            for key, item in value.items()
            if key.casefold() not in SENSITIVE_KEYS
        }

    if isinstance(value, list):
        return [scrub_sensitive_fields(item) for item in value]

    return value


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = scrub_sensitive_fields(payload)

    if not isinstance(sanitized, dict):
        raise TypeError("Sanitized API payload must remain a dictionary.")

    return sanitized


def format_validation_error(exc: ValidationError) -> str:
    messages: list[str] = []

    for error in exc.errors(
        include_url=False,
        include_input=False,
    ):
        location = ".".join(str(part) for part in error["loc"])
        messages.append(f"{location}: {error['msg']}")

    return " | ".join(messages)


def validate_records(
    entity: EntityName,
    records: list[dict[str, Any]],
) -> ValidationResult:
    result = ValidationResult()
    model_class = MODEL_BY_ENTITY[entity]

    for original_payload in records:
        payload = sanitize_payload(original_payload)

        source_id = payload.get("id")
        source_record_id = None if source_id is None else str(source_id)

        try:
            model = model_class.model_validate(payload)
        except ValidationError as exc:
            result.rejected.append(
                ValidationIssue(
                    source_record_id=source_record_id,
                    error_message=format_validation_error(exc),
                    payload=payload,
                )
            )
            continue

        result.valid.append(
            ValidatedRecord(
                model=model,
                payload=payload,
            )
        )

    return result
