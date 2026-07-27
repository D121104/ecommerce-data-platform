from typing import Any

import httpx

from ecommerce_pipeline.api_client import PlatziApiClient
from ecommerce_pipeline.config import Settings
from ecommerce_pipeline.validation import validate_records


def make_settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "api_base_url": "https://example.test/api/v1",
        "api_page_size": 2,
        "api_max_pages": 10,
        "api_max_attempts": 1,
        "api_backoff_seconds": 0,
    }
    values.update(overrides)

    return Settings(_env_file=None, **values)


def test_products_are_paginated() -> None:
    requested_offsets: list[int] = []

    pages = {
        0: [
            {
                "id": 1,
                "title": "Product 1",
                "price": 10,
                "images": [],
            },
            {
                "id": 2,
                "title": "Product 2",
                "price": 20,
                "images": [],
            },
        ],
        2: [
            {
                "id": 3,
                "title": "Product 3",
                "price": 30,
                "images": [],
            }
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params["offset"])
        requested_offsets.append(offset)

        return httpx.Response(
            status_code=200,
            json=pages.get(offset, []),
        )

    transport = httpx.MockTransport(handler)

    with PlatziApiClient(
        make_settings(),
        transport=transport,
    ) as client:
        records = list(client.iter_all("products"))

    assert [record["id"] for record in records] == [1, 2, 3]
    assert requested_offsets == [0, 2]


def test_retry_on_server_error() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            return httpx.Response(status_code=503)

        return httpx.Response(status_code=200, json=[])

    transport = httpx.MockTransport(handler)

    settings = make_settings(
        api_max_attempts=2,
        api_backoff_seconds=0,
    )

    with PlatziApiClient(
        settings,
        transport=transport,
    ) as client:
        records = list(client.iter_all("products"))

    assert records == []
    assert attempts == 2


def test_user_password_is_removed() -> None:
    records = [
        {
            "id": 1,
            "email": "user@example.com",
            "password": "must-not-be-stored",
            "name": "Test User",
            "role": "customer",
            "avatar": "https://example.com/avatar.png",
            "metadata": {
                "password": "nested-secret",
            },
        }
    ]

    result = validate_records("users", records)

    assert len(result.valid) == 1
    assert result.rejected == []

    validated_record = result.valid[0]

    assert "password" not in validated_record.payload
    assert "password" not in validated_record.payload["metadata"]
    assert "password" not in validated_record.model.model_dump()
