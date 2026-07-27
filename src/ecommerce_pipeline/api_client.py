import logging
from collections.abc import Iterator
from typing import Any, Literal

import httpx
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from ecommerce_pipeline.config import Settings

logger = logging.getLogger(__name__)

EntityName = Literal["categories", "products", "users"]

ENTITY_ENDPOINTS: dict[EntityName, str] = {
    "categories": "categories",
    "products": "products",
    "users": "users",
}


class ApiClientError(RuntimeError):
    """Base exception for errors raised by the API client."""


class ApiResponseError(ApiClientError):
    """The API returned an unexpected response structure."""


class PaginationError(ApiClientError):
    """Pagination did not terminate safely."""


class PlatziApiClient:
    def __init__(
        self,
        settings: Settings,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.settings = settings

        timeout = httpx.Timeout(
            connect=settings.api_connect_timeout_seconds,
            read=settings.api_read_timeout_seconds,
            write=settings.api_write_timeout_seconds,
            pool=settings.api_pool_timeout_seconds,
        )

        base_url = str(settings.api_base_url).rstrip("/") + "/"

        self.client = httpx.Client(
            base_url=base_url,
            timeout=timeout,
            transport=transport,
            follow_redirects=True,
            headers={
                "Accept": "application/json",
                "User-Agent": "ecommerce-data-platform/0.1.0",
            },
        )

    def __enter__(self) -> "PlatziApiClient":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self.client.close()

    def iter_all(self, entity: EntityName) -> Iterator[dict[str, Any]]:
        endpoint = ENTITY_ENDPOINTS[entity]

        if entity != "products":
            yield from self._request_list(endpoint)
            return

        yield from self._iter_paginated_products(endpoint)

    def _iter_paginated_products(
        self,
        endpoint: str,
    ) -> Iterator[dict[str, Any]]:
        offset = 0
        page_size = self.settings.api_page_size
        seen_page_signatures: set[tuple[str, ...]] = set()

        for _page_number in range(1, self.settings.api_max_pages + 1):
            page = self._request_list(
                endpoint,
                params={
                    "offset": offset,
                    "limit": page_size,
                },
            )

            if not page:
                return

            signature = tuple(str(record.get("id")) for record in page)

            if signature in seen_page_signatures:
                raise PaginationError(
                    "API returned a repeated product page; "
                    "pagination has been stopped to prevent an infinite loop."
                )

            seen_page_signatures.add(signature)

            yield from page

            if len(page) < page_size:
                return

            offset += len(page)

        raise PaginationError(
            f"Product pagination reached the safety limit of "
            f"{self.settings.api_max_pages} pages."
        )

    def _request_list(
        self,
        endpoint: str,
        params: dict[str, int] | None = None,
    ) -> list[dict[str, Any]]:
        retrying = Retrying(
            stop=stop_after_attempt(self.settings.api_max_attempts),
            wait=wait_exponential(
                multiplier=self.settings.api_backoff_seconds,
                min=self.settings.api_backoff_seconds,
                max=10,
            ),
            retry=retry_if_exception(self._is_retryable),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            reraise=True,
        )

        return retrying(
            self._request_list_once,
            endpoint,
            params,
        )

    def _request_list_once(
        self,
        endpoint: str,
        params: dict[str, int] | None,
    ) -> list[dict[str, Any]]:
        response = self.client.get(endpoint, params=params)
        response.raise_for_status()

        try:
            data = response.json()
        except ValueError as exc:
            raise ApiResponseError(
                f"Endpoint {endpoint!r} did not return valid JSON."
            ) from exc

        if not isinstance(data, list):
            raise ApiResponseError(
                f"Endpoint {endpoint!r} returned "
                f"{type(data).__name__}; expected a list."
            )

        if not all(isinstance(record, dict) for record in data):
            raise ApiResponseError(
                f"Endpoint {endpoint!r} returned a list "
                "containing a non-object item."
            )

        return data

    @staticmethod
    def _is_retryable(exc: BaseException) -> bool:
        if isinstance(exc, httpx.TransportError):
            return True

        if isinstance(exc, httpx.HTTPStatusError):
            status_code = exc.response.status_code
            return status_code == 429 or status_code >= 500

        return False
