import argparse
import logging

import httpx

from ecommerce_pipeline.api_client import (
    ENTITY_ENDPOINTS,
    ApiClientError,
    EntityName,
    PlatziApiClient,
)
from ecommerce_pipeline.config import get_settings
from ecommerce_pipeline.validation import validate_records

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch and validate data from the Platzi Fake Store API."
    )

    parser.add_argument(
        "--entity",
        choices=["all", *ENTITY_ENDPOINTS],
        default="all",
        help="Entity to retrieve. Default: all.",
    )

    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = parse_arguments()
    settings = get_settings()

    if args.entity == "all":
        entities: tuple[EntityName, ...] = (
            "categories",
            "products",
            "users",
        )
    else:
        entities = (args.entity,)

    found_empty_entity = False

    try:
        with PlatziApiClient(settings) as client:
            for entity in entities:
                records = list(client.iter_all(entity))
                result = validate_records(entity, records)

                print(
                    f"{entity}: "
                    f"extracted={len(records)}, "
                    f"valid={len(result.valid)}, "
                    f"rejected={len(result.rejected)}"
                )

                if not records:
                    found_empty_entity = True

                for issue in result.rejected[:3]:
                    print(f"  rejected id={issue.source_record_id}: {issue.error_message}")

    except (httpx.HTTPError, ApiClientError) as exc:
        logger.error("API extraction failed: %s", exc)
        return 2

    return 1 if found_empty_entity else 0
