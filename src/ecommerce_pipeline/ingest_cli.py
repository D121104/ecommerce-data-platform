import argparse
import logging
from uuid import UUID

from ecommerce_pipeline.ingestion import run_ingestion


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest the ecommerce catalog into the raw layer.",
    )
    parser.add_argument(
        "--batch-id",
        type=UUID,
        help="Stable pipeline batch ID for an orchestrated retry.",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format=("%(asctime)s %(levelname)s %(name)s: %(message)s"),
    )

    args = parse_arguments()

    try:
        summary = run_ingestion(batch_id=args.batch_id)
    except Exception:
        logging.exception("Catalog ingestion failed.")
        return 2

    print()
    print(f"batch_id={summary.batch_id}")
    print(f"status={summary.status}")
    print(f"records_extracted={summary.records_extracted}")
    print(f"records_inserted={summary.records_inserted}")
    print(f"records_updated={summary.records_updated}")
    print(f"records_rejected={summary.records_rejected}")

    if summary.error_message:
        print(f"error_message={summary.error_message}")

    if summary.status == "success":
        return 0

    if summary.status == "partial":
        return 1

    return 2
