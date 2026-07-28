import logging

from ecommerce_pipeline.ingestion import run_ingestion


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format=("%(asctime)s %(levelname)s %(name)s: %(message)s"),
    )

    try:
        summary = run_ingestion()
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
