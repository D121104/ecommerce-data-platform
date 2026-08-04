import argparse
import logging
from datetime import UTC, date, datetime, timedelta

from ecommerce_pipeline.order_generation import (
    run_order_generation,
)


def default_business_date() -> date:
    return datetime.now(UTC).date() - timedelta(days=1)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic ecommerce orders.")

    parser.add_argument(
        "--business-date",
        type=date.fromisoformat,
        default=default_business_date(),
        help="Business date in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--orders",
        type=int,
        default=100,
        help="Number of orders to generate.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=20260728,
        help="Seed used for deterministic generation.",
    )

    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format=("%(asctime)s %(levelname)s %(name)s: %(message)s"),
    )

    args = parse_arguments()

    try:
        summary = run_order_generation(
            business_date=args.business_date,
            order_count=args.orders,
            seed=args.seed,
        )
    except Exception:
        logging.exception("Synthetic order generation failed.")
        return 2

    print()
    print(f"batch_id={summary.batch_id}")
    print(f"business_date={summary.business_date}")
    print(f"orders_generated={summary.orders_generated}")
    print(f"items_generated={summary.items_generated}")
    print(f"orders_inserted={summary.orders_inserted}")
    print(f"items_inserted={summary.items_inserted}")

    return 0
