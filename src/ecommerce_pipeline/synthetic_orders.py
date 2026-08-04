import random
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

ORDER_NAMESPACE = uuid5(
    NAMESPACE_URL,
    "ecommerce-data-platform/synthetic-orders",
)


@dataclass(frozen=True)
class ProductSnapshot:
    product_id: int
    unit_price: Decimal


@dataclass(frozen=True)
class SyntheticOrderItem:
    line_number: int
    product_id: int
    quantity: int
    unit_price: Decimal


@dataclass(frozen=True)
class SyntheticOrder:
    order_id: UUID
    customer_id: int
    order_status: str
    ordered_at: datetime
    currency: str
    payment_method: str
    shipping_country: str | None
    payload: dict[str, Any]
    items: tuple[SyntheticOrderItem, ...]


@dataclass
class OrderLoadStats:
    orders_generated: int = 0
    items_generated: int = 0
    orders_inserted: int = 0
    items_inserted: int = 0


def load_generation_inputs(
    connection: psycopg.Connection[dict[str, Any]],
) -> tuple[list[int], list[ProductSnapshot]]:
    user_rows = connection.execute(
        """
        SELECT user_id
        FROM raw.users
        WHERE is_active = true
        ORDER BY user_id
        """
    ).fetchall()

    product_rows = connection.execute(
        """
        SELECT
            product_id,
            price
        FROM raw.products
        WHERE
            is_active = true
            AND price >= 0
        ORDER BY product_id
        """
    ).fetchall()

    customer_ids = [row["user_id"] for row in user_rows]

    products = [
        ProductSnapshot(
            product_id=row["product_id"],
            unit_price=row["price"],
        )
        for row in product_rows
    ]

    if not customer_ids:
        raise RuntimeError("No active users are available for order generation.")

    if not products:
        raise RuntimeError("No active products are available for order generation.")

    return customer_ids, products


def generate_orders(
    *,
    business_date: date,
    order_count: int,
    seed: int,
    customer_ids: list[int],
    products: list[ProductSnapshot],
) -> list[SyntheticOrder]:
    if order_count < 1:
        raise ValueError("order_count must be greater than zero.")

    if not customer_ids:
        raise ValueError("customer_ids must not be empty.")

    if not products:
        raise ValueError("products must not be empty.")

    rng = random.Random(f"{business_date.isoformat()}:{seed}")

    statuses = (
        "pending",
        "paid",
        "shipped",
        "delivered",
        "cancelled",
    )

    status_weights = (
        5,
        15,
        20,
        55,
        5,
    )

    payment_methods = (
        "credit_card",
        "paypal",
        "bank_transfer",
        "cash_on_delivery",
    )

    shipping_countries = (
        "US",
        "GB",
        "DE",
        "FR",
        "VN",
        "TH",
        "SG",
        "AU",
    )

    start_of_day = datetime.combine(
        business_date,
        time.min,
        tzinfo=UTC,
    )

    generated_orders: list[SyntheticOrder] = []

    for order_index in range(1, order_count + 1):
        # UUID5 sinh cùng một UUID khi đầu vào giống nhau.
        # Vì vậy cùng ngày và cùng index sẽ không tạo order mới.
        order_id = uuid5(
            ORDER_NAMESPACE,
            f"{business_date.isoformat()}:{order_index}",
        )

        ordered_at = start_of_day + timedelta(seconds=rng.randrange(24 * 60 * 60))

        maximum_items = min(5, len(products))
        item_count = rng.randint(1, maximum_items)

        selected_products = rng.sample(
            products,
            k=item_count,
        )

        items = tuple(
            SyntheticOrderItem(
                line_number=line_number,
                product_id=product.product_id,
                quantity=rng.randint(1, 3),
                unit_price=product.unit_price,
            )
            for line_number, product in enumerate(
                selected_products,
                start=1,
            )
        )

        order_status = rng.choices(
            statuses,
            weights=status_weights,
            k=1,
        )[0]

        generated_orders.append(
            SyntheticOrder(
                order_id=order_id,
                customer_id=rng.choice(customer_ids),
                order_status=order_status,
                ordered_at=ordered_at,
                currency="USD",
                payment_method=rng.choice(payment_methods),
                shipping_country=rng.choice(shipping_countries),
                payload={
                    "generator": "synthetic_orders",
                    "generator_version": "0.1.0",
                    "business_date": (business_date.isoformat()),
                    "order_sequence": order_index,
                    "synthetic": True,
                },
                items=items,
            )
        )

    return generated_orders


def load_orders(
    connection: psycopg.Connection[dict[str, Any]],
    *,
    batch_id: UUID,
    orders: list[SyntheticOrder],
) -> OrderLoadStats:
    stats = OrderLoadStats(
        orders_generated=len(orders),
        items_generated=sum(len(order.items) for order in orders),
    )

    with connection.transaction():
        for order in orders:
            order_cursor = connection.execute(
                """
                INSERT INTO raw.orders (
                    order_id,
                    customer_id,
                    order_status,
                    ordered_at,
                    currency,
                    payment_method,
                    shipping_country,
                    payload,
                    batch_id
                )
                VALUES (
                    %(order_id)s,
                    %(customer_id)s,
                    %(order_status)s,
                    %(ordered_at)s,
                    %(currency)s,
                    %(payment_method)s,
                    %(shipping_country)s,
                    %(payload)s,
                    %(batch_id)s
                )
                ON CONFLICT (order_id)
                DO NOTHING
                """,
                {
                    "order_id": order.order_id,
                    "customer_id": order.customer_id,
                    "order_status": order.order_status,
                    "ordered_at": order.ordered_at,
                    "currency": order.currency,
                    "payment_method": order.payment_method,
                    "shipping_country": (order.shipping_country),
                    "payload": Jsonb(order.payload),
                    "batch_id": batch_id,
                },
            )

            stats.orders_inserted += order_cursor.rowcount

            for item in order.items:
                item_cursor = connection.execute(
                    """
                    INSERT INTO raw.order_items (
                        order_id,
                        line_number,
                        product_id,
                        quantity,
                        unit_price,
                        batch_id
                    )
                    VALUES (
                        %(order_id)s,
                        %(line_number)s,
                        %(product_id)s,
                        %(quantity)s,
                        %(unit_price)s,
                        %(batch_id)s
                    )
                    ON CONFLICT (
                        order_id,
                        line_number
                    )
                    DO NOTHING
                    """,
                    {
                        "order_id": order.order_id,
                        "line_number": (item.line_number),
                        "product_id": item.product_id,
                        "quantity": item.quantity,
                        "unit_price": item.unit_price,
                        "batch_id": batch_id,
                    },
                )

                stats.items_inserted += item_cursor.rowcount

    return stats
