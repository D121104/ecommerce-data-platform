from datetime import date
from decimal import Decimal

import pytest

from ecommerce_pipeline.synthetic_orders import (
    ProductSnapshot,
    generate_orders,
)

CUSTOMER_IDS = [101, 102, 103]

PRODUCTS = [
    ProductSnapshot(
        product_id=1,
        unit_price=Decimal("10.50"),
    ),
    ProductSnapshot(
        product_id=2,
        unit_price=Decimal("20.00"),
    ),
    ProductSnapshot(
        product_id=3,
        unit_price=Decimal("35.75"),
    ),
]


def test_generate_orders_returns_requested_count() -> None:
    orders = generate_orders(
        business_date=date(2026, 7, 27),
        order_count=10,
        seed=12345,
        customer_ids=CUSTOMER_IDS,
        products=PRODUCTS,
    )

    assert len(orders) == 10


def test_generate_orders_is_deterministic() -> None:
    parameters = {
        "business_date": date(2026, 7, 27),
        "order_count": 10,
        "seed": 12345,
        "customer_ids": CUSTOMER_IDS,
        "products": PRODUCTS,
    }

    first_result = generate_orders(**parameters)
    second_result = generate_orders(**parameters)

    assert first_result == second_result


def test_order_ids_do_not_depend_on_seed() -> None:
    first_result = generate_orders(
        business_date=date(2026, 7, 27),
        order_count=5,
        seed=100,
        customer_ids=CUSTOMER_IDS,
        products=PRODUCTS,
    )

    second_result = generate_orders(
        business_date=date(2026, 7, 27),
        order_count=5,
        seed=999,
        customer_ids=CUSTOMER_IDS,
        products=PRODUCTS,
    )

    first_ids = [order.order_id for order in first_result]
    second_ids = [order.order_id for order in second_result]

    assert first_ids == second_ids


def test_different_dates_produce_different_order_ids() -> None:
    first_result = generate_orders(
        business_date=date(2026, 7, 27),
        order_count=5,
        seed=12345,
        customer_ids=CUSTOMER_IDS,
        products=PRODUCTS,
    )

    second_result = generate_orders(
        business_date=date(2026, 7, 28),
        order_count=5,
        seed=12345,
        customer_ids=CUSTOMER_IDS,
        products=PRODUCTS,
    )

    first_ids = {order.order_id for order in first_result}
    second_ids = {order.order_id for order in second_result}

    assert first_ids.isdisjoint(second_ids)


def test_generated_items_are_valid() -> None:
    orders = generate_orders(
        business_date=date(2026, 7, 27),
        order_count=20,
        seed=12345,
        customer_ids=CUSTOMER_IDS,
        products=PRODUCTS,
    )

    valid_product_ids = {product.product_id for product in PRODUCTS}

    for order in orders:
        assert order.customer_id in CUSTOMER_IDS
        assert len(order.items) >= 1

        line_numbers = [item.line_number for item in order.items]

        assert line_numbers == list(range(1, len(order.items) + 1))

        product_ids = [item.product_id for item in order.items]

        assert len(product_ids) == len(set(product_ids))

        for item in order.items:
            assert item.product_id in valid_product_ids
            assert 1 <= item.quantity <= 3
            assert item.unit_price >= 0


@pytest.mark.parametrize(
    ("order_count", "customer_ids", "products"),
    [
        (0, CUSTOMER_IDS, PRODUCTS),
        (-1, CUSTOMER_IDS, PRODUCTS),
        (1, [], PRODUCTS),
        (1, CUSTOMER_IDS, []),
    ],
)
def test_generate_orders_rejects_invalid_inputs(
    order_count: int,
    customer_ids: list[int],
    products: list[ProductSnapshot],
) -> None:
    with pytest.raises(ValueError):
        generate_orders(
            business_date=date(2026, 7, 27),
            order_count=order_count,
            seed=12345,
            customer_ids=customer_ids,
            products=products,
        )
