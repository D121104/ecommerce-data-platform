from datetime import date

import pytest

from ecommerce_pipeline.database import (
    DatabaseSettings,
    connect_database,
    create_pipeline_run,
)
from ecommerce_pipeline.synthetic_orders import (
    generate_orders,
    load_generation_inputs,
    load_orders,
)


@pytest.mark.integration
def test_load_orders_is_idempotent() -> None:
    settings = DatabaseSettings()
    connection = connect_database(settings)

    try:
        # force_rollback bảo đảm dữ liệu test không tồn tại
        # sau khi test kết thúc.
        with connection.transaction(force_rollback=True):
            batch_id = create_pipeline_run(
                connection,
                pipeline_name=("synthetic_order_generation_test"),
                source_name="pytest",
                metadata={
                    "test": True,
                    "business_date": "2099-01-01",
                },
            )

            customer_ids, products = load_generation_inputs(connection)

            orders = generate_orders(
                business_date=date(2099, 1, 1),
                order_count=3,
                seed=12345,
                customer_ids=customer_ids,
                products=products,
            )

            first_load = load_orders(
                connection,
                batch_id=batch_id,
                orders=orders,
            )

            second_load = load_orders(
                connection,
                batch_id=batch_id,
                orders=orders,
            )

            assert first_load.orders_inserted == 3
            assert first_load.items_inserted >= 3

            assert second_load.orders_inserted == 0
            assert second_load.items_inserted == 0

            order_ids = [order.order_id for order in orders]

            order_count = connection.execute(
                """
                SELECT count(*) AS row_count
                FROM raw.orders
                WHERE order_id = ANY(%s::uuid[])
                """,
                (order_ids,),
            ).fetchone()

            item_count = connection.execute(
                """
                SELECT count(*) AS row_count
                FROM raw.order_items
                WHERE order_id = ANY(%s::uuid[])
                """,
                (order_ids,),
            ).fetchone()

            assert order_count is not None
            assert item_count is not None

            assert order_count["row_count"] == 3
            assert item_count["row_count"] == first_load.items_inserted

    finally:
        connection.close()
