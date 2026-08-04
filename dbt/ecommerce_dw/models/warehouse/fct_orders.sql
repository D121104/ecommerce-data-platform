with orders as (

    select *
    from {{ ref('stg_orders') }}

),

order_item_metrics as (

    select
        order_id,
        count(*)::bigint                         as order_line_count,
        sum(quantity)::bigint                    as total_quantity,
        sum(line_amount)::numeric(18, 2)         as order_amount
    from {{ ref('stg_order_items') }}
    group by order_id

),

customers as (

    select
        customer_id,
        customer_key
    from {{ ref('dim_customers') }}

),

final as (

    select
        md5('order|' || o.order_id::text)        as order_key,

        o.order_id,
        o.customer_id,
        c.customer_key,

        o.order_status,
        o.ordered_at,
        o.currency_code,
        o.payment_method,
        o.shipping_country_code,

        coalesce(m.order_line_count, 0)::bigint
                                                    as order_line_count,

        coalesce(m.total_quantity, 0)::bigint
                                                    as total_quantity,

        coalesce(m.order_amount, 0)::numeric(18, 2)
                                                    as order_amount,

        o.batch_id,
        o.ingested_at
    from orders as o

    left join order_item_metrics as m
        on o.order_id = m.order_id

    left join customers as c
        on o.customer_id = c.customer_id

)

select *
from final