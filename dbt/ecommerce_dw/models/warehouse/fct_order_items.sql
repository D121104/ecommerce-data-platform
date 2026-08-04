with order_items as (

    select *
    from {{ ref('stg_order_items') }}

),

orders as (

    select
        order_id,
        customer_id,
        ordered_at,
        currency_code
    from {{ ref('stg_orders') }}

),

customers as (

    select
        customer_id,
        customer_key
    from {{ ref('dim_customers') }}

),

products as (

    select
        product_id,
        product_key,
        category_key
    from {{ ref('dim_products') }}

),

final as (

    select
        md5(
            'order_item|'
            || i.order_id::text
            || '|'
            || i.line_number::text
        )                                       as order_item_key,

        md5('order|' || i.order_id::text)       as order_key,

        i.order_id,
        i.line_number,

        o.customer_id,
        c.customer_key,

        i.product_id,
        p.product_key,
        p.category_key,

        o.ordered_at,
        o.currency_code,

        i.quantity,
        i.unit_price,
        i.line_amount,

        i.batch_id,
        i.ingested_at
    from order_items as i

    left join orders as o
        on i.order_id = o.order_id

    left join customers as c
        on o.customer_id = c.customer_id

    left join products as p
        on i.product_id = p.product_id

)

select *
from final