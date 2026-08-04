with order_items as (

    select *
    from {{ ref('fct_order_items') }}

),

orders as (

    select
        order_key,
        order_status
    from {{ ref('fct_orders') }}

),

product_metrics as (

    select
        i.product_key,
        i.currency_code,

        min(i.ordered_at)                          as first_ordered_at,
        max(i.ordered_at)                          as last_ordered_at,

        count(distinct i.order_id)::bigint         as order_count,
        count(*)::bigint                           as order_line_count,
        sum(i.quantity)::bigint                    as units_sold,
        sum(i.line_amount)::numeric(18, 2)          as gross_sales_amount,

        sum(
            case when o.order_status = 'delivered'
                then i.quantity else 0
            end
        )::bigint                                  as delivered_units,

        sum(
            case when o.order_status = 'delivered'
                then i.line_amount else 0
            end
        )::numeric(18, 2)                          as delivered_sales_amount

    from order_items as i
    inner join orders as o
        on i.order_key = o.order_key

    group by
        i.product_key,
        i.currency_code

),

products as (

    select
        product_key,
        product_id,
        product_name,
        category_id,
        category_key,
        is_active
    from {{ ref('dim_products') }}

),

final as (

    select
        m.product_key,
        p.product_id,
        p.product_name,
        p.category_id,
        p.category_key,
        p.is_active,

        m.currency_code,
        m.first_ordered_at,
        m.last_ordered_at,

        m.order_count,
        m.order_line_count,
        m.units_sold,
        m.gross_sales_amount,
        m.delivered_units,
        m.delivered_sales_amount

    from product_metrics as m
    inner join products as p
        on m.product_key = p.product_key

)

select *
from final