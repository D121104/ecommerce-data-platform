with orders as (

    select *
    from {{ ref('fct_orders') }}

),

customer_order_metrics as (

    select
        customer_key,
        currency_code,

        min(ordered_at)                            as first_ordered_at,
        max(ordered_at)                            as last_ordered_at,

        count(*)::bigint                           as order_count,
        sum(order_line_count)::bigint              as order_line_count,
        sum(total_quantity)::bigint                as total_quantity,
        sum(order_amount)::numeric(18, 2)           as gross_order_amount,

        sum(
            case when order_status = 'delivered'
                then 1 else 0
            end
        )::bigint                                  as delivered_order_count,

        sum(
            case when order_status = 'delivered'
                then order_amount else 0
            end
        )::numeric(18, 2)                          as delivered_order_amount

    from orders
    group by
        customer_key,
        currency_code

),

customers as (

    select
        customer_key,
        customer_id,
        customer_name,
        email,
        customer_role,
        is_active
    from {{ ref('dim_customers') }}

),

final as (

    select
        m.customer_key,
        c.customer_id,
        c.customer_name,
        c.email,
        c.customer_role,
        c.is_active,

        m.currency_code,
        m.first_ordered_at,
        m.last_ordered_at,

        m.order_count,
        m.order_line_count,
        m.total_quantity,
        m.gross_order_amount,
        m.delivered_order_count,
        m.delivered_order_amount

    from customer_order_metrics as m
    inner join customers as c
        on m.customer_key = c.customer_key

)

select *
from final