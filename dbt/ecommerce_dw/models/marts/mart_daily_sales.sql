with orders as (

    select *
    from {{ ref('fct_orders') }}

),

final as (

    select
        ordered_at::date                          as ordered_date,
        currency_code,

        count(*)::bigint                          as order_count,
        sum(order_line_count)::bigint             as order_line_count,
        sum(total_quantity)::bigint               as total_quantity,
        sum(order_amount)::numeric(18, 2)          as gross_order_amount,

        sum(
            case when order_status = 'pending'
                then 1 else 0
            end
        )::bigint                                  as pending_order_count,

        sum(
            case when order_status = 'paid'
                then 1 else 0
            end
        )::bigint                                  as paid_order_count,

        sum(
            case when order_status = 'shipped'
                then 1 else 0
            end
        )::bigint                                  as shipped_order_count,

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
        ordered_at::date,
        currency_code

)

select *
from final