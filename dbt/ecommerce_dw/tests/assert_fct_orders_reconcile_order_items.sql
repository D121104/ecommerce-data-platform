with order_item_totals as (

    select
        order_key,
        count(*)                                as calculated_line_count,
        sum(quantity)                           as calculated_quantity,
        sum(line_amount)::numeric(18, 2)        as calculated_amount
    from {{ ref('fct_order_items') }}
    group by order_key

),

reconciled as (

    select
        o.order_id,
        o.order_key,

        o.order_line_count,
        coalesce(i.calculated_line_count, 0)     as calculated_line_count,

        o.total_quantity,
        coalesce(i.calculated_quantity, 0)       as calculated_quantity,

        o.order_amount,
        coalesce(i.calculated_amount, 0)::numeric(18, 2)
                                                as calculated_amount
    from {{ ref('fct_orders') }} as o

    left join order_item_totals as i
        on o.order_key = i.order_key

)

select *
from reconciled
where order_line_count <> calculated_line_count
   or total_quantity <> calculated_quantity
   or order_amount <> calculated_amount