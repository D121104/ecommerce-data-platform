with fact_totals as (

    select
        ordered_at::date                          as ordered_date,
        currency_code,
        count(*)::bigint                          as order_count,
        sum(total_quantity)::bigint               as total_quantity,
        sum(order_amount)::numeric(18, 2)          as gross_order_amount
    from {{ ref('fct_orders') }}
    group by
        ordered_at::date,
        currency_code

)

select
    m.ordered_date,
    m.currency_code,

    m.order_count,
    f.order_count as expected_order_count,

    m.total_quantity,
    f.total_quantity as expected_total_quantity,

    m.gross_order_amount,
    f.gross_order_amount as expected_gross_order_amount

from {{ ref('mart_daily_sales') }} as m
full outer join fact_totals as f
    on m.ordered_date = f.ordered_date
   and m.currency_code = f.currency_code

where m.ordered_date is null
   or f.ordered_date is null
   or m.order_count <> f.order_count
   or m.total_quantity <> f.total_quantity
   or m.gross_order_amount <> f.gross_order_amount