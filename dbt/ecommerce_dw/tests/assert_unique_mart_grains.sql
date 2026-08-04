select
    'mart_daily_sales' as model_name,
    ordered_date::text as grain_value_1,
    currency_code as grain_value_2,
    count(*) as duplicate_count
from {{ ref('mart_daily_sales') }}
group by
    ordered_date,
    currency_code
having count(*) > 1

union all

select
    'mart_customer_sales',
    customer_key::text,
    currency_code,
    count(*)
from {{ ref('mart_customer_sales') }}
group by
    customer_key,
    currency_code
having count(*) > 1

union all

select
    'mart_product_sales',
    product_key::text,
    currency_code,
    count(*)
from {{ ref('mart_product_sales') }}
group by
    product_key,
    currency_code
having count(*) > 1