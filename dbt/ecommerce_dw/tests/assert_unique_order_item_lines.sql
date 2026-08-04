select
    order_id,
    line_number,
    count(*) as duplicate_count
from {{ source('ecommerce_raw', 'order_items') }}
group by
    order_id,
    line_number
having count(*) > 1