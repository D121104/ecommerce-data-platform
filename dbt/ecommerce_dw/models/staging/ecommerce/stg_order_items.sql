with source as (

    select *
    from {{ source('ecommerce_raw', 'order_items') }}

),

renamed as (

    select
        order_id::uuid                              as order_id,
        line_number::smallint                       as line_number,
        product_id::bigint                          as product_id,
        quantity::smallint                          as quantity,
        unit_price::numeric(18, 2)                  as unit_price,
        batch_id::uuid                              as batch_id,
        ingested_at::timestamptz                    as ingested_at,

        quantity * unit_price::numeric(18, 2)       as line_amount
    from source

)

select *
from renamed