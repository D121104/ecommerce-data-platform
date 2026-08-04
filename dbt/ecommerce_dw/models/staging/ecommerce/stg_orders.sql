with source as (

    select *
    from {{ source('ecommerce_raw', 'orders') }}

),

renamed as (

    select
        order_id::uuid                              as order_id,
        customer_id::bigint                         as customer_id,
        lower(nullif(trim(order_status), ''))       as order_status,
        ordered_at::timestamptz                     as ordered_at,
        upper(nullif(trim(currency), ''))            as currency_code,
        lower(nullif(trim(payment_method), ''))     as payment_method,
        upper(nullif(trim(shipping_country), ''))   as shipping_country_code,
        payload::jsonb                              as raw_payload,
        batch_id::uuid                              as batch_id,
        ingested_at::timestamptz                    as ingested_at
    from source

)

select *
from renamed