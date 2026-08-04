with source as (

    select *
    from {{ source('ecommerce_raw', 'products') }}

),

renamed as (

    select
        product_id::bigint                          as product_id,
        nullif(trim(title), '')                     as product_name,
        nullif(trim(slug), '')                      as product_slug,
        price::numeric(18, 2)                       as product_price,
        nullif(trim(description), '')               as product_description,
        category_id::bigint                         as category_id,
        images::text[]                              as image_urls,
        source_created_at::timestamptz              as source_created_at,
        source_updated_at::timestamptz              as source_updated_at,
        payload::jsonb                              as raw_payload,
        trim(record_hash)::text                     as record_hash,
        batch_id::uuid                              as batch_id,
        is_active::boolean                          as is_active,
        first_seen_at::timestamptz                  as first_seen_at,
        last_seen_at::timestamptz                   as last_seen_at
    from source

)

select *
from renamed