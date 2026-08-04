with source as (

    select *
    from {{ source('ecommerce_raw', 'categories') }}

),

renamed as (

    select
        category_id::bigint                         as category_id,
        nullif(trim(name), '')                      as category_name,
        nullif(trim(slug), '')                      as category_slug,
        nullif(trim(image_url), '')                 as category_image_url,
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