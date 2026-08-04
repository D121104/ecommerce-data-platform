with source as (

    select *
    from {{ source('ecommerce_raw', 'users') }}

),

renamed as (

    select
        user_id::bigint                             as user_id,
        lower(nullif(trim(email), ''))              as email,
        nullif(trim(name), '')                      as user_name,
        lower(nullif(trim(role), ''))               as user_role,
        nullif(trim(avatar_url), '')                 as avatar_url,
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