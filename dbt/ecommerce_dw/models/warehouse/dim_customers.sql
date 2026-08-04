with customers as (

    select *
    from {{ ref('stg_users') }}

),

final as (

    select
        md5('customer|' || user_id::text) as customer_key,

        user_id                           as customer_id,
        email,
        user_name                         as customer_name,
        user_role                         as customer_role,
        avatar_url,

        is_active,
        source_created_at,
        source_updated_at,
        first_seen_at,
        last_seen_at,

        record_hash,
        batch_id
    from customers

)

select *
from final