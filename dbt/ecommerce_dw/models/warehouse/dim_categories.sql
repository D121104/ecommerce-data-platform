with categories as (

    select *
    from {{ ref('stg_categories') }}

),

final as (

    select
        md5('category|' || category_id::text) as category_key,

        category_id,
        category_name,
        category_slug,
        category_image_url,

        is_active,
        source_created_at,
        source_updated_at,
        first_seen_at,
        last_seen_at,

        record_hash,
        batch_id
    from categories

)

select *
from final