with products as (

    select *
    from {{ ref('stg_products') }}

),

categories as (

    select
        category_id,
        category_key
    from {{ ref('dim_categories') }}

),

final as (

    select
        md5('product|' || p.product_id::text) as product_key,

        p.product_id,
        p.product_name,
        p.product_slug,
        p.product_description,
        p.product_price,
        p.image_urls,

        p.category_id,
        c.category_key,

        p.is_active,
        p.source_created_at,
        p.source_updated_at,
        p.first_seen_at,
        p.last_seen_at,

        p.record_hash,
        p.batch_id
    from products as p
    left join categories as c
        on p.category_id = c.category_id

)

select *
from final