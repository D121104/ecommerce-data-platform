select
    'categories' as entity,
    category_id::text as record_id,
    first_seen_at,
    last_seen_at
from {{ ref('stg_categories') }}
where last_seen_at < first_seen_at

union all

select
    'products',
    product_id::text,
    first_seen_at,
    last_seen_at
from {{ ref('stg_products') }}
where last_seen_at < first_seen_at

union all

select
    'users',
    user_id::text,
    first_seen_at,
    last_seen_at
from {{ ref('stg_users') }}
where last_seen_at < first_seen_at