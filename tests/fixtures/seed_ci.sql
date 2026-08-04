\set ON_ERROR_STOP on

BEGIN;

INSERT INTO ops.pipeline_runs (
    batch_id,
    pipeline_name,
    source_name,
    status,
    started_at,
    finished_at,
    records_extracted,
    records_inserted,
    records_updated,
    records_rejected,
    run_metadata
)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'ci_fixture_seed',
    'static_fixture',
    'success',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:01+00',
    14,
    14,
    0,
    0,
    '{"environment": "ci", "deterministic": true}'::jsonb
)
ON CONFLICT (batch_id) DO NOTHING;


INSERT INTO raw.categories (
    category_id,
    name,
    slug,
    image_url,
    source_created_at,
    source_updated_at,
    payload,
    record_hash,
    batch_id
)
VALUES
(
    1,
    'Clothes',
    'clothes',
    'https://example.com/categories/clothes.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":1,"name":"Clothes","slug":"clothes"}'::jsonb,
    repeat('a', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    2,
    'Electronics',
    'electronics',
    'https://example.com/categories/electronics.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":2,"name":"Electronics","slug":"electronics"}'::jsonb,
    repeat('b', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    3,
    'Furniture',
    'furniture',
    'https://example.com/categories/furniture.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":3,"name":"Furniture","slug":"furniture"}'::jsonb,
    repeat('c', 64),
    '00000000-0000-0000-0000-000000000001'
)
ON CONFLICT (category_id) DO NOTHING;


INSERT INTO raw.products (
    product_id,
    title,
    slug,
    price,
    description,
    category_id,
    images,
    source_created_at,
    source_updated_at,
    payload,
    record_hash,
    batch_id
)
VALUES
(
    101,
    'Classic T-Shirt',
    'classic-t-shirt',
    25.00,
    'Cotton T-shirt',
    1,
    ARRAY['https://example.com/products/tshirt.jpg'],
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":101,"title":"Classic T-Shirt","price":25.00,"category":{"id":1}}'::jsonb,
    repeat('d', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    102,
    'Denim Jacket',
    'denim-jacket',
    65.00,
    'Blue denim jacket',
    1,
    ARRAY['https://example.com/products/jacket.jpg'],
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":102,"title":"Denim Jacket","price":65.00,"category":{"id":1}}'::jsonb,
    repeat('e', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    201,
    'Wireless Headphones',
    'wireless-headphones',
    99.90,
    'Bluetooth headphones',
    2,
    ARRAY['https://example.com/products/headphones.jpg'],
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":201,"title":"Wireless Headphones","price":99.90,"category":{"id":2}}'::jsonb,
    repeat('f', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    202,
    'Mechanical Keyboard',
    'mechanical-keyboard',
    79.50,
    'Mechanical keyboard',
    2,
    ARRAY['https://example.com/products/keyboard.jpg'],
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":202,"title":"Mechanical Keyboard","price":79.50,"category":{"id":2}}'::jsonb,
    repeat('1', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    301,
    'Office Chair',
    'office-chair',
    149.00,
    'Ergonomic office chair',
    3,
    ARRAY['https://example.com/products/chair.jpg'],
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":301,"title":"Office Chair","price":149.00,"category":{"id":3}}'::jsonb,
    repeat('2', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    302,
    'Wooden Desk',
    'wooden-desk',
    220.00,
    'Wooden working desk',
    3,
    ARRAY['https://example.com/products/desk.jpg'],
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":302,"title":"Wooden Desk","price":220.00,"category":{"id":3}}'::jsonb,
    repeat('3', 64),
    '00000000-0000-0000-0000-000000000001'
)
ON CONFLICT (product_id) DO NOTHING;


INSERT INTO raw.users (
    user_id,
    email,
    name,
    role,
    avatar_url,
    source_created_at,
    source_updated_at,
    payload,
    record_hash,
    batch_id
)
VALUES
(
    1001,
    'alice@example.com',
    'Alice Nguyen',
    'customer',
    'https://example.com/users/alice.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":1001,"email":"alice@example.com","name":"Alice Nguyen","role":"customer"}'::jsonb,
    repeat('4', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    1002,
    'bob@example.com',
    'Bob Tran',
    'customer',
    'https://example.com/users/bob.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":1002,"email":"bob@example.com","name":"Bob Tran","role":"customer"}'::jsonb,
    repeat('5', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    1003,
    'carol@example.com',
    'Carol Le',
    'customer',
    'https://example.com/users/carol.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":1003,"email":"carol@example.com","name":"Carol Le","role":"customer"}'::jsonb,
    repeat('6', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    1004,
    'david@example.com',
    'David Pham',
    'customer',
    'https://example.com/users/david.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":1004,"email":"david@example.com","name":"David Pham","role":"customer"}'::jsonb,
    repeat('7', 64),
    '00000000-0000-0000-0000-000000000001'
),
(
    1005,
    'emma@example.com',
    'Emma Hoang',
    'customer',
    'https://example.com/users/emma.jpg',
    '2026-08-01 00:00:00+00',
    '2026-08-01 00:00:00+00',
    '{"id":1005,"email":"emma@example.com","name":"Emma Hoang","role":"customer"}'::jsonb,
    repeat('8', 64),
    '00000000-0000-0000-0000-000000000001'
)
ON CONFLICT (user_id) DO NOTHING;

COMMIT;