-- STAGING: products (clean/standardize only)
with source as (
  select * from {{ source('raw_data', 'products') }}
),
cleaned as (
  select
    cast(product_id as number)        as product_id,
    initcap(trim(product_name))       as product_name,
    cast(manufacturer_id as number)   as manufacturer_id,
    initcap(trim(category))           as category,
    cast(unit_cost as number(10,2))   as unit_cost,
    cast(unit_price as number(10,2))  as unit_price,
    upper(trim(sku))                  as sku,
    coalesce(is_active, true)         as is_active
  from source
  where product_id is not null
)
select * from cleaned
