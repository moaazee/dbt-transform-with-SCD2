{{
    config(
        materialized='incremental',
        unique_key='product_key',
        on_schema_change='sync_all_columns'
    )
}}

-- SCD2 DIM_PRODUCTS

with src as (
  select * from {{ ref('stg_products') }}
),
incoming as (
  select
    md5(cast(product_id as varchar) || coalesce(product_name,'') || coalesce(category,'') || coalesce(sku,'')) as product_key,
    product_id,
    manufacturer_id,
    product_name,
    category,
    unit_cost,
    unit_price,
    sku,
    is_active,
    current_date() as valid_from_date,
    cast(null as date) as valid_to_date,
    true as is_current
  from src
),
curr as (
  select * from {{ this }} where is_current = true
),
changes as (
  select i.*
  from incoming i
  left join curr c on i.product_id = c.product_id
  where c.product_id is null
     or i.product_name <> c.product_name
     or i.category <> c.category
     or i.unit_cost <> c.unit_cost
     or i.unit_price <> c.unit_price
     or i.sku <> c.sku
     or i.is_active <> c.is_active
)
select * from (
  select * from curr where product_id not in (select product_id from changes)
  union all
  select
    c.product_key, c.product_id, c.manufacturer_id, c.product_name, c.category,
    c.unit_cost, c.unit_price, c.sku, c.is_active,
    c.valid_from_date, current_date() as valid_to_date, false as is_current
  from curr c join changes ch on c.product_id = ch.product_id
  union all
  select * from changes
) final;
