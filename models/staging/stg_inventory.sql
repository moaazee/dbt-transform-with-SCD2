-- STAGING: inventory (clean/standardize only)
with source as (
  select * from {{ source('raw_data', 'inventory') }}
),
cleaned as (
  select
    cast(inventory_id as number)      as inventory_id,
    cast(product_id as number)        as product_id,
    cast(quantity_on_hand as number)  as quantity_on_hand,
    cast(low_stock_threshold as number) as low_stock_threshold,
    cast(last_restocked_date as date) as last_restocked_date,
    initcap(trim(warehouse_location)) as warehouse_location
  from source
  where inventory_id is not null
)
select * from cleaned;
