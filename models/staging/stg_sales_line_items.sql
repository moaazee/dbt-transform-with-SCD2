-- STAGING: sales_line_items (clean/standardize only)
with source as (
  select * from {{ source('raw_data', 'sales_line_items') }}
),
cleaned as (
  select
    cast(sale_item_id as number)       as sale_item_id,
    cast(sale_id as number)            as sale_id,
    cast(product_id as number)         as product_id,
    cast(quantity as number)           as quantity,
    cast(unit_price as number(10,2))   as unit_price,
    cast(line_total_amount as number(12,2)) as line_total_amount
  from source
  where sale_item_id is not null
)
select * from cleaned
