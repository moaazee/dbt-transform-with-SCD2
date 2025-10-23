{{ config(materialized='table') }}

-- FACT_SALES_LINE_ITEMS (Type 1; join to current product version)

with li as (
  select * from {{ ref('stg_sales_line_items') }}
),
dp as (
  select product_id, product_key
  from {{ ref('dim_products') }}
  where is_current = true
)
select
  li.sale_item_id,
  li.sale_id,
  dp.product_key,
  li.quantity,
  li.unit_price,
  coalesce(li.line_total_amount, li.quantity * li.unit_price) as line_total_amount
from li
left join dp using (product_id)
