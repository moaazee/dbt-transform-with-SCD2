{{ config(materialized='table') }}

with line as (
  select product_key, line_total_amount
  from {{ ref('fact_sales_line_items') }}
),
p as (
  select product_key, product_name, category
  from {{ ref('dim_products') }}
  where is_current = true
)
select
  p.product_key,
  p.product_name,
  p.category,
  sum(coalesce(l.line_total_amount, 0)) as revenue
from line l
left join p using (product_key)
group by 1, 2, 3
order by revenue desc;
