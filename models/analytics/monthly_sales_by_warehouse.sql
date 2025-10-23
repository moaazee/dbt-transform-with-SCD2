{{ config(materialized='table') }}

with line as (
  select sale_id, product_key, line_total_amount
  from {{ ref('fact_sales_line_items') }}
),
prod as (
  select product_key, product_id from {{ ref('dim_products') }} where is_current = true
),
inv as (
  select product_id, warehouse_location from {{ ref('dim_inventory') }} where is_current = true
),
sales as (
  select sale_id, sale_date from {{ ref('fact_sales') }}
)
select
  date_trunc('month', s.sale_date) as month,
  i.warehouse_location,
  sum(coalesce(l.line_total_amount, 0)) as revenue
from line l
left join prod p using (product_key)
left join inv i using (product_id)
left join sales s using (sale_id)
group by 1, 2
order by 1, 2;
