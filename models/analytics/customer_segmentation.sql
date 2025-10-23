{{ config(materialized='table') }}

with sales_by_customer as (
  select
    fs.customer_key,
    sum(fs.total_amount) as total_revenue,
    count(distinct fs.sale_id) as orders_count,
    min(fs.sale_date) as first_purchase,
    max(fs.sale_date) as last_purchase
  from {{ ref('fact_sales') }} fs
  group by 1
)
select
  dc.customer_key,
  dc.customer_id,
  dc.customer_name,
  dc.customer_type,
  dc.city,
  dc.state,
  dc.zip_code,
  s.total_revenue,
  s.orders_count,
  s.first_purchase,
  s.last_purchase,
  case
    when s.total_revenue >= 10000 then 'VIP'
    when s.total_revenue >= 3000  then 'Gold'
    when s.total_revenue >= 1000  then 'Silver'
    else 'Bronze'
  end as segment
from {{ ref('dim_customers') }} dc
left join sales_by_customer s using (customer_key)
where dc.is_current = true;
