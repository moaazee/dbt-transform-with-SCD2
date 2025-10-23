{{ config(materialized='table') }}

-- FACT_SALES (Type 1; join to current dimension versions via natural keys)

with s as (
  select * from {{ ref('stg_sales') }}
),
dc as (
  select customer_id, customer_key
  from {{ ref('dim_customers') }}
  where is_current = true
)
select
  s.sale_id,
  dc.customer_key,
  s.sale_date,
  s.total_amount,
  s.status,
  s.payment_method,
  s.invoice_number
from s
left join dc using (customer_id);
