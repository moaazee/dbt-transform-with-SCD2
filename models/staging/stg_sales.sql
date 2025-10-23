-- STAGING: sales (clean/standardize only)
with source as (
  select * from {{ source('raw_data', 'sales') }}
),
cleaned as (
  select
    cast(sale_id as number)            as sale_id,
    cast(customer_id as number)        as customer_id,
    cast(sale_date as date)            as sale_date,
    cast(total_amount as number(12,2)) as total_amount,
    initcap(trim(status))              as status,
    initcap(trim(payment_method))      as payment_method,
    upper(trim(invoice_number))        as invoice_number
  from source
  where sale_id is not null
)
select * from cleaned;
