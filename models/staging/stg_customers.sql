-- STAGING: customers (clean/standardize only)
with source as (
  select * from {{ source('raw_data', 'customers') }}
),
cleaned as (
  select
    cast(customer_id as number)       as customer_id,
    initcap(trim(customer_name))      as customer_name,
    initcap(trim(type))               as customer_type,
    lower(trim(email))                as email,
    trim(phone)                       as phone,
    initcap(trim(address))            as address,
    initcap(trim(city))               as city,
    initcap(trim(state))              as state,
    trim(zip_code)                    as zip_code,
    coalesce(is_active, true)         as is_active
  from source
  where customer_id is not null
)
select * from cleaned;
