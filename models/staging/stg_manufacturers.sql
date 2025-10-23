-- STAGING: manufacturers (clean/standardize only)
with source as (
  select * from {{ source('raw_data', 'manufacturers') }}
),
cleaned as (
  select
    cast(manufacturer_id as number)   as manufacturer_id,
    initcap(trim(manufacturer_name))  as manufacturer_name,
    initcap(trim(country))            as country,
    lower(trim(contact_email))        as contact_email,
    trim(phone)                       as phone,
    coalesce(is_active, true)         as is_active
  from source
  where manufacturer_id is not null
)
select * from cleaned
