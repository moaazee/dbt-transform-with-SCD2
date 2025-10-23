{{
    config(
        materialized='incremental',
        unique_key='manufacturer_key',
        on_schema_change='sync_all_columns'
    )
}}

-- SCD2 DIM_MANUFACTURERS

with src as (
  select * from {{ ref('stg_manufacturers') }}
),
incoming as (
  select
    md5(cast(manufacturer_id as varchar) || coalesce(manufacturer_name,'') || coalesce(country,'')) as manufacturer_key,
    manufacturer_id,
    manufacturer_name,
    country,
    contact_email,
    phone,
    is_active,
    current_date() as valid_from_date,
    cast(null as date) as valid_to_date,
    true as is_current
  from src
),
curr as (
  select * from {{ this }} where is_current = true
),
changes as (
  select i.*
  from incoming i
  left join curr c on i.manufacturer_id = c.manufacturer_id
  where c.manufacturer_id is null
     or i.manufacturer_name <> c.manufacturer_name
     or i.country <> c.country
     or i.contact_email <> c.contact_email
     or i.phone <> c.phone
     or i.is_active <> c.is_active
)
select * from (
  select * from curr where manufacturer_id not in (select manufacturer_id from changes)
  union all
  select
    c.manufacturer_key, c.manufacturer_id, c.manufacturer_name, c.country, c.contact_email, c.phone, c.is_active,
    c.valid_from_date, current_date() as valid_to_date, false as is_current
  from curr c join changes ch on c.manufacturer_id = ch.manufacturer_id
  union all
  select * from changes
) final
