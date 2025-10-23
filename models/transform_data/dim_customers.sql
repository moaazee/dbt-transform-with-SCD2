{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        on_schema_change='sync_all_columns'
    )
}}

-- SCD2 DIM_CUSTOMERS
-- Tracks historical changes to customer attributes.
-- Uses is_current, valid_from_date, valid_to_date for versioning.

with src as (
  select * from {{ ref('stg_customers') }}
),
incoming as (
  -- Build a surrogate key derived from the natural key + selected changing attributes
  select
    md5(cast(customer_id as varchar) || coalesce(customer_name,'') || coalesce(address,'') || coalesce(city,'') || coalesce(state,'') || coalesce(zip_code,'')) as customer_key,
    customer_id,
    customer_name,
    customer_type,
    email,
    phone,
    address,
    city,
    state,
    zip_code,
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
  -- New or changed (attribute drift) vs current
  select i.*
  from incoming i
  left join curr c on i.customer_id = c.customer_id
  where c.customer_id is null
     or i.customer_name <> c.customer_name
     or i.address <> c.address
     or i.city <> c.city
     or i.state <> c.state
     or i.zip_code <> c.zip_code
     or i.email <> c.email
     or i.phone <> c.phone
     or i.is_active <> c.is_active
)
select * from (
  -- keep current records that did not change
  select * from curr
  where customer_id not in (select customer_id from changes)

  union all

  -- close out old versions where change detected
  select
    c.customer_key,
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.email,
    c.phone,
    c.address,
    c.city,
    c.state,
    c.zip_code,
    c.is_active,
    c.valid_from_date,
    current_date() as valid_to_date,
    false as is_current
  from curr c
  join changes ch on c.customer_id = ch.customer_id

  union all

  -- insert new versions
  select * from changes
) final;
