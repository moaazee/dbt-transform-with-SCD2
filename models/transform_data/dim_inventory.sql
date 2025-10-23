{{
    config(
        materialized='incremental',
        unique_key='inventory_key',
        on_schema_change='sync_all_columns'
    )
}}

-- SCD2 DIM_INVENTORY

with src as (
  select * from {{ ref('stg_inventory') }}
),
incoming as (
  select
    md5(cast(inventory_id as varchar) || coalesce(warehouse_location,'')) as inventory_key,
    inventory_id,
    product_id,
    quantity_on_hand,
    low_stock_threshold,
    last_restocked_date,
    warehouse_location,
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
  left join curr c on i.inventory_id = c.inventory_id
  where c.inventory_id is null
     or i.product_id <> c.product_id
     or i.quantity_on_hand <> c.quantity_on_hand
     or i.low_stock_threshold <> c.low_stock_threshold
     or i.last_restocked_date <> c.last_restocked_date
     or i.warehouse_location <> c.warehouse_location
)
select * from (
  select * from curr where inventory_id not in (select inventory_id from changes)
  union all
  select
    c.inventory_key, c.inventory_id, c.product_id, c.quantity_on_hand, c.low_stock_threshold,
    c.last_restocked_date, c.warehouse_location,
    c.valid_from_date, current_date() as valid_to_date, false as is_current
  from curr c join changes ch on c.inventory_id = ch.inventory_id
  union all
  select * from changes
) final;
