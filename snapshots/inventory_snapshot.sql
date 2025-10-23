{% snapshot inventory_snapshot %}
{{
    config(
      target_database='SALES_DB',
      target_schema='TRANSFORM_DATA',
      unique_key='inventory_id',
      strategy='check',
      check_cols=['product_id', 'quantity_on_hand', 'low_stock_threshold', 'warehouse_location']
    )
}}
select inventory_id, product_id, quantity_on_hand, low_stock_threshold, last_restocked_date, warehouse_location
from {{ ref('stg_inventory') }}
{% endsnapshot %}
