{% snapshot products_snapshot %}
{{
    config(
      target_database='SALES_DB',
      target_schema='TRANSFORM_DATA',
      unique_key='product_id',
      strategy='check',
      check_cols=['product_name', 'category', 'unit_cost', 'unit_price', 'sku', 'is_active']
    )
}}
select product_id, manufacturer_id, product_name, category, unit_cost, unit_price, sku, is_active
from {{ ref('stg_products') }}
{% endsnapshot %}
