{% snapshot customers_snapshot %}
{{
    config(
      target_database='SALES_DB',
      target_schema='TRANSFORM_DATA',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_name', 'address', 'city', 'state', 'zip_code', 'email', 'phone', 'is_active']
    )
}}
select customer_id, customer_name, customer_type, email, phone, address, city, state, zip_code, is_active
from {{ ref('stg_customers') }}
{% endsnapshot %}
