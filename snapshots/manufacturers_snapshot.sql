{% snapshot manufacturers_snapshot %}
{{
    config(
      target_database='SALES_DB',
      target_schema='TRANSFORM_DATA',
      unique_key='manufacturer_id',
      strategy='check',
      check_cols=['manufacturer_name', 'country', 'contact_email', 'phone', 'is_active']
    )
}}
select manufacturer_id, manufacturer_name, country, contact_email, phone, is_active
from {{ ref('stg_manufacturers') }}
{% endsnapshot %}
