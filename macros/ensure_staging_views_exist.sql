{% macro ensure_staging_views_exist() %}
    {% set sql %}
        CREATE SCHEMA IF NOT EXISTS SALES_DB.STAGING;

        -- Ensure each staging view is (re)created
        CREATE OR REPLACE VIEW SALES_DB.STAGING.STG_CUSTOMERS AS
        SELECT * FROM {{ source('raw_data', 'customers') }};

        CREATE OR REPLACE VIEW SALES_DB.STAGING.STG_PRODUCTS AS
        SELECT * FROM {{ source('raw_data', 'products') }};

        CREATE OR REPLACE VIEW SALES_DB.STAGING.STG_MANUFACTURERS AS
        SELECT * FROM {{ source('raw_data', 'manufacturers') }};

        CREATE OR REPLACE VIEW SALES_DB.STAGING.STG_INVENTORY AS
        SELECT * FROM {{ source('raw_data', 'inventory') }};

        CREATE OR REPLACE VIEW SALES_DB.STAGING.STG_SALES AS
        SELECT * FROM {{ source('raw_data', 'sales') }};

        CREATE OR REPLACE VIEW SALES_DB.STAGING.STG_SALES_LINE_ITEMS AS
        SELECT * FROM {{ source('raw_data', 'sales_line_items') }};
    {% endset %}

    {% do run_query(sql) %}
    {{ log(" Verified and recreated all STAGING views in SALES_DB.STAGING", info=True) }}
{% endmacro %}
