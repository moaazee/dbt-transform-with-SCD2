# Ingestion (Snowflake)

This folder contains SQL used outside dbt to prepare and load RAW tables in `SALES_DB.RAW_DATA`.

- `01_create_db_schemas.sql` — creates SALES_DB and schemas (RAW_DATA, COMMON_DATA, TRANSFORM_DATA)
- `02_stage_and_format.sql` — creates S3 stage (replace credentials!) and CSV file format
- `03_raw_tables.sql` — creates RAW_DATA tables (manufacturers, products, customers, inventory, sales, sales_line_items)
- `04_loader_procedure.sql` — creates COMMON_DATA.load_data_from_stage() procedure to COPY INTO RAW tables

> IMPORTANT: Do **not** commit real AWS credentials. Replace placeholders before running.
