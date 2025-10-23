# LEAP_CASE_v2_with_staging_SCD2

Refactor of your LEAP case to add a dedicated **STAGING** layer and implement **SCD Type 2** for dimensions.
- Database: `SALES_DB`
- Schemas: `RAW_DATA`, `COMMON_DATA`, `TRANSFORM_DATA`, plus dbt-created `STAGING` and `ANALYTICS`
- Loader procedure: `COMMON_DATA.load_data_from_stage`

## Flow
1) Ingestion (outside dbt): call `COMMON_DATA.load_data_from_stage('ALL_TABLES', 'ETL_S3_STAGE_DATA', 'csv_format', 'SALES_DB', 'RAW_DATA')`
2) dbt STAGING: `stg_*.sql` clean/standardize raw tables
3) dbt TRANSFORM:
   - SCD2 Dimensions: `dim_customers`, `dim_manufacturers`, `dim_products`, `dim_inventory`
   - Facts: `fact_sales` (joins to customer_key), `fact_sales_line_items` (joins to product_key)
4) Analytics: `customer_segmentation`, `monthly_sales_by_warehouse`, `top_products_by_revenue`

## Commands
```bash
dbt deps
dbt build
```

## Notes
- Replace AWS credentials in `ingestion/02_stage_and_format.sql` before running.
- Ensure your `~/.dbt/profiles.yml` target points to SALES_DB and a working warehouse/role.

## 🔁 Nightly Automation
A Snowflake Task (`COMMON_DATA.auto_load_raw_data`) runs nightly at 02:00 UTC to load new CSV data from S3 into `RAW_DATA`.
It calls `COMMON_DATA.load_data_from_stage` via the wrapper procedure `run_full_ingestion`.

## 🧩 dbt Snapshots
Snapshots for all dimension tables are in `/snapshots/`. They automatically track changes using dbt’s SCD2 mechanism.
Run:
```
dbt snapshot
```
after ingestion to record new history.
