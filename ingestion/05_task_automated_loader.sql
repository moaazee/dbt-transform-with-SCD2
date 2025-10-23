USE DATABASE SALES_DB;
USE SCHEMA COMMON_DATA;

CREATE OR REPLACE PROCEDURE COMMON_DATA.run_full_ingestion()
RETURNS STRING
LANGUAGE SQL
AS
$$
    CALL COMMON_DATA.load_data_from_stage(
        table_name => 'ALL_TABLES',
        stage_name => 'ETL_S3_STAGE_DATA',
        file_format_name => 'csv_format',
        database_name => 'SALES_DB',
        schema_name => 'RAW_DATA'
    );
    RETURN 'Data load completed successfully.';
$$;

CREATE OR REPLACE TASK COMMON_DATA.auto_load_raw_data
  WAREHOUSE = CASE_WH
  SCHEDULE = 'USING CRON 0 2 * * * UTC'
AS
  CALL COMMON_DATA.run_full_ingestion();

ALTER TASK COMMON_DATA.auto_load_raw_data RESUME;
