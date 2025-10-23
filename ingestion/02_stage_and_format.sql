USE DATABASE SALES_DB;
USE SCHEMA COMMON_DATA;

-- Replace AWS credentials before running in your own environment.
CREATE OR REPLACE STAGE ETL_S3_STAGE_DATA
  URL = 's3://etl-data-csv-s3-bucket/'
  CREDENTIALS = (
    AWS_KEY_ID = '<REPLACE_AWS_KEY_ID>'
    AWS_SECRET_KEY = '<REPLACE_AWS_SECRET_KEY>'
  )
  COMMENT = 'S3 csv stage for data loading';

CREATE OR REPLACE FILE FORMAT COMMON_DATA.csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('NULL', 'null')
  COMPRESSION = AUTO
  COMMENT = 'CSV format with comma delimiter, double quotes, and header skip';
