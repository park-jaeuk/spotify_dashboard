CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::793619800697:role/mysnowflakerole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://airflow-gin-bucket');

-- 참고 https://docs.snowflake.com/ko/user-guide/data-load-s3-config-storage-integration