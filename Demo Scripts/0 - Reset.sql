/*-------------------------------------------------------------------
-- 0 - Reset.sql
-- 
-------------------------------------------------------------------*/
/*

1.  SSMS 22 
    - Switch theme -> Cool Breeze from Midnight Glow
    * Restart SSMS to ensure resultset grid is changed
    - check resultset font sizing


1.  PuTTy to 192.168.150.129 
    - Start MinIO
    - Open MinIO console (MS Edge): 
    https://192.168.150.129:9001/

2.  Turn off "real time protection"
Start -> "Virus & threat protection" -> "Virus & threat protection settings -> Real-Time Protection = OFF" 

3.  Open S3 Browser
    - Clean up following buckets/prefixes:
    * /helloworld/my_first_parquet/
    * /recipes-reviews-flat-/nocollate/

4.  ZoomIt

*/



USE CookbookDemo_Bloat
GO


IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'test_CSV')
    DROP EXTERNAL TABLE cetas_minio.test_CSV
GO

IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_pq')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_pq
GO
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat
GO
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2018')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2018
GO
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2019')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2019
GO
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2020')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2020
GO
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2021')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2021
GO
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2022')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2022
GO
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2023')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2023
GO

IF EXISTS(SELECT 1 FROM sys.external_file_formats WHERE name = 'CSV_text_file_format')
    DROP EXTERNAL FILE FORMAT CSV_text_file_format;
GO
IF EXISTS(SELECT 1 FROM sys.external_file_formats WHERE name = 'parquet_file_format_object')
    DROP EXTERNAL FILE FORMAT parquet_file_format_object;
GO
IF EXISTS(SELECT 1 FROM sys.external_data_sources WHERE name = 'minio_local')
    DROP EXTERNAL DATA SOURCE minio_local
GO
IF EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'minio_local_db_cred')
    DROP DATABASE SCOPED CREDENTIAL minio_local_db_cred
GO

IF EXISTS(SELECT 1 FROM sys.external_data_sources WHERE name = 'AzureStorage_SQLBek')
    DROP EXTERNAL DATA SOURCE AzureStorage_SQLBek
GO
IF EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'SQLBekStorage')
    DROP DATABASE SCOPED CREDENTIAL SQLBekStorage
GO

IF EXISTS(SELECT * FROM sys.symmetric_keys WHERE [name] = '##MS_DatabaseMasterKey##')
    DROP MASTER KEY
GO



USE CookbookDemo_Bloat
GO


-----
-- Test MINIO plumbing 
-- Query via OPENROWSET() example
-- Setup bare minimum then tear it down again
-----
-- Create a Database Master Key
IF EXISTS(SELECT * FROM sys.symmetric_keys WHERE [name] = '##MS_DatabaseMasterKey##')
    DROP MASTER KEY
GO
CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'Give Sebastian a treat! He is a good boy!';
GO

IF EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'minio_local_db_cred')
    DROP DATABASE SCOPED CREDENTIAL minio_local_db_cred
GO
CREATE DATABASE SCOPED CREDENTIAL minio_local_db_cred
WITH 
    IDENTITY = 'S3 Access Key',     -- MUST be "S3 Access Key"
    SECRET = 'andy_local:andy_local';
GO

IF EXISTS(SELECT 1 FROM sys.external_data_sources WHERE name = 'minio_local')
    DROP EXTERNAL DATA SOURCE minio_local
GO
CREATE EXTERNAL DATA SOURCE minio_local
WITH (
    PUSHDOWN = ON,
    LOCATION = 's3://192.168.150.129:9000',  -- s3://[bucket]@[address]:port
    CREDENTIAL = [minio_local_db_cred]
);
GO

SELECT * 
FROM OPENROWSET (
    BULK '/helloworld/testCSV.csv',
    FORMAT = 'CSV',
    DATA_SOURCE = 'minio_local',
    FIRSTROW = 1
)
WITH (
    MyValue VARCHAR(50),
    AnotherValue VARCHAR(50),
    Foobar VARCHAR(50)
) AS [cc];
GO


IF EXISTS(SELECT 1 FROM sys.external_data_sources WHERE name = 'minio_local')
    DROP EXTERNAL DATA SOURCE minio_local
GO
IF EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'minio_local_db_cred')
    DROP DATABASE SCOPED CREDENTIAL minio_local_db_cred
GO
IF EXISTS(SELECT * FROM sys.symmetric_keys WHERE [name] = '##MS_DatabaseMasterKey##')
    DROP MASTER KEY
GO



-----------------------------------------------------------
-- Confirm everything is gone
-----------------------------------------------------------

-----
-- Show current master key
SELECT * 
FROM sys.symmetric_keys 
WHERE name = '##MS_DatabaseMasterKey##'
GO

-----
-- Show current database scoped credentials
SELECT *
FROM sys.database_scoped_credentials
GO

-----
-- Show current external data sources
SELECT *
FROM sys.external_tables;
GO

-----
-- Show current external data sources
SELECT *
FROM sys.external_data_sources;
GO

-----
-- Show current external file formats
SELECT *
FROM sys.external_file_formats;
GO
