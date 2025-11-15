/*-----------------------------------------------------------------------------
-- 1 - Introduce Data Virtualization Setup.sql
--
-- Written By: Andy Yun
-- Created On: 2025-11-12
-- 
-- Summary: 
-- 
-----------------------------------------------------------------------------*/
USE CookbookDemo_Bloat;
GO




-----
-- What version of SQL Server?
-- Is Polybase installed AND enabled?  Only needed for SQL Server 2022
SELECT @@SERVERNAME, @@VERSION, SERVERPROPERTY ('IsPolyBaseInstalled') AS IsPolyBaseInstalled; 
GO








-----
-- Enable/Disable Polybase - Only needs to be enabled on 2022, NOT needed in 2025
EXEC sp_configure 'polybase enabled', 0;

-- Enable INSERT into external table (2022 & 2025)
EXEC sp_configure 'allow polybase export', 1;
RECONFIGURE
GO

SELECT name, value_in_use, description
FROM sys.configurations
WHERE name IN ('allow polybase export', 'polybase enabled')
GO








----------------------------------------------------
-- Start Setup
----------------------------------------------------


-----
-- Create a Database Master Key
IF EXISTS(SELECT * FROM sys.symmetric_keys WHERE [name] = '##MS_DatabaseMasterKey##')
    DROP MASTER KEY
GO
CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'Give Sebastian a treat! He is a good boy!';
GO

-- symmetric key used to protect the private keys of certificates and 
-- asymmetric keys that are present in the database and secrets in 
-- database scoped credentials
-- Reference: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-master-key-transact-sql?view=sql-server-ver17#remarks
-----








-----
-- Create a Database Scoped Credential
IF EXISTS(SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'minio_local_db_cred')
    DROP DATABASE SCOPED CREDENTIAL minio_local_db_cred
GO
CREATE DATABASE SCOPED CREDENTIAL minio_local_db_cred
WITH 
    IDENTITY = 'S3 Access Key',     -- MUST be "S3 Access Key"
    SECRET = 'andy_local:andy_local';
GO

-- NOTE: Switch to MinIO console
-- Users:
-- Identity (Administrator) -> Users
--    * Policies -> consoleAdmin (aka root/sa)
--
-- Access Keys (User)
--    * Create Access Key
-----








-----
-- Create an External Data Source - MinIO
IF EXISTS(SELECT 1 FROM sys.external_data_sources WHERE name = 'minio_local')
    DROP EXTERNAL DATA SOURCE minio_local
CREATE EXTERNAL DATA SOURCE minio_local
WITH (
    PUSHDOWN = ON,
    LOCATION = 's3://192.168.150.129:9000',  -- s3://[bucket]@[address]:port
    CREDENTIAL = minio_local_db_cred
);
GO


/*
-- Want to use Azure Blob Storage?  
-- Here's a quick example
CREATE EXTERNAL DATA SOURCE my_azure_blob_storage
WITH (
    PUSHDOWN = ON,
    LOCATION = 'abs://container_name@storage_account_name.blob.core.windows.net',
    CREDENTIAL = my_azure_db_scoped_credential
);
GO
-- Reference: https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-data-source-transact-sql?view=sql-server-ver17&tabs=dedicated#location--prefixpathport-4
-----
*/








-----
-- Create Parquet External File Format 
IF EXISTS(SELECT 1 FROM sys.external_file_formats WHERE name = 'parquet_file_format_object')
    DROP EXTERNAL FILE FORMAT parquet_file_format_object;
GO
CREATE EXTERNAL FILE FORMAT parquet_file_format_object
WITH (
	FORMAT_TYPE = PARQUET
);
GO


-- Create CSV External File Format 
IF EXISTS(SELECT 1 FROM sys.external_file_formats WHERE name = 'CSV_text_file_format')
    DROP EXTERNAL FILE FORMAT CSV_text_file_format;
GO
CREATE EXTERNAL FILE FORMAT CSV_text_file_format 
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR =',',
        USE_TYPE_DEFAULT = TRUE
    )
)  
GO








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
FROM sys.external_data_sources;
GO

-----
-- Show current external file formats
SELECT *
FROM sys.external_file_formats;
GO








-----
-- We can now test plumbing!
-- Query via OPENROWSET() example
--
-- Show S3 Browser
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
) AS my_csv_file;
GO
