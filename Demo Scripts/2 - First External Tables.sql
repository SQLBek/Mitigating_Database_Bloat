/*-----------------------------------------------------------------------------
-- 2 - First External tables.sql
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
-- Let's create our first external table
-- First a CSV example
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'test_CSV')
    DROP EXTERNAL TABLE cetas_minio.test_CSV
GO
CREATE EXTERNAL TABLE cetas_minio.test_CSV (  
    MyValue VARCHAR(50),
    AnotherValue VARCHAR(50),
    FooBar VARCHAR(50)
)  
WITH (LOCATION='/helloworld/testCSV.csv',
      DATA_SOURCE = minio_local,  
      FILE_FORMAT = CSV_text_file_format  
);  
GO








-----
-- Query via External Table example
SELECT *
FROM cetas_minio.test_CSV
GO








-----
-- Let's take a look at our largest set of test data
SELECT COUNT(1) AS num_rows
FROM dbo.recipes_reviews_flat
GO

SELECT TOP 100 *
FROM dbo.recipes_reviews_flat
GO

EXEC sp_help 'dbo.recipes_reviews_flat';
GO








-----
-- Create our first Parquet based external table 
-- using 'Create External Table As Select' (CETAS)
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_pq')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_pq
GO
CREATE EXTERNAL TABLE cetas_minio.recipes_reviews_flat_pq
WITH (
    LOCATION = 'helloworld/my_first_parquet/',    -- bucket name/prefix/
    DATA_SOURCE = minio_local,  
    FILE_FORMAT = parquet_file_format_object
) AS 
SELECT
    [recipes_reviews_flat_ID], [RecipeID], [RecipeName], [DatePublished], [CategoryName], [AuthorAlias], [AuthorFirstName], 
    [AuthorLastName], [Ingredients], [Keywords], [Calories], [Fat_g], [SaturatedFat_g], [Cholesterol_mg], [Sodium_mg], 
    [Carbohydrate_g], [Fiber_g], [Sugar_g], [Protein_g], [CookTimeMin], [PrepTimeMin], [TotalTimeMin], [RecipeServings], 
    [CategoryID], [RecipeAuthorID], [ReviewID], [ReviewAuthorID], [Rating], [DateSubmitted], [DateModified], 
    [ReviewText], [Approved], [HelpfulScore]
FROM dbo.recipes_reviews_flat
GO








-----
-- Query TOP 1000 *
SELECT TOP 1000 *
FROM cetas_minio.recipes_reviews_flat_pq;
GO








-----
-- Give me a Predicate!
SELECT TOP 1000 
    RecipeName, DatePublished
FROM cetas_minio.recipes_reviews_flat_pq
WHERE DatePublished BETWEEN '2021-01-01' AND '2022-01-01'
GO








-----
-- What about a wildcard keyword predicate?
SELECT TOP 1000 
    RecipeName, DatePublished, Keywords
FROM cetas_minio.recipes_reviews_flat_pq
WHERE DatePublished BETWEEN '2021-01-01' AND '2022-01-01'
    AND Keywords LIKE '%Breads%'
GO








-----
-- What about a wildcard keyword predicate 
-- with aggregates over everything?
SELECT 
    'Bread Recipes', 
    COUNT(1) AS num_recipes,
    AVG(Calories) AS avg_calories
FROM cetas_minio.recipes_reviews_flat_pq
WHERE Keywords LIKE '%Breads%'
GO








-----
-- How much space did the source table consume?
SELECT
    objects.name AS table_name,
    indexes.type_desc,
    partitions.data_compression_desc,
    FORMAT((SUM(allocation_units.used_pages) * 8.0) / 1024, 'N2') AS used_space_MB,
    FORMAT(partitions.rows, 'N0') AS row_count
FROM sys.objects
INNER JOIN sys.indexes 
    ON objects.object_id = indexes.object_id
INNER JOIN sys.partitions 
    ON indexes.object_id = partitions.object_id 
    AND indexes.index_id = partitions.index_id
INNER JOIN sys.allocation_units 
    ON partitions.partition_id = allocation_units.container_id
WHERE
    objects.type = 'U'
    AND objects.object_id = OBJECT_ID(N'dbo.recipes_reviews_flat')
    AND indexes.index_id = 1
GROUP BY 
    objects.name,
    indexes.type_desc,
    partitions.rows,
    partitions.data_compression_desc;


-- Check S3 Browser
-- /helloworld/my_first_parquet/
-----
