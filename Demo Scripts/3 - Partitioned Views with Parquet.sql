/*-----------------------------------------------------------------------------
-- 3 - Partitioned Views with Parquet.sql
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
-- Data distribution by year published 
SELECT 
    YEAR(DatePublished) AS year_published, 
    COUNT(1) AS num_recipes
FROM dbo.recipes_reviews_flat
GROUP BY YEAR(DatePublished)
ORDER BY YEAR(DatePublished)
GO



-----
-- Data distribution by recipe category
SELECT TOP 20
    CategoryName, 
    COUNT(1) AS num_recipes
FROM dbo.recipes_reviews_flat
GROUP BY CategoryName
ORDER BY COUNT(1) DESC
GO


-----
-- Data distribution by year published & recipe category
SELECT 
    YEAR(DatePublished) AS year_published, 
    CategoryName, 
    COUNT(1) AS num_recipes
FROM dbo.recipes_reviews_flat
GROUP BY YEAR(DatePublished), CategoryName
ORDER BY YEAR(DatePublished), CategoryName
GO



SET STATISTICS TIME ON
SET STATISTICS IO ON

/*
ALTER TABLE dbo.recipes_reviews_flat
    ADD recipe_year_published AS (YEAR(DatePublished));
GO
*/





-----
-- cetas_minio.recipes_reviews_flat_2018
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2018')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2018
GO
CREATE EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2018
WITH (
    LOCATION = 'recipes-reviews-flat/recipe_year_published=2018/',    -- bucket name/prefix/
    DATA_SOURCE = minio_local,  
    FILE_FORMAT = parquet_file_format_object
) AS 
SELECT
    recipes_reviews_flat_ID, RecipeID, 
    RecipeName COLLATE Latin1_General_100_BIN2_UTF8 AS RecipeName, 
    DatePublished, 
    CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
    AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
    AuthorFirstName,      -- COLLATE omitted for demo
    AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
    Ingredients COLLATE Latin1_General_100_BIN2_UTF8 AS Ingredients, 
    Keywords COLLATE Latin1_General_100_BIN2_UTF8 AS Keywords, 
    Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, Carbohydrate_g, Fiber_g, 
    Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, CategoryID, 
    RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, 
    ReviewText COLLATE Latin1_General_100_BIN2_UTF8 AS ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM dbo.recipes_reviews_flat
WHERE recipe_year_published = 2018
GO


-----
-- cetas_minio.recipes_reviews_flat_2019
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2019')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2019
GO
CREATE EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2019
WITH (
    LOCATION = 'recipes-reviews-flat/recipe_year_published=2019/',    -- bucket name/prefix/
    DATA_SOURCE = minio_local,  
    FILE_FORMAT = parquet_file_format_object
) AS 
SELECT
    recipes_reviews_flat_ID, RecipeID, 
    RecipeName COLLATE Latin1_General_100_BIN2_UTF8 AS RecipeName, 
    DatePublished, 
    CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
    AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
    AuthorFirstName,      -- COLLATE omitted for demo
    AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
    Ingredients COLLATE Latin1_General_100_BIN2_UTF8 AS Ingredients, 
    Keywords COLLATE Latin1_General_100_BIN2_UTF8 AS Keywords, 
    Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, Carbohydrate_g, Fiber_g, 
    Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, CategoryID, 
    RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, 
    ReviewText COLLATE Latin1_General_100_BIN2_UTF8 AS ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM dbo.recipes_reviews_flat
WHERE recipe_year_published = 2019
GO


-----
-- cetas_minio.recipes_reviews_flat_2020
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2020')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2020
GO
CREATE EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2020
WITH (
    LOCATION = 'recipes-reviews-flat/recipe_year_published=2020/',    -- bucket name/prefix/
    DATA_SOURCE = minio_local,  
    FILE_FORMAT = parquet_file_format_object
) AS 
SELECT
    recipes_reviews_flat_ID, RecipeID, 
    RecipeName COLLATE Latin1_General_100_BIN2_UTF8 AS RecipeName, 
    DatePublished, 
    CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
    AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
    AuthorFirstName,      -- COLLATE omitted for demo
    AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
    Ingredients COLLATE Latin1_General_100_BIN2_UTF8 AS Ingredients, 
    Keywords COLLATE Latin1_General_100_BIN2_UTF8 AS Keywords, 
    Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, Carbohydrate_g, Fiber_g, 
    Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, CategoryID, 
    RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, 
    ReviewText COLLATE Latin1_General_100_BIN2_UTF8 AS ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM dbo.recipes_reviews_flat
WHERE recipe_year_published = 2020
GO


-----
-- cetas_minio.recipes_reviews_flat_2021
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2021')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2021
GO
CREATE EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2021
WITH (
    LOCATION = 'recipes-reviews-flat/recipe_year_published=2021/',    -- bucket name/prefix/
    DATA_SOURCE = minio_local,  
    FILE_FORMAT = parquet_file_format_object
) AS 
SELECT
    recipes_reviews_flat_ID, RecipeID, 
    RecipeName COLLATE Latin1_General_100_BIN2_UTF8 AS RecipeName, 
    DatePublished, 
    CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
    AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
    AuthorFirstName,      -- COLLATE omitted for demo
    AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
    Ingredients COLLATE Latin1_General_100_BIN2_UTF8 AS Ingredients, 
    Keywords COLLATE Latin1_General_100_BIN2_UTF8 AS Keywords, 
    Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, Carbohydrate_g, Fiber_g, 
    Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, CategoryID, 
    RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, 
    ReviewText COLLATE Latin1_General_100_BIN2_UTF8 AS ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM dbo.recipes_reviews_flat
WHERE recipe_year_published = 2021
GO


-----
-- cetas_minio.recipes_reviews_flat_2022
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2022')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2022
GO
CREATE EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2022
WITH (
    LOCATION = 'recipes-reviews-flat/recipe_year_published=2022/',    -- bucket name/prefix/
    DATA_SOURCE = minio_local,  
    FILE_FORMAT = parquet_file_format_object
) AS 
SELECT
    recipes_reviews_flat_ID, RecipeID, 
    RecipeName COLLATE Latin1_General_100_BIN2_UTF8 AS RecipeName, 
    DatePublished, 
    CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
    AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
    AuthorFirstName,      -- COLLATE omitted for demo
    AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
    Ingredients COLLATE Latin1_General_100_BIN2_UTF8 AS Ingredients, 
    Keywords COLLATE Latin1_General_100_BIN2_UTF8 AS Keywords, 
    Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, Carbohydrate_g, Fiber_g, 
    Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, CategoryID, 
    RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, 
    ReviewText COLLATE Latin1_General_100_BIN2_UTF8 AS ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM dbo.recipes_reviews_flat
WHERE recipe_year_published = 2022
GO


-----
-- cetas_minio.recipes_reviews_flat_2023
IF EXISTS(SELECT 1 FROM sys.external_tables WHERE name = 'recipes_reviews_flat_2023')
    DROP EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2023
GO
CREATE EXTERNAL TABLE cetas_minio.recipes_reviews_flat_2023
WITH (
    LOCATION = 'recipes-reviews-flat/recipe_year_published=2023/',    -- bucket name/prefix/
    DATA_SOURCE = minio_local,  
    FILE_FORMAT = parquet_file_format_object
) AS 
SELECT
    recipes_reviews_flat_ID, RecipeID, 
    RecipeName COLLATE Latin1_General_100_BIN2_UTF8 AS RecipeName, 
    DatePublished, 
    CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
    AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
    AuthorFirstName,      -- COLLATE omitted for demo
    AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
    Ingredients COLLATE Latin1_General_100_BIN2_UTF8 AS Ingredients, 
    Keywords COLLATE Latin1_General_100_BIN2_UTF8 AS Keywords, 
    Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, Carbohydrate_g, Fiber_g, 
    Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, CategoryID, 
    RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, 
    ReviewText COLLATE Latin1_General_100_BIN2_UTF8 AS ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM dbo.recipes_reviews_flat
WHERE recipe_year_published = 2023
GO


-----
-- cetas_minio.recipes_reviews_flat_2024_current
IF EXISTS(SELECT 1 FROM sys.objects WHERE name = 'recipes_reviews_flat_2024_current')
    DROP TABLE cetas_minio.recipes_reviews_flat_2024_current
GO
CREATE TABLE cetas_minio.recipes_reviews_flat_2024_current (
	recipes_reviews_flat_ID INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED,
	RecipeID int NOT NULL, RecipeName varchar(250) NULL, DatePublished datetime2(7) NULL,
	CategoryName varchar(250) NULL, AuthorAlias varchar(250) NULL, AuthorFirstName varchar(250) NULL,
	AuthorLastName varchar(250) NULL, Ingredients nvarchar(4000) NULL, Keywords nvarchar(4000) NULL,
	Calories decimal(10, 2) NULL, Fat_g decimal(10, 2) NULL, SaturatedFat_g decimal(10, 2) NULL,
	Cholesterol_mg decimal(10, 2) NULL, Sodium_mg decimal(10, 2) NULL, Carbohydrate_g decimal(10, 2) NULL,
	Fiber_g decimal(10, 2) NULL, Sugar_g decimal(10, 2) NULL, Protein_g decimal(10, 2) NULL,
	CookTimeMin int NULL, PrepTimeMin int NULL, TotalTimeMin int NULL, RecipeServings smallint NULL,
	CategoryID int NULL, RecipeAuthorID int NULL, ReviewID int NULL, ReviewAuthorID int NULL,
	Rating tinyint NULL, DateSubmitted datetime NULL, DateModified datetime NULL,
	ReviewText nvarchar(4000) NULL, Approved bit  NULL, HelpfulScore smallint NULL,
    recipe_year_published AS (YEAR(DatePublished))
) ON Flat_FG
GO
ALTER TABLE cetas_minio.recipes_reviews_flat_2024_current ADD  DEFAULT ((0)) FOR Approved
GO
ALTER TABLE cetas_minio.recipes_reviews_flat_2024_current ADD  DEFAULT ((0)) FOR HelpfulScore
GO


SET IDENTITY_INSERT cetas_minio.recipes_reviews_flat_2024_current ON

INSERT INTO cetas_minio.recipes_reviews_flat_2024_current (
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore
)
SELECT
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore
FROM dbo.recipes_reviews_flat
WHERE recipe_year_published >= 2024

SET IDENTITY_INSERT cetas_minio.recipes_reviews_flat_2024_current OFF
GO








-----
-- Create a "partitioned view" the four 
-- external tables plus local table
CREATE OR ALTER VIEW cetas_minio.pv_recipes_reviews_flat
/* WITH SCHEMABINDING */
AS
SELECT
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM cetas_minio.recipes_reviews_flat_2018
UNION ALL
SELECT
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM cetas_minio.recipes_reviews_flat_2019
UNION ALL
SELECT
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM cetas_minio.recipes_reviews_flat_2020
UNION ALL
SELECT
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM cetas_minio.recipes_reviews_flat_2021
UNION ALL
SELECT
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM cetas_minio.recipes_reviews_flat_2022
UNION ALL
SELECT
    recipes_reviews_flat_ID, RecipeID, RecipeName, DatePublished, CategoryName, AuthorAlias, AuthorFirstName, 
    AuthorLastName, Ingredients, Keywords, Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, 
    Carbohydrate_g, Fiber_g, Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, 
    CategoryID, RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM cetas_minio.recipes_reviews_flat_2023
UNION ALL
SELECT
    recipes_reviews_flat_ID, RecipeID, 
    RecipeName COLLATE Latin1_General_100_BIN2_UTF8 AS RecipeName, 
    DatePublished, 
    CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
    AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
    AuthorFirstName AS AuthorFirstName,     -- COLLATE omitted for demo
    AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
    Ingredients COLLATE Latin1_General_100_BIN2_UTF8 AS Ingredients, 
    Keywords COLLATE Latin1_General_100_BIN2_UTF8 AS Keywords, 
    Calories, Fat_g, SaturatedFat_g, Cholesterol_mg, Sodium_mg, Carbohydrate_g, Fiber_g, 
    Sugar_g, Protein_g, CookTimeMin, PrepTimeMin, TotalTimeMin, RecipeServings, CategoryID, 
    RecipeAuthorID, ReviewID, ReviewAuthorID, Rating, DateSubmitted, DateModified, 
    ReviewText COLLATE Latin1_General_100_BIN2_UTF8 AS ReviewText, 
    Approved, HelpfulScore, recipe_year_published
FROM cetas_minio.recipes_reviews_flat_2024_current;
GO




---------------------
-- Three Questions
---------------------

-----
-- 1. Why did you comment out WITH SCHEMABINDING?  
--
--    /* WITH SCHEMABINDING */
--    Isn't that required?
--








-----
-- 2. What's up with your LOCATION values?
--
--    LOCATION = 'recipes-reviews-flat/recipe_year_published=2021/'
--    LOCATION = 'recipes-reviews-flat/recipe_year_published=2022/'
--    LOCATION = 'recipes-reviews-flat/recipe_year_published=2023/'
--








-----
-- Hive-Style Partitioning via prefixes
-- Enabled file elimination
/*
recipes-reviews-flat/
   |__ recipe_year_published=2021/
       |__ parquet_file_01.parquet
       |__ parquet_file_02.parquet

   |__ recipe_year_published=2022/
       |__ parquet_file_01.parquet

   |__ recipe_year_published=2023/
       |__ parquet_file_01.parquet
       |__ parquet_file_02.parquet
       |__ parquet_file_03.parquet




LOCATION = 'recipes-reviews-flat/recipe_year_published=2023/recipe_category=desserts/'

recipes-reviews-flat/
   |__ recipe_year_published=2021/
       |__ recipe_category=desserts/
           |__ parquet_file_01.parquet
           |__ parquet_file_02.parquet
       |__ recipe_category=appetizers/
           |__ parquet_file_01.parquet
           |__ parquet_file_02.parquet
       |__ recipe_category=roasts/
           |__ parquet_file_01.parquet
           |__ parquet_file_02.parquet

*/








-----
-- 3. Why did you COLLATE your text columns?
--
-- CategoryName COLLATE Latin1_General_100_BIN2_UTF8 AS CategoryName, 
-- AuthorAlias COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorAlias, 
--
-- AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8 AS AuthorLastName, 
-- AuthorFirstName AS AuthorFirstName,     -- COLLATE omitted for demo
-----








-- Get a few names real quick
SELECT DISTINCT TOP 5 AuthorFirstName, AuthorLastName
FROM dbo.recipes_reviews_flat;
GO




-----
-- First query the non-UTF column: AuthorFirstName
SELECT 'non-UTF-8 column, Unicode (UTF-16) predicate', CategoryName, COUNT(1)
FROM cetas_minio.pv_recipes_reviews_flat
WHERE AuthorFirstName = N'Marcus'   -- UNICODE
    AND DatePublished BETWEEN '2019-04-01' AND '2021-06-15'
GROUP BY CategoryName

SELECT 'non-UTF-8 column, ASCII predicate', CategoryName, COUNT(1)
FROM cetas_minio.pv_recipes_reviews_flat
WHERE AuthorFirstName = 'Marcus'   -- ASCII
    AND DatePublished BETWEEN '2019-04-01' AND '2021-06-15'
GROUP BY CategoryName
GO


-- View Messages tab
-- How many appeared?
-----








-----
-- Repeat with AuthorLastName COLLATE Latin1_General_100_BIN2_UTF8
-- Turn on Actual Execution Plan: Ctrl-M
-- Check Messages tab & Exec Plan
SELECT 'UTF-8 COLLATE''d column, Unicode (UTF-16) predicate', CategoryName, COUNT(1)
FROM cetas_minio.pv_recipes_reviews_flat
WHERE AuthorLastName = N'Field'   -- UNICODE
    AND DatePublished BETWEEN '2019-04-01' AND '2021-06-15'
GROUP BY CategoryName


SELECT 'UTF-8 COLLATE''d column, ASCII predicate', CategoryName, COUNT(1)
FROM cetas_minio.pv_recipes_reviews_flat
WHERE AuthorLastName = 'Field'  -- ASCII
    AND DatePublished BETWEEN '2019-04-01' AND '2021-06-15'
GROUP BY CategoryName
GO


-- Check:
--    * Exec Plan - Number of Batches
--    * What's up with the COMPUTE SCALAR operation?
--    * Operation execution times: Did Unicode take longer than ASCII?
--
--    * F4 - Properties Pane -> Defined Values
-----








-----
-- Data Type Choices MATTER!
-- 
-- 
-- UTF-8 is enabled in existing the data types CHAR and VARCHAR. 
-- String data is automatically encoded to UTF-8 when creating or 
-- changing an object’s collation to a collation with the “_UTF8” suffix
--
-- NCHAR and NVARCHAR remains unchanged and allows UCS-2/UTF-16 encoding.
-- Reference: 
--    https://techcommunity.microsoft.com/blog/sqlserver/introducing-utf-8-support-for-sql-server/734928
--    https://learn.microsoft.com/en-us/sql/relational-databases/collations/collation-and-unicode-support?view=sql-server-ver16#utf-8-support
-----








-----
-- What about other datatypes?
-- Date as a DATETIME vs DATETIME2
DECLARE 
    @StartDate  DATETIME  = '2019-04-01', 
    @StartDate2 DATETIME2 = '2019-04-01', 
    @EndDate    DATETIME  = '2019-06-15',
    @EndDate2   DATETIME2 = '2019-06-15';

SELECT 'DATETIME string predicate', CategoryName, COUNT(1)
FROM cetas_minio.pv_recipes_reviews_flat
WHERE DatePublished BETWEEN @StartDate AND @EndDate
GROUP BY CategoryName


SELECT 'DATETIME2 string predicate', CategoryName, COUNT(1)
FROM cetas_minio.pv_recipes_reviews_flat
WHERE DatePublished BETWEEN @StartDate2 AND @EndDate2
GROUP BY CategoryName
GO








-----
-- Why?
-- Your datatype choices matter!


--
-- sp_describe_first_result_set
-- Go to S3 Browser and get one of our Parquet file names
EXEC sp_describe_first_result_set N'
	SELECT TOP 0 * 
	FROM OPENROWSET (
	BULK ''/recipes-reviews-flat/recipe_year_published=2023/xxxx.parquet'',
	FORMAT = ''PARQUET'',
	DATA_SOURCE = ''minio_local''
) AS data';
GO








-----
-- Can also interrogate a parquet file from originating from
-- somewhere else like kaggle.com datasets
EXEC sp_describe_first_result_set N'
	SELECT TOP 0 * 
	FROM OPENROWSET (
	BULK ''/helloworld/food.com/reviews.parquet'',
	FORMAT = ''PARQUET'',
	DATA_SOURCE = ''minio_local''
) AS data';
GO


SELECT TOP 100 *
FROM OPENROWSET (
	BULK '/helloworld/food.com/reviews.parquet',
	FORMAT = 'PARQUET',
	DATA_SOURCE = 'minio_local'
)  AS reviews_parquet
GO


