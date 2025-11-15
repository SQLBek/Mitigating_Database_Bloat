/*-------------------------------------------------------------------
-- 9 - Space Used Comparisons.sql
-- 
-------------------------------------------------------------------*/
USE CookbookDemo_Bloat
GO



-----
-- List all objects used in this presentation and corresponding
-- used space on disk
SELECT
    schemas.name AS schema_name,
    objects.name AS table_name,
    indexes.name AS index_name,
    indexes.type_desc,
    partitions.data_compression_desc,
    FORMAT((SUM(allocation_units.used_pages) * 8.0) / 1024, 'N2') AS used_space_MB,
    FORMAT(partitions.rows, 'N0') AS row_count
FROM sys.objects
INNER JOIN sys.schemas 
    ON objects.schema_id = schemas.schema_id
INNER JOIN sys.indexes 
    ON objects.object_id = indexes.object_id
INNER JOIN sys.partitions 
    ON indexes.object_id = partitions.object_id 
    AND indexes.index_id = partitions.index_id
INNER JOIN sys.allocation_units 
    ON partitions.partition_id = allocation_units.container_id
WHERE
    objects.type = 'U'
    AND allocation_units.used_pages > 0
GROUP BY 
    schemas.name,
    objects.name,
    indexes.name,
    indexes.type_desc,
    partitions.rows,
    partitions.data_compression_desc
ORDER BY
    schemas.name,
    objects.name,
    indexes.type_desc,
    indexes.name



-----
-- Total Space Used, per Clustered Index only
SELECT
    schemas.name AS schema_name,
    objects.name AS table_name,
    indexes.type_desc,
    partitions.data_compression_desc,
    FORMAT((SUM(allocation_units.used_pages) * 8.0) / 1024, 'N2') AS used_space_MB,
    FORMAT(partitions.rows, 'N0') AS row_count
FROM sys.objects
INNER JOIN sys.schemas 
    ON objects.schema_id = schemas.schema_id
INNER JOIN sys.indexes 
    ON objects.object_id = indexes.object_id
INNER JOIN sys.partitions 
    ON indexes.object_id = partitions.object_id 
    AND indexes.index_id = partitions.index_id
INNER JOIN sys.allocation_units 
    ON partitions.partition_id = allocation_units.container_id
WHERE
    objects.type = 'U'
    AND allocation_units.used_pages > 0
    AND indexes.type_desc IN ('CLUSTERED', 'CLUSTERED COLUMNSTORE')
GROUP BY 
    schemas.name,
    objects.name,
    indexes.type_desc,
    partitions.rows,
    partitions.data_compression_desc
ORDER BY
    schemas.name,
    objects.name,
    indexes.type_desc;


-----
-- Summary: Total Space Used by Compression Type
-- Clustered Index only
WITH used_space_CTE AS (
    SELECT
        schemas.name AS schema_name,
        -- indexes.type_desc,
        -- partitions.data_compression_desc,
        CASE 
            WHEN data_compression_desc = 'NONE' THEN 'NONE'
            WHEN data_compression_desc = 'ROW' THEN 'ROW'
            WHEN data_compression_desc = 'PAGE' THEN 'PAGE'
            WHEN data_compression_desc = 'COLUMNSTORE' THEN 'COLUMNSTORE'
            WHEN data_compression_desc = 'COLUMNSTORE_ARCHIVE' THEN 'COLUMNSTORE'
        END AS compression,
        (allocation_units.used_pages * 8.0) / 1024 AS used_space_MB,
        partitions.rows AS row_count,
        CASE 
            WHEN data_compression_desc = 'NONE' THEN 0
            WHEN data_compression_desc = 'ROW' THEN 1
            WHEN data_compression_desc = 'PAGE' THEN 2
            WHEN data_compression_desc = 'COLUMNSTORE' THEN 3
            WHEN data_compression_desc = 'COLUMNSTORE_ARCHIVE' THEN 3
        END AS sort_order

    FROM sys.objects
    INNER JOIN sys.schemas 
        ON objects.schema_id = schemas.schema_id
    INNER JOIN sys.indexes 
        ON objects.object_id = indexes.object_id
    INNER JOIN sys.partitions 
        ON indexes.object_id = partitions.object_id 
        AND indexes.index_id = partitions.index_id
    INNER JOIN sys.allocation_units 
        ON partitions.partition_id = allocation_units.container_id
    WHERE
        objects.type = 'U'
        AND allocation_units.used_pages > 0
        AND indexes.type_desc IN ('CLUSTERED', 'CLUSTERED COLUMNSTORE')
)
SELECT 
    schema_name,
    compression,
    FORMAT(SUM(used_space_MB), 'N2') AS used_space_MB,
    FORMAT(SUM(row_count), 'N0') AS row_count
FROM used_space_CTE
GROUP BY 
    sort_order, 
    schema_name,
    compression
ORDER BY 
    sort_order, 
    schema_name
