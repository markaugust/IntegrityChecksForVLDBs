/*
1. Create Table for holding tblBucket info
2. DBCC CHECKALLOC
3. Create Database Snapshot
4. DBCC CHECKCATALOG
5. DBCC CHECKTABLE on each table
*/


--Populate/UPSERT Table with Table Names and used page count from all databases
--Likely #tblObj should persist
IF OBJECT_ID('tempdb..#tblDBs') IS NOT NULL
DROP TABLE #tblDBs
IF OBJECT_ID('tempdb..#tblObj') IS NOT NULL
DROP TABLE #tblObj

--Add other fields like Last Run Time, Duration
CREATE TABLE #tblObj(
    [database_name] nvarchar(128),
    [object_id] int,
    [name] sysname,
    [schema] sysname,
    [type] CHAR(2),
    type_desc NVARCHAR(60),
    used_page_count bigint
)

select name, 0 as isdone
INTO #tblDBs
from sys.databases
where is_read_only = 0
and state = 0
and database_id <> 2

WHILE (SELECT COUNT(name) from #tblDBs where isdone = 0) > 0
BEGIN
    DECLARE @sqlcmd nvarchar(max) = ''
    DECLARE @dbname nvarchar (128) = (SELECT TOP 1 name from #tblDBs where isdone = 0)
    SET @sqlcmd = 'SELECT ''' + @dbname + ''' as dbname, so.[object_id], so.[name], ss.name, so.[type], so.type_desc, SUM(sps.used_page_count) AS used_page_count
    FROM [' + @dbname + '].sys.objects so
    INNER JOIN [' + @dbname + '].sys.dm_db_partition_stats sps ON so.[object_id] = sps.[object_id]
    INNER JOIN [' + @dbname + '].sys.indexes si ON so.[object_id] = si.[object_id]
    INNER JOIN [' + @dbname + '].sys.schemas ss ON so.[schema_id] = ss.[schema_id] 
    WHERE so.[type] IN (''S'', ''U'', ''V'')
    GROUP BY so.[object_id], so.[name], ss.name, so.[type], so.type_desc'

    insert into #tblObj
    exec sp_executesql @sqlcmd
    --print @sqlcmd

    UPDATE #tblDBs
    SET isdone = 1
    where name = @dbname
END

select *
from #tblObj