/*
1. Create Table for holding tblBucket info
2. DBCC CHECKALLOC
3. Create Database Snapshot
4. DBCC CHECKCATALOG
5. DBCC CHECKTABLE on each table
*/

--Create persistant table to hold information
--Add other fields like Last Run Time, Duration
IF NOT EXISTS (SELECT 1 FROM sys.objects where object_id = OBJECT_ID(N'[dbo].[tblObjects]') and type in (N'U'))
CREATE TABLE tblObjects(
    [database_name] nvarchar(128),
    [object_id] int,
    [name] sysname,
    [schema] sysname,
    [type] CHAR(2),
    type_desc NVARCHAR(60),
    used_page_count bigint
)

--Declare temporary table variables to gather info
DECLARE @tblObj TABLE (
    [database_name] nvarchar(128),
    [object_id] int,
    [name] sysname,
    [schema] sysname,
    [type] CHAR(2),
    [type_desc] NVARCHAR(60),
    [used_page_count] bigint
)
DECLARE @tblDBs TABLE (
    [name] sysname,
    [isdone] bit
)

--Get names of databases and track for loop
INSERT INTO @tblDBs (name, isdone)
select name, 0 as isdone
from sys.databases
where is_read_only = 0 --only databases that are READ_WRITE
and state = 0 --only databases that are ONLINE
and database_id <> 2 --exclude tempdb

--loop through all databases gathered and get page count for each table in the database
WHILE (SELECT COUNT(name) from @tblDBs where isdone = 0) > 0
BEGIN
    DECLARE @sqlcmd nvarchar(max) = ''
    DECLARE @dbname nvarchar (128) = (SELECT TOP 1 name from @tblDBs where isdone = 0)
    SET @sqlcmd = 'SELECT ''' + @dbname + ''' as dbname, so.[object_id], so.[name], ss.name, so.[type], so.type_desc, SUM(sps.used_page_count) AS used_page_count
    FROM [' + @dbname + '].sys.objects so
    INNER JOIN [' + @dbname + '].sys.dm_db_partition_stats sps ON so.[object_id] = sps.[object_id]
    INNER JOIN [' + @dbname + '].sys.indexes si ON so.[object_id] = si.[object_id]
    INNER JOIN [' + @dbname + '].sys.schemas ss ON so.[schema_id] = ss.[schema_id] 
    WHERE so.[type] IN (''S'', ''U'', ''V'')
    GROUP BY so.[object_id], so.[name], ss.name, so.[type], so.type_desc'

    insert into @tblObj
    exec sp_executesql @sqlcmd
    --print @sqlcmd

    UPDATE @tblDBs
    SET isdone = 1
    where name = @dbname
END

--select *
--from @tblObj

--Merge into persistent table
--Match on database name, schema name, and table name
--When Match and page count is different, update page count in persistent table
--when not found in persistent table, insert
--when found in persistent table but not in source, then delete from persistent table
MERGE master.dbo.tblObjects as [Target]
USING (SELECT * FROM @tblObj) as [Source]
ON (Target.database_name = Source.database_name AND Target.[schema] = Source.[schema] AND Target.name = Source.name)
WHEN MATCHED AND Target.used_page_count <> source.used_page_count THEN
    UPDATE SET Target.used_page_count = source.used_page_count
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([database_name]
      ,[object_id]
      ,[name]
      ,[schema]
      ,[type]
      ,[type_desc]
      ,[used_page_count])
    VALUES (Source.[database_name]
      ,Source.[object_id]
      ,Source.[name]
      ,Source.[schema]
      ,Source.[type]
      ,Source.[type_desc]
      ,Source.[used_page_count])
WHEN NOT MATCHED BY SOURCE THEN
    DELETE
;

select *
from tblObjects