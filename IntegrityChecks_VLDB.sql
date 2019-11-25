/*
1. Create Table for holding tblBucket info
2. Create Database Snapshot
3. DBCC CHECKALLOC on all databases
4. DBCC CHECKCATALOG on all databases
5. DBCC CHECKTABLE on each table

Need to add:
Make sure CheckTable continues to run on same database
Add option for manual snapshots
Add Calculations for length of time to run and logic to then pick tables that will fit in that time frame

*/
SET NOCOUNT ON
GO
use master
go

DECLARE @TimeLimit int = 30 --in seconds, currently only for CheckTable
DECLARE @dbname sysname, @dbid int, @sqlcmd nvarchar(max)
DECLARE @JobStartTime datetime = GETDATE()

--DROP TABLE dbo.tblObjects

--Create persistant table to hold information
--Add other fields like Last Run Time, Duration
IF NOT EXISTS (SELECT 1 FROM sys.objects where object_id = OBJECT_ID(N'[dbo].[tblObjects]') and type in (N'U'))
CREATE TABLE dbo.tblObjects(
    [database_name] nvarchar(128),
    [dbid] int,
    [object_id] int,
    [name] sysname,
    [schema] sysname,
    [type] CHAR(2),
    type_desc NVARCHAR(60),
    used_page_count bigint,
    [StartTime] datetime,
    [EndTime] datetime,
    LastRunDuration_MS int,
    AvgRunDuration_MS int
)

--Declare temporary table variables to gather info
DECLARE @tblDBs TABLE (
    [name] sysname,
    [dbid] int,
    [isdone] bit
)
DECLARE @tblObj TABLE (
    [database_name] nvarchar(128),
    [dbid] int,
    [object_id] int,
    [name] sysname,
    [schema] sysname,
    [type] CHAR(2),
    [type_desc] NVARCHAR(60),
    [used_page_count] bigint
)


--Get names of databases and track for loop
--This is where you would adjust what databases you want to check
INSERT INTO @tblDBs (name, dbid, isdone)
SELECT name, database_id, 0 as isdone
FROM sys.databases
WHERE is_read_only = 0 --only databases that are READ_WRITE
AND state = 0 --only databases that are ONLINE
--AND database_id <> 2 --exclude tempdb
AND name = 'StackOverflow2010' --for testing

--loop through all databases gathered and get page count for each table in the database
WHILE (SELECT COUNT(name) FROM @tblDBs WHERE isdone = 0) > 0
BEGIN
    SET @dbname = ''
    SET @dbid = 0
    SET @sqlcmd = ''

    SELECT TOP 1 @dbname = name, @dbid = dbid from @tblDBs where isdone = 0

    --This is where you would adjust what tables you want to select
    SET @sqlcmd = 'SELECT ''' + @dbname + ''' as database_name, ' + CAST(@dbid as varchar) + ' as dbid, so.[object_id], so.[name], ss.name, so.[type], so.type_desc, SUM(sps.used_page_count) AS used_page_count
    FROM [' + @dbname + '].sys.objects so
    INNER JOIN [' + @dbname + '].sys.dm_db_partition_stats sps ON so.[object_id] = sps.[object_id]
    INNER JOIN [' + @dbname + '].sys.indexes si ON so.[object_id] = si.[object_id]
    INNER JOIN [' + @dbname + '].sys.schemas ss ON so.[schema_id] = ss.[schema_id] 
    WHERE so.[type] IN (''S'', ''U'', ''V'')
    GROUP BY so.[object_id], so.[name], ss.name, so.[type], so.type_desc'

    insert into @tblObj
    exec sp_executesql @sqlcmd
    --print @sqlcmd

    --update loop counter
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
      ,[dbid]
      ,[object_id]
      ,[name]
      ,[schema]
      ,[type]
      ,[type_desc]
      ,[used_page_count])
    VALUES (Source.[database_name]
      ,Source.[dbid]
      ,Source.[object_id]
      ,Source.[name]
      ,Source.[schema]
      ,Source.[type]
      ,Source.[type_desc]
      ,Source.[used_page_count])
WHEN NOT MATCHED BY SOURCE THEN
    DELETE
;

--select *
--from tblObjects


----------RUN CHECKALLOC AND CHECKCATALOG------------------------

--For Testing Only--------------
--DROP TABLE dbo.CommandsRun
IF NOT EXISTS (SELECT 1 FROM sys.objects where object_id = OBJECT_ID(N'[dbo].[CommandsRun]') and type in (N'U'))
CREATE TABLE dbo.CommandsRun(
    [command] nvarchar(max),
    [object] nvarchar(max)
)
TRUNCATE TABLE dbo.CommandsRun
----------------------------------

--Reset DB status for CheckAlloc and CheckCatalog
UPDATE @tblDBs
SET isdone = 0

--loop through all databases in @tblDBs and run CheckAlloc and CheckCatalog
WHILE (SELECT COUNT(name) from @tblDBs where isdone = 0) > 0
BEGIN
    SET @dbname = ''
    SET @dbid = 0
    SET @sqlcmd = ''

    SELECT TOP 1 @dbname = name, @dbid = dbid from @tblDBs where isdone = 0

    SET @sqlcmd = 'DBCC CHECKALLOC(' + CAST(@dbid as varchar) + ')'
    --EXEC sp_executesql @sqlcmd
    INSERT INTO dbo.CommandsRun (command, object)
    select @sqlcmd, @dbname

    SET @sqlcmd = 'DBCC CHECKCATALOG(' + CAST(@dbid as varchar) + ')'
    --EXEC sp_executesql @sqlcmd
    INSERT INTO dbo.CommandsRun (command, object)
    select @sqlcmd, @dbname

    --update loop counter
    UPDATE @tblDBs
    SET isdone = 1
    where name = @dbname
END

--select *
--from dbo.CommandsRun

----------------------------------------------------------------

--For Testing--------------
SET @JobStartTime = GETDATE()
SET @TimeLimit = 5
---------------------------

WHILE (GETDATE() < DATEADD(SS, @TimeLimit, @JobStartTime) OR @TimeLimit IS NULL)
BEGIN
    DECLARE @tablename sysname, @schemaname sysname
    SET @dbname = ''
    SET @schemaname = ''
    SET @tablename = ''
    SET @dbid = 0
    SET @sqlcmd = ''

    select top 1 @dbname = [database_name], @schemaname = [schema], @tablename = [name]
    from tblObjects
    where StartTime IS NULL or (StartTime = (select min(StartTime) from tblObjects) AND CAST(StartTime as date) <> CAST(@JobStartTime as date))
    
    --This will break the loop if all tables are done before the time limit
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    SET @sqlcmd = 'USE [' + @dbname + ']; DBCC CHECKTABLE (''' + @schemaname + '.' + @tablename + ''')'

    UPDATE dbo.tblObjects
    SET StartTime = GETDATE()
    WHERE @dbname = [database_name] AND @schemaname = [schema] AND @tablename = [name]

    INSERT INTO dbo.CommandsRun (command, object)
    select @sqlcmd, QUOTENAME(@dbname) + '.' + QUOTENAME(@schemaname) + '.' + QUOTENAME(@tablename)
    EXEC sp_executesql @sqlcmd

    UPDATE dbo.tblObjects
    SET EndTime = GETDATE()
    WHERE @dbname = [database_name] AND @schemaname = [schema] AND @tablename = [name]

END

select *
from tblObjects

select *
from CommandsRun