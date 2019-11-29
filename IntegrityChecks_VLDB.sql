/*
1. Create Table for holding tblBucket info
2. Create Database Snapshot
3. DBCC CHECKALLOC on all databases
4. DBCC CHECKCATALOG on all databases
5. DBCC CHECKTABLE on each table

Need to add:
Make sure CheckTable continues to run on same database (done)
Add option for manual snapshots
Add Calculations for length of time to run and logic to then pick tables that will fit in that time frame (done)
Add Calculations for average pages/sec (optional, maybe not needed)

*/
SET NOCOUNT ON
GO
use master
go

DECLARE @TimeLimit int = NULL --in seconds, currently only for CheckTable

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
    [StartTime] datetime DEFAULT '1900-01-01 00:00:00.000',
    [EndTime] datetime,
    [LastRunDuration_MS] AS DATEDIFF(ms, StartTime, EndTime),
    [NumberOfExecutions] int DEFAULT 0,
    [AvgRunDuration_MS] int DEFAULT 0
)

DECLARE @JobStartTime datetime = GETDATE()
DECLARE @JobEndTime datetime = DATEADD(SS, @TimeLimit, @JobStartTime)
DECLARE @dbname sysname, @dbid int, @tablename sysname, @schemaname sysname, @sqlcmd nvarchar(max)

--Declare table variables to gather info
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
DECLARE @checkTableDbOrder TABLE (
    [name] sysname,
    [dbid] int,
    [MinStartTime] datetime,
    [isDone] bit
)

--Get names of databases and track for loop
--This is where you would adjust what databases you want to check
INSERT INTO @tblDBs (name, dbid, isdone)
SELECT name, database_id, 0 as isdone
FROM sys.databases
WHERE is_read_only = 0 --only databases that are READ_WRITE
AND state = 0 --only databases that are ONLINE
AND database_id <> 2 --exclude tempdb
AND name IN ('StackOverflow2010','AdventureWorks2017') --for testing

--loop through all databases gathered and get page count for each table in the database
WHILE 1=1
BEGIN
    SELECT TOP 1 @dbname = name, @dbid = dbid FROM @tblDBs WHERE isdone = 0
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    --This query is derived and taken from the MS Tiger Scripts
    --This is where you would adjust what tables you want to select
    SET @sqlcmd = 'SELECT ''' + @dbname + ''' as database_name, ' + CAST(@dbid as varchar) + ' as dbid,
    so.[object_id], so.[name], ss.name, so.[type], so.type_desc, SUM(sps.used_page_count) AS used_page_count
    FROM [' + @dbname + '].sys.objects so
    INNER JOIN [' + @dbname + '].sys.dm_db_partition_stats sps ON so.[object_id] = sps.[object_id]
    INNER JOIN [' + @dbname + '].sys.indexes si ON so.[object_id] = si.[object_id]
    INNER JOIN [' + @dbname + '].sys.schemas ss ON so.[schema_id] = ss.[schema_id] 
    WHERE so.[type] IN (''S'', ''U'', ''V'')
    GROUP BY so.[object_id], so.[name], ss.name, so.[type], so.type_desc'

    INSERT INTO @tblObj
    EXEC sp_executesql @sqlcmd

    --update loop counter
    UPDATE @tblDBs
    SET isdone = 1
    WHERE name = @dbname
END

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
WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
BEGIN
    SELECT TOP 1 @dbname = name, @dbid = dbid from @tblDBs where isdone = 0
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    SET @sqlcmd = 'DBCC CHECKALLOC(' + CAST(@dbid as varchar) + ') WITH NO_INFOMSGS, ALL_ERRORMSGS'
    EXEC sp_executesql @sqlcmd
    INSERT INTO dbo.CommandsRun (command, object)
    SELECT @sqlcmd, @dbname

    SET @sqlcmd = 'DBCC CHECKCATALOG(' + CAST(@dbid as varchar) + ') WITH NO_INFOMSGS'
    EXEC sp_executesql @sqlcmd
    INSERT INTO dbo.CommandsRun (command, object)
    SELECT @sqlcmd, @dbname

    --update loop counter
    UPDATE @tblDBs
    SET isdone = 1
    where name = @dbname
END

----------RUN CHECKTABLE------------------------

INSERT INTO @checkTableDbOrder ([name], [dbid], [MinStartTime], [isDone])
SELECT [database_name], [dbid], min([StartTime]), 0
FROM tblObjects GROUP BY [database_name], [dbid]

WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
BEGIN
    --Ensures only 1 database is checked at a time, rather than randomly checking tables from random databases
    SELECT TOP 1 @dbname = [name] from @checkTableDbOrder WHERE isDone = 0 ORDER BY MinStartTime
    --This will break the loop if all databases are done before the time limit
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    --Run the CheckTable commands
    WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
    BEGIN
        SELECT TOP 1 @schemaname = [schema], @tablename = [name]
        FROM tblObjects
        WHERE @dbname = [database_name]
        AND StartTime = (SELECT MIN(StartTime) FROM tblObjects WHERE [database_name] = @dbname)  --Makes sure it's the oldest entry for that database
        AND CAST(StartTime as date) <> CAST(@JobStartTime as date)  --makes sure it's not the same day, as we don't need to run it again
        AND NumberOfExecutions = (SELECT MIN(NumberOfExecutions) FROM tblObjects)  --makes sure to distribute to other objects and databases
        AND (DATEADD(MS, AvgRunDuration_MS, GETDATE()) < @JobEndTime OR @TimeLimit IS NULL)  --makes sure it won't select an object that will surpass the end run time

        --This will break the loop if all tables are done before the time limit
        IF @@ROWCOUNT = 0
        BEGIN
            BREAK
        END

        SET @sqlcmd = 'USE [' + @dbname + ']; DBCC CHECKTABLE (''' + @schemaname + '.' + @tablename + ''') WITH NO_INFOMSGS, ALL_ERRORMSGS, DATA_PURITY'

        UPDATE dbo.tblObjects
        SET StartTime = GETDATE()
        WHERE @dbname = [database_name] AND @schemaname = [schema] AND @tablename = [name]

        --Log the command
        INSERT INTO dbo.CommandsRun (command, object)
        SELECT @sqlcmd, QUOTENAME(@dbname) + '.' + QUOTENAME(@schemaname) + '.' + QUOTENAME(@tablename)
        --Execute the command
        EXEC sp_executesql @sqlcmd

        --Update End Time
        UPDATE dbo.tblObjects
        SET EndTime = GETDATE(), [NumberOfExecutions] = [NumberOfExecutions] + 1
        WHERE @dbname = [database_name] AND @schemaname = [schema] AND @tablename = [name]

        --Calculate new Average Runtime
        UPDATE dbo.tblObjects
        SET [AvgRunDuration_MS] = [AvgRunDuration_MS] + ([LastRunDuration_MS] - [AvgRunDuration_MS]) / [NumberOfExecutions]
        WHERE @dbname = [database_name] AND @schemaname = [schema] AND @tablename = [name]

    END

    UPDATE @checkTableDbOrder
    SET isDone = 1
    WHERE name = @dbname
END


select *
from tblObjects
order by StartTime desc

select *
from CommandsRun
order by object

--select SUM(LastRunDuration_MS)/1000
--from tblObjects

/*
update tblObjects
SET StartTime = DATEADD(DAY, -1, StartTime), EndTime = DATEADD(DAY, -1, EndTime)
where EndTime IS NOT NULL
*/