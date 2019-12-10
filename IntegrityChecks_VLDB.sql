/*
1. Create Table for holding info
2. Create Database Snapshot (not implemented yet)
3. DBCC CHECKALLOC on all databases
4. DBCC CHECKCATALOG on all databases
5. DBCC CHECKTABLE on each table

Parallel table checks?

*/
SET NOCOUNT ON
GO
use master
go

DECLARE @TimeLimit int = 15

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
    [type_desc] nvarchar(60),
    [used_page_count] bigint,
    [StartTime] datetime,
    [EndTime] datetime,
    [RunDuration_MS] int,
    [Command] nvarchar(max),
    [NumberOfExecutions] int DEFAULT 0,
    [AvgRunDuration_MS] int DEFAULT 0,
    [PreviousRunDate] datetime,
    [PreviousRunDuration_MS] int,
    [LastCheckDate] date DEFAULT '1900-01-01'
    
)

-----------------------------------------------------------------
----------SETUP VARIABLES AND TABLE VARIABLES--------------------
-----------------------------------------------------------------

DECLARE @JobStartTime datetime = GETDATE()
DECLARE @JobEndTime datetime = DATEADD(SS, @TimeLimit, @JobStartTime)
DECLARE @dbname sysname, @dbid int, @tablename sysname, @schemaname sysname, @sqlcmd nvarchar(max), @avgRun int, @command nvarchar(max)
DECLARE @previousRunDate datetime, @prevousRunDuration_MS int, @cmdStartTime datetime, @cmdEndTime datetime
DECLARE @origExecutionCount int, @newRunDuration int, @newExecutionCount int

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
    [MinLastCheckDate] datetime,
    [isDone] bit
)

-----------------------------------------------------------------
----------BUILD LIST OF DATABASES AND OBJECTS--------------------
-----------------------------------------------------------------

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
    WHERE dbid = @dbid
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

-----------------------------------------------------------------
----------RUN CHECKALLOC AND CHECKCATALOG------------------------
-----------------------------------------------------------------

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
    where dbid = @dbid
END

-----------------------------------------------------------------
----------RUN CHECKTABLE-----------------------------------------
-----------------------------------------------------------------

INSERT INTO @checkTableDbOrder ([name], [dbid], [MinLastCheckDate], [isDone])
SELECT [database_name], [dbid], min([LastCheckDate]), 0
FROM tblObjects GROUP BY [database_name], [dbid]

DECLARE @InitialRunCheck bit = 0
DECLARE @OrderBySmallest bit = 0

WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
BEGIN
    --Ensures only 1 database is checked at a time, rather than randomly checking tables from random databases
    SELECT TOP 1 @dbname = [name] from @checkTableDbOrder WHERE isDone = 0 ORDER BY MinLastCheckDate
    --This will break the loop if all databases are done before the time limit
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    --if the number of new tables (execution count = 0) is greater than existing (execution count > 0)
    IF (SELECT count([database_name]) from tblObjects WHERE @dbname = [database_name] and NumberOfExecutions = 0) > (SELECT count([database_name]) from tblObjects WHERE @dbname = [database_name] and NumberOfExecutions > 0)
        SET @InitialRunCheck = 1

    --Run the CheckTable commands
    WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
    BEGIN

        --If GetDate is Greater than the Halfway point, make sure to get smallest tables first
        IF @InitialRunCheck = 1 AND GETDATE() > DATEADD(MS, DATEDIFF(MS, @JobStartTime, @JobEndTime)/2, @JobStartTime)
            SET @OrderBySmallest = 1

        SELECT TOP 1
            @schemaname = [schema],
            @tablename = [name],
            @avgRun = [AvgRunDuration_MS],
            @previousRunDate = [StartTime],
            @prevousRunDuration_MS = [RunDuration_MS],
            @origExecutionCount = [NumberOfExecutions],
            @cmdStartTime = [StartTime],
            @cmdEndTime = [EndTime]
        FROM tblObjects
        WHERE @dbname = [database_name]
        AND LastCheckDate = (SELECT MIN(LastCheckDate) FROM tblObjects WHERE [database_name] = @dbname)  --Makes sure it's the oldest entry for that database
        AND LastCheckDate <> CAST(@JobStartTime as date)  --makes sure it's not the same day, as we don't need to run it again
        ORDER BY
        CASE WHEN @OrderBySmallest = 1 THEN used_page_count END ASC,
        CASE WHEN @OrderBySmallest = 0 THEN database_name END ASC

        --This will break the loop and move to the next database if all tables are done
        IF @@ROWCOUNT = 0
        BEGIN
            BREAK
        END

        --If average run time is longer than remaining time + one minute to give a little overhead
        IF @TimeLimit IS NOT NULL AND DATEADD(MS, @avgRun, @cmdStartTime) > DATEADD(MI, 1, @JobEndTime)
        BEGIN
            SET @command = 'Skipped due to TimeLimit Constraint: ' + CONVERT(nvarchar, DATEADD(MS, @avgRun, @cmdStartTime), 121) + ' is greater than ' + CONVERT(nvarchar, DATEADD(MI, 1, @JobEndTime), 121)
        END
        ELSE
        BEGIN
            SET @cmdStartTime = GETDATE()
            SET @sqlcmd = 'USE [' + @dbname + ']; DBCC CHECKTABLE (''' + @schemaname + '.' + @tablename + ''') WITH NO_INFOMSGS, ALL_ERRORMSGS, DATA_PURITY'

            --Log the command
            INSERT INTO dbo.CommandsRun (command, object)
            SELECT @sqlcmd, QUOTENAME(@dbname) + '.' + QUOTENAME(@schemaname) + '.' + QUOTENAME(@tablename)
            --Execute the command
            EXEC sp_executesql @sqlcmd

            --Set End Time
            SET @cmdEndTime = GETDATE()

            --Set run duration of this run and the new execution count
            SET @newRunDuration = DATEDIFF(ms, @cmdStartTime, @cmdEndTime)
            SET @newExecutionCount = @origExecutionCount + 1

            --Calculate the new average run time
            --This formula works since the number of executions is being updated in the previous step            
            SET @avgRun = @avgRun + ((@newRunDuration - @avgRun) / [@newExecutionCount])

            SET @command = 'Command Executed: ' + @sqlcmd
        END

        --Update tblObjects with new information
        UPDATE dbo.tblObjects
        SET [LastCheckDate] = @lastCheckDate
        , [Command] = @command
        , [AvgRunDuration_MS] = @avgRun
        , PreviousRunDate = @previousRunDate
        , PreviousRunDuration_MS = @prevousRunDuration_MS
        , StartTime = @cmdStartTime
        , EndTime = @cmdEndTime
        , [RunDuration_MS] = @newRunDuration
        , [NumberOfExecutions] = @newExecutionCount
        WHERE @dbname = [database_name] AND @schemaname = [schema] AND @tablename = [name]

    END

    UPDATE @checkTableDbOrder
    SET isDone = 1
    WHERE name = @dbname
END

-----------------------------------------------------------------
----------DONE---------------------------------------------------
-----------------------------------------------------------------

select *
from tblObjects
order by StartTime desc

select *
from CommandsRun
order by object

--select SUM(RunDuration_MS)/1000
--from tblObjects

/*
update tblObjects
SET LastCheckDate = DATEADD(DAY, -1, LastCheckDate)
*/