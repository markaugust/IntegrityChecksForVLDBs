/*

Uncomment return code at end when creating stored proc

1. Create Table for holding info
2. Create Database Snapshot for each database as it loops through
3. DBCC CHECKALLOC on all databases
4. DBCC CHECKCATALOG on all databases
5. Drop Database Snapshot
6. Create Database Snapshot for each database as it loops through
7. DBCC CHECKTABLE on each table
8. Drop Database Snapshot
*/
SET NOCOUNT ON
GO
use master
go

--These will be the Stored Proc Parameters
DECLARE
    @Databases nvarchar(max) = NULL,
    @PhysicalOnly nvarchar(max) = 'N',
    @MaxDOP int = NULL,
    @TimeLimit int = NULL,
    @SnapshotPath nvarchar(300) = NULL,
    @LogToTable nvarchar(max) = 'Y',
    @Execute nvarchar(max) = 'Y'


----------

IF @Databases IS NULL
    SET @Databases = 'ALL_DATABASES'

--DROP TABLE dbo.CheckTableObjects

--Create persistant table to hold information
--Add other fields like Last Run Time, Duration
IF NOT EXISTS (SELECT 1 FROM sys.objects where object_id = OBJECT_ID(N'[dbo].[CheckTableObjects]') and type in (N'U'))
CREATE TABLE dbo.CheckTableObjects(
    ID int IDENTITY,
    [dbid] int,
    [database_name] nvarchar(128),
    [dbtype] nvarchar(max),
    [schema_id] int,
    [schema] sysname,
    [object_id] int,
    [object_name] sysname,
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
    [LastCheckDate] date DEFAULT '1900-01-01',
    [Active] bit
    
)

-----------------------------------------------------------------
----------SETUP VARIABLES AND TABLE VARIABLES--------------------
-----------------------------------------------------------------

DECLARE @JobStartTime datetime = GETDATE()
DECLARE @JobEndTime datetime = DATEADD(SS, @TimeLimit, @JobStartTime)
DECLARE @Version numeric(18,10) = CAST(LEFT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)),CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - 1) + '.' + REPLACE(RIGHT(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)), LEN(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max))) - CHARINDEX('.',CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(max)))),'.','') AS numeric(18,10))

DECLARE @dbname sysname, @dbid int, @dbtype nvarchar(max), @tablename sysname, @schemaname sysname, @sqlcmd nvarchar(max), @avgRun int, @command nvarchar(max)
DECLARE @previousRunDate datetime, @prevousRunDuration_MS int, @cmdStartTime datetime, @cmdEndTime datetime
DECLARE @origExecutionCount int, @newRunDuration int, @newExecutionCount int, @lastCheckDate date
DECLARE @hasMemOptFG bit, @snapName nvarchar(128), @snapCreated bit, @checkDbName sysname
DECLARE @EndMessage nvarchar(max), @DatabaseMessage nvarchar(max)

--Declare table variables to gather info
DECLARE @tblDBs TABLE (
    [name] sysname,
    [dbid] int,
    [dbtype] nvarchar(max),
    [isdone] bit
)
DECLARE @tblObj TABLE (
    [database_name] nvarchar(128),
    [dbid] int,
    [dbtype] nvarchar(max),
    [object_id] int,
    [object_name] sysname,
    [schema_id] int,
    [schema] sysname,
    [type] CHAR(2),
    [type_desc] NVARCHAR(60),
    [used_page_count] bigint
)
DECLARE @checkTableDbOrder TABLE (
    [name] sysname,
    [dbid] int,
    [dbtype] nvarchar(max),
    [MinLastCheckDate] datetime,
    [isDone] bit
)

-----------------------------------------------------------------
----------BUILD LIST OF DATABASES AND OBJECTS--------------------
-----------------------------------------------------------------

--below is from Ola's scripts and how he compiles the list of databases

-----------------------------------------------------------------
----------vvvv LOVINGLY STOLEN FROM OLA vvvv---------------------
-----------------------------------------------------------------
DECLARE @ErrorMessage nvarchar(max)
DECLARE @Error int
DECLARE @ReturnCode int

DECLARE @EmptyLine nvarchar(max)

SET @Error = 0
SET @ReturnCode = 0

SET @EmptyLine = CHAR(9)
----------------------------------------------------------------------------------------------------
--// Check core requirements                                                                    //--
----------------------------------------------------------------------------------------------------

IF NOT (SELECT [compatibility_level] FROM sys.databases WHERE database_id = DB_ID()) >= 90
BEGIN
    SET @ErrorMessage = 'The database ' + QUOTENAME(DB_NAME(DB_ID())) + ' has to be in compatibility level 90 or higher.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF NOT (SELECT uses_ansi_nulls FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
BEGIN
    SET @ErrorMessage = 'ANSI_NULLS has to be set to ON for the stored procedure.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF NOT (SELECT uses_quoted_identifier FROM sys.sql_modules WHERE [object_id] = @@PROCID) = 1
BEGIN
    SET @ErrorMessage = 'QUOTED_IDENTIFIER has to be set to ON for the stored procedure.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute')
BEGIN
    SET @ErrorMessage = 'The stored procedure CommandExecute is missing. Download https://ola.hallengren.com/scripts/CommandExecute.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'P' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandExecute' AND OBJECT_DEFINITION(objects.[object_id]) NOT LIKE '%@LockMessageSeverity%')
BEGIN
    SET @ErrorMessage = 'The stored procedure CommandExecute needs to be updated. Download https://ola.hallengren.com/scripts/CommandExecute.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF @LogToTable = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'CommandLog')
BEGIN
    SET @ErrorMessage = 'The table CommandLog is missing. Download https://ola.hallengren.com/scripts/CommandLog.sql.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

-- IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'Queue')
-- BEGIN
--     SET @ErrorMessage = 'The table Queue is missing. Download https://ola.hallengren.com/scripts/Queue.sql.'
--     RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
--     SET @Error = @@ERROR
--     RAISERROR(@EmptyLine,10,1) WITH NOWAIT
-- END

-- IF @DatabasesInParallel = 'Y' AND NOT EXISTS (SELECT * FROM sys.objects objects INNER JOIN sys.schemas schemas ON objects.[schema_id] = schemas.[schema_id] WHERE objects.[type] = 'U' AND schemas.[name] = 'dbo' AND objects.[name] = 'QueueDatabase')
-- BEGIN
--     SET @ErrorMessage = 'The table QueueDatabase is missing. Download https://ola.hallengren.com/scripts/QueueDatabase.sql.'
--     RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
--     SET @Error = @@ERROR
--     RAISERROR(@EmptyLine,10,1) WITH NOWAIT
-- END

IF @@TRANCOUNT <> 0
BEGIN
    SET @ErrorMessage = 'The transaction count is not 0.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF @Error <> 0
    BEGIN
    SET @ReturnCode = @Error
    GOTO Logging
END

----------------------------------------------------------------------------------------------------
--// Select databases                                                                           //--
----------------------------------------------------------------------------------------------------

DECLARE @SelectedDatabases TABLE (DatabaseName nvarchar(max),
                                DatabaseType nvarchar(max),
                                AvailabilityGroup nvarchar(max),
                                StartPosition int,
                                Selected bit)

DECLARE @tmpDatabases TABLE (ID int IDENTITY,
                            DatabaseName nvarchar(max),
                            DatabaseType nvarchar(max),
                            AvailabilityGroup bit,
                            [Snapshot] bit,
                            StartPosition int,
                            LastCommandTime datetime,
                            DatabaseSize bigint,
                            LastGoodCheckDbTime datetime,
                            [Order] int,
                            Selected bit,
                            Completed bit,
                            PRIMARY KEY(Selected, Completed, [Order], ID))

SET @Databases = REPLACE(@Databases, CHAR(10), '')
SET @Databases = REPLACE(@Databases, CHAR(13), '')

WHILE CHARINDEX(', ',@Databases) > 0 SET @Databases = REPLACE(@Databases,', ',',')
WHILE CHARINDEX(' ,',@Databases) > 0 SET @Databases = REPLACE(@Databases,' ,',',')

SET @Databases = LTRIM(RTRIM(@Databases));

WITH Databases1 (StartPosition, EndPosition, DatabaseItem) AS
(
SELECT 1 AS StartPosition,
       ISNULL(NULLIF(CHARINDEX(',', @Databases, 1), 0), LEN(@Databases) + 1) AS EndPosition,
       SUBSTRING(@Databases, 1, ISNULL(NULLIF(CHARINDEX(',', @Databases, 1), 0), LEN(@Databases) + 1) - 1) AS DatabaseItem
WHERE @Databases IS NOT NULL
UNION ALL
SELECT CAST(EndPosition AS int) + 1 AS StartPosition,
       ISNULL(NULLIF(CHARINDEX(',', @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) AS EndPosition,
       SUBSTRING(@Databases, EndPosition + 1, ISNULL(NULLIF(CHARINDEX(',', @Databases, EndPosition + 1), 0), LEN(@Databases) + 1) - EndPosition - 1) AS DatabaseItem
FROM Databases1
WHERE EndPosition < LEN(@Databases) + 1
),
Databases2 (DatabaseItem, StartPosition, Selected) AS
(
SELECT CASE WHEN DatabaseItem LIKE '-%' THEN RIGHT(DatabaseItem,LEN(DatabaseItem) - 1) ELSE DatabaseItem END AS DatabaseItem,
       StartPosition,
       CASE WHEN DatabaseItem LIKE '-%' THEN 0 ELSE 1 END AS Selected
FROM Databases1
),
Databases3 (DatabaseItem, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
(
SELECT CASE WHEN DatabaseItem IN('ALL_DATABASES','SYSTEM_DATABASES','USER_DATABASES','AVAILABILITY_GROUP_DATABASES') THEN '%' ELSE DatabaseItem END AS DatabaseItem,
       CASE WHEN DatabaseItem = 'SYSTEM_DATABASES' THEN 'S' WHEN DatabaseItem = 'USER_DATABASES' THEN 'U' ELSE NULL END AS DatabaseType,
       CASE WHEN DatabaseItem = 'AVAILABILITY_GROUP_DATABASES' THEN 1 ELSE NULL END AvailabilityGroup,
       StartPosition,
       Selected
FROM Databases2
),
Databases4 (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected) AS
(
SELECT CASE WHEN LEFT(DatabaseItem,1) = '[' AND RIGHT(DatabaseItem,1) = ']' THEN PARSENAME(DatabaseItem,1) ELSE DatabaseItem END AS DatabaseItem,
       DatabaseType,
       AvailabilityGroup,
       StartPosition,
       Selected
FROM Databases3
)
INSERT INTO @SelectedDatabases (DatabaseName, DatabaseType, AvailabilityGroup, StartPosition, Selected)
SELECT DatabaseName,
       DatabaseType,
       AvailabilityGroup,
       StartPosition,
       Selected
FROM Databases4
OPTION (MAXRECURSION 0)

INSERT INTO @tmpDatabases (DatabaseName, DatabaseType, AvailabilityGroup, [Snapshot], [Order], Selected, Completed)
SELECT [name] AS DatabaseName,
       CASE WHEN name IN('master','msdb','model') OR is_distributor = 1 THEN 'S' ELSE 'U' END AS DatabaseType,
       NULL AS AvailabilityGroup,
       CASE WHEN source_database_id IS NOT NULL THEN 1 ELSE 0 END AS [Snapshot],
       0 AS [Order],
       0 AS Selected,
       0 AS Completed
FROM sys.databases
ORDER BY [name] ASC

--Adds wanted databases to selection
UPDATE tmpDatabases
SET tmpDatabases.Selected = SelectedDatabases.Selected
FROM @tmpDatabases tmpDatabases
INNER JOIN @SelectedDatabases SelectedDatabases
ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
WHERE SelectedDatabases.Selected = 1

--Removes unwanted databases from selection
UPDATE tmpDatabases
SET tmpDatabases.Selected = SelectedDatabases.Selected
FROM @tmpDatabases tmpDatabases
INNER JOIN @SelectedDatabases SelectedDatabases
ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
AND NOT ((tmpDatabases.DatabaseName = 'tempdb' OR tmpDatabases.[Snapshot] = 1) AND tmpDatabases.DatabaseName <> SelectedDatabases.DatabaseName)
WHERE SelectedDatabases.Selected = 0

--Update Start Position (for ordering)
UPDATE tmpDatabases
SET tmpDatabases.StartPosition = SelectedDatabases2.StartPosition
FROM @tmpDatabases tmpDatabases
INNER JOIN (SELECT tmpDatabases.DatabaseName, MIN(SelectedDatabases.StartPosition) AS StartPosition
            FROM @tmpDatabases tmpDatabases
            INNER JOIN @SelectedDatabases SelectedDatabases
            ON tmpDatabases.DatabaseName LIKE REPLACE(SelectedDatabases.DatabaseName,'_','[_]')
            AND (tmpDatabases.DatabaseType = SelectedDatabases.DatabaseType OR SelectedDatabases.DatabaseType IS NULL)
            AND (tmpDatabases.AvailabilityGroup = SelectedDatabases.AvailabilityGroup OR SelectedDatabases.AvailabilityGroup IS NULL)
            WHERE SelectedDatabases.Selected = 1
            GROUP BY tmpDatabases.DatabaseName) SelectedDatabases2
ON tmpDatabases.DatabaseName = SelectedDatabases2.DatabaseName

IF @Databases IS NOT NULL AND (NOT EXISTS(SELECT * FROM @SelectedDatabases) OR EXISTS(SELECT * FROM @SelectedDatabases WHERE DatabaseName IS NULL OR DatabaseName = ''))
BEGIN
  SET @ErrorMessage = 'The value for the parameter @Databases is not supported.'
  RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
  SET @Error = @@ERROR
  RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

;WITH tmpDatabases AS (
    SELECT DatabaseName, [Order], ROW_NUMBER() OVER (ORDER BY StartPosition ASC, DatabaseName ASC) AS RowNumber
    FROM @tmpDatabases tmpDatabases
    WHERE Selected = 1
)
UPDATE tmpDatabases
SET [Order] = RowNumber

----------------------------------------------------------------------------------------------------
--// Check input parameters                                                                     //--
----------------------------------------------------------------------------------------------------

IF @PhysicalOnly NOT IN ('Y','N') OR @PhysicalOnly IS NULL
BEGIN
    SET @ErrorMessage = 'The value for the parameter @PhysicalOnly is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF @MaxDOP < 0 OR @MaxDOP > 64 OR (@MaxDOP IS NOT NULL AND NOT (@Version >= 12.050000 OR SERVERPROPERTY('EngineEdition') IN (5, 8)))
BEGIN
    SET @ErrorMessage = 'The value for the parameter @MaxDOP is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF @TimeLimit < 0
BEGIN
    SET @ErrorMessage = 'The value for the parameter @TimeLimit is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF @LogToTable NOT IN('Y','N') OR @LogToTable IS NULL
BEGIN
    SET @ErrorMessage = 'The value for the parameter @LogToTable is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF @Execute NOT IN('Y','N') OR @Execute IS NULL
BEGIN
    SET @ErrorMessage = 'The value for the parameter @Execute is not supported.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    SET @Error = @@ERROR
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
END

IF @Error <> 0
BEGIN
    SET @ErrorMessage = 'The documentation is available at https://ola.hallengren.com/sql-server-integrity-check.html.'
    RAISERROR('%s',16,1,@ErrorMessage) WITH NOWAIT
    RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    SET @ReturnCode = @Error
    GOTO Logging
END

-----------------------------------------------------------------
----------^^^^ LOVINGLY STOLEN FROM OLA ^^^^---------------------
-----------------------------------------------------------------

-----------------------------------------------------------------
----------GENERATE LIST OF OBJECTS TO BE CHECKED-----------------
-----------------------------------------------------------------
--Take ouput of the Ola section and put it into our own table
INSERT INTO @tblDBs (name, dbid, dbtype, isdone)
SELECT DatabaseName, DB_ID(DatabaseName), DatabaseType, 0 as isdone
FROM @tmpDatabases
WHERE Selected = 1

--loop through all databases gathered and get page count for each table in the database
WHILE 1=1
BEGIN
    SELECT TOP 1 @dbname = name, @dbid = dbid, @dbtype = dbtype FROM @tblDBs WHERE isdone = 0
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    --This query is derived and taken from the MS Tiger Scripts
    --This is where you would adjust what tables you want to select and what information to pull
    SET @sqlcmd = 'USE ' + QUOTENAME(@dbname) + ' SELECT DB_ID() as dbid, DB_NAME() as database_name, ''' + @dbtype + ''' as dbtype, 
    ss.[schema_id], ss.[name] as [schema], so.[object_id], so.[name] as object_name, so.[type], so.type_desc, SUM(sps.used_page_count) AS used_page_count
    FROM sys.objects so
    INNER JOIN sys.dm_db_partition_stats sps ON so.[object_id] = sps.[object_id]
    INNER JOIN sys.indexes si ON so.[object_id] = si.[object_id]
    INNER JOIN sys.schemas ss ON so.[schema_id] = ss.[schema_id]
    LEFT JOIN sys.tables st ON so.[object_id] = st.[object_id]
    WHERE so.[type] IN (''S'', ''U'', ''V'')'
    + CASE WHEN @Version >= 12 THEN ' AND (st.is_memory_optimized = 0 OR st.is_memory_optimized IS NULL)' ELSE '' END
    + 'GROUP BY so.[object_id], so.[name], ss.name, ss.[schema_id], so.[type], so.type_desc'

    INSERT INTO @tblObj (dbid, database_name, dbtype, schema_id, [schema], object_id, object_name, type, type_desc, used_page_count)
    EXEC sp_executesql @sqlcmd

    --update loop counter
    UPDATE @tblDBs
    SET isdone = 1
    WHERE dbid = @dbid
END

--Merge into persistent table
--Match on database name, schema name, and table name
--When Match and page count is different, update page count in persistent table and set active flag
--when not found in persistent table, insert
--when found in persistent table but not in source, then delete from persistent table
MERGE master.dbo.CheckTableObjects as [Target]
USING (SELECT * FROM @tblObj) as [Source]
ON (Target.database_name = Source.database_name AND Target.[schema] = Source.[schema] AND Target.object_name = Source.object_name)
WHEN MATCHED /*AND Target.used_page_count <> source.used_page_count */ THEN
    UPDATE SET Target.used_page_count = source.used_page_count, Target.Active = 1
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([database_name]
      ,[dbid]
      ,[dbtype]
      ,[object_id]
      ,[object_name]
      ,[schema_id]
      ,[schema]
      ,[type]
      ,[type_desc]
      ,[used_page_count]
      ,[Active])
    VALUES (Source.[database_name]
      ,Source.[dbid]
      ,Source.[dbtype]
      ,Source.[object_id]
      ,Source.[object_name]
      ,Source.[schema_id]
      ,Source.[schema]
      ,Source.[type]
      ,Source.[type_desc]
      ,Source.[used_page_count]
      ,1)
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET Active = 0
;

--For Testing Only--------------
--DROP TABLE dbo.CommandsRun
-- IF NOT EXISTS (SELECT 1 FROM sys.objects where object_id = OBJECT_ID(N'[dbo].[CommandsRun]') and type in (N'U'))
-- CREATE TABLE dbo.CommandsRun(
--     [command] nvarchar(max),
--     [object] nvarchar(max)
-- )
-- TRUNCATE TABLE dbo.CommandsRun
----------------------------------

-----------------------------------------------------------------
----------RUN CHECKALLOC AND CHECKCATALOG------------------------
-----------------------------------------------------------------

--Reset DB status for CheckAlloc and CheckCatalog
UPDATE @tblDBs
SET isdone = 0

--loop through all databases in @tblDBs and run CheckAlloc and CheckCatalog
WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
BEGIN
    SELECT TOP 1 @dbname = [name], @dbid = [dbid], @dbtype = [dbtype] from @tblDBs where isdone = 0
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    SET @snapCreated = 0
    SET @checkDbName = @dbname
    --Check if database has MemOptFG
    SET @sqlcmd = 'IF EXISTS (SELECT 1 from ' + QUOTENAME(@dbname) + '.sys.filegroups where type = ''FX'') BEGIN SET @currentHasMemOptFG = 1 END ELSE BEGIN SET @currentHasMemOptFG = 0 END'
    exec sp_executesql @statement = @sqlcmd, @params = N'@currentHasMemOptFG bit OUTPUT', @currentHasMemOptFG = @hasMemOptFG output
    --if it's not MemOptFG and not a System DB, Create manual snapshot
    IF NOT (@hasMemOptFG = 1 OR @dbtype = 'S')
    BEGIN
        --Build and execute create snapshot statement
        SET @snapName = @dbname + '_CHKALOCCAT_snapshot_' + CONVERT(nvarchar, @JobStartTime, 112)
        SET @sqlcmd = 'CREATE DATABASE ' + QUOTENAME(@snapName) + ' ON '
        SELECT @sqlcmd = @sqlcmd + '(Name = ' + QUOTENAME(name) + ', Filename = '''
            + CASE WHEN @SnapshotPath IS NULL THEN physical_name ELSE @SnapshotPath + '\' + name END
            + '_CHKALOCCAT_snapshot_' + CONVERT(nvarchar, @JobStartTime, 112) + '''),'
        FROM sys.master_files WHERE database_id = @dbid AND type = 0
        SET @sqlcmd = LEFT(@sqlcmd, LEN(@sqlcmd) - 1)
        SET @sqlcmd = @sqlcmd + ' AS SNAPSHOT OF ' + QUOTENAME(@dbname)
        EXEC sp_executesql @sqlcmd
        SET @snapCreated = 1
    END

    --if snapshot is created, use snapshot name for database name
    IF @snapCreated = 1
    BEGIN
        SET @checkDbName = @snapName

        SET @DatabaseMessage = 'Snapshot created: ' + QUOTENAME(@snapName)
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
        SET @DatabaseMessage = 'Command: ' + @sqlCmd
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
        RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    --Run CheckAlloc
    SET @sqlcmd = 'DBCC CHECKALLOC([' + @checkDbName + ']) WITH NO_INFOMSGS, ALL_ERRORMSGS'
    EXECUTE [dbo].[CommandExecute] @Command = @sqlcmd, @CommandType = 'Marks Custom CheckAlloc', @Mode = 1, @DatabaseName = @dbname, @LogToTable = @LogToTable, @Execute = @Execute
    -- EXEC sp_executesql @sqlcmd
    -- INSERT INTO dbo.CommandsRun (command, object)
    -- SELECT @sqlcmd, @checkDbName

    --Run CheckCatalog
    SET @sqlcmd = 'DBCC CHECKCATALOG([' + @checkDbName + ']) WITH NO_INFOMSGS'
    EXECUTE [dbo].[CommandExecute] @Command = @sqlcmd, @CommandType = 'Marks Custom CheckCatalog', @Mode = 1, @DatabaseName = @dbname, @LogToTable = @LogToTable, @Execute = @Execute
    -- EXEC sp_executesql @sqlcmd
    -- INSERT INTO dbo.CommandsRun (command, object)
    -- SELECT @sqlcmd, @checkDbName

    --Drop Database Snapshot if one was created manually
    IF @snapCreated = 1
    BEGIN
        SET @sqlcmd = 'IF EXISTS (SELECT name FROM sys.databases WHERE name = ''' + @checkDbName
            + ''') AND (SELECT source_database_id FROM sys.databases WHERE name = ''' + @checkDbName
            + ''') IS NOT NULL BEGIN DROP DATABASE ' + QUOTENAME(@checkDbName) + 'END'
        EXEC sp_executesql @sqlcmd

        SET @DatabaseMessage = 'Snapshot dropped: ' + QUOTENAME(@snapName)
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
        RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    --update loop counter
    UPDATE @tblDBs
    SET isdone = 1
    where dbid = @dbid
END

-----------------------------------------------------------------
----------RUN CHECKTABLE-----------------------------------------
-----------------------------------------------------------------

INSERT INTO @checkTableDbOrder ([name], [dbid], [dbtype], [MinLastCheckDate], [isDone])
SELECT [database_name], [dbid], [dbtype], min([LastCheckDate]), 0
FROM CheckTableObjects
WHERE Active = 1
GROUP BY [database_name], [dbid], [dbtype]

DECLARE @InitialRunCheck bit = 0
DECLARE @OrderBySmallest bit = 0

WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
BEGIN
    --Ensures only 1 database is checked at a time, rather than randomly checking tables from random databases
    SELECT TOP 1 @dbname = [name], @dbid = [dbid], @dbtype = [dbtype] from @checkTableDbOrder WHERE isDone = 0 ORDER BY MinLastCheckDate
    --This will break the loop if all databases are done before the time limit
    IF @@ROWCOUNT = 0
    BEGIN
        BREAK
    END

    --if the number of new tables (execution count = 0) is greater than existing (execution count > 0)
    IF (SELECT count([database_name]) from CheckTableObjects WHERE @dbname = [database_name] and NumberOfExecutions = 0) > (SELECT count([database_name]) from CheckTableObjects WHERE @dbname = [database_name] and NumberOfExecutions > 0)
        SET @InitialRunCheck = 1




    --Create Snapshot
    SET @snapCreated = 0
    SET @checkDbName = @dbname
    --Check if database has MemOptFG
    SET @sqlcmd = 'IF EXISTS (SELECT 1 from ' + QUOTENAME(@dbname) + '.sys.filegroups where type = ''FX'') BEGIN SET @currentHasMemOptFG = 1 END ELSE BEGIN SET @currentHasMemOptFG = 0 END'
    exec sp_executesql @statement = @sqlcmd, @params = N'@currentHasMemOptFG bit OUTPUT', @currentHasMemOptFG = @hasMemOptFG output
    --if it's not MemOptFG and not a System DB, Create manual snapshot
    IF NOT (@hasMemOptFG = 1 OR @dbtype = 'S')
    BEGIN
        --Build and execute create snapshot statement
        SET @snapName = @dbname + '_CHKTABLE_snapshot_' + CONVERT(nvarchar, @JobStartTime, 112)
        SET @sqlcmd = 'CREATE DATABASE ' + QUOTENAME(@snapName) + ' ON '
        SELECT @sqlcmd = @sqlcmd + '(Name = ' + QUOTENAME(name) + ', Filename = '''
            + CASE WHEN @SnapshotPath IS NULL THEN physical_name ELSE @SnapshotPath + '\' + name END
            + '_CHKTABLE_snapshot_' + CONVERT(nvarchar, @JobStartTime, 112) + '''),'
        FROM sys.master_files WHERE database_id = @dbid AND type = 0
        SET @sqlcmd = LEFT(@sqlcmd, LEN(@sqlcmd) - 1)
        SET @sqlcmd = @sqlcmd + ' AS SNAPSHOT OF ' + QUOTENAME(@dbname)
        EXEC sp_executesql @sqlcmd
        SET @snapCreated = 1
    END
    --if snapshot is created, use snapshot name for database name
    IF @snapCreated = 1
    BEGIN
        SET @checkDbName = @snapName

        SET @DatabaseMessage = 'Snapshot created: ' + QUOTENAME(@snapName)
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
        SET @DatabaseMessage = 'Command: ' + @sqlCmd
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
        RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END


    --Run the CheckTable commands
    WHILE (GETDATE() < @JobEndTime OR @TimeLimit IS NULL)
    BEGIN

        --If GetDate is Greater than the Halfway point, make sure to get smallest tables first
        IF @InitialRunCheck = 1 AND GETDATE() > DATEADD(MS, DATEDIFF(MS, @JobStartTime, @JobEndTime)/2, @JobStartTime)
            SET @OrderBySmallest = 1

        SELECT TOP 1
            @schemaname = [schema],
            @tablename = [object_name],
            @avgRun = [AvgRunDuration_MS],
            @previousRunDate = [StartTime],
            @prevousRunDuration_MS = [RunDuration_MS],
            @origExecutionCount = [NumberOfExecutions],
            @cmdStartTime = [StartTime],
            @cmdEndTime = [EndTime],
            @lastCheckDate = [LastCheckDate]
        FROM CheckTableObjects
        WHERE @dbname = [database_name]
        AND Active = 1
        AND LastCheckDate = (SELECT MIN(LastCheckDate) FROM CheckTableObjects WHERE [database_name] = @dbname)  --Makes sure it's the oldest entry for that database
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
        IF @TimeLimit IS NOT NULL AND DATEADD(MS, @avgRun, GETDATE()) > DATEADD(MI, 1, @JobEndTime)
        BEGIN
            SET @command = 'Skipped due to TimeLimit Constraint: ' + CONVERT(nvarchar, DATEADD(MS, @avgRun, GETDATE()), 121) + ' is greater than ' + CONVERT(nvarchar, DATEADD(MI, 1, @JobEndTime), 121)
        END
        ELSE
        BEGIN
            SET @cmdStartTime = GETDATE()
            SET @sqlcmd = 'USE [' + @checkDbName + ']; DBCC CHECKTABLE (''' + QUOTENAME(@schemaname) + '.' + QUOTENAME(@tablename) + ''') WITH NO_INFOMSGS, ALL_ERRORMSGS'
            IF @PhysicalOnly = 'N' SET @sqlcmd = @sqlcmd + ', DATA_PURITY'
            IF @PhysicalOnly = 'Y' SET @sqlcmd = @sqlcmd + ', PHYSICAL_ONLY'
            IF @MaxDOP IS NOT NULL SET @sqlcmd = @sqlcmd + ', MAXDOP = ' + CAST(@MaxDOP AS nvarchar)

            --Log the command
            -- INSERT INTO dbo.CommandsRun (command, object)
            -- SELECT @sqlcmd, QUOTENAME(@checkDbName) + '.' + QUOTENAME(@schemaname) + '.' + QUOTENAME(@tablename)
            -- --Execute the command
            -- EXEC sp_executesql @sqlcmd
            EXECUTE [dbo].[CommandExecute] @Command = @sqlcmd, @CommandType = 'Marks Custom CheckTable', @Mode = 1, @DatabaseName = @dbname, @SchemaName = @schemaname, @ObjectName = @tablename, @ObjectType = NULL, @LogToTable = @LogToTable, @Execute = @Execute

            --Set End Time and last check date
            SET @cmdEndTime = GETDATE()
            SET @lastCheckDate = @JobStartTime

            --Set run duration of this run and the new execution count
            SET @newRunDuration = DATEDIFF(ms, @cmdStartTime, @cmdEndTime)
            SET @newExecutionCount = @origExecutionCount + 1

            --Calculate the new average run time
            --This formula works since the number of executions is being updated in the previous step            
            SET @avgRun = @avgRun + ((@newRunDuration - @avgRun) / @newExecutionCount)

            SET @command = 'Command Executed: ' + @sqlcmd
        END

        --Update CheckTableObjects with new information
        UPDATE dbo.CheckTableObjects
        SET [LastCheckDate] = @lastCheckDate
        , [Command] = @command
        , [AvgRunDuration_MS] = @avgRun
        , PreviousRunDate = @previousRunDate
        , PreviousRunDuration_MS = @prevousRunDuration_MS
        , StartTime = @cmdStartTime
        , EndTime = @cmdEndTime
        , [RunDuration_MS] = @newRunDuration
        , [NumberOfExecutions] = @newExecutionCount
        WHERE @dbname = [database_name] AND @schemaname = [schema] AND @tablename = [object_name]

    END


    --Drop Snapshot
    --Drop Database Snapshot if one was created manually
    IF @snapCreated = 1
    BEGIN
        SET @sqlcmd = 'IF EXISTS (SELECT name FROM sys.databases WHERE name = ''' + @checkDbName
            + ''') AND (SELECT source_database_id FROM sys.databases WHERE name = ''' + @checkDbName
            + ''') IS NOT NULL BEGIN DROP DATABASE ' + QUOTENAME(@checkDbName) + 'END'
        EXEC sp_executesql @sqlcmd

        SET @DatabaseMessage = 'Snapshot dropped: ' + QUOTENAME(@snapName)
        RAISERROR('%s',10,1,@DatabaseMessage) WITH NOWAIT
        RAISERROR(@EmptyLine,10,1) WITH NOWAIT
    END

    UPDATE @checkTableDbOrder
    SET isDone = 1
    WHERE name = @dbname
END

-----------------------------------------------------------------
----------DONE---------------------------------------------------
-----------------------------------------------------------------

----------------------------------------------------------------------------------------------------
--// Log completing information                                                                 //--
----------------------------------------------------------------------------------------------------

Logging:
SET @EndMessage = 'Date and time: ' + CONVERT(nvarchar,GETDATE(),120)
RAISERROR('%s',10,1,@EndMessage) WITH NOWAIT

RAISERROR(@EmptyLine,10,1) WITH NOWAIT

-- IF @ReturnCode <> 0
-- BEGIN
--     RETURN @ReturnCode
-- END


-----------------------------------------------------------------
----------END----------------------------------------------------
-----------------------------------------------------------------

/*
select *
from CheckTableObjects
order by StartTime desc

-- select *
-- from CommandsRun
-- order by object

select * from @tmpDatabases
select * from @tblDBs
select * from @tblObj
select * from @checkTableDbOrder

--select SUM(RunDuration_MS)/1000
--from CheckTableObjects
*/
/*
update CheckTableObjects
SET LastCheckDate = DATEADD(DAY, -1, LastCheckDate)
*/