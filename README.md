# IntegrityChecksForVLDBs
My solution to managing Integrity Checks within a time limit on a Very Large Database (VLDB).  This process picks up where it left off last time to continue checking databases.


## Parameters:
These are very similar to Ola's parameters, as the logic was taken from there and implemented here

**@Databases** - Select databases. The keywords SYSTEM_DATABASES, USER_DATABASES, ALL_DATABASES, and AVAILABILITY_GROUP_DATABASES are supported. The hyphen character (-) is used to exclude databases, and the percent character (%) is used for wildcard selection. All of these operations can be combined by using the comma (,).<br>

| Value                                         | Description |
| :---                                          | :---        |
| SYSTEM_DATABASES                              | All system databases (master, msdb, and model) |
| USER_DATABASES                                | All user databases |
| ALL_DATABASES                                 | All databases |
| AVAILABILITY_GROUP_DATABASES                  | All databases in availability groups |
| USER_DATABASES, -AVAILABILITY_GROUP_DATABASES | All user databases that are not in availability groups |
| Db1                                           | The database Db1 |
| Db1, Db2                                      | The databases Db1 and Db2 |
| USER_DATABASES, -Db1                          | All user databases, except Db1 |
| %Db%                                          | All databases that have “Db” in the name |
| %Db%, -Db1                                    | All databases that have “Db” in the name, except Db1 |
| ALL_DATABASES, -%Db%                          | All databases that do not have “Db” in the name |

**@TimeLimit** - The time, in seconds, after which no commands are executed.  Default is unlimited <br>
**@SnapshotPath** - Specify the location Database Snapshots should be stored during execution.  Default is the same location as the individual datafiles <br>
**@LogToTable** - (Y,N) Log commands to the table dbo.CommandLog <br>
**@Execute** - (Y,N) Execute the commands.  If 'N' then only print commands. <br>
**@PhysicalOnly** - (Y,N) The PhysicalOnly option in DatabaseIntegrityCheck uses the PHYSICAL_ONLY option in the SQL Server [DBCC CHECKDB](https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checkdb-transact-sql), [DBCC CHECKFILEGROUP](https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checkfilegroup-transact-sql), and [DBCC CHECKTABLE](https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checktable-transact-sql) commands. <br>
**@MaxDop** - Specify the number of CPUs to use when checking the database, filegroup or table. If this number is not specified, the global maximum degree of parallelism is used.  The MaxDOP option in DatabaseIntegrityCheck uses the MAXDOP option in the SQL Server [DBCC CHECKDB](https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checkdb-transact-sql), [DBCC CHECKFILEGROUP](https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checkfilegroup-transact-sql), and [DBCC CHECKTABLE](https://docs.microsoft.com/en-us/sql/t-sql/database-console-commands/dbcc-checktable-transact-sql) commands. <br>


## Overview of code
* Lines 20-27: These variables will be the parameters of the Stored Proc
* Lines 39-61: This is the table (dbo.tblObjects) that stores persistent info
* Lines 67-101: These lines declare variables and table variables used in the script
* Lines 112-397: This code is taken directly from Ola's scripts
  * Lines 112-202: Checks Core Requirements for the script to run.  If any fail, it will GOTO command on line 201
  * Lines 208-344: Parses the Databases parameter and selectes the appropriate databases.  This is the logic used to exclude or include certain databases.
  * Lines 350-397: Checks parameters and verifies they are legit.  If any fail, it will GOTO command on line 396
* Lines 407-410:  Gets databases from Ola's logic into a table variable (@tblDBs) to use
* Lines 413-441: Loops through selected databases and gets table info into a table variable (@tblObj).  It excludes memory optimized tables, as these cannot have CheckTable run against them
* Lines 448-476: Merges results from @tblObj into the dbo.tblObjects table
  * Matches on database name, schema name, and table name
  * When a match is found, it updates the used_page_count and sets the Active flag to 1
  * When a row is not in dbo.tblObjects but in @tblObj, the row is inserted into dbo.tblObjects
  * When a row is not in @tblObj but in dbo.tblObjects, the Active flag is set to 0, since @tblObj holds the tables we want to check
* Lines 497-569: Loops through and runs a CheckAlloc and CheckCatalog command on each database
  * Lines 505-536:  Creates a snapshot of the database if it is not a system database or if it does not have a memory optimzed filegroup
  * Lines 538-547: Runs the CheckAlloc and CheckCatalog commands using Ola's CommandExecute procedure.  This helps with logging as well
  * Lines 553-563: Drops the snapshot if one was created
* Lines 575-579: Selects the database order to check first based on the oldest object checked into the table variable @checkTableDbOrder
* Lines 584-736: Loops through all databases selected and the tables and runs CheckTable as long as there is time left
  * Lines 595-596: If there are more objects that have never been checked than objects that have, assume this is the first time running, and use the appropriate logic to order databases in line 640
  * Lines 602-632:  Creates a snapshot of the database if it is not a system database or if it does not have a memory optimzed filegroup
  * Lines 632-701: Loops through all the tables for the database and runs CheckTable
    * Line 640: See Lines 595-596
    * Lines 643-660:  Selects the table to run the command on, based on the criteria
    * Lines 669-701: Run the CheckTable command on the selected table (if there is time left) on the snapshot that was created (if there was one)
    * Lines 704-714: Updates dbo.tblObjects with new runtime stats
  * Lines 721-731: Drops the database snapshot if one was created