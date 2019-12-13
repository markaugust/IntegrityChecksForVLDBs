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
**@SnapshotPath** - Specify the location Database Snapshots should be stored during execution.  Default is the same location as the individual datafiles
