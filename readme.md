# OracleSync2MySQL

([CN](https://github.com/iverycd/OracleSync2MySQL/blob/master/readme_cn.md))

## Features


Online migration of Oracle databases to target MySQL kernel databases, such as MySQL, PolarDB, Percona Server MySQL, MariaDB, OceanBase, TiDB, GaussDB for MySQL

- Migrate the entire database table structure and table row data to the target database
- Multi threaded batch migration of table row data
- Data comparison between source and target databases


## Pre-requirement
The running client PC needs to be able to connect to both the source  database and the target database simultaneously

run on Windows,Linux,macOS

## Installation

tar and run 

e.g.

`[root@localhost opt]# tar -zxvf OracleSync2MySQL-linux64-0.0.1.tar.gz`

## How to use

The following is an example of a Windows platform, with the same command-line parameters as other operating systems

`Note`: Please run this tool in `CMD` on a `Windows` system, or in a directory with read and write permissions on `MacOS` or `Linux`

### 1 Edit yml configuration file

Edit the `example.cfg` file and input the source(src) and target(dest) database information separately

```yaml
src:
  host: 192.168.1.200
  port: 1521
  database: orcl
  username: admin
  password: oracle
dest:
  host: 192.168.1.37
  port: 3306
  database: test_polar
  username: root
  password: 11111
pageSize: 100000
maxParallel: 100
batchRowSize: 1000
tables:
  test:
    - select * from test
exclude:
  operationlog
```

database: `src` is oracle service_name,`dest` is database name

pageSize: Number of records per page for pagination query

maxParallel: The maximum number of concurrency that can run goroutine simultaneously

tables: Customized migrated tables and customized query source tables, indented in yml format

exclude: Tables that do not migrate to target database, indented in yml format

batchRowSize: Number of rows in batch insert target table

### 2 Full database migration

Migrate entire database table structure, row data, views, index constraints, and self increasing columns to target database

OracleSync2MySQL.exe  --config file.yml
```
e.g.
OracleSync2MySQL.exe --config example.yml

on Linux and MacOS you can run
./OracleSync2MySQL --config example.yml
```

### 3 View Migration Summary

After the entire database migration is completed, a migration summary will be generated to observe if there are any failed objects. By querying the migration log, the failed objects can be analyzed

```bash
+-------------------------+---------------------+-------------+----------+
|        SourceDb         |       DestDb        | MaxParallel | PageSize |
+-------------------------+---------------------+-------------+----------+
| 192.168.149.37-sourcedb | 192.168.149.33-test |     30      |  100000  |
+-------------------------+---------------------+-------------+----------+

+-----------+----------------------------+----------------------------+-------------+--------------+
|Object     |         BeginTime          |          EndTime           |FailedTotal  |ElapsedTime   |
+-----------+----------------------------+----------------------------+-------------+--------------+
|Table      | 2023-07-21 17:12:51.680525 | 2023-07-21 17:12:52.477100 |0            |796.579837ms  |
|TableData  | 2023-07-21 17:12:52.477166 | 2023-07-21 17:12:59.704021 |0            |7.226889553s  |
+-----------+----------------------------+----------------------------+-------------+--------------+

Table Create finish elapsed time  5.0256021s

```

### 4 Compare Source and Target database

After migration finish you can compare source table and target database table rows,displayed failed table only

`OracleSync2MySQL.exe --config your_file.yml compareDb`

```
e.g.
OracleSync2MySQL.exe --config example.yml compareDb

on Linux and MacOS you can run
./OracleSync2MySQL --config example.yml compareDb
```

```bash
Table Compare Result (Only Not Ok Displayed)
+-----------------------+------------+----------+-------------+------+
|Table                  |SourceRows  |DestRows  |DestIsExist  |isOk  |
+-----------------------+------------+----------+-------------+------+
|abc_testinfo           |7458        |0         |YES          |NO    |
|log1_qweharddiskweqaz  |0           |0         |NO           |NO    |
|abcdef_jkiu_button     |4           |0         |YES          |NO    |
|abcdrf_yuio            |5           |0         |YES          |NO    |
|zzz_ss_idcard          |56639       |0         |YES          |NO    |
|asdxz_uiop             |290497      |190497    |YES          |NO    |
|abcd_info              |1052258     |700000    |YES          |NO    |
+-----------------------+------------+----------+-------------+------+ 
INFO[0040] Table Compare finish elapsed time 11.307881434s 
```







## Other migration modes


#### 1 Full database migration

Migrate entire database table structure, row data, views, index constraints, and self increasing columns to target database

OracleSync2MySQL.exe  --config file.yml

```
e.g.
OracleSync2MySQL.exe --config example.yml
```

#### 2 Custom SQL Query Migration

only migrate some tables not entire database, and migrate the table structure and table data to the target database according to the custom query statement in file.yml

OracleSync2MySQL.exe  --config file.yml -s

```
e.g.
OracleSync2MySQL.exe  --config example.yml -s
```

#### 3 Migrate all table structures in the entire database

Create all table structure(only table metadata not row data) to  target database

OracleSync2MySQL.exe  --config file.yml createTable -t

```
e.g.
OracleSync2MySQL.exe  --config example.yml createTable -t
```

#### 4 Migrate the table structure of custom tables

Read custom tables from yml file and create target table 

OracleSync2MySQL.exe  --config file.yml createTable -s -t

```
e.g.
OracleSync2MySQL.exe  --config example.yml createTable -s -t
```