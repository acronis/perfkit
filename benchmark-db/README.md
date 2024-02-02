# Acronis Database Benchmark

This project provides a set of benchmarking utilities for various databases. It includes various tests and options for database performance analysis.

## Supported Databases

The Acronis Database Benchmark supports the following databases:

1. MySQL / MariaDB
2. PostgreSQL
3. MS SQL Server
4. SQLite
5. ClickHouse
6. Cassandra

## Usage

To use the benchmarking tool, use the following command structure:
```bash
go install acronis-db-bench
acronis-db-bench --driver <database_driver> --dsn "<data_source_name>" ...
``` 
Replace <database_driver> with the driver for your database (mysql, postgres, mssql, sqlite, clickhouse) and <data_source_name> with the appropriate data source name for your database.

### Options

#### Database options

```
  --driver=              db driver (postgres|mysql|sqlite3|clickhouse|cassandra) (default: postgres)
  --dsn=                 dsn connection string (default: host=127.0.0.1 sslmode=disable user=test_user)
  --dont-cleanup         do not cleanup DB content before/after the test in '-t all' mode
  --use-truncate         use TRUNCATE instead of DROP TABLE in cleanup procedure
  --maxopencons=         Set sql/db MaxOpenConns per worker, default value is set to 2 because the benchmark uses it's own workers pool (default: 2)
  --mysql-engine=        mysql engine (innodb|myisam|xpand|...) (default: innodb)
  --reconnect            reconnect to DB before every test iteration
  --dry-run              do not execute any INSERT/UPDATE/DELETE queries on DB-side
```

#### Common options

```
  -v, --verbose              Show verbose debug information (-v - info, -vv - debug)
  -c, --concurrency=         sets number of goroutines that runs testing function (default: 0)
  -l, --loops=               sets TOTAL(not per worker) number of iterations of testing function(greater priority than DurationSec) (default: 0)
  -d, --duration=            sets duration(in seconds) for work time for every loop (default: 5)
  -S, --sleep=               sleep given amount of msec between requests (default: 0)
  -r, --repeat=              repeat the test given amount of times (default: 1)
  -Q, --quiet                be quiet and print as less information as possible
  -s, --randseed=            Seed used for random number generation (default: 1)
```

#### Benchmark specific options

```
  -b, --batch=               batch sets the amount of rows per transaction (default: 0)
  -t, --test=                select a test to execute, run --list to see available tests list
  -a, --list                 list available tests
  -C, --cleanup              delete/truncate all test DB tables and exit
  -I, --init                 create all test DB tables and exit
  -s, --randseed=            Seed used for random number generation (default: 1)
  -u, --chunk=               chunk size for 'all' test (default: 500000)
  -U, --limit=               total rows limit for 'all' test (default: 2000000)
  -i, --info                 provide information about tables & indexes
  -e, --events               simulate event generation for every new object
      --tenants-working-set= set tenants working set (default: 10000)
      --ctis-working-set=    set CTI working set (default: 1000)
      --profiler-port=       open profiler on given port (e.g. 6060) (default: 0)
      --describe             describe what test is going to do
      --describe-all         describe all the tests
      --explain              prepend the test queries by EXPLAIN ANALYZE
  -q, --query=               execute given query, one can use:
                             {CTI} - for random CTI UUID
                             {TENANT} - randon tenant UUID
```

### DB specific usage

#### PostgreSQL

```bash
acronis-db-bench --driver postgres --dsn "host=localhost port=5432 user=<USER> password=<PASSWORD> dbname=<DATABASE NAME> sslmode=disable|enable"
```

#### MySQL / MariaDB

```bash
acronis-db-bench --driver mysql --dsn "<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>"
```

#### MS SQL Server

```bash
acronis-db-bench --driver mssql --dsn "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>"
```

#### SQLite

```bash
acronis-db-bench --driver sqlite --dsn "<DATABASE FILE>"
```

#### ClickHouse

```bash
acronis-db-bench --driver clickhouse --dsn "tcp://<HOST>:<PORT>?username=<USER>&password=<PASSWORD>&database=<DATABASE NAME>"
```

#### Cassandra

```bash
acronis-db-bench --driver cassandra --dsn "<HOST>&port=<PORT>&username=<USER>&password=<PASSWORD>&keyspace=<DATABASE NAME>"
```

### Examples

#### Run all tests

```bash
acronis-db-bench --driver postgres --dsn "host=localhost port=5432 user=<USER> password=<PASSWORD> dbname=<DATABASE NAME> sslmode=disable" -t all
```

#### Run a specific test

```bash
acronis-db-bench --driver postgres --dsn "host=localhost port=5432 user=<USER> password=<PASSWORD> dbname=<DATABASE NAME> sslmode=disable" -t insert-medium
acronis-db-bench --driver postgres --dsn "host=localhost port=5432 user=<USER> password=<PASSWORD> dbname=<DATABASE NAME> sslmode=disable" -t select-medium-rand
```

#### Run a specific test with concurrency (16 workers) and repeat (3 times)

```bash
acronis-db-bench --driver postgres --dsn "host=localhost port=5432 user=<USER> password=<PASSWORD> dbname=<DATABASE NAME> sslmode=disable" -t insert-light -c 16 -r 3
```

#### Tests available to run

```bash
acronis-db-bench --list
```
##### Output
```bash
========================================================================================================================
Acronis Database Benchmark: version v1.0.0
========================================================================================================================

  -- Base tests group -------------------------------------------------------------------------------------------------------------

  all                                     : [PMWSCA] : execute all tests in the 'base' group
  copy-heavy                              : [P-W---] : copy a row into the 'heavy' table
  copy-light                              : [P-W---] : copy a row into the 'light' table
  copy-medium                             : [P-W---] : copy a row into the 'medium' table
  insert-cti                              : [PMWSCA] : insert a CTI entity into the 'cti' table
  insert-heavy                            : [PMWS--] : insert a row into the 'heavy' table
  insert-heavy-multivalue                 : [PMWS--] : insert a row into the 'heavy' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) 
  insert-heavy-prepared                   : [PMWS--] : insert a row into the 'heavy' table using prepared statement for the batch
  insert-light                            : [PMWSCA] : insert a row into the 'light' table
  insert-light-multivalue                 : [PMWS-A] : insert a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) 
  insert-light-prepared                   : [PMWS--] : insert a row into the 'light' table using prepared statement for the batch
  insert-medium                           : [PMWSCA] : insert a row into the 'medium' table
  insert-medium-multivalue                : [PMWS-A] : insert a row into the 'medium' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) 
  insert-medium-prepared                  : [PMWS--] : insert a row into the 'medium' table using prepared statement for the batch
  insert-tenant                           : [PMWSCA] : insert a tenant into the 'tenants' table
  select-1                                : [PMWSCA] : just do 'SELECT 1'
  select-heavy-last                       : [PMWS--] : select last row from the 'heavy' table
  select-heavy-minmax-in-tenant           : [PMWS--] : select min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {}
  select-heavy-minmax-in-tenant-and-state : [PMWS--] : select min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {} AND state = {}
  select-heavy-rand                       : [PMWS--] : select random row from the 'heavy' table
  select-medium-last                      : [PMWSCA] : select last row from the 'medium' table with few columns and 1 index
  select-medium-rand                      : [PMWSCA] : select random row from the 'medium' table with few columns and 1 index
  update-heavy                            : [PMWS--] : update random row in the 'heavy' table
  update-medium                           : [PMWS--] : update random row in the 'medium' table

  -- Advanced tests group ---------------------------------------------------------------------------------------------------------

  bulkupdate-heavy                        : [PMWS--] : update N rows (see --batch=, default 50000) in the 'heavy' table by single transaction
  dbr-bulkupdate-heavy                    : [PMWS--] : update N rows (see --update-rows-count= ) in the 'heavy' table by single transaction using DBR query builder
  insert-json                             : [PMWS--] : insert a row into a table with JSON(b) column
  ping                                    : [PMWSCA] : just ping DB
  search-json-by-indexed-value            : [PMWS--] : search a row from the 'json' table using some json condition using LIKE {}
  search-json-by-nonindexed-value         : [PMWS--] : search a row from the 'json' table using some json condition using LIKE {}
  select-heavy-for-update-skip-locked     : [PMWS--] : do SELECT FOR UPDATE SKIP LOCKED and then UPDATE
  select-json-by-indexed-value            : [PMWS--] : select a row from the 'json' table by some json condition
  select-json-by-nonindexed-value         : [PMWS--] : select a row from the 'json' table by some json condition
  select-nextval                          : [PMWS--] : increment a DB sequence in a loop (or use SELECT FOR UPDATE, UPDATE)
  update-heavy-partial-sameval            : [PMWS--] : update random row in the 'heavy' table putting two values, where one of them is already exists in this row
  update-heavy-sameval                    : [PMWS--] : update random row in the 'heavy' table putting the value which already exists

  -- Tenant-aware tests -----------------------------------------------------------------------------------------------------------

  select-heavy-last-in-tenant             : [PMWS--] : select the last row from the 'heavy' table WHERE tenant_id = {random tenant uuid}
  select-heavy-last-in-tenant-and-cti     : [PMWS--] : select the last row from the 'heavy' table WHERE tenant_id = {} AND cti = {}
  select-heavy-rand-in-tenant-like        : [PMWS--] : select random row from the 'heavy' table WHERE tenant_id = {} AND resource_name LIKE {}
  select-medium-last-in-tenant            : [PMWSCA] : select the last row from the 'medium' table WHERE tenant_id = {random tenant uuid}

  -- Blob tests -------------------------------------------------------------------------------------------------------------------

  copy-blob                               : [P-W---] : copy a row with large random blob into the 'blob' table
  insert-blob                             : [PMWSCA] : insert a row with large random blob into the 'blob' table
  insert-largeobj                         : [P-----] : insert a row with large random object into the 'largeobject' table
  select-blob-last-in-tenant              : [PMWSCA] : select the last row from the 'blob' table WHERE tenant_id = {random tenant uuid}

  -- Time-series tests ------------------------------------------------------------------------------------------------------------

  insert-ts-sql                           : [PMWS-A] : batch insert into the 'timeseries' SQL table
  select-ts-sql                           : [PMWS-A] : batch select from the 'timeseries' SQL table

  -- Golang DBR query builder tests -----------------------------------------------------------------------------------------------

  dbr-insert-heavy                        : [PMWS--] : insert a row into the 'heavy' table using golang DB query builder
  dbr-insert-json                         : [PMWS--] : insert a row into a table with JSON(b) column using golang DBR driver
  dbr-insert-light                        : [PMWS--] : insert a row into the 'light' table using goland DBR query builder
  dbr-insert-medium                       : [PMWS--] : insert a row into the 'medium' table using goland DBR query builder
  dbr-select-1                            : [PMWS--] : do 'SELECT 1' using golang DBR query builder
  dbr-select-heavy-last                   : [PMWS--] : select last row from the 'heavy' table using golang DBR driver
  dbr-select-heavy-rand                   : [PMWS--] : select random row from the 'heavy' table using golang DBR query builder
  dbr-select-medium-last                  : [PMWS--] : select last row from the 'medium' table with few columns and 1 index
  dbr-select-medium-rand                  : [PMWS--] : select random row from the 'medium' table using golang DBR query builder
  dbr-update-heavy                        : [PMWS--] : update random row in the 'heavy' table using golang DB driver
  dbr-update-medium                       : [PMWS--] : update random row in the 'medium' table using golang DB driver

  -- Advanced monitoring tests ----------------------------------------------------------------------------------------------------

  insert-advmagentresources               : [P-----] : insert into the 'adv monitoring agent resources' table
  insert-advmagents                       : [P-----] : insert into the 'adv monitoring agents' table
  insert-advmarchives                     : [P-----] : insert into the 'adv monitoring archives' table
  insert-advmbackupresources              : [P-----] : insert into the 'adv monitoring backup resources' table
  insert-advmbackups                      : [P-----] : insert into the 'adv monitoring backups' table
  insert-advmdevices                      : [P-----] : insert into the 'adv monitoring devices' table
  insert-advmresources                    : [P-----] : insert into the 'adv monitoring resources' table
  insert-advmresourcesstatuses            : [P-----] : insert into the 'adv monitoring resources statuses' table
  insert-advmtasks                        : [P-----] : insert into the 'adv monitoring tasks' table
  insert-advmvaults                       : [P-----] : insert into the 'adv monitoring vaults' table
  select-advmtasks-codeperweek            : [P-----] : get number of rows grouped by week+result_code
  select-advmtasks-last                   : [P-----] : get number of rows grouped by week+result_code

Databases symbol legend:

  P - PostgreSQL; M - MySQL/MariaDB; W - MSSQL; S - SQLite; C - ClickHouse; A - Cassandra;
```
