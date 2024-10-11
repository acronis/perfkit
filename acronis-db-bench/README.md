# Acronis Database Benchmark

The Acronis Database Benchmark provides an extensive suite of tests designed to cover a wide range of database
performance scenarios. It addresses both typical queries and edge cases, allowing you to adjust parameters and explore
the impact of different data access patterns on performance. This makes it ideal for testing scenarios such as
high-concurrency inserts, complex updates, and various types of searches, including facet, indexed, and vector-based
searches

Core features of the tool:

* Coverage of both SQL and NoSQL engines, allowing direct comparisons between different database technologies.
* Support for various data access patterns such as insert, update, facet search, indexed search, and vector search.
* Simulation of typical patterns in multi-tenant environments, enabling testing in SaaS-like scenarios.
* Support of queries using [CTI](https://github.com/acronis/go-cti) data types.
* Advanced logging facilities helping developers and DBA engineers troubleshoot discovered issues faster.

Use Cases:

* Compare the efficiency of different database engines.
* Test the impact of hardware and virtualized environments on database performance.
* Measure the performance overhead of database cluster solutions compared to a single database instance.
* Analyze the impact of different database configuration parameters on overall performance.
* Compare the performance and resource utilization of single-tenant versus multi-tenant data structures and queries.
* Simulate and research performance issues related to high-cardinality datasets.
* Investigate the effectiveness of indexing strategies and their impact on query performance.
* Evaluate how well databases handle multiple concurrent transactions and identify potential bottlenecks.
* Model and analyze production-like datasets and queries for rapid assessment of initial database performance limits.

## Supported Databases

The Acronis Database Benchmark supports the following databases:

1. MySQL / MariaDB
2. PostgreSQL
3. MS SQL Server
4. SQLite
5. ClickHouse
6. Cassandra
7. ElasticSearch
8. OpenSearch

## Usage

To use the benchmarking tool, use the following command structure:

```bash
go install acronis-db-bench
acronis-db-bench --connection-string "<connection_string>" ...
``` 

Replace <connection_string> with the data source for your database.

### Options

#### Database options

```
  --connection-string=   connection string (default: sqlite://:memory:)
  --maxopencons=         Set sql/db MaxOpenConns per worker, default value is set to 2 because the benchmark uses it's own workers pool (default: 2)
  --reconnect            reconnect to DB before every test iteration
  --dry-run              do not execute any INSERT/UPDATE/DELETE queries on DB-side
  --log-queries          log all queries
  --log-readed-rows      log all readed rows
  --log-query-time       log query time
  --dont-cleanup         do not cleanup DB content before/after the test in '-t all' mode
  --use-truncate         use TRUNCATE instead of DROP TABLE in cleanup procedure
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
  -b, --batch=                             batch sets the amount of rows per transaction (default: 0)
  -t, --test=                              select a test to execute, run --list to see available tests list
  -a, --list                               list available tests
  -C, --cleanup                            delete/truncate all test DB tables and exit
  -I, --init                               create all test DB tables and exit
  -s, --randseed=                          Seed used for random number generation (default: 1)
  -u, --chunk=                             chunk size for 'all' test (default: 500000)
  -U, --limit=                             total rows limit for 'all' test (default: 2000000)
  -i, --info                               provide information about tables & indexes
  -e, --events                             simulate event generation for every new object
      --tenants-working-set=               set tenants working set (default: 10000)
      --tenants-storage-connection-string= connection string for tenant storage
      --ctis-working-set=                  set CTI working set (default: 1000)
      --profiler-port=                     open profiler on given port (e.g. 6060) (default: 0)
      --describe                           describe what test is going to do
      --describe-all                       describe all the tests
      --explain                            prepend the test queries by EXPLAIN ANALYZE
  -q, --query=                             execute given query, one can use:
                                           {CTI} - for random CTI UUID
                                           {TENANT} - randon tenant UUID
```

### DB specific usage

#### PostgreSQL

```bash
acronis-db-bench --connection-string "postgresql://<USER>:<PASSWORD>@localhost:5432/<DATABASE NAME>?sslmode=disable|enable" ...
```

#### Embedded PostgreSQL

```bash
acronis-db-bench --connection-string "postgresql://localhost:5433/postgres?sslmode=disable&embedded-postgres=true&ep-port=5433&ep-max-connections=512"
```

For details, see [embedded Postgres](https://github.com/fergusstrange/embedded-postgres/)
and [embedded Postgres binaries](https://github.com/zonkyio/embedded-postgres-binaries)

#### MySQL / MariaDB

```bash
acronis-db-bench --connection-string "mysql://<USER>:<PASSWORD>@tcp(<HOST>:<PORT>)/<DATABASE NAME>"
```

#### MS SQL Server

```bash
acronis-db-bench --connection-string "sqlserver://<USER>:<PASSWORD>@<HOST>:<PORT>?database=<DATABASE NAME>"
```

#### SQLite

```bash
acronis-db-bench --connection-string "sqlite://<DATABASE FILE>"
```

#### ClickHouse

```bash
acronis-db-bench --connection-string "clickhouse://<USER>:<PASSWORD>@<HOST>:<PORT>/<DATABASE NAME>"
```

#### Cassandra

```bash
acronis-db-bench --connection-string "cql://<USER>:<PASSWORD>@<HOST>:<PORT>?keyspace=<DATABASE NAME>"
```

#### ElasticSearch

```bash
acronis-db-bench --connection-string "es://<USER>::<PASSWORD>@<HOST>:<PORT>"
```

#### OpenSearch

```bash
acronis-db-bench --connection-string "opensearch://<USER>::<PASSWORD>@<HOST>:<PORT>"
```

### Examples

#### Run all tests

```bash
acronis-db-bench --connection-string "postgresql://<USER>:<PASSWORD>@localhost:5432/<DATABASE NAME>?sslmode=disable"-t all
```

#### Run a specific test

```bash
acronis-db-bench --connection-string "postgresql://<USER>:<PASSWORD>@localhost:5432/<DATABASE NAME>?sslmode=disable" -t insert-medium
acronis-db-bench --connection-string "postgresql://<USER>:<PASSWORD>@localhost:5432/<DATABASE NAME>?sslmode=disable" -t select-medium-rand
```

#### Run a specific test with concurrency (16 workers) and repeat (3 times)

```bash
acronis-db-bench --connection-string "postgresql://<USER>:<PASSWORD>@localhost:5432/<DATABASE NAME>?sslmode=disable" -t insert-light -c 16 -r 3
```

#### Tests available to run

```bash
acronis-db-bench --list
```

##### Output

```bash
========================================================================================================================
Acronis Database Benchmark: version v1-main-dev
========================================================================================================================

  -- Base tests group -------------------------------------------------------------------------------------------------------------

  all                                     : [PMWSCAEO] : execute all tests in the 'base' group
  insert-cti                              : [PMWSCAEO] : insert a CTI entity into the 'cti' table
  insert-heavy                            : [PMWSCAEO] : insert a row into the 'heavy' table
  insert-heavy-multivalue                 : [PMWSCAEO] : insert a row into the 'heavy' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) 
  insert-heavy-prepared                   : [PMWS----] : insert a row into the 'heavy' table using prepared statement for the batch
  insert-light                            : [PMWSCAEO] : insert a row into the 'light' table
  insert-light-multivalue                 : [PMWSCAEO] : insert a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) 
  insert-light-prepared                   : [PMWS----] : insert a row into the 'light' table using prepared statement for the batch
  insert-medium                           : [PMWSCAEO] : insert a row into the 'medium' table
  insert-medium-multivalue                : [PMWS-A--] : insert a row into the 'medium' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) 
  insert-medium-prepared                  : [PMWS----] : insert a row into the 'medium' table using prepared statement for the batch
  insert-tenant                           : [PMWSCAEO] : insert a tenant into the 'tenants' table
  select-1                                : [PMWSCAEO] : just do 'SELECT 1'
  select-heavy-last                       : [PMWS----] : select last row from the 'heavy' table
  select-heavy-minmax-in-tenant           : [PMWS----] : select min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {}
  select-heavy-minmax-in-tenant-and-state : [PMWS----] : select min(completion_time_ns) and max(completion_time_ns) value from the 'heavy' table WHERE tenant_id = {} AND state = {}
  select-heavy-rand                       : [PMWS----] : select random row from the 'heavy' table
  select-heavy-rand-customer-update-time-page : [PMWSCAEO] : select first page from the 'heavy' table WHERE customer_id = {} AND update_time_ns in 1h interval ORDER BY update_time DESC
  select-heavy-rand-in-customer-count     : [PMWSCAEO] : select COUNT(0) from the 'heavy' table WHERE tenant_id = {}
  select-heavy-rand-in-customer-recent    : [PMWSCAEO] : select first page from the 'heavy' table WHERE tenant_id = {} ORDER BY enqueue_time DESC
  select-heavy-rand-in-customer-recent-like : [PMWSCAEO] : select first page from the 'heavy' table WHERE tenant_id = {} AND policy_name LIKE '%k%' ORDER BY enqueue_time DESC
  select-heavy-rand-in-partner-recent     : [PMWSCAEO] : select first page from the 'heavy' table WHERE partner_id = {} ORDER BY enqueue_time DESC
  select-heavy-rand-page-by-uuid          : [PMWSCAEO] : select page from the 'heavy' table WHERE uuid IN (...)
  select-heavy-rand-partner-start-update-time-page : [PMWSCAEO] : select first page from the 'heavy' table WHERE partner_id = {} ORDER BY enqueue_time DESC
  select-medium-last                      : [PMWSCAEO] : select last row from the 'medium' table with few columns and 1 index
  select-medium-rand                      : [PMWSCAEO] : select random row from the 'medium' table with few columns and 1 index
  update-heavy                            : [PMWS----] : update random row in the 'heavy' table
  update-medium                           : [PMWS----] : update random row in the 'medium' table

  -- Advanced tests group ---------------------------------------------------------------------------------------------------------

  bulkupdate-heavy                        : [PMWS----] : update N rows (see --batch=, default 50000) in the 'heavy' table by single transaction
  dbr-bulkupdate-heavy                    : [PMWS----] : update N rows (see --update-rows-count= ) in the 'heavy' table by single transaction using DBR query builder
  insert-json                             : [PMWS----] : insert a row into a table with JSON(b) column
  ping                                    : [PMWSCAEO] : just ping DB
  search-json-by-indexed-value            : [PMWS----] : search a row from the 'json' table using some json condition using LIKE {}
  search-json-by-nonindexed-value         : [PMWS----] : search a row from the 'json' table using some json condition using LIKE {}
  select-heavy-for-update-skip-locked     : [PMWS----] : do SELECT FOR UPDATE SKIP LOCKED and then UPDATE
  select-json-by-indexed-value            : [PMWS----] : select a row from the 'json' table by some json condition
  select-json-by-nonindexed-value         : [PMWS----] : select a row from the 'json' table by some json condition
  select-nextval                          : [PMWS----] : increment a DB sequence in a loop (or use SELECT FOR UPDATE, UPDATE)
  update-heavy-partial-sameval            : [PMWS----] : update random row in the 'heavy' table putting two values, where one of them is already exists in this row
  update-heavy-sameval                    : [PMWS----] : update random row in the 'heavy' table putting the value which already exists

  -- Tenant-aware tests -----------------------------------------------------------------------------------------------------------

  select-heavy-last-in-tenant             : [PMWS----] : select the last row from the 'heavy' table WHERE tenant_id = {random tenant uuid}
  select-heavy-last-in-tenant-and-cti     : [PMWS----] : select the last row from the 'heavy' table WHERE tenant_id = {} AND cti = {}
  select-heavy-rand-in-tenant-like        : [PMWS----] : select random row from the 'heavy' table WHERE tenant_id = {} AND resource_name LIKE {}
  select-medium-last-in-tenant            : [PMWSCAEO] : select the last row from the 'medium' table WHERE tenant_id = {random tenant uuid}

  -- Blob tests -------------------------------------------------------------------------------------------------------------------

  insert-blob                             : [PMWSCAEO] : insert a row with large random blob into the 'blob' table
  select-blob-last-in-tenant              : [PMWSCAEO] : select the last row from the 'blob' table WHERE tenant_id = {random tenant uuid}

  -- Timeseries tests -------------------------------------------------------------------------------------------------------------

  insert-ts-sql                           : [PMWS-A--] : batch insert into the 'timeseries' SQL table
  select-ts-sql                           : [PMWS-A--] : batch select from the 'timeseries' SQL table

  -- Golang DBR query builder tests -----------------------------------------------------------------------------------------------

  dbr-insert-heavy                        : [PMWS----] : insert a row into the 'heavy' table using golang DB query builder
  dbr-insert-json                         : [PMWS----] : insert a row into a table with JSON(b) column using golang DBR driver
  dbr-insert-light                        : [PMWS----] : insert a row into the 'light' table using goland DBR query builder
  dbr-insert-medium                       : [PMWS----] : insert a row into the 'medium' table using goland DBR query builder
  dbr-select-heavy-last                   : [PMWS----] : select last row from the 'heavy' table using golang DBR driver
  dbr-select-heavy-rand                   : [PMWS----] : select random row from the 'heavy' table using golang DBR query builder
  dbr-select-medium-last                  : [PMWS----] : select last row from the 'medium' table with few columns and 1 index
  dbr-select-medium-rand                  : [PMWS----] : select random row from the 'medium' table using golang DBR query builder
  dbr-update-heavy                        : [PMWS----] : update random row in the 'heavy' table using golang DB driver
  dbr-update-medium                       : [PMWS----] : update random row in the 'medium' table using golang DB driver

  -- Advanced monitoring tests ----------------------------------------------------------------------------------------------------

  insert-advmagentresources               : [P-------] : insert into the 'adv monitoring agent resources' table
  insert-advmagents                       : [P-------] : insert into the 'adv monitoring agents' table
  insert-advmarchives                     : [P-------] : insert into the 'adv monitoring archives' table
  insert-advmbackupresources              : [P-------] : insert into the 'adv monitoring backup resources' table
  insert-advmbackups                      : [P-------] : insert into the 'adv monitoring backups' table
  insert-advmdevices                      : [P-------] : insert into the 'adv monitoring devices' table
  insert-advmresources                    : [P-------] : insert into the 'adv monitoring resources' table
  insert-advmresourcesstatuses            : [P-------] : insert into the 'adv monitoring resources statuses' table
  insert-advmtasks                        : [P-------] : insert into the 'adv monitoring tasks' table
  insert-advmvaults                       : [P-------] : insert into the 'adv monitoring vaults' table
  select-advmtasks-codeperweek            : [P-------] : get number of rows grouped by week+result_code
  select-advmtasks-last                   : [P-------] : get number of rows grouped by week+result_code

Databases symbol legend:

  P - PostgreSQL; M - MySQL/MariaDB; W - MSSQL; S - SQLite; C - ClickHouse; A - Cassandra; E - Elasticsearch; O - OpenSearch;
```

## Versions

v1.0.0      - initial version
v1.1.0      - added Embedded Postgres support (see --embedded-postgres and --ep-* options)
v1.1.1      - fix table truncate mode (--use-truncate) on PostgreSQL
v1.1.1-beta - changed internal architecture, added ElasticSearch and OpenSearch support
