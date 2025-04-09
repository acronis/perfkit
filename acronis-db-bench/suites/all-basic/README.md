# All Tests Suite (-t all)

This suite includes a comprehensive set of database performance tests that cover various operations and scenarios. The tests are divided into base tests and additional tests.

## Purpose

This test suite is designed to:

- Verify database cluster configuration and performance
- Test infrastructure setup and stability
- Benchmark different database operations under various loads
- Validate database connection and basic functionality
- Test tenant-specific operations and JSON handling capabilities
- Measure performance of different types of queries (select, insert, update)

The suite is particularly useful for:

- Initial database cluster setup verification
- Infrastructure performance testing
- Database configuration validation

## Base Tests

### Connection and Basic Operations

- `select-1`: Simple connection test that executes "SELECT 1" to verify basic database connectivity.

### Insert Operations

- `insert-light`: Inserts rows into the `acronis_db_bench_light` table
- `insert-medium`: Inserts rows into the `acronis_db_bench_medium` table
- `insert-heavy`: Inserts rows into the `acronis_db_bench_heavy` table
- `insert-json`: Inserts data with JSON/JSONB values
- `insert-ts-sql`: Inserts timeseries data

### Update Operations

- `update-medium`: Updates N random rows in the `acronis_db_bench_medium` table (N is controlled by --batch parameter)
- `update-heavy`: Updates N random rows in the `acronis_db_bench_heavy` table (N is controlled by --batch parameter)
- `update-heavy-partial-sameval`: Updates partial data with same values in heavy table
- `update-heavy-sameval`: Updates data with same values in heavy table

### Select Operations

- `select-medium-rand`: Selects N random rows from the `acronis_db_bench_medium` table
- `select-heavy-rand`: Selects N random rows from the `acronis_db_bench_heavy` table
- `select-medium-last`: Selects N last rows from the `acronis_db_bench_medium` table
- `select-heavy-last`: Selects N last rows from the `acronis_db_bench_heavy` table

### Tenant-Specific Operations

- `select-heavy-last-in-tenant`: Simulates fetching the last task in random tenant children
- `select-heavy-rand-in-tenant-like`: Selects random rows from tenant with LIKE condition
- `select-heavy-last-in-tenant-and-cti`: Simulates fetching the last task in random tenant children with respect to CTI id
- `select-heavy-minmax-in-tenant`: Gets min/max values within tenant
- `select-heavy-minmax-in-tenant-and-state`: Gets min/max values within tenant and state

### JSON Operations

- `select-json-by-indexed-value`: Selects data by indexed JSON path
- `select-json-by-nonindexed-value`: Selects data by non-indexed JSON path

### Timeseries Operations

- `select-ts-sql`: Selects timeseries data

## Usage

To run all tests:
```bash
./acronis-db-bench -t all
```

To run specific tests, use the test names listed above with the `-t` flag:
```bash
./acronis-db-bench -t insert-light
```

## Configuration Options

- `--batch`: Controls the number of rows for operations in many tests
- `--workers`: Controls the number of concurrent workers for parallel operations
- `--chunk`: Controls the number of loops for insert and update operations

## Reference Results

We will show reference results for following cases.

### Infrastructure

#### Macbook PRO M1

Overall characteristics

``` bash
> system_profiler SPHardwareDataType
Hardware:

    Hardware Overview:

      Model Name: MacBook Pro
      Model Identifier: MacBookPro18,1
      Model Number: ...
      Chip: Apple M1 Pro
      Total Number of Cores: 10 (8 performance and 2 efficiency)
      Memory: 32 GB
      ...
```

RAM Info:
``` bash
> system_profiler SPMemoryDataType
Memory:

      Memory: 32 GB
      Type: LPDDR5
      ...
```

Disk Performance

``` bash
> fio --name=randwrite --ioengine=sync --rw=randwrite --bs=4k --size=32G --numjobs=1 --runtime=60 --group_reporting
randwrite: (g=0): rw=randwrite, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=sync, iodepth=1
fio-3.39
Starting 1 process
randwrite: Laying out IO file (1 file / 32768MiB)
Jobs: 1 (f=1): [w(1)][100.0%][w=13.9MiB/s][w=3549 IOPS][eta 00m:00s]
randwrite: (groupid=0, jobs=1): err= 0: pid=18394: Wed Apr  9 12:49:11 2025
  write: IOPS=3612, BW=14.1MiB/s (14.8MB/s)(847MiB/60001msec); 0 zone resets
    clat (nsec): min=2000, max=11488k, avg=275944.96, stdev=234095.85
     lat (nsec): min=2000, max=11488k, avg=275975.78, stdev=234096.82
    clat percentiles (usec):
     |  1.00th=[   78],  5.00th=[   86], 10.00th=[   95], 20.00th=[  247],
     | 30.00th=[  262], 40.00th=[  269], 50.00th=[  273], 60.00th=[  281],
     | 70.00th=[  285], 80.00th=[  297], 90.00th=[  314], 95.00th=[  326],
     | 99.00th=[ 1860], 99.50th=[ 2057], 99.90th=[ 2540], 99.95th=[ 2835],
     | 99.99th=[ 3163]
   bw (  KiB/s): min=12048, max=37093, per=100.00%, avg=14452.92, stdev=4197.41, samples=119
   iops        : min= 3012, max= 9273, avg=3613.02, stdev=1049.34, samples=119
  lat (usec)   : 4=0.32%, 10=0.37%, 20=0.04%, 50=0.01%, 100=11.30%
  lat (usec)   : 250=8.92%, 500=77.23%, 750=0.18%, 1000=0.05%
  lat (msec)   : 2=0.93%, 4=0.65%, 10=0.01%, 20=0.01%
  cpu          : usr=0.31%, sys=60.54%, ctx=211314, majf=0, minf=6
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=0,216766,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=1

Run status group 0 (all jobs):
  WRITE: bw=14.1MiB/s (14.8MB/s), 14.1MiB/s-14.1MiB/s (14.8MB/s-14.8MB/s), io=847MiB (888MB), run=60001-60001msec
```

#### Production-like Server

CPU Info

``` bash
> lscpu | grep -E "Model name|Architecture|CPU"
Architecture:          x86_64
CPU(s):                96
Model name:            Intel(R) Xeon(R) Gold 6248R CPU @ 3.00GHz
CPU MHz:               2733.032
CPU max MHz:           4000.0000
CPU min MHz:           1200.0000
...
```

RAM info:

``` bash
> sudo dmidecode --type memory | egrep "Size:|Speed:|Locator:|Type:"
  ...
  Type: DDR4
  Speed: 3200 MT/s
  Configured Memory Speed: 2934 MT/s
  ...
```

Disk Performance

``` bash
> fio --name=randwrite --ioengine=libaio --rw=randwrite --bs=4k --size=32G --numjobs=1 --runtime=60 --group_reporting
randwrite: (g=0): rw=randwrite, bs=(R) 4096B-4096B, (W) 4096B-4096B, (T) 4096B-4096B, ioengine=libaio, iodepth=1
fio-3.14
Starting 1 process
Jobs: 1 (f=1): [w(1)][98.0%][w=780MiB/s][w=200k IOPS][eta 00m:01s]
randwrite: (groupid=0, jobs=1): err= 0: pid=830265: Wed Apr  9 09:18:40 2025
  write: IOPS=171k, BW=667MiB/s (700MB/s)(32.0GiB/49100msec)
    slat (nsec): min=1956, max=61399k, avg=4227.80, stdev=23150.15
    clat (nsec): min=333, max=1014.4k, avg=386.73, stdev=389.15
     lat (usec): min=2, max=61404, avg= 4.67, stdev=23.16
    clat percentiles (nsec):
     |  1.00th=[  362],  5.00th=[  362], 10.00th=[  366], 20.00th=[  370],
     | 30.00th=[  374], 40.00th=[  378], 50.00th=[  378], 60.00th=[  382],
     | 70.00th=[  386], 80.00th=[  390], 90.00th=[  430], 95.00th=[  434],
     | 99.00th=[  446], 99.50th=[  450], 99.90th=[  510], 99.95th=[  788],
     | 99.99th=[ 6496]
   bw (  KiB/s): min=604584, max=937680, per=100.00%, avg=796334.96, stdev=70227.47, samples=84
   iops        : min=151146, max=234420, avg=199083.71, stdev=17556.87, samples=84
  lat (nsec)   : 500=99.89%, 750=0.06%, 1000=0.01%
  lat (usec)   : 2=0.01%, 4=0.01%, 10=0.03%, 20=0.01%, 50=0.01%
  lat (usec)   : 100=0.01%, 250=0.01%
  lat (msec)   : 2=0.01%
  cpu          : usr=15.77%, sys=83.99%, ctx=149, majf=0, minf=11
  IO depths    : 1=100.0%, 2=0.0%, 4=0.0%, 8=0.0%, 16=0.0%, 32=0.0%, >=64=0.0%
     submit    : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     complete  : 0=0.0%, 4=100.0%, 8=0.0%, 16=0.0%, 32=0.0%, 64=0.0%, >=64=0.0%
     issued rwts: total=0,8388608,0,0 short=0,0,0,0 dropped=0,0,0,0
     latency   : target=0, window=0, percentile=100.00%, depth=1

Run status group 0 (all jobs):
  WRITE: bw=667MiB/s (700MB/s), 667MiB/s-667MiB/s (700MB/s-700MB/s), io=32.0GiB (34.4GB), run=49100-49100msec

Disk stats (read/write):
    dm-0: ios=0/1563694, merge=0/0, ticks=0/4289503, in_queue=4295970, util=54.97%, aggrios=2/1495805, aggrmerge=0/93154, aggrticks=2/3699640, aggrin_queue=3701523, aggrutil=55.47%
  sda: ios=2/1495805, merge=0/93154, ticks=2/3699640, in_queue=3701523, util=55.47%
```

### Databases configurations

#### Docker containers

For local testing and development, we use Docker containers from the `db/testing/docker` directory. This directory contains configurations for various database systems including:

- MariaDB (including vector-enabled version)
- PostgreSQL (including vector-enabled version)

These containers are configured with development-friendly settings and can be used to run the benchmark suite locally. Each database has its own Docker configuration with appropriate settings for testing purposes.

#### MariaDB Galera Cluster

The MariaDB Galera Cluster is deployed on the "Production-like Server" infrastructure described above. The cluster consists of three nodes, each running in its own dedicated virtual machine deployed using Virtuozzo virtualization technology. The nodes are configured in a full master-master replication setup using Galera clustering technology.

Each node in the cluster has the following specifications:

- 2 CPU cores
- 16 GB RAM
- 250 GB disk space

This configuration ensures high availability and data consistency across all nodes, as any node can accept both read and write operations.

#### Postgresql Patroni cluster

The PostgreSQL Patroni cluster is deployed on the "Production-like Server" infrastructure described above. The cluster consists of three nodes, each running in its own dedicated virtual machine deployed using Virtuozzo virtualization technology. The replication setup includes one primary node and two replica nodes, where one replica is configured for synchronous replication and the other for asynchronous replication.

Each node in the cluster has the following specifications:

- 2 CPU cores
- 16 GB RAM
- 250 GB disk space

This configuration provides high availability and data redundancy, with the synchronous replica ensuring zero data loss in case of primary node failure, while the asynchronous replica provides additional read capacity and backup options.

### Results for MariaDB

#### Macbook PRO M1 Docker Container

The acronis-db-bench loader is running directly on the MacBook M1 PRO host, testing the MariaDB container deployed in Docker.

``` bash
> acronis-db-bench --connection-string="mysql://user:password@tcp(localhost:3306)/perfkit_db_ci" -t all --limit 200000 --chunk 200000
========================================================================================================================
Acronis Database Benchmark: version v1-main-dev
Connected to 'mysql' database: 10.11.9-MariaDB-ubu2204 (mariadb.org binary distribution)
========================================================================================================================

database settings checks:
 - innodb_buffer_pool_size (aka primary DB cache)........................ 134217728     ERR: min value should be at least 8589934592
 - innodb_log_file_size (aka InnoDB redo log size)....................... 100663296     ERR: min value should be at least 536870912
 - max_connections (aka max allowed number of DB connections)............ 151           ERR: min value should be at least 512
 - query_cache_type (aka query cache policy)............................. OFF           OK
 - performance_schema (aka performance-related server metrics)........... OFF           WRN: expected to have 'ON'

cleaning up the test tables ... done
creating the tables ... done
test: select-1                                ; rows-before-test:        0; time:  10.0 sec; workers:  1; loops:    22018; batch:    1; rate:     2202 select/sec;
test: insert-tenant                           ; rows-before-test:        0; time:  30.4 sec; workers:  1; loops:    10000; batch:    1; rate:    328.9 tenants/sec;
test: insert-cti                              ; rows-before-test:        0; time:   1.5 sec; workers:  1; loops:     1000; batch:    1; rate:    665.5 ctiEntity/sec;
test: insert-light                            ; rows-before-test:        0; time:  16.7 sec; workers:  1; loops:    10000; batch:    1; rate:    600.3 rows/sec;
test: insert-medium                           ; rows-before-test:        0; time:  16.7 sec; workers:  1; loops:    10000; batch:    1; rate:    598.9 rows/sec;
test: insert-heavy                            ; rows-before-test:        0; time:  17.1 sec; workers:  1; loops:    10000; batch:    1; rate:    586.5 rows/sec;
test: insert-json                             ; rows-before-test:        0; time:  20.3 sec; workers:  1; loops:    10000; batch:    1; rate:    492.2 rows/sec;
test: insert-ts-sql                           ; rows-before-test:        0; time:  10.3 sec; workers:  1; loops:    10240; batch:  256; rate:    997.4 values/sec;
test: insert-light                            ; rows-before-test:    10000; time:  47.0 sec; workers: 16; loops:   190000; batch:    1; rate:     4038 rows/sec;
test: insert-medium                           ; rows-before-test:    10000; time:  47.5 sec; workers: 16; loops:   190000; batch:    1; rate:     4000 rows/sec;
test: insert-json                             ; rows-before-test:    10000; time:  48.9 sec; workers: 16; loops:   190000; batch:    1; rate:     3889 rows/sec;
test: insert-ts-sql                           ; rows-before-test:    10240; time:  23.7 sec; workers: 16; loops:   192512; batch:  256; rate:     8113 values/sec;
test: update-medium                           ; rows-before-test:   200000; time:   8.1 sec; workers:  1; loops:     4000; batch:    1; rate:    494.2 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:   6.1 sec; workers:  1; loops:     4000; batch:    1; rate:    654.0 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:   5.7 sec; workers:  1; loops:     4000; batch:    1; rate:    696.9 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:   6.7 sec; workers:  1; loops:     4000; batch:    1; rate:    592.8 rows/sec;
test: update-medium                           ; rows-before-test:   200000; time:  14.3 sec; workers: 16; loops:    56000; batch:    1; rate:     3928 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:  14.5 sec; workers: 16; loops:    56000; batch:    1; rate:     3869 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:  14.0 sec; workers: 16; loops:    56000; batch:    1; rate:     4002 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:  13.7 sec; workers: 16; loops:    56000; batch:    1; rate:     4078 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    20210; batch:    1; rate:     2021 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    16477; batch:    1; rate:     1648 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   170320; batch:    1; rate:    17030 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   160845; batch:    1; rate:    16082 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    22224; batch:    1; rate:     2222 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    21793; batch:    1; rate:     2179 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   163019; batch:    1; rate:    16300 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   161443; batch:    1; rate:    16142 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    15615; batch:    1; rate:     1561 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    21391; batch:    1; rate:     2139 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    15995; batch:    1; rate:     1599 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    10373; batch:    1; rate:     1037 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:     5357; batch:    1; rate:    535.7 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers:  1; loops:  5340160; batch:  256; rate:   533991 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    20703; batch:    1; rate:     2070 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    20957; batch:    1; rate:     2096 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   105723; batch:    1; rate:    10548 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   161618; batch:    1; rate:    16159 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   105085; batch:    1; rate:    10504 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:    74766; batch:    1; rate:     7474 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:    38891; batch:    1; rate:     3886 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers: 16; loops: 40345088; batch:  256; rate:  4034042 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   156624; batch:    1; rate:    15660 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   152390; batch:    1; rate:    15237 rows/sec;
--------------------------------------------------------------------
select geomean: 6883
insert geomean: 1249
update geomean: 1549
cleaning up the test tables ... done
```

#### Production-like Galera Cluster

The acronis-db-bench loader is running in a dedicated VM on the same host as the database cluster, testing the MariaDB Galera cluster.

``` bash
> acronis-db-bench --connection-string="mysql://bench_user:bench_pass@tcp(<ip-address>:3306)/perf_db" -t all --limit 200000 --chunk 200000
========================================================================================================================
Acronis Database Benchmark: version v1-main-dev
Connected to 'mysql' database: 10.11.11-MariaDB (MariaDB Server)
========================================================================================================================

database settings checks:
 - innodb_buffer_pool_size (aka primary DB cache)........................ 3543138304    ERR: min value should be at least 8589934592
 - innodb_log_file_size (aka InnoDB redo log size)....................... 536870912     WRN: recommended value should be at least 2147483648
 - max_connections (aka max allowed number of DB connections)............ 1500          WRN: recommended value should be at least 2048
 - query_cache_type (aka query cache policy)............................. OFF           OK
 - performance_schema (aka performance-related server metrics)........... ON            OK

cleaning up the test tables ... done
creating the tables ... done
test: select-1                                ; rows-before-test:        0; time:  10.0 sec; workers:  1; loops:   139333; batch:    1; rate:    13933 select/sec;
test: insert-tenant                           ; rows-before-test:        0; time:  11.4 sec; workers:  1; loops:    10000; batch:    1; rate:    878.3 tenants/sec;
test: insert-cti                              ; rows-before-test:        0; time:   0.6 sec; workers:  1; loops:     1000; batch:    1; rate:     1578 ctiEntity/sec;
test: insert-light                            ; rows-before-test:        0; time:   5.2 sec; workers:  1; loops:    10000; batch:    1; rate:     1908 rows/sec;
test: insert-medium                           ; rows-before-test:        0; time:   5.5 sec; workers:  1; loops:    10000; batch:    1; rate:     1812 rows/sec;
test: insert-heavy                            ; rows-before-test:        0; time:   8.1 sec; workers:  1; loops:    10000; batch:    1; rate:     1233 rows/sec;
test: insert-json                             ; rows-before-test:        0; time:   6.7 sec; workers:  1; loops:    10000; batch:    1; rate:     1490 rows/sec;
test: insert-ts-sql                           ; rows-before-test:        0; time:   2.2 sec; workers:  1; loops:    10240; batch:  256; rate:     4572 values/sec;
test: insert-light                            ; rows-before-test:    10000; time:  31.5 sec; workers: 16; loops:   190000; batch:    1; rate:     6030 rows/sec;
test: insert-medium                           ; rows-before-test:    10000; time:  34.8 sec; workers: 16; loops:   190000; batch:    1; rate:     5453 rows/sec;
test: insert-json                             ; rows-before-test:    10000; time:  43.8 sec; workers: 16; loops:   190000; batch:    1; rate:     4340 rows/sec;
test: insert-ts-sql                           ; rows-before-test:    10240; time:  12.7 sec; workers: 16; loops:   192512; batch:  256; rate:    15212 values/sec;
test: update-medium                           ; rows-before-test:   200000; time:   1.8 sec; workers:  1; loops:     4000; batch:    1; rate:     2278 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:   2.0 sec; workers:  1; loops:     4000; batch:    1; rate:     1995 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:   1.7 sec; workers:  1; loops:     4000; batch:    1; rate:     2288 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:   1.4 sec; workers:  1; loops:     4000; batch:    1; rate:     2943 rows/sec;
test: update-medium                           ; rows-before-test:   200000; time:   6.2 sec; workers: 16; loops:    56000; batch:    1; rate:     9072 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:   8.0 sec; workers: 16; loops:    56000; batch:    1; rate:     7019 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:   7.0 sec; workers: 16; loops:    56000; batch:    1; rate:     7983 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:   4.3 sec; workers: 16; loops:    56000; batch:    1; rate:    12959 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    72785; batch:    1; rate:     7278 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    71370; batch:    1; rate:     7137 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   221379; batch:    1; rate:    22136 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   207939; batch:    1; rate:    20792 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    92713; batch:    1; rate:     9271 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    89119; batch:    1; rate:     8912 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   325381; batch:    1; rate:    32536 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   303436; batch:    1; rate:    30341 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    20759; batch:    1; rate:     2073 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    60931; batch:    1; rate:     6093 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    20327; batch:    1; rate:     2033 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    37881; batch:    1; rate:     3788 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:     5738; batch:    1; rate:    573.8 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers:  1; loops: 12724736; batch:  256; rate:  1272452 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    61717; batch:    1; rate:     6171 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    53867; batch:    1; rate:     5387 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.1 sec; workers: 16; loops:    39751; batch:    1; rate:     3941 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   176733; batch:    1; rate:    17671 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:    48268; batch:    1; rate:     4808 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:    90826; batch:    1; rate:     9081 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:    12024; batch:    1; rate:     1201 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers: 16; loops: 35033600; batch:  256; rate:  3502764 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   172748; batch:    1; rate:    17273 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   148585; batch:    1; rate:    14857 rows/sec;
--------------------------------------------------------------------
select geomean: 11296
insert geomean: 2813
update geomean: 4603
cleaning up the test tables ... done
```

### PostgreSQL

#### Macbook PRO M1 Docker Container

The acronis-db-bench loader is running directly on the MacBook M1 PRO host, testing the PostgreSQL container deployed in Docker.

``` bash
> acronis-db-bench --connection-string="postgres://root:password@localhost:5432/perfkit_db_ci?sslmode=disable" -t all --limit 200000 --chunk 200000
========================================================================================================================
Acronis Database Benchmark: version v1-main-dev
Connected to 'postgres' database: PostgreSQL 13.16 on aarch64-unknown-linux-musl, compiled by gcc (Alpine 13.2.1_git20240309) 13.2.1 20240309, 64-bit
========================================================================================================================

database settings checks:
 - shared_buffers (aka primary DB cache)................................. 128MB         ERR: min value should be at least 1073741824
 - effective_cache_size (aka OS cache)................................... 4GB           WRN: recommended value should be at least 8589934592
 - work_mem (aka Mem for internal sorting & hash tables)................. 4MB           ERR: min value should be at least 8388608
 - maintenance_work_mem (aka Mem for VACUUM, CREATE INDEX, etc).......... 64MB          ERR: min value should be at least 134217728
 - wal_buffers (aka Mem used in shared memory for WAL data).............. 4MB           ERR: min value should be at least 8388608
 - max_wal_size (aka max WAL size)....................................... 1GB           OK
 - min_wal_size (aka min WAL size)....................................... 80MB          OK
 - max_connections (aka max allowed number of DB connections)............ 100           ERR: min value should be at least 512
 - random_page_cost (aka it should be 1.1 as it is typical for SSD)...... 4             WRN: expected to have '1.1'
 - track_activities (aka collects session activities info)............... on            OK
 - track_counts (aka track tables/indexes access count).................. on            OK
 - log_checkpoints (aka logs information about each checkpoint).......... off           WRN: expected to have 'on'
 - jit (aka JIT compilation feature)..................................... on            WRN: expected to have 'off'
 - wal_level (aka amount of information written to WAL).................. replica       WRN: expected to have 'minimal'
 - max_wal_senders (aka max num of simultaneous replication connections). 10            WRN: expected to have '0'

cleaning up the test tables ... done
creating the tables ... done
test: select-1                                ; rows-before-test:        0; time:  10.0 sec; workers:  1; loops:    21333; batch:    1; rate:     2133 select/sec;
test: insert-tenant                           ; rows-before-test:        0; time:  26.3 sec; workers:  1; loops:    10000; batch:    1; rate:    379.7 tenants/sec;
test: insert-cti                              ; rows-before-test:        0; time:   1.4 sec; workers:  1; loops:     1000; batch:    1; rate:    695.8 ctiEntity/sec;
test: insert-light                            ; rows-before-test:        0; time:  13.6 sec; workers:  1; loops:    10000; batch:    1; rate:    735.7 rows/sec;
test: insert-medium                           ; rows-before-test:        0; time:  13.9 sec; workers:  1; loops:    10000; batch:    1; rate:    721.2 rows/sec;
test: insert-heavy                            ; rows-before-test:        0; time:  15.9 sec; workers:  1; loops:    10000; batch:    1; rate:    629.0 rows/sec;
test: insert-json                             ; rows-before-test:        0; time:  14.7 sec; workers:  1; loops:    10000; batch:    1; rate:    678.2 rows/sec;
test: insert-ts-sql                           ; rows-before-test:        0; time:   8.5 sec; workers:  1; loops:    10240; batch:  256; rate:     1211 values/sec;
test: insert-light                            ; rows-before-test:    10000; time:  47.4 sec; workers: 16; loops:   190000; batch:    1; rate:     4012 rows/sec;
test: insert-medium                           ; rows-before-test:    10000; time:  48.1 sec; workers: 16; loops:   190000; batch:    1; rate:     3952 rows/sec;
test: insert-json                             ; rows-before-test:    10000; time:  49.8 sec; workers: 16; loops:   190000; batch:    1; rate:     3813 rows/sec;
test: insert-ts-sql                           ; rows-before-test:    10240; time:  23.6 sec; workers: 16; loops:   192512; batch:  256; rate:     8143 values/sec;
test: update-medium                           ; rows-before-test:   200000; time:   5.7 sec; workers:  1; loops:     4000; batch:    1; rate:    696.9 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:   6.2 sec; workers:  1; loops:     4000; batch:    1; rate:    645.0 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:   5.8 sec; workers:  1; loops:     4000; batch:    1; rate:    690.1 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:   5.7 sec; workers:  1; loops:     4000; batch:    1; rate:    696.9 rows/sec;
test: update-medium                           ; rows-before-test:   200000; time:  14.1 sec; workers: 16; loops:    56000; batch:    1; rate:     3962 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:  15.4 sec; workers: 16; loops:    56000; batch:    1; rate:     3643 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:  14.6 sec; workers: 16; loops:    56000; batch:    1; rate:     3838 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:  14.5 sec; workers: 16; loops:    56000; batch:    1; rate:     3857 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    22289; batch:    1; rate:     2229 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    21148; batch:    1; rate:     2115 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   152005; batch:    1; rate:    15199 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   143555; batch:    1; rate:    14353 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    22959; batch:    1; rate:     2296 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    21803; batch:    1; rate:     2180 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   154234; batch:    1; rate:    15422 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   147188; batch:    1; rate:    14717 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:     8973; batch:    1; rate:    897.3 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    17604; batch:    1; rate:     1760 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:     6734; batch:    1; rate:    673.3 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    16490; batch:    1; rate:     1649 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    17242; batch:    1; rate:     1724 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers:  1; loops:  5400320; batch:  256; rate:   540028 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    17479; batch:    1; rate:     1748 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    16815; batch:    1; rate:     1681 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:    67235; batch:    1; rate:     6720 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   136226; batch:    1; rate:    13621 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:    50480; batch:    1; rate:     5046 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   122388; batch:    1; rate:    12236 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   130796; batch:    1; rate:    13078 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers: 16; loops: 37069568; batch:  256; rate:  3706499 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   123420; batch:    1; rate:    12339 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   105631; batch:    1; rate:    10561 rows/sec;
--------------------------------------------------------------------
select geomean: 6687
insert geomean: 1384
update geomean: 1615
cleaning up the test tables ... done
```

#### Production-like Patroni Cluster

The acronis-db-bench loader is running in a dedicated VM on the same host as the database cluster, testing the PostgreSQL Patroni cluster.

``` bash
> acronis-db-bench --connection-string="postgresql://bench_user:bench_pass@<ip-address>:5432/perf_db?sslmode=disable" -t all --limit 200000 --chunk 200000
========================================================================================================================
Acronis Database Benchmark: version v1-main-dev
Connected to 'postgres' database: PostgreSQL 14.6 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-44), 64-bit
========================================================================================================================

database settings checks:
 - shared_buffers (aka primary DB cache)................................. 4032MB        WRN: recommended value should be at least 4294967296
 - effective_cache_size (aka OS cache)................................... 12096MB       OK
 - work_mem (aka Mem for internal sorting & hash tables)................. 8MB           WRN: recommended value should be at least 16777216
 - maintenance_work_mem (aka Mem for VACUUM, CREATE INDEX, etc).......... 2GB           OK
 - wal_buffers (aka Mem used in shared memory for WAL data).............. 16MB          OK
 - max_wal_size (aka max WAL size)....................................... 160MB         ERR: min value should be at least 536870912
 - min_wal_size (aka min WAL size)....................................... 1GB           OK
 - max_connections (aka max allowed number of DB connections)............ 1000          WRN: recommended value should be at least 2048
 - random_page_cost (aka it should be 1.1 as it is typical for SSD)...... 1.1           OK
 - track_activities (aka collects session activities info)............... on            OK
 - track_counts (aka track tables/indexes access count).................. on            OK
 - log_checkpoints (aka logs information about each checkpoint).......... off           WRN: expected to have 'on'
 - jit (aka JIT compilation feature)..................................... off           OK
 - wal_level (aka amount of information written to WAL).................. logical       WRN: expected to have 'replica'

cleaning up the test tables ... done
creating the tables ... done
test: select-1                                ; rows-before-test:        0; time:  10.0 sec; workers:  1; loops:   181791; batch:    1; rate:    18179 select/sec;
test: insert-tenant                           ; rows-before-test:        0; time:  11.3 sec; workers:  1; loops:    10000; batch:    1; rate:    887.9 tenants/sec;
test: insert-cti                              ; rows-before-test:        0; time:   0.6 sec; workers:  1; loops:     1000; batch:    1; rate:     1626 ctiEntity/sec;
test: insert-light                            ; rows-before-test:        0; time:   5.4 sec; workers:  1; loops:    10000; batch:    1; rate:     1858 rows/sec;
test: insert-medium                           ; rows-before-test:        0; time:   6.0 sec; workers:  1; loops:    10000; batch:    1; rate:     1671 rows/sec;
test: insert-heavy                            ; rows-before-test:        0; time:  10.9 sec; workers:  1; loops:    10000; batch:    1; rate:    916.8 rows/sec;
test: insert-json                             ; rows-before-test:        0; time:   7.3 sec; workers:  1; loops:    10000; batch:    1; rate:     1361 rows/sec;
test: insert-ts-sql                           ; rows-before-test:        0; time:   1.6 sec; workers:  1; loops:    10240; batch:  256; rate:     6606 values/sec;
test: insert-light                            ; rows-before-test:    10000; time:  20.4 sec; workers: 16; loops:   190000; batch:    1; rate:     9292 rows/sec;
test: insert-medium                           ; rows-before-test:    10000; time:  20.7 sec; workers: 16; loops:   190000; batch:    1; rate:     9195 rows/sec;
test: insert-json                             ; rows-before-test:    10000; time:  27.9 sec; workers: 16; loops:   190000; batch:    1; rate:     6805 rows/sec;
test: insert-ts-sql                           ; rows-before-test:    10240; time:   9.9 sec; workers: 16; loops:   192512; batch:  256; rate:    19435 values/sec;
test: update-medium                           ; rows-before-test:   200000; time:   3.9 sec; workers:  1; loops:     4000; batch:    1; rate:     1013 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:   4.0 sec; workers:  1; loops:     4000; batch:    1; rate:     1000 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:   2.9 sec; workers:  1; loops:     4000; batch:    1; rate:     1399 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:   3.8 sec; workers:  1; loops:     4000; batch:    1; rate:     1060 rows/sec;
test: update-medium                           ; rows-before-test:   200000; time:   7.1 sec; workers: 16; loops:    56000; batch:    1; rate:     7939 rows/sec;
test: update-heavy                            ; rows-before-test:    10000; time:  14.7 sec; workers: 16; loops:    56000; batch:    1; rate:     3804 rows/sec;
test: update-heavy-partial-sameval            ; rows-before-test:    10000; time:   8.8 sec; workers: 16; loops:    56000; batch:    1; rate:     6388 rows/sec;
test: update-heavy-sameval                    ; rows-before-test:    10000; time:   8.5 sec; workers: 16; loops:    56000; batch:    1; rate:     6592 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    98071; batch:    1; rate:     9807 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    78301; batch:    1; rate:     7830 rows/sec;
test: select-medium-rand                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   269672; batch:    1; rate:    26965 rows/sec;
test: select-heavy-rand                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   190647; batch:    1; rate:    19063 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:   113637; batch:    1; rate:    11364 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    87797; batch:    1; rate:     8780 rows/sec;
test: select-medium-last                      ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:   328180; batch:    1; rate:    32816 rows/sec;
test: select-heavy-last                       ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   203296; batch:    1; rate:    20328 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    10357; batch:    1; rate:     1036 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    57600; batch:    1; rate:     5760 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:     6584; batch:    1; rate:    658.3 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    23296; batch:    1; rate:     2330 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers:  1; loops:    28516; batch:    1; rate:     2852 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers:  1; loops: 19835136; batch:  256; rate:  1983489 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    37132; batch:    1; rate:     3713 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers:  1; loops:    34235; batch:    1; rate:     3423 rows/sec;
test: select-heavy-last-in-tenant             ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:    21027; batch:    1; rate:     2101 rows/sec;
test: select-heavy-rand-in-tenant-like        ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:   134475; batch:    1; rate:    13446 rows/sec;
test: select-heavy-last-in-tenant-and-cti     ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:    13987; batch:    1; rate:     1395 rows/sec;
test: select-json-by-indexed-value            ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:    48288; batch:    1; rate:     4822 rows/sec;
test: select-json-by-nonindexed-value         ; rows-before-test:   200000; time:  10.0 sec; workers: 16; loops:    61537; batch:    1; rate:     6152 rows/sec;
test: select-ts-sql                           ; rows-before-test:   202752; time:  10.0 sec; workers: 16; loops: 47796736; batch:  256; rate:  4779219 values/sec;
test: select-heavy-minmax-in-tenant           ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:    84435; batch:    1; rate:     8442 rows/sec;
test: select-heavy-minmax-in-tenant-and-state ; rows-before-test:    10000; time:  10.0 sec; workers: 16; loops:    76086; batch:    1; rate:     7607 rows/sec;
--------------------------------------------------------------------
select geomean: 10048
insert geomean: 3243
update geomean: 2571
cleaning up the test tables ... done
```
