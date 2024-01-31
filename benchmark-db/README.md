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