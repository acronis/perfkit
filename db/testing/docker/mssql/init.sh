#!/usr/bin/env bash

# Wait for SQL Server to be ready (max 30 attempts)
for i in {1..30}; do
  if /opt/mssql-tools/bin/sqlcmd -S mssql -U sa -P MyP@ssw0rd123 -Q "SELECT 1" -b -o /dev/null; then
    echo "SQL Server is ready!"
    break
  fi
  echo "Waiting for SQL Server... (attempt $i/30)";
  sleep 5;
done

# Create database and user
/opt/mssql-tools/bin/sqlcmd -S mssql -U sa -P MyP@ssw0rd123 -Q "
  IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'perfkit_db_ci')
  BEGIN
    CREATE DATABASE perfkit_db_ci;
  END
  GO
  USE perfkit_db_ci;
  GO
  IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'perfkit_db_runner')
  BEGIN
    CREATE LOGIN perfkit_db_runner WITH PASSWORD = 'MyP@ssw0rd123';
  END
  GO
  IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = 'perfkit_db_runner')
  BEGIN
    CREATE USER perfkit_db_runner FOR LOGIN perfkit_db_runner;
  END
  GO
  ALTER ROLE db_owner ADD MEMBER perfkit_db_runner;
  GO
"
echo "Database and user created successfully" 