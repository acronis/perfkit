package db

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"
)

type DialectName string

// Supported dialect
const (
	SQLITE        DialectName = "sqlite"        // SQLITE is the SQLite driver name
	SQLITE3       DialectName = "sqlite3"       // SQLITE3 is the SQLite driver name
	POSTGRES      DialectName = "postgres"      // POSTGRES is the PostgreSQL driver name
	MYSQL         DialectName = "mysql"         // MYSQL is the MySQL driver name
	MSSQL         DialectName = "mssql"         // MSSQL is the Microsoft SQL Server driver name
	CLICKHOUSE    DialectName = "clickhouse"    // CLICKHOUSE is the ClickHouse driver name
	CASSANDRA     DialectName = "cassandra"     // CASSANDRA is the Cassandra driver name
	ELASTICSEARCH DialectName = "elasticsearch" // ELASTICSEARCH is the Elasticsearch driver name
	OPENSEARCH    DialectName = "opensearch"    // OPENSEARCH is the OpenSearch driver name
)

// Special conditions for searching
const (
	// SpecialConditionIsNull can be used as a value in search conditions to check if a field is NULL
	// Example usage:
	//   Where: map[string][]string{
	//       "email": []string{"isnull()"}, // Will generate: WHERE email IS NULL
	//   }
	SpecialConditionIsNull = "isnull()"

	// SpecialConditionIsNotNull can be used as a value in search conditions to check if a field is NOT NULL
	// Example usage:
	//   Where: map[string][]string{
	//       "email": []string{"notnull()"}, // Will generate: WHERE email IS NOT NULL
	//   }
	SpecialConditionIsNotNull = "notnull()"
)

// Connector is an interface for registering database connectors without knowing the specific connector implementations.
// It provides methods to create connection pools and determine the dialect name for a given database scheme.
//
// To add a new database adapter:
//
//  1. Create a new file named after your database (e.g., db/sql/newdb.go or db/newdb/newdb.go)
//
//  2. Define your connector struct:
//     ```go
//     type newDBConnector struct{}
//     ```
//
//  3. Register your connector in init():
//     ```
//     func init() {
//     // You can register multiple schema names for the same connector
//     for _, dbNameStyle := range []string{"newdb", "newdb-alt"} {
//     if err := db.Register(dbNameStyle, &newDBConnector{}); err != nil {
//     panic(err)
//     }
//     }
//     }
//     ```
//
//  4. Implement ConnectionPool method:
//     ```go
//     func (c *newDBConnector) ConnectionPool(cfg db.Config) (db.Database, error) {
//     // Parse connection string if needed
//     // Initialize connection pool
//     // Set connection parameters (max connections, timeouts, etc)
//     // Return database interface implementation
//     }
//     ```
//
//  5. Implement DialectName method:
//     ```go
//     func (c *newDBConnector) DialectName(scheme string) (db.DialectName, error) {
//     return db.NEWDB, nil // Add your dialect to DialectName type
//     }
//     ```
//
//  6. Import your adapter package in the main application:
//     ```go
//     import (
//     _ "github.com/acronis/perfkit/db/newdb" // newdb driver
//     )
//     ```
//     This is required for the init() function to be called and register the adapter.
//     Example of importing existing adapters:
//     ```go
//     import (
//     _ "github.com/acronis/perfkit/db/es"  // es drivers
//     _ "github.com/acronis/perfkit/db/sql" // sql drivers
//     )
//     ```
//
// Common implementation patterns:
// - SQL databases: Extend sqlDatabase struct and use database/sql package
// - NoSQL databases: Create custom Database interface implementation
// - Always include proper error handling with descriptive messages
// - Implement connection string parsing and validation
// - Handle connection pool management
// - Support query logging where applicable
type Connector interface {
	// ConnectionPool creates a new database connection pool using the provided configuration
	ConnectionPool(cfg Config) (Database, error)

	// DialectName returns the database dialect name for a given connection scheme
	DialectName(scheme string) (DialectName, error)
}

var (
	// dbRegistry stores registered database connectors mapped by their schema names
	dbRegistry   = make(map[string]Connector)
	registryLock = sync.Mutex{}
)

// Register registers a database connector for a given schema.
// This function is typically called from init() functions in database-specific packages.
// Returns an error if the schema is already registered.
//
// Example usage:
//
//	func init() {
//	    if err := db.Register("mysql", &mysqlConnector{}); err != nil {
//	        panic(err)
//	    }
//	}
func Register(schema string, conn Connector) error {
	registryLock.Lock()
	defer registryLock.Unlock()

	if _, ok := dbRegistry[schema]; ok {
		return fmt.Errorf("schema %s already exists", schema)
	}

	dbRegistry[schema] = conn

	return nil
}

// Config is a struct for database configuration settings
type Config struct {
	// ConnString is the database connection string/URL. Format varies by database type:
	// - Cassandra: cql://user:pass@host:port/keyspace
	// - ClickHouse: clickhouse://host:port/dbname
	// - MSSQL: sqlserver://user:pass@host:port/dbname
	// - MySQL: mysql://user:pass@host:port/dbname
	// - PostgreSQL: postgres://user:pass@host:port/dbname
	// - SQLite: sqlite:///path/to/file.db or sqlite://:memory:
	// - Elasticsearch/OpenSearch: es://user:pass@host:port
	ConnString string

	// MaxOpenConns controls the maximum number of open connections to the database.
	// Usage by database:
	// - Cassandra: Uses math.Max(1, MaxOpenConns) for connection pool size
	// - ClickHouse: Uses math.Max(1, MaxConnLifetime) for max connections
	// - MSSQL: Uses math.Max(1, MaxOpenConns) for both max open and idle connections
	// - MySQL: Uses math.Max(1, MaxOpenConns) for both max open and idle connections
	// - PostgreSQL: Uses math.Max(1, MaxOpenConns) for both max open and idle connections
	// - SQLite: Sets max open and idle connections directly
	// - Elasticsearch/OpenSearch: Not used (managed by client library)
	MaxOpenConns int

	// MaxConnLifetime is the maximum amount of time a connection may be reused.
	// Usage by database:
	// - Cassandra: Not used
	// - ClickHouse: Used if > 0 to set connection max lifetime
	// - MSSQL: Used if > 0 to set connection max lifetime
	// - MySQL: Used if > 0 to set connection max lifetime
	// - PostgreSQL: Sets connection max lifetime directly
	// - SQLite: Not used
	// - Elasticsearch/OpenSearch: Not used
	MaxConnLifetime time.Duration

	// MaxPacketSize controls the maximum size of network packets/requests.
	// Usage by database:
	// - MySQL: Added to connection string as maxAllowedPacket parameter
	// - All other databases: Not used
	MaxPacketSize int

	// QueryStringInterpolation controls how SQL queries are constructed for inserts:
	//
	// When false (default, recommended):
	// - Uses parameterized queries with placeholders
	// - Values are passed separately from the query string
	// - Provides better SQL injection protection
	// - Different placeholder syntax per dialect:
	//   - PostgreSQL: Uses $1, $2, $3, etc.
	//   Example:
	//   ```sql
	//   INSERT INTO users(id, name) VALUES ($1, $2), ($3, $4);
	//   -- values passed separately: [1, "john", 2, "jane"]
	//   ```
	//   - Other SQL databases: Uses ?
	//   Example:
	//   ```sql
	//   INSERT INTO users(id, name) VALUES (?, ?), (?, ?);
	//   -- values passed separately: [1, "john", 2, "jane"]
	//   ```
	//
	// When true:
	// - Uses string interpolation
	// - Values are converted to strings and embedded directly in the query
	// - Each value is formatted according to its type and dialect
	// - Less secure but may be needed for specific database requirements
	// Example:
	// ```sql
	// INSERT INTO users(id, name) VALUES (1, 'john'), (2, 'jane');
	// ```
	//
	// Security Considerations:
	// - Prefer parameterized queries (false) for better SQL injection protection
	// - Use string interpolation (true) only when required by specific database features
	// - Values are still escaped according to dialect rules when using interpolation
	QueryStringInterpolation bool

	// DryRun controls whether SQL operations are actually executed:
	//
	// When true:
	// - Queries are logged but not executed
	// - All operations return success without affecting the database
	// - Useful for testing and debugging SQL generation
	// - Logs include special markers for skipped operations
	//
	// Examples of logged output:
	// 1. Single line queries:
	//    ```sql
	//    -- INSERT INTO users(id, name) VALUES (1, 'john') -- skip because of 'dry-run' mode
	//    ```
	//
	// 2. Multi-line queries:
	//    ```sql
	//    -- skip because of 'dry-run' mode
	//    /*
	//    BEGIN BATCH
	//        INSERT INTO users(id, name) VALUES (1, 'john');
	//        INSERT INTO users(id, name) VALUES (2, 'jane');
	//    APPLY BATCH;
	//    */
	//    ```
	//
	// 3. Transactions:
	//    ```sql
	//    -- BEGIN -- skip because of 'dry-run' mode
	//    -- INSERT INTO users(id) VALUES (1) -- skip because of 'dry-run' mode
	//    -- COMMIT -- skip because of 'dry-run' mode
	//    ```
	DryRun bool

	// UseTruncate controls table cleanup behavior:
	//
	// When true:
	// - Uses TRUNCATE TABLE instead of DROP TABLE
	// - Keeps table structure but removes all data
	// - Different behavior per dialect:
	//   - PostgreSQL: "TRUNCATE TABLE tablename CASCADE"
	//   - Others: "TRUNCATE TABLE tablename"
	//
	// When false:
	// - Uses "DROP TABLE IF EXISTS tablename"
	// - Removes both data and table structure
	// - Consistent behavior across all dialects
	//
	// Example Usage:
	// 1. With UseTruncate = true:
	//    ```sql
	//    -- PostgreSQL
	//    TRUNCATE TABLE users CASCADE;
	//
	//    -- Other databases
	//    TRUNCATE TABLE users;
	//    ```
	//
	// 2. With UseTruncate = false:
	//    ```sql
	//    DROP TABLE IF EXISTS users;
	//    ```
	//
	// Use Cases:
	// - Set true when you want to preserve table structure:
	//   - Keeping indexes and constraints
	//   - Maintaining foreign key relationships
	//   - Faster when recreating same data structure
	// - Set false when you want complete removal:
	//   - Clean slate for schema changes
	//   - Removing test databases
	//   - Full cleanup operations
	UseTruncate bool

	// TLSEnabled controls whether TLS/HTTPS is used for Elasticsearch connections:
	//
	// When true:
	// - Uses HTTPS scheme for connections
	// - Automatically enabled when username/password are provided
	// - Requires proper TLS configuration
	//
	// Example URL transformations:
	// ```go
	// // TLSEnabled = false
	// "http://localhost:9200"
	//
	// // TLSEnabled = true
	// "https://localhost:9200"
	// ```
	TLSEnabled bool

	// TLSCACert contains the CA certificate for TLS verification
	//
	// When provided:
	// - Creates a new certificate pool with the provided CA cert
	// - Used to verify server certificates
	// - Required for secure production deployments
	//
	// When empty:
	// - Uses InsecureSkipVerify=true (not recommended for production)
	// - Allows any server certificate
	// - Useful for development/testing
	//
	// Example usage:
	// ```go
	// cfg := db.Config{
	//     TLSEnabled: true,
	//     TLSCACert: []byte("-----BEGIN CERTIFICATE-----\n..."),
	// }
	// ```
	//
	// Security considerations:
	// 1. Always provide TLSCACert in production
	// 2. TLS is automatically enabled with authentication
	// 3. Verify server identity to prevent MITM attacks
	TLSCACert []byte

	// QueryLogger logs all SQL queries before execution
	// When configured:
	// 1. Regular Queries:
	//    ```sql
	//    SELECT * FROM users WHERE id > 100;
	//    ```
	//
	// 2. Prepared Statements:
	//    ```sql
	//    PREPARE stmt FROM 'SELECT * FROM users WHERE id = $1';
	//    EXECUTE stmt;
	//    DEALLOCATE PREPARE stmt;
	//    ```
	//
	// 3. DryRun Mode:
	//    ```sql
	//    -- INSERT INTO users(id, name) VALUES (1, 'john') -- skip because of 'dry-run' mode
	//    ```
	//
	// 4. Multi-line Queries:
	//    ```sql
	//    -- skip because of 'dry-run' mode
	//    /*
	//    BEGIN BATCH
	//        INSERT INTO users(id, name) VALUES (1, 'john');
	//        INSERT INTO users(id, name) VALUES (2, 'jane');
	//    APPLY BATCH;
	//    */
	//    ```
	QueryLogger Logger

	// ReadRowsLogger logs the data returned from queries
	// When configured:
	// 1. Regular Row Output:
	//    ```
	//    Row: id=1 name="john" age=25
	//    Row: id=2 name="jane" age=30
	//    ```
	//
	// 2. With UseTruncate=true:
	//    ```
	//    Row: id=1 name="john" age=25
	//    Row: id=2 name="jane" age=30
	//    ... truncated ...
	//    ```
	//    Note: Truncates after maxRowsToPrint (10) rows
	//
	// 3. Single Row Queries:
	//    ```
	//    Row: count=42
	//    ```
	//
	// Usage Example:
	// ```go
	// type customLogger struct{}
	//
	// func (l *customLogger) Log(format string, args ...interface{}) {
	//     fmt.Printf(format + "\n", args...)
	// }
	//
	// cfg := db.Config{
	//     QueryLogger: &customLogger{},
	//     ReadRowsLogger: &customLogger{},
	// }
	// ```
	ReadRowsLogger Logger

	// ExplainLogger receives query execution plan output when Explain is true.
	// The output format varies by dialect:
	//
	// 1. PostgreSQL:
	//    ```sql
	//    SELECT * FROM users WHERE id > 100;
	//    [args]
	//      Seq Scan on users (cost=0.00..35.50 rows=500 width=244)
	//        Filter: (id > 100)
	//      Planning Time: 0.083 ms
	//      Execution Time: 0.184 ms
	//    ```
	//
	// 2. MySQL:
	//    ```sql
	//    SELECT * FROM users WHERE id > 100;
	//    [args]
	//      id: 1
	//      select_type: SIMPLE
	//      table: users
	//      type: range
	//      possible_keys: PRIMARY
	//      key: PRIMARY
	//      rows: 500
	//    ```
	//
	// 3. SQLite:
	//    ```sql
	//    SELECT * FROM users WHERE id > 100;
	//    [args]
	//    ID: 1, Parent: 0, Not Used: 0, Detail: SCAN TABLE users
	//    ```
	//
	// 4. Cassandra:
	//    ```sql
	//    TRACING ON; SELECT * FROM users WHERE id > 100;
	//    [args]
	//      <tracing output>
	//    ```
	//
	// Implementation details:
	// - Activated when both Explain=true and ExplainLogger is set
	// - Adds EXPLAIN prefix to queries based on dialect
	// - Captures and formats execution plan output
	// - Works with Select(), Query(), and QueryRow() operations
	// - Returns error if explain not supported by dialect
	ExplainLogger Logger

	// SystemLogger logs system-level database operations and events.
	// Primary uses:
	// 1. Embedded PostgreSQL:
	//    - Logs database initialization and shutdown
	//    - Reports data directory creation and usage
	//    - Captures PostgreSQL server logs
	//    Example output:
	//    ```
	//    -- embedded postgres: creating data dir: /home/user/.embedded-postgres-go/data
	//    -- embedded postgres: using data dir: /home/user/.embedded-postgres-go/data
	//    -- embedded postgres: database system is ready to accept connections
	//    ```
	//
	// 2. Other Database Systems:
	//    - Logs critical system events
	//    - Reports initialization status
	//    - Captures server-side messages
	SystemLogger Logger

	// LogOperationsTime controls whether to log the time taken for database operations.
	// When enabled (true), all SQL operations include timing information in logs.
	//
	// Features:
	// - Adds timing information (duration) to all query logs
	// - Works with queries, prepared statements, and transactions
	// - Measurements are taken using high-precision time.Since()
	//
	// Example output with LogOperationsTime=true:
	// ```
	// INSERT INTO users(id, name) VALUES (1, 'john') -- duration: 2.3ms
	// SELECT * FROM users WHERE id = 1 -- duration: 1.5ms
	// BEGIN -- duration: 0.2ms
	// COMMIT -- duration: 5.1ms
	// ```
	//
	// Without LogOperationsTime (false):
	// ```
	// INSERT INTO users(id, name) VALUES (1, 'john')
	// SELECT * FROM users WHERE id = 1
	// BEGIN
	// COMMIT
	// ```
	//
	// Usage recommendation:
	// - Enable during development and testing for performance insights
	// - Use for debugging slow queries and transactions
	// - Can be enabled in production for detailed monitoring
	LogOperationsTime bool
}

// Open opens a database connection using the provided configuration.
// The function works in the following steps:
//
//  1. Parse the connection string to extract the scheme:
//     ```go
//     "mysql://user:pass@localhost:3306/dbname" -> scheme: "mysql"
//     "postgres://localhost:5432/mydb" -> scheme: "postgres"
//     "es://localhost:9200" -> scheme: "es"
//     ```
//
// 2. Look up the registered connector for the scheme:
//   - Uses dbRegistry to find the appropriate Connector implementation
//   - Returns error if no connector is registered for the scheme
//
// 3. Create connection pool using the connector:
//   - Calls ConnectionPool() on the found connector
//   - Passes the complete Config struct to the connector
//   - Returns the Database interface implementation
//
// Example usage:
//
//	```go
//	cfg := db.Config{
//	    ConnString: "mysql://user:pass@localhost:3306/dbname",
//	    MaxOpenConns: 10,
//	    MaxConnLifetime: time.Minute * 5,
//	}
//	db, err := db.Open(cfg)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//	```
//
// Supported database types (when appropriate drivers are imported):
// - MySQL: "mysql://"
// - PostgreSQL: "postgres://" or "postgresql://"
// - SQLite: "sqlite://"
// - MSSQL: "mssql://" or "sqlserver://"
// - Cassandra: "cql://"
// - ClickHouse: "clickhouse://"
// - Elasticsearch: "es://" or "elastic://" or "elasticsearch://"
// - OpenSearch: "os://" or "opensearch://"
func Open(cfg Config) (Database, error) {
	var scheme, _, err = ParseScheme(cfg.ConnString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s to scheme: %v", cfg.ConnString, err)
	}

	registryLock.Lock()
	var conn, ok = dbRegistry[scheme]
	registryLock.Unlock()

	if !ok {
		return nil, fmt.Errorf("scheme %s doesn't exist in registry", scheme)
	}

	return conn.ConnectionPool(cfg)
}

// GetDialectName returns the database dialect name for a given connection string.
// This is useful when you need to know the database type without actually
// establishing a connection.
//
// The function works similarly to Open:
// 1. Parses the connection string to get the scheme
// 2. Looks up the registered connector
// 3. Calls DialectName() on the connector
//
// Returns a DialectName enum value that identifies the database type:
// - db.MYSQL for MySQL
// - db.POSTGRES for PostgreSQL
// - db.SQLITE for SQLite
// - db.MSSQL for SQL Server
// - db.CASSANDRA for Cassandra
// - db.CLICKHOUSE for ClickHouse
// - db.ELASTICSEARCH for Elasticsearch
// - db.OPENSEARCH for OpenSearch
//
// Example usage:
//
//	```go
//	dialect, err := db.GetDialectName("mysql://localhost:3306/dbname")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	switch dialect {
//	case db.MYSQL:
//	    // Handle MySQL specific logic
//	case db.POSTGRES:
//	    // Handle PostgreSQL specific logic
//	}
//	```
//
// Note: The appropriate database driver must be imported for the scheme
// to be recognized, even though no connection is established.
func GetDialectName(cs string) (DialectName, error) {
	var scheme, _, err = ParseScheme(cs)
	if err != nil {
		return "", fmt.Errorf("failed to parse %s to scheme: %v", cs, err)
	}

	registryLock.Lock()
	var conn, ok = dbRegistry[scheme]
	registryLock.Unlock()

	if !ok {
		return "", fmt.Errorf("scheme %s doesn't exist in registry", scheme)
	}

	return conn.DialectName(scheme)
}

// Logger is an interface for logging database operations.
// Example implementation:
//
//	```go
//	type customLogger struct{}
//
//	func (l *customLogger) Log(format string, args ...interface{}) {
//	    fmt.Printf(format + "\n", args...)
//	}
//
//	cfg := db.Config{
//	    QueryLogger: &customLogger{},
//	    ReadRowsLogger: &customLogger{},
//	}
//	```
//
// Note: The Logger interface is designed to be simple and flexible,
// allowing for easy integration with existing logging systems.
type Logger interface {
	Log(format string, args ...interface{})
}

// Page is a struct for storing pagination information
// Example usage:
//
//	ctrl := &SelectCtrl{
//	    Fields: []string{"id", "name", "created_at"},
//	    Where: map[string][]string{
//	        "status": {"active"},
//	    },
//	    Page: Page{
//	        Limit: 10,   // Number of rows to return
//	        Offset: 20,  // Skip first 20 rows
//	    },
//	}
//
// Note: If Limit is 0 or negative, pagination is disabled and all matching rows are returned.
type Page struct {
	Limit  int64
	Offset int64
}

// SelectCtrl is a struct for storing select control information
type SelectCtrl struct {
	// Fields specifies which columns to return in the query result
	// - Empty array means select all columns
	// - ["COUNT(0)"] means select count
	// - ["column1", "column2"] means select specific columns
	Fields []string

	// Where contains filter conditions for the query
	// The map key is the column name and the value array contains one or more conditions
	// Supported filter functions:
	// - No function: exact match (e.g. "value")
	// - lt: less than (e.g. "lt(5)")
	// - le: less than or equal (e.g. "le(5)")
	// - gt: greater than (e.g. "gt(5)")
	// - ge: greater than or equal (e.g. "ge(5)")
	// - ne: not equal (e.g. "ne(value)")
	// - like: contains substring (e.g. "like(value)")
	// - hlike: starts with (e.g. "hlike(value)")
	// - tlike: ends with (e.g. "tlike(value)")
	// Special conditions:
	// - "isnull()": matches NULL values
	// - "notnull()": matches non-NULL values
	Where map[string][]string

	// Order specifies the sort order for results
	// Supported order functions:
	// - asc(column): ascending order
	// - desc(column): descending order
	// - nearest(column;operator;value): proximity search (e.g. "nearest(vector;L2;[1,2,3])")
	Order []string

	// Page controls result pagination
	Page Page

	// OptimizeConditions enables query optimization for better performance.
	// When enabled, the following optimizations are applied:
	//
	// 1. Integer Field Optimization:
	//    - Combines multiple range conditions into a single range
	//    - Example: ["gt(123)", "lt(129)", "124"] -> only keeps "124" as it satisfies both conditions
	//    - Example: ["gt(123)", "le(129)"] -> converts to range min=123, max=130
	//    - Eliminates redundant conditions
	//    - Returns empty result if conditions are contradictory (e.g., "gt(129)", "le(121)")
	//
	// 2. Enum String Optimization:
	//    - Converts string-based enum values to their integer representations
	//    - Combines multiple conditions into a minimal set
	//    - Example: ["normal", "high"] -> converts to [0, 20] (internal enum values)
	//    - Example: ["gt(normal)", "le(belowNormal)"] -> returns empty as conditions contradict
	//    - Optimizes range queries on enum values
	//
	// 3. Time Field Optimization:
	//    - Normalizes time values to UTC
	//    - Combines overlapping time ranges
	//    - Example: ["gt(2023-03-28)", "lt(2023-03-29)"] -> converts to min/max time range
	//    - Example: ["2023-03-28", "lt(2023-03-29)"] -> keeps only exact match if within range
	//    - Supports various time formats (RFC3339, Unix timestamp, etc.)
	//    - Returns empty result for impossible time ranges
	//
	// The optimization process:
	// 1. Analyzes all conditions for a field
	// 2. Converts values to their internal representations
	// 3. Determines the effective range or set of values
	// 4. Eliminates redundant or contradictory conditions
	// 5. May convert multiple conditions into a single optimized form
	//
	// Example optimizations:
	//    ```go
	//    // Original conditions
	//    ctrl := &SelectCtrl{
	//        Where: map[string][]string{
	//            "id": {"gt(123)", "lt(129)", "124", "126"},
	//            "priority": {"gt(normal)", "le(high)"},
	//            "created_at": {"gt(2023-03-28)", "lt(2023-03-29)"},
	//        },
	//        OptimizeConditions: true,
	//    }
	//
	//    // Optimized to
	//    // id: ["124", "126"] (only values that satisfy all ranges)
	//    // priority: [10, 20] (converted to internal enum values within range)
	//    // created_at: range from 2023-03-28 to 2023-03-29
	//    ```
	OptimizeConditions bool
}

// Examples:
//
// 1. Simple exact match filter:
//    ```go
//    ctrl := &SelectCtrl{
//        Where: map[string][]string{
//            "id": {"1"},
//        },
//    }
//    ```
//
// 2. Multiple conditions on same field:
//    ```go
//    ctrl := &SelectCtrl{
//        Where: map[string][]string{
//            "id": {"1", "2"}, // Translates to: id IN (1, 2)
//        },
//    }
//    ```
//
// 3. Range conditions:
//    ```go
//    ctrl := &SelectCtrl{
//        Where: map[string][]string{
//            "age": {"gt(18)", "lt(65)"}, // Translates to: age > 18 AND age < 65
//        },
//    }
//    ```
//
// 4. String matching:
//    ```go
//    ctrl := &SelectCtrl{
//        Where: map[string][]string{
//            "name": {"like(john)"}, // Translates to: name LIKE '%john%'
//        },
//    }
//    ```
//
// 5. Sorting and pagination:
//    ```go
//    ctrl := &SelectCtrl{
//        Order: []string{"desc(created_at)"},
//        Page: Page{
//            Limit: 10,
//            Offset: 0,
//        },
//    }
//    ```
//
// 6. Count query:
//    ```go
//    ctrl := &SelectCtrl{
//        Fields: []string{"COUNT(0)"},
//    }
//    ```
//
// 7. Select specific fields with conditions:
//    ```go
//    ctrl := &SelectCtrl{
//        Fields: []string{"name", "age"},
//        Where: map[string][]string{
//            "state": {"active"},
//            "age": {"ge(18)"},
//        },
//    }
//    ```
//
// 8. UUID field matching:
//    ```go
//    ctrl := &SelectCtrl{
//        Where: map[string][]string{
//            "uuid": {"00000000-0000-0000-0000-000000000001"},
//        },
//    }
//    ```
//
// 9. Time range query:
//    ```go
//    ctrl := &SelectCtrl{
//        Where: map[string][]string{
//            "enqueue_time": {"gt(10000)", "lt(20000)"}, // Unix timestamp range
//        },
//    }
//    ```

// databaseSelector provides methods for querying data from the database
type databaseSelector interface {
	// Select queries data from a table using the provided selection control parameters.
	// It supports filtering, sorting, field selection and pagination through the SelectCtrl struct.
	Select(tableName string, c *SelectCtrl) (Rows, error)
}

// InsertStats is a struct for storing insert statistics
type InsertStats struct {
	Successful        int64
	Failed            int64
	Total             int64
	ExpectedSuccesses int64
}

func (s *InsertStats) String() string {
	return fmt.Sprintf("successful: %d, failed: %d, total: %d", s.Successful, s.Failed, s.Total)
}

// databaseInserter provides methods for inserting data into the database
type databaseInserter interface {
	// BulkInsert inserts multiple rows into a table in a single operation.
	// Parameters:
	//   - tableName: name of the table to insert into
	//   - rows: slice of rows, where each row is a slice of values matching columnNames
	//   - columnNames: names of the columns in the order they appear in rows
	//
	// The function supports different database dialects with optimized implementations:
	//
	// SQL Databases (PostgreSQL, MySQL, etc.):
	// - Uses parameterized queries by default for SQL injection protection
	// - Falls back to string interpolation if QueryStringInterpolation is true
	// - Generates multi-value INSERT statements for better performance
	// Example:
	//    ```go
	//    err := db.BulkInsert("users",
	//        [][]interface{}{
	//            {1, "john", 25},
	//            {2, "jane", 30},
	//        },
	//        []string{"id", "name", "age"})
	//    ```
	// Generates: "INSERT INTO users(id, name, age) VALUES ($1, $2, $3), ($4, $5, $6);"
	//
	// Cassandra:
	// - Uses batch statements for multiple inserts
	// - Wraps multiple inserts in BEGIN BATCH/APPLY BATCH
	// Example:
	//    ```go
	//    err := db.BulkInsert("users",
	//        [][]interface{}{
	//            {1, "john", 25},
	//            {2, "jane", 30},
	//        },
	//        []string{"id", "name", "age"})
	//    ```
	// Generates:
	//    ```sql
	//    BEGIN BATCH
	//        INSERT INTO users(id, name, age) VALUES (?, ?, ?);
	//        INSERT INTO users(id, name, age) VALUES (?, ?, ?);
	//    APPLY BATCH;
	//    ```
	//
	// Elasticsearch:
	// - Uses bulk API for efficient indexing
	// - Automatically adds @timestamp if not provided
	// - Supports JSON document structure
	// Example:
	//    ```go
	//    err := db.BulkInsert("users",
	//        [][]interface{}{
	//            {time.Now(), 1, "john", []string{"admin", "user"}},
	//            {time.Now(), 2, "jane", []string{"user"}},
	//        },
	//        []string{"@timestamp", "id", "name", "roles"})
	//    ```
	//
	// Common Features:
	// - Validates that row length matches column names length
	// - Handles empty input gracefully (returns nil)
	// - Returns detailed error messages for invalid inputs
	// - Supports transactions when used within Session.Transact()
	//
	// Returns error if:
	// - Row length doesn't match column names length
	// - Database execution fails
	// - Invalid data types for the specified columns
	BulkInsert(tableName string, rows [][]interface{}, columnNames []string) error
}

// databaseQuerier provides low-level query execution methods.
// This interface is implemented by both direct database connections and transactions.
//
// Example usage:
//
//	```go
//	// Direct query execution
//	db.Exec("INSERT INTO users (name, age) VALUES (?, ?)", "John", 30)
//
//	// Single row query
//	var name string
//	row := db.QueryRow("SELECT name FROM users WHERE id = ?", 1)
//	row.Scan(&name)
//
//	// Multiple rows query
//	rows, _ := db.Query("SELECT id, name FROM users WHERE age > ?", 18)
//	defer rows.Close()
//	for rows.Next() {
//	    var id int
//	    var name string
//	    rows.Scan(&id, &name)
//	}
//	```
//
// Features:
// - Context support for cancellation and timeouts
// - Query parameter binding for SQL injection protection
// - Result interface for getting LastInsertId and RowsAffected
// - Row and Rows interfaces for scanning results
// - Automatic resource cleanup (when using defer rows.Close())
// - Query explain support for debugging and optimization
type databaseQuerier interface {
	// Exec executes a query that doesn't return rows
	// Example: INSERT, UPDATE, DELETE statements
	Exec(format string, args ...interface{}) (Result, error)

	// QueryRow executes a query that returns a single row
	// Example: SELECT for unique key lookups
	QueryRow(format string, args ...interface{}) Row

	// Query executes a query that returns multiple rows
	// Example: SELECT with WHERE clause matching multiple records
	Query(format string, args ...interface{}) (Rows, error)
}

// Result is an interface for database query results
// It provides methods to get information about the executed query
type Result interface {
	// LastInsertId returns the ID generated for an AUTO_INCREMENT column by the last INSERT operation
	// Note: Not all databases support this (e.g., PostgreSQL requires RETURNING clause)
	LastInsertId() (int64, error)

	// RowsAffected returns the number of rows affected by an INSERT, UPDATE, or DELETE operation
	// Note: Some databases like ClickHouse might not support this functionality
	RowsAffected() (int64, error)
}

// Stmt is an interface for database prepared statements
// Prepared statements help prevent SQL injection and improve performance
// for queries that are executed multiple times
//
// Example usage:
//
//	```go
//	// Prepare the statement
//	stmt, err := db.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
//	if err != nil {
//	    return err
//	}
//	defer stmt.Close()  // Always close statements to free resources
//
//	// Execute multiple times with different parameters
//	result, err := stmt.Exec("John", 30)
//	if err != nil {
//	    return err
//	}
//	affected, _ := result.RowsAffected()
//
//	result, err = stmt.Exec("Jane", 25)
//	```
//
// Different SQL dialects use different parameter placeholders:
//
//	```go
//	// MySQL, SQLite: Use ?
//	"INSERT INTO users VALUES (?, ?)"
//
//	// PostgreSQL: Use $1, $2, etc.
//	"INSERT INTO users VALUES ($1, $2)"
//
//	// MSSQL: Use @ParamName
//	"INSERT INTO users VALUES (@Name, @Age)"
//	stmt.Exec(sql.Named("Name", "John"), sql.Named("Age", 30))
//	```
type Stmt interface {
	// Exec executes the prepared statement with the given arguments
	Exec(args ...any) (Result, error)

	// Close releases the database resources associated with the statement
	Close() error
}

// databaseQueryPreparer provides methods for preparing statements
type databaseQueryPreparer interface {
	// Prepare creates a prepared statement for later queries or executions
	//
	// Example usage in a transaction:
	//    ```go
	//    err := db.Transact(func(tx DatabaseAccessor) error {
	//        stmt, err := tx.Prepare("INSERT INTO users VALUES (?, ?)")
	//        if err != nil {
	//            return err
	//        }
	//        defer stmt.Close()
	//
	//        result, err := stmt.Exec(1, "John")
	//        if err != nil {
	//            return err
	//        }
	//
	//        affected, err := result.RowsAffected()
	//        if err != nil {
	//            return err
	//        }
	//        if affected != 1 {
	//            return fmt.Errorf("expected 1 row affected, got %d", affected)
	//        }
	//
	//        return nil
	//    })
	//    ```
	Prepare(query string) (Stmt, error)
}

// UpdateCtrl is a struct for storing update control information
type UpdateCtrl struct {
	// Set contains the values to update in the format map[columnName][]string{value}
	// Example:
	//   Set: map[string][]string{
	//       "status": {"active"},
	//       "updated_at": {"now()"},
	//   }
	Set map[string][]string

	// Where contains filter conditions for the update operation
	// The map key is the column name and the value array contains one or more conditions
	// Supported filter functions:
	// - No function: exact match (e.g. "value")
	// - lt: less than (e.g. "lt(5)")
	// - le: less than or equal (e.g. "le(5)")
	// - gt: greater than (e.g. "gt(5)")
	// - ge: greater than or equal (e.g. "ge(5)")
	// - ne: not equal (e.g. "ne(value)")
	// - like: contains substring (e.g. "like(value)")
	// - hlike: starts with (e.g. "hlike(value)")
	// - tlike: ends with (e.g. "tlike(value)")
	// Special conditions:
	// - "isnull()": matches NULL values
	// - "notnull()": matches non-NULL values
	Where map[string][]string

	// OptimizeConditions enables query optimization for better performance.
	// When enabled, the following optimizations are applied:
	//
	// 1. Integer Field Optimization:
	//    - Combines multiple range conditions into a single range
	//    - Example: ["gt(123)", "lt(129)", "124"] -> only keeps "124" as it satisfies both conditions
	//    - Example: ["gt(123)", "le(129)"] -> converts to range min=123, max=130
	//    - Eliminates redundant conditions
	//    - Returns empty result if conditions are contradictory (e.g., "gt(129)", "le(121)")
	//
	// 2. Enum String Optimization:
	//    - Converts string-based enum values to their integer representations
	//    - Combines multiple conditions into a minimal set
	//    - Example: ["normal", "high"] -> converts to [0, 20] (internal enum values)
	//    - Example: ["gt(normal)", "le(belowNormal)"] -> returns empty as conditions contradict
	//    - Optimizes range queries on enum values
	//
	// 3. Time Field Optimization:
	//    - Normalizes time values to UTC
	//    - Combines overlapping time ranges
	//    - Example: ["gt(2023-03-28)", "lt(2023-03-29)"] -> converts to min/max time range
	//    - Example: ["2023-03-28", "lt(2023-03-29)"] -> keeps only exact match if within range
	//    - Supports various time formats (RFC3339, Unix timestamp, etc.)
	//    - Returns empty result for impossible time ranges
	OptimizeConditions bool
}

// Examples:
//
// 1. Simple update with exact match:
//    ```go
//    ctrl := &UpdateCtrl{
//        Set: map[string][]string{
//            "status": {"active"},
//        },
//        Where: map[string][]string{
//            "id": {"1"},
//        },
//    }
//    ```
//
// 2. Update with multiple conditions:
//    ```go
//    ctrl := &UpdateCtrl{
//        Set: map[string][]string{
//            "status": {"inactive"},
//            "updated_at": {"now()"},
//        },
//        Where: map[string][]string{
//            "status": {"active"},
//            "last_login": {"lt(2023-01-01)"},
//        },
//    }
//    ```
//
// 3. Update with range conditions:
//    ```go
//    ctrl := &UpdateCtrl{
//        Set: map[string][]string{
//            "priority": {"high"},
//        },
//        Where: map[string][]string{
//            "priority": {"gt(normal)", "le(high)"},
//        },
//    }
//    ```

// databaseUpdater provides methods for updating data in the database
type databaseUpdater interface {
	// Update modifies existing records in a table using the provided update control parameters.
	// It supports filtering through the Where clause and setting new values through the Set clause.
	//
	// Example usage:
	//   ```go
	//   ctrl := &UpdateCtrl{
	//       Set: map[string][]string{
	//           "status": {"active"},
	//           "updated_at": {"now()"},
	//       },
	//       Where: map[string][]string{
	//           "id": {"1"},
	//       },
	//   }
	//   affected, err := db.Update("users", ctrl)
	//   ```
	//
	// The function returns:
	// - number of affected rows
	// - error if the update operation fails
	//
	// Database-specific behavior:
	// 1. SQL Databases:
	//    - Generates UPDATE statement with SET and WHERE clauses
	//    - Returns number of affected rows
	//    - Example: "UPDATE users SET status = 'active', updated_at = now() WHERE id = 1"
	//
	// 2. Elasticsearch:
	//    - Uses update_by_query API
	//    - Returns number of updated documents
	//    - Example: POST /users/_update_by_query with query and script
	//
	// 3. Cassandra:
	//    - Generates UPDATE statement with SET and WHERE clauses
	//    - Returns number of affected rows
	//    - Example: "UPDATE users SET status = 'active', updated_at = toTimestamp(now()) WHERE id = 1"
	Update(tableName string, c *UpdateCtrl) (int64, error)
}

// DatabaseAccessor provides core database access operations including selecting data,
// inserting data, and executing raw queries.
type DatabaseAccessor interface {
	databaseSelector
	databaseInserter
	databaseUpdater
	databaseQuerier
	databaseQueryPreparer
}

// Session represents a database session that can execute operations either in a transaction
// or as standalone operations.
type Session interface {
	DatabaseAccessor

	// Transact executes the provided function within a database transaction.
	// If the function returns an error, the transaction is rolled back.
	// Otherwise, the transaction is committed.
	Transact(func(tx DatabaseAccessor) error) error

	// GetNextVal retrieves the next value from a sequence.
	// This operation is restricted to be used only outside of transactions.
	GetNextVal(sequenceName string) (uint64, error)
}

// TableRow defines a single column in a database table
// Example usage:
//
//	```go
//	rows := []TableRow{
//	    {
//	        Name: "id",
//	        Type: DataTypeId,
//	        PrimaryKey: true,
//	        NotNull: true,
//	    },
//	    {
//	        Name: "name",
//	        Type: DataTypeVarChar,
//	        NotNull: true,
//	        Indexed: true,  // Creates searchable index in Elasticsearch
//	    },
//	}
//	```
type TableRow struct {
	Name       string
	Type       DataType
	PrimaryKey bool
	NotNull    bool
	Indexed    bool // only for Elasticsearch
}

// ResilienceSettings defines database replication and sharding configuration
// SQL databases: Typically ignored
// Elasticsearch: Controls cluster configuration
//
//	```go
//	settings := ResilienceSettings{
//	    NumberOfShards: 3,    // Split data across 3 shards
//	    NumberOfReplicas: 1,  // Keep 1 backup copy
//	}
//	```
type ResilienceSettings struct {
	NumberOfShards   int
	NumberOfReplicas int
}

// TableDefinition defines the complete structure of a database table
// Example usage:
//
//	```go
//	tableDef := &TableDefinition{
//	    TableRows: []TableRow{...},
//	    PrimaryKey: []string{"id"},
//	    Engine: "InnoDB",  // For MySQL
//	    Resilience: ResilienceSettings{...},  // For Elasticsearch
//	    LMPolicy: "hot-warm-delete",  // Elasticsearch lifecycle policy
//	}
//
//	// Create table in SQL database
//	err := db.CreateTable("users", tableDef, "")
//
//	// Create index in Elasticsearch
//	err := db.CreateTable("users", tableDef, "")
//	```
type TableDefinition struct {
	TableRows  []TableRow
	PrimaryKey []string
	Engine     string
	Resilience ResilienceSettings
	LMPolicy   string // only for Elasticsearch
}

// IndexType defines the type of database index to create
// Currently supported types:
//   - IndexTypeBtree: "btree" - Standard balanced tree index
//
// Example usage:
//
//	```go
//	// 1. Create a single-column index
//	err := db.CreateIndex(
//	    "idx_user_email",     // index name
//	    "users",              // table name
//	    []string{"email"},    // columns
//	    IndexTypeBtree,       // index type
//	)
//
//	// 2. Create a multi-column (composite) index
//	err = db.CreateIndex(
//	    "idx_user_name",
//	    "users",
//	    []string{"last_name", "first_name"},
//	    IndexTypeBtree,
//	)
//
//	// 3. Verify index exists
//	exists, err := db.IndexExists("idx_user_email", "users")
//
//	// 4. Drop index when no longer needed
//	err = db.DropIndex("idx_user_email", "users")
//	```
//
// Database Support:
// - MySQL/MariaDB: Supports BTREE indexes
// - PostgreSQL: Supports BTREE indexes (default)
// - SQLite: Supports BTREE indexes
// - MSSQL: Supports BTREE indexes
// - Cassandra: Index operations are no-ops
// - ClickHouse: Index operations are no-ops
type IndexType string

const (
	IndexTypeBtree IndexType = "btree"
)

// Constraint represents a database constraint
// Example usage:
//
//  1. Foreign Key Constraints:
//     ```go
//     constraints := []Constraint{
//     {
//     Name: "fk_user_role",
//     TableName: "users",
//     Definition: "FOREIGN KEY (role_id) REFERENCES roles(id)",
//     },
//     {
//     Name: "fk_user_department",
//     TableName: "users",
//     Definition: "FOREIGN KEY (dept_id) REFERENCES departments(id)",
//     },
//     }
//
//     // Add constraints
//     err := db.AddConstraints(constraints)
//
//     // Later, remove constraints
//     err = db.DropConstraints(constraints)
//     ```
//
//  2. Unique Constraints:
//     ```go
//     constraints := []Constraint{
//     {
//     Name: "uq_email",
//     TableName: "users",
//     Definition: "UNIQUE (email)",
//     },
//     }
//     ```
//
//  3. Check Constraints:
//     ```go
//     constraints := []Constraint{
//     {
//     Name: "chk_age",
//     TableName: "users",
//     Definition: "CHECK (age >= 18)",
//     },
//     }
//     ```
//
// Note: Constraint support varies by database:
// - PostgreSQL: Supports all constraint types
// - MySQL: Supports most constraints (CHECK constraints in newer versions)
// - SQLite: Basic constraint support
// - Elasticsearch: Does not support constraints (methods are no-ops)
type Constraint struct {
	Name       string `json:"name"`       // Unique identifier for the constraint
	TableName  string `json:"table_name"` // Table the constraint applies to
	Definition string `json:"definition"` // SQL definition of the constraint
}

// databaseMigrator provides methods for database schema migrations and management
// The implementation differs significantly between SQL databases and Elasticsearch:
//
//  1. SQL Databases (MySQL, PostgreSQL, SQLite, MSSQL):
//     ```go
//     // Create table
//     err := db.CreateTable("users", &TableDefinition{
//     TableRows: []TableRow{
//     {Name: "id", Type: DataTypeId, PrimaryKey: true},
//     {Name: "email", Type: DataTypeVarChar, NotNull: true},
//     },
//     }, "")
//
//     // Create BTREE index
//     err = db.CreateIndex("idx_email", "users", []string{"email"}, IndexTypeBtree)
//
//     // Add foreign key constraint (PostgreSQL only)
//     err = db.AddConstraints([]Constraint{{
//     Name: "fk_user_role",
//     TableName: "users",
//     Definition: "FOREIGN KEY (role_id) REFERENCES roles(id)",
//     }})
//     ```
//
//  2. Elasticsearch:
//     ```go
//     // Create index with mappings and lifecycle policy
//     err := db.CreateTable("users", &TableDefinition{
//     TableRows: []TableRow{
//     {Name: "email", Type: DataTypeVarChar, Indexed: true},
//     {Name: "name", Type: DataTypeText},
//     },
//     Resilience: ResilienceSettings{
//     NumberOfShards: 3,
//     NumberOfReplicas: 1,
//     },
//     }, "")
//
//     // Indexes are defined in TableRow.Indexed field
//     // Constraints are not supported
//     ```
type databaseMigrator interface {
	// ApplyMigrations applies DDL statements to a table
	ApplyMigrations(tableName, tableMigrationDDL string) error

	// TableExists checks if a table exists
	TableExists(tableName string) (bool, error)

	// CreateTable creates a new table
	CreateTable(tableName string, tableDefinition *TableDefinition, tableMigrationDDL string) error

	// DropTable removes a table
	DropTable(tableName string) error

	// IndexExists checks if an index exists
	IndexExists(indexName string, tableName string) (bool, error)

	// CreateIndex creates a new index
	CreateIndex(indexName string, tableName string, columns []string, indexType IndexType) error

	// DropIndex removes an index
	DropIndex(indexName string, tableName string) error

	// ReadConstraints returns all constraints
	ReadConstraints() ([]Constraint, error)

	// AddConstraints adds new constraints
	AddConstraints(constraints []Constraint) error

	// DropConstraints removes constraints
	DropConstraints(constraints []Constraint) error

	// CreateSequence creates a new sequence
	CreateSequence(sequenceName string) error

	// DropSequence removes a sequence
	DropSequence(sequenceName string) error
}

// databaseDescriber provides methods for retrieving database metadata and statistics
type databaseDescriber interface {
	// GetVersion returns the database version information
	// Example output:
	//   - PostgreSQL: "PostgreSQL 14.5"
	//   - MySQL: "8.0.28 (MySQL Community Server - GPL)"
	//   - SQLite: "3.39.2"
	GetVersion() (DialectName, string, error)

	// GetInfo returns database configuration and settings information
	// Different databases return different formats:
	//
	// 1. PostgreSQL:
	//    ```
	//    |-------------------------------------|--------------------------------------------------------------|------------|
	//    | Name                                | Setting                                                      | Unit       |
	//    |-------------------------------------|--------------------------------------------------------------|------------|
	//    | max_connections                     | 100                                                          |            |
	//    | shared_buffers                      | 16384                                                        | 8kB        |
	//    |-------------------------------------|--------------------------------------------------------------|------------|
	//    ```
	//
	// 2. MySQL:
	//    ```
	//    -----------------------------------------|-----------------------------------------------
	//    Variable_Name                            | Value
	//    -----------------------------------------|-----------------------------------------------
	//    innodb_buffer_pool_size                 | 134217728
	//    max_connections                         | 151
	//    -----------------------------------------|-----------------------------------------------
	//    ```
	GetInfo(version string) ([]string, *Info, error)

	// GetTablesSchemaInfo returns schema information for specified tables
	// Example output:
	//    ```
	//    TABLE: users
	//      Columns:
	//       - id: bigint
	//       - email: varchar(255)
	//       - created_at: timestamp
	//      Indexes:
	//       - pk_users: PRIMARY KEY (id)
	//       - idx_email: BTREE (email)
	//    ```
	GetTablesSchemaInfo(tableNames []string) ([]string, error)

	// GetTablesVolumeInfo returns size and row count information
	// Example output:
	//    ```
	//    TABLE NAME                                               ROWS    DATA SIZE (MB)    IDX SIZE (MB)
	//    ------------------------------------------------------------------------------------
	//    users                                                   1500            12               4
	//    products                                                500             8                2
	//    ```
	GetTablesVolumeInfo(tableNames []string) ([]string, error)
}

// Stats is a struct for storing database statistics
type Stats struct {
	OpenConnections int // The number of established connections both in use and idle.
	InUse           int // The number of connections currently in use.
	Idle            int // The number of idle connections.
}

// Context is a struct for storing database context and timing metrics
type Context struct {
	// Ctx is the context.Context for this database operation
	Ctx context.Context

	// Explain enables query execution plan analysis.
	// When true, adds EXPLAIN prefix to queries based on dialect:
	//  - MySQL: "EXPLAIN query"
	//  - PostgreSQL: "EXPLAIN ANALYZE query"
	//  - SQLite: "EXPLAIN QUERY PLAN query"
	//  - Cassandra: "TRACING ON; query"
	//
	// Example outputs:
	//  - PostgreSQL: Shows cost estimates and actual timings
	//  - MySQL: Shows execution plan with index usage
	//  - SQLite: Shows the query execution strategy
	//  - Cassandra: Shows detailed query tracing
	//
	// Note: Requires ExplainLogger to be configured to capture output
	Explain bool

	// Timing metrics for different database operations.
	// All times are stored as nanoseconds in atomic Int64s.
	//
	// Example usage:
	// ```go
	// db, session, ctx := // ... get session ...
	//
	// // Execute some operations
	// session.Exec("INSERT INTO users ...")
	// session.Query("SELECT * FROM users")
	//
	// // Check timing metrics
	// fmt.Printf("Query execution time: %v\n",
	//     time.Duration(ctx.QueryTime.Load()))
	// ```
	//
	// Implementation details:
	// - Uses atomic counters for thread-safe accumulation
	// - Measures with nanosecond precision using time.Since()
	// - Metrics persist across multiple operations in a session
	// - Captured using deferred timing in operation wrappers
	BeginTime   *atomic.Int64 // Transaction start time
	PrepareTime *atomic.Int64 // Statement preparation time
	ExecTime    *atomic.Int64 // Query execution time (Exec)
	QueryTime   *atomic.Int64 // Query execution time (Query/QueryRow)
	DeallocTime *atomic.Int64 // Statement cleanup time
	CommitTime  *atomic.Int64 // Transaction commit time

	// TxRetries tracks the number of transaction retry attempts
	TxRetries int
}

// Database is the main interface for database operations. It provides methods for
// connection management, schema migrations, and session handling.
//
// Example usage:
//
//	// 1. Open a database connection
//	dbo, err := db.Open(db.Config{
//	    ConnString:      "mysql://user:pass@localhost:3306/mydb",  // Connection string
//	    MaxOpenConns:    16,                                       // Connection pool settings
//	    MaxConnLifetime: 100 * time.Millisecond,                  // Connection lifetime
//	    Explain:         true,                                     // Enable query explanations
//	    QueryLogger:     logger,                                   // Query logging
//	})
//	if err != nil {
//	    return fmt.Errorf("failed to open db: %v", err)
//	}
//	defer dbo.Close()
//
//	// 2. Create a context for timing metrics
//	ctx := dbo.Context(context.Background())
//	// ctx contains atomic counters for:
//	//   - BeginTime:   Transaction start
//	//   - PrepareTime: Statement preparation
//	//   - ExecTime:    Query execution (Exec)
//	//   - QueryTime:   Query execution (Query/QueryRow)
//	//   - DeallocTime: Statement cleanup
//	//   - CommitTime:  Transaction commit
//
//	// 3. Create a session for database operations
//	session := dbo.Session(ctx)
//
//	// 4. Basic Operations:
//	// 4.1. Direct query execution
//	rows, err := session.Select("users", &db.SelectCtrl{
//	    Fields: []string{"id", "name"},
//	    Where:  map[string][]string{"active": {"true"}},
//	})
//	defer rows.Close()
//
//	// 4.2. Bulk insert
//	err = session.BulkInsert("users",
//	    [][]interface{}{
//	        {1, "Alice", true},
//	        {2, "Bob", true},
//	    },
//	    []string{"id", "name", "active"},
//	)
//
//	// 4.3. Transactional operations
//	err = session.Transact(func(tx db.DatabaseAccessor) error {
//	    // All operations in this function are in a single transaction
//	    err := tx.BulkInsert("users", ...)
//	    if err != nil {
//	        return err // Will trigger rollback
//	    }
//
//	    rows, err := tx.Select("users", ...)
//	    if err != nil {
//	        return err // Will trigger rollback
//	    }
//
//	    return nil // Will trigger commit
//	})
//
// Implementation differences:
//
// 1. SQL Databases:
//
//   - Full transaction support
//
//   - Complex SELECT with WHERE clauses
//
//   - Traditional table operations
//
//     // SQL Example:
//     _, err := s.Exec(`
//     INSERT INTO perf_table (origin, type, name)
//     VALUES (2, 2, 'test'),
//     (3, 4, 'perf');
//     `)
//
//     rows, err := s.Select("perf_table", &db.SelectCtrl{
//     Fields: []string{"origin", "name"},
//     Where: map[string][]string{"name": {"perf"}},
//     Page: struct{Limit uint64}{Limit: 2},
//     })
//
// 2. Elasticsearch:
//
//   - Document-oriented storage
//
//   - Search-optimized queries
//
//   - Asynchronous indexing
//
//     // Elasticsearch Example:
//     type testStruct struct {
//     Timestamp time.Time `db:"@timestamp"`
//     Id        int64     `db:"id"`
//     Uuid      string    `db:"uuid"`
//     }
//
//     err := s.BulkInsert("perf_table", [][]interface{}{
//     {now, 1, uuid1},
//     {now2, 2, uuid2},
//     }, []string{"@timestamp", "id", "uuid"})
//
//     // Need to wait for indexing
//     time.Sleep(2 * time.Second)
//
//     rows, err := s.Select("perf_table", &db.SelectCtrl{
//     Fields: []string{"id", "uuid"},
//     Where: map[string][]string{"accessors": {"tenant_2"}},
//     Order: []string{"desc(start_time)"},
//     })
type Database interface {
	// Ping verifies the database connection is still alive
	Ping(ctx context.Context) error

	// DialectName returns the database dialect name
	DialectName() DialectName

	// UseTruncate returns whether TRUNCATE should be used instead of DELETE
	UseTruncate() bool

	databaseMigrator
	databaseDescriber

	// Context creates a new database context with timing metrics
	Context(ctx context.Context, explain bool) *Context

	// Session creates a new database session with the given context
	Session(ctx *Context) Session

	// RawSession returns the underlying database session implementation
	RawSession() interface{}

	// Stats returns current database connection statistics
	Stats() *Stats

	// Close closes the database connection
	Close() error
}

// DataType represents the type of a database column. Each database dialect maps these
// types to their specific implementations.
//
// Example usage:
//
//	// 1. Creating a table with different column types
//	tableSpec := &TableDefinition{
//	    TableRows: []TableRow{
//	        {Name: "id", Type: DataTypeId},                    // Primary key
//	        {Name: "name", Type: DataTypeVarChar128},          // Variable-length string
//	        {Name: "description", Type: DataTypeText},         // Long text
//	        {Name: "created_at", Type: DataTypeTimestamp6},    // Timestamp with microseconds
//	        {Name: "is_active", Type: DataTypeBoolean},        // Boolean flag
//	        {Name: "metadata", Type: DataTypeJSON},            // JSON document
//	        {Name: "document", Type: DataTypeLongBlob},        // Binary data
//	        {Name: "embedding", Type: DataTypeVector768Float32} // Vector for ML
//	    },
//	}
//
//	err := db.CreateTable("users", tableSpec, "")
//
// Database-specific mappings:
//
// 1. PostgreSQL:
//   - DataTypeId -> "bigserial not null PRIMARY KEY"
//   - DataTypeVarChar -> "character varying"
//   - DataTypeJSON -> "jsonb"
//   - DataTypeVector768Float32 -> "vector(768)"
//
// 2. MySQL/MariaDB:
//   - DataTypeId -> "bigint not null AUTO_INCREMENT PRIMARY KEY"
//   - DataTypeVarChar -> "varchar"
//   - DataTypeJSON -> "json"
//   - DataTypeLongBlob -> "longblob"
//
// 3. SQLite:
//   - DataTypeId -> "INTEGER PRIMARY KEY"
//   - DataTypeVarChar -> "TEXT"
//   - DataTypeJSON -> "TEXT"
//   - DataTypeBinaryBlobType -> "BLOB"
//
// 4. Elasticsearch:
//   - DataTypeVarChar -> "keyword"
//   - DataTypeText -> "text"
//   - DataTypeJSON -> "object"
//   - DataTypeVector768Float32 -> "dense_vector"
type DataType string

const (
	// Primary Keys and IDs
	DataTypeId                DataType = "{$id}"                   // Database-specific auto-incrementing primary key
	DataTypeTenantUUIDBoundID DataType = "{$tenant_uuid_bound_id}" // Composite key with tenant UUID

	// Integer Types
	DataTypeInt             DataType = "{$int}"               // Standard integer
	DataTypeBigInt          DataType = "{$bigint}"            // Large integer
	DataTypeBigIntAutoIncPK DataType = "{$bigint_autoinc_pk}" // Auto-incrementing big integer primary key
	DataTypeBigIntAutoInc   DataType = "{$bigint_autoinc}"    // Auto-incrementing big integer
	DataTypeSmallInt        DataType = "{$smallint}"          // Small integer
	DataTypeTinyInt         DataType = "{$tinyint}"           // Tiny integer

	// String Types
	DataTypeVarChar    DataType = "{$varchar}"    // Variable-length string
	DataTypeVarChar32  DataType = "{$varchar32}"  // VARCHAR(32)
	DataTypeVarChar36  DataType = "{$varchar36}"  // VARCHAR(36)
	DataTypeVarChar64  DataType = "{$varchar64}"  // VARCHAR(64)
	DataTypeVarChar128 DataType = "{$varchar128}" // VARCHAR(128)
	DataTypeVarChar256 DataType = "{$varchar256}" // VARCHAR(256)
	DataTypeText       DataType = "{$text}"       // Unlimited length text
	DataTypeLongText   DataType = "{$longtext}"   // Long text
	DataTypeAscii      DataType = "{$ascii}"      // ASCII text only

	// UUID Types
	DataTypeUUID        DataType = "{$uuid}"         // UUID data type
	DataTypeVarCharUUID DataType = "{$varchar_uuid}" // UUID stored as VARCHAR

	// Binary Types
	DataTypeLongBlob       DataType = "{$longblob}"       // Large binary object
	DataTypeHugeBlob       DataType = "{$hugeblob}"       // Very large binary object
	DataTypeBinary20       DataType = "{$binary20}"       // Fixed 20-byte binary
	DataTypeBinaryBlobType DataType = "{$binaryblobtype}" // Generic binary blob

	// Date and Time Types
	DataTypeDateTime          DataType = "{$datetime}"           // Date and time
	DataTypeDateTime6         DataType = "{$datetime6}"          // Date and time with microseconds
	DataTypeTimestamp         DataType = "{$timestamp}"          // Timestamp
	DataTypeTimestamp6        DataType = "{$timestamp6}"         // Timestamp with microseconds
	DataTypeCurrentTimeStamp6 DataType = "{$current_timestamp6}" // Current timestamp with microseconds

	// Boolean Types
	DataTypeBoolean      DataType = "{$boolean}"       // Boolean
	DataTypeBooleanFalse DataType = "{$boolean_false}" // Boolean defaulting to false
	DataTypeBooleanTrue  DataType = "{$boolean_true}"  // Boolean defaulting to true

	// Special Types
	DataTypeJSON             DataType = "{$json}"               // JSON document
	DataTypeVector3Float32   DataType = "{$vector_3_float32}"   // 3D vector of float32
	DataTypeVector768Float32 DataType = "{$vector_768_float32}" // 768D vector of float32

	// Constraints and Modifiers
	DataTypeUnique  DataType = "{$unique}"  // Unique constraint
	DataTypeEngine  DataType = "{$engine}"  // Storage engine specification
	DataTypeNotNull DataType = "{$notnull}" // NOT NULL constraint
	DataTypeNull    DataType = "{$null}"    // NULL allowed
)

// Dialect is an interface for database dialects
type Dialect interface {
	GetType(id DataType) string
}

// Recommendation represents a database configuration recommendation with explanations
// and expected/recommended values. It is used to validate and suggest optimal
// database settings.
//
// Example usage:
//
//	// 1. Create a database connection
//	dbo, err := db.Open(db.Config{
//	    ConnString: "mysql://user:pass@localhost:3306/mydb",
//	})
//
//	// 2. Create Info object to check recommendations
//	info := db.NewDBInfo(dbo, version)
//
//	// 3. Add current database settings
//	info.AddSetting("innodb_buffer_pool_size", "8589934592")  // 8GB
//	info.AddSetting("max_connections", "1000")
//
//	// 4. Show recommendations and checks
//	info.ShowRecommendations()
//
//	// Output example:
//	// database settings checks:
//	//  - innodb_buffer_pool_size (primary DB cache).................. 8589934592   OK
//	//  - max_connections (max allowed connections)................... 1000         WRN: recommended value should be at least 2048
//
// Different databases have different recommendations:
//
// 1. MySQL/MariaDB (varies by version):
//   - MySQL 5.6:
//   - innodb_buffer_pool_size: 2GB min, 3GB recommended
//   - innodb_buffer_pool_instances: 4 min, 8 recommended
//   - innodb_log_file_size: 256MB min, 512MB recommended
//   - MySQL 8+:
//   - innodb_buffer_pool_size: 8GB min, 12GB recommended
//   - innodb_log_file_size: 512MB min, 2GB recommended
//   - Common:
//   - max_connections: 512 min, 2048 recommended
//   - query_cache_type: Should be OFF
//   - performance_schema: Should be ON
//
// 2. PostgreSQL:
//   - shared_buffers: 1GB min, 4GB recommended
//   - effective_cache_size: 2GB min, 8GB recommended
//   - work_mem: 8MB min, 16MB recommended
//   - maintenance_work_mem: 128MB min, 256MB recommended
//   - max_connections: 512 min, 2048 recommended
//   - random_page_cost: Should be 1.1 for SSD
//   - track_activities: Should be ON
//   - jit: Should be OFF
type Recommendation struct {
	// Setting is the database configuration parameter name
	Setting string

	// Meaning provides a human-readable explanation of the setting
	Meaning string

	// ExpectedValue is the exact value expected (if applicable)
	// Used for boolean/enum settings like "ON"/"OFF"
	ExpectedValue string

	// MinVal is the minimum acceptable value
	// Used for numeric settings like buffer sizes
	MinVal int64

	// RecommendedVal is the recommended value
	// Used for numeric settings like buffer sizes
	RecommendedVal int64
}

// DBType - database type
type DBType struct {
	Driver DialectName // driver name (used in the code)
	Symbol string      // short symbol for the driver (used in the command line)
	Name   string      // full name of the driver (used in the command line)
}

// GetDatabases returns a list of supported databases
func GetDatabases() []DBType {
	var ret []DBType
	ret = append(ret, DBType{Driver: POSTGRES, Symbol: "P", Name: "PostgreSQL"})
	ret = append(ret, DBType{Driver: MYSQL, Symbol: "M", Name: "MySQL/MariaDB"})
	ret = append(ret, DBType{Driver: MSSQL, Symbol: "W", Name: "MSSQL"})
	ret = append(ret, DBType{Driver: SQLITE, Symbol: "S", Name: "SQLite"})
	ret = append(ret, DBType{Driver: CLICKHOUSE, Symbol: "C", Name: "ClickHouse"})
	// "A" is used as the latest symbol of the "Cassandra" due to duplicate with ClickHouse "C"
	ret = append(ret, DBType{Driver: CASSANDRA, Symbol: "A", Name: "Cassandra"})
	ret = append(ret, DBType{Driver: ELASTICSEARCH, Symbol: "E", Name: "Elasticsearch"})
	ret = append(ret, DBType{Driver: OPENSEARCH, Symbol: "O", Name: "OpenSearch"})

	return ret
}
