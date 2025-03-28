package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
)

const SequenceName = "acronis_db_bench_sequence" // SequenceName is the name of the sequence used for generating IDs

// DatabaseOpts represents common flags for every test
type DatabaseOpts struct {
	// Driver and Dsn are deprecated, keep for backward compatibility, use ConnString instead
	Driver string `long:"driver" description:"(deprecated) db driver (postgres|mysql|sqlite3)" required:"false"`
	Dsn    string `long:"dsn" description:"(deprecated) dsn connection string" required:"false"`

	ConnString   string `long:"connection-string" description:"connection string (can also be set via ACRONIS_DB_BENCH_CONNECTION_STRING environment variable)" required:"false"`
	MaxOpenConns int    `long:"max-open-cons" description:"max open connections per worker" default:"2" required:"false"`
	Reconnect    bool   `long:"reconnect" description:"reconnect to DB before every test iteration" required:"false"`

	EnableQueryStringInterpolation bool `long:"enable-query-string-interpolation" description:"enable query string interpolation during insert queries construction" required:"false"`

	DryRun  bool `long:"dry-run" description:"do not execute any INSERT/UPDATE/DELETE queries on DB-side" required:"false"`
	Explain bool `long:"explain" description:"prepend the test queries by EXPLAIN ANALYZE" required:"false"`

	LogQueries   bool `long:"log-queries" description:"log queries" required:"false"`
	LogReadRows  bool `long:"log-read-rows" description:"log read rows" required:"false"`
	LogQueryTime bool `long:"log-query-time" description:"log query time" required:"false"`
	LogSystemOps bool `long:"log-system-operations" description:"log system operations on database side" required:"false"`

	DontCleanup bool `long:"dont-cleanup" description:"do not cleanup DB content before/after the test in '-t all' mode" required:"false"`
	UseTruncate bool `long:"use-truncate" description:"use TRUNCATE instead of DROP TABLE in cleanup procedure" required:"false"`
}

// dbConnectorsPool is a simple connection pool, required not to saturate DB connection pool
type dbConnectorsPool struct {
	lock      sync.Mutex
	pool      map[string]*DBConnector
	isClosing bool // New field to track shutdown state
}

// key returns a unique key for the connection pool
func (p *dbConnectorsPool) key(connectionString string, workerID int) string {
	return fmt.Sprintf("%s-%d", connectionString, workerID)
}

// take returns a connection from the pool or nil if the pool is empty
func (p *dbConnectorsPool) take(dbOpts *DatabaseOpts, workerID int) *DBConnector {
	k := p.key(dbOpts.ConnString, workerID)

	p.lock.Lock()
	defer p.lock.Unlock()

	// Don't give out new connections if we're shutting down
	if p.isClosing {
		return nil
	}

	conn, exists := p.pool[k]
	if exists {
		delete(p.pool, k)
		conn.Log(benchmark.LogTrace, "taking connection from the connection pool")
		return conn
	}

	return nil
}

// put puts a connection to the pool
func (p *dbConnectorsPool) put(conn *DBConnector) {
	if conn == nil {
		return
	}

	k := p.key(conn.DbOpts.ConnString, conn.WorkerID)

	p.lock.Lock()
	defer p.lock.Unlock()

	// If we're shutting down, close the connection instead of returning it to the pool
	if p.isClosing {
		if conn.database != nil {
			conn.database.Close()
			conn.database = nil
		}
		return
	}

	// Check if there's already a connection in the pool
	_, exists := p.pool[k]
	if exists {
		// If there is, close the new connection instead of panicking
		if conn.database != nil {
			conn.database.Close()
			conn.database = nil
		}
		conn.Log(benchmark.LogWarn, "connection already exists in pool, closing new connection")
		return
	}

	p.pool[k] = conn
	conn.Log(benchmark.LogTrace, "releasing connection to the connection pool")
}

// shutdown gracefully closes all connections in the pool
func (p *dbConnectorsPool) shutdown() {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.isClosing = true

	// Close all connections in the pool
	for k, conn := range p.pool {
		if conn.database != nil {
			conn.database.Close()
			conn.database = nil
		}
		delete(p.pool, k)
	}
}

// newDBConnectorsPool creates a new connection pool
func newDBConnectorsPool() *dbConnectorsPool {
	p := dbConnectorsPool{}
	p.pool = make(map[string]*DBConnector)

	return &p
}

// connPool is a global connection pool
var connPool = newDBConnectorsPool()

// connectionsChecker checks for potential connections leak
func connectionsChecker(conn *DBConnector) {
	for {
		if conn.database != nil {
			openConnections := 0

			conn.lock.Lock()
			if conn.database != nil {
				stats := conn.database.Stats()
				openConnections = stats.OpenConnections
			}
			conn.lock.Unlock()

			if openConnections > 1 {
				conn.Log(benchmark.LogError, "internal error: potential connections leak detected")
			}
		}
		time.Sleep(3 * time.Second)
	}
}

// DBWorkerData is a structure to store all the worker data
type DBWorkerData struct {
	workingConn  *DBConnector
	tenantsCache *DBConnector
}

func (d *DBWorkerData) release() {
	if d.workingConn != nil {
		d.workingConn.Release()
	}
	if d.tenantsCache != nil {
		d.tenantsCache.Release()
	}
}

/*
 * DB connection management
 */

// DBConnector is a wrapper for DB connection
type DBConnector struct {
	Logger        *benchmark.Logger
	DbOpts        *DatabaseOpts
	RetryAttempts int
	WorkerID      int

	lock     sync.Mutex
	database db.Database
}

// NewDBConnector creates a new DBConnector
func NewDBConnector(dbOpts *DatabaseOpts, workerID int, logger *benchmark.Logger, retryAttempts int) (*DBConnector, error) {
	c := connPool.take(dbOpts, workerID)
	if c != nil {
		return c, nil
	}

	var queryLogger, readRowsLogger, explainLogger, systemLogger db.Logger
	if dbOpts.LogQueries {
		logger.LogLevel = benchmark.LogInfo
		queryLogger = &dbLogger{level: benchmark.LogInfo, worker: workerID, logger: logger}
	}

	if dbOpts.LogReadRows {
		logger.LogLevel = benchmark.LogInfo
		readRowsLogger = &dbLogger{level: benchmark.LogInfo, worker: workerID, logger: logger}
	}

	if dbOpts.Explain {
		logger.LogLevel = benchmark.LogInfo
		explainLogger = &dbLogger{level: benchmark.LogInfo, worker: workerID, logger: logger}
	}

	if dbOpts.LogSystemOps {
		logger.LogLevel = benchmark.LogInfo
		systemLogger = &dbLogger{level: benchmark.LogInfo, worker: workerID, logger: logger}
	}

	var dbConn, err = db.Open(db.Config{
		ConnString:               dbOpts.ConnString,
		MaxOpenConns:             dbOpts.MaxOpenConns,
		QueryStringInterpolation: dbOpts.EnableQueryStringInterpolation,
		DryRun:                   dbOpts.DryRun,
		UseTruncate:              dbOpts.UseTruncate,

		QueryLogger:    queryLogger,
		ReadRowsLogger: readRowsLogger,
		ExplainLogger:  explainLogger,
		SystemLogger:   systemLogger,
	})
	if err != nil {
		return nil, err
	}

	c = &DBConnector{
		Logger:        logger,
		DbOpts:        dbOpts,
		RetryAttempts: retryAttempts,
		WorkerID:      workerID,
		database:      dbConn,
	}

	// go connectionsChecker(c)

	return c, nil
}

// Release safely releases the connection back to the pool
func (c *DBConnector) Release() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.database != nil {
		connPool.put(c)
	}
}

// Close forcefully closes the connection
func (c *DBConnector) Close() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.database != nil {
		c.database.Close()
		c.database = nil
	}
}

// SetLogLevel sets log level
func (c *DBConnector) SetLogLevel(logLevel int) {
	c.Logger.LogLevel = logLevel
}

// Log logs a message
func (c *DBConnector) Log(LogLevel int, format string, args ...interface{}) {
	c.Logger.Log(LogLevel, c.WorkerID, format, args...)
}

// Logn logs a message without a newline
func (c *DBConnector) Logn(LogLevel int, format string, args ...interface{}) {
	c.Logger.Logn(LogLevel, c.WorkerID, format, args...)
}

// Exit exits with an error message
func (c *DBConnector) Exit(fmts string, args ...interface{}) {
	fmt.Printf(fmts, args...)
	fmt.Println()
	os.Exit(127)
}

type dbLogger struct {
	level  int
	worker int
	logger *benchmark.Logger
}

func (l *dbLogger) Log(format string, args ...interface{}) {
	l.logger.Log(l.level, l.worker, format, args...)
}
