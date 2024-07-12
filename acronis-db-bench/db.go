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
	ConnString   string `long:"connection-string" description:"connection string" default:"sqlite://:memory:" required:"false"`
	MaxOpenConns int    `long:"max-open-cons" description:"max open connections per worker" default:"2" required:"false"`
	Reconnect    bool   `long:"reconnect" description:"reconnect to DB before every test iteration" required:"false"`

	DryRun bool `long:"dry-run" description:"do not execute any INSERT/UPDATE/DELETE queries on DB-side" required:"false"`

	LogQueries    bool `long:"log-queries" description:"log queries" required:"false"`
	LogReadedRows bool `long:"log-readed-rows" description:"log readed rows" required:"false"`
	LogQueryTime  bool `long:"log-query-time" description:"log query time" required:"false"`

	DontCleanup bool `long:"dont-cleanup" description:"do not cleanup DB content before/after the test in '-t all' mode" required:"false"`
	UseTruncate bool `long:"use-truncate" description:"use TRUNCATE instead of DROP TABLE in cleanup procedure" required:"false"`
}

// dbConnectorsPool is a simple connection pool, required not to saturate DB connection pool
type dbConnectorsPool struct {
	lock sync.Mutex
	pool map[string]*DBConnector
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
	k := p.key(conn.DbOpts.ConnString, conn.WorkerID)

	p.lock.Lock()
	defer p.lock.Unlock()

	_, exists := p.pool[k]
	if exists {
		FatalError("trying to put connection while another connection in the pool already exists")
	}
	p.pool[k] = conn
	conn.Log(benchmark.LogTrace, "releasing connection to the connection pool")
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

	var queryLogger, readedRowsLogger, queryTimeLogger db.Logger
	if dbOpts.LogQueries {
		queryLogger = &dbLogger{level: benchmark.LogTrace, worker: workerID, logger: logger}
	}

	if dbOpts.LogReadedRows {
		readedRowsLogger = &dbLogger{level: benchmark.LogTrace, worker: workerID, logger: logger}
	}

	if dbOpts.LogQueryTime {
		queryTimeLogger = &dbLogger{level: benchmark.LogTrace, worker: workerID, logger: logger}
	}

	var dbConn, err = db.Open(db.Config{
		ConnString:   dbOpts.ConnString,
		MaxOpenConns: dbOpts.MaxOpenConns,
		DryRun:       dbOpts.DryRun,
		UseTruncate:  dbOpts.UseTruncate,

		QueryLogger:      queryLogger,
		ReadedRowsLogger: readedRowsLogger,
		QueryTimeLogger:  queryTimeLogger,
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

// Release releases the connection to the pool
func (c *DBConnector) Release() {
	connPool.put(c)
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
