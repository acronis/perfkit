package benchmark

// This file to be removed after final migration to DBR

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MichaelS11/go-cql-driver"
	"github.com/gocraft/dbr"
)

// dbConnectorsPool is a simple connection pool, required not to saturate DB connection pool
type dbConnectorsPool struct {
	lock sync.Mutex
	pool map[string]*DBConnector
}

// key returns a unique key for the connection pool
func (p *dbConnectorsPool) key(driver string, dsn string, workerID int) string {
	return fmt.Sprintf("%s-%s-%d", driver, dsn, workerID)
}

// take returns a connection from the pool or nil if the pool is empty
func (p *dbConnectorsPool) take(dbOpts *DatabaseOpts, workerID int) *DBConnector {
	k := p.key(dbOpts.Driver, dbOpts.Dsn, workerID)

	p.lock.Lock()
	defer p.lock.Unlock()

	conn, exists := p.pool[k]
	if exists {
		delete(p.pool, k)
		conn.Log(LogTrace, "taking connection from the connection pool")

		return conn
	}

	return nil
}

// put puts a connection to the pool
func (p *dbConnectorsPool) put(conn *DBConnector) {
	k := p.key(conn.DbOpts.Driver, conn.DbOpts.Dsn, conn.WorkerID)

	p.lock.Lock()
	defer p.lock.Unlock()

	_, exists := p.pool[k]
	if exists {
		FatalError("trying to put connection while another connection in the pool already exists")
	}
	p.pool[k] = conn
	conn.Log(LogTrace, "releasing connection to the connection pool")
}

// newDBConnectorsPool creates a new connection pool
func newDBConnectorsPool() *dbConnectorsPool {
	p := dbConnectorsPool{}
	p.pool = make(map[string]*DBConnector)

	return &p
}

// connPool is a global connection pool
var connPool = newDBConnectorsPool()

/*
 * DB connection management
 */

// DBConnector is a wrapper for DB connection
type DBConnector struct {
	Logger        *Logger
	DbOpts        *DatabaseOpts
	RetryAttempts int
	WorkerID      int

	lock      sync.Mutex
	lastQuery string
	logLevel  int
	dbSess    *sql.DB
	dbrSess   *dbr.Session
	tx        *sql.Tx
	txStart   time.Time
}

// connectionsChecker checks for potential connections leak
func connectionsChecker(conn *DBConnector) {
	for {
		if conn.dbSess != nil {
			openConnections := 0

			conn.lock.Lock()
			if conn.dbSess != nil {
				stats := conn.dbSess.Stats()
				openConnections = stats.OpenConnections
			}
			conn.lock.Unlock()

			if openConnections > 1 {
				conn.Log(LogError, fmt.Sprintf("internal error: potential connections leak detected, ensure the previous DB query closed the connection:\n%s\n",
					conn.lastQuery))
			}
		}
		time.Sleep(3 * time.Second)
	}
}

// NewDBConnector creates a new DBConnector
func NewDBConnector(dbOpts *DatabaseOpts, workerID int, logger *Logger, retryAttempts int) *DBConnector {
	c := connPool.take(dbOpts, workerID)
	if c != nil {
		return c
	}

	c = &DBConnector{
		Logger:        logger,
		DbOpts:        dbOpts,
		RetryAttempts: retryAttempts,
		WorkerID:      workerID,
	}

	go connectionsChecker(c)

	c.SetLogLevel(LogTrace)

	return c
}

// Release releases the connection to the pool
func (c *DBConnector) Release() {
	connPool.put(c)
}

// SetLogLevel sets log level
func (c *DBConnector) SetLogLevel(logLevel int) {
	c.logLevel = logLevel
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
	if c.Logger.LogLevel >= LogDebug {
		fmt.Println()
		printStack()
	}
	fmt.Printf(fmts, args...)
	fmt.Println()
	os.Exit(127)
}

// db returns a DB connection
func (c *DBConnector) db() *sql.DB {
	if c.Logger.LogLevel >= LogDebug && c.dbSess != nil {
		stats := c.dbSess.Stats()
		if stats.OpenConnections > 1 {
			c.Log(LogError, "Potential connections leak detected, ensure the previous DB query closed the connection: %s", c.lastQuery)
		}
	}

	if c.dbSess == nil {
		c.Connect()
	}

	return c.dbSess
}

// Ping pings the DB
func (c *DBConnector) Ping() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c.Log(c.logLevel, "ping")
	err := c.db().PingContext(ctx)
	if err != nil {
		err = fmt.Errorf("ping failed: %w", err)
		c.Log(c.logLevel, err.Error())
	}

	return err
}

// Connect connects to the DB
func (c *DBConnector) Connect() {
	if c.dbSess != nil {
		return
	}

	dsn := c.DbOpts.Dsn

	switch c.DbOpts.Driver {
	case SQLITE, POSTGRES, MYSQL, MSSQL, CLICKHOUSE, CASSANDRA:
		break
	default:
		c.Exit("unsupported driver: '%v', supported drivers are: %s", c.DbOpts.Driver, SupportedDrivers)
	}

	connect := func() {
		c.Log(LogTrace, "connecting to DB (native) ... ")

		connected := false
		var err error
		var sess *sql.DB

		driver := c.DbOpts.Driver
		if driver == SQLITE {
			driver = "sqlite3"
		}
		// dsn example: host1,host2?keyspace=mykeyspace&consistency=QUORUM&timeout=30s&connectTimeout=10s&numConns=5&username=user&password=pass&enableHostVerification=true&certPath=/path/to/cert&keyPath=/path/to/key&caPath=/path/to/ca
		// for the tests it is enough just to set 127.0.0.1
		//
		if driver == CASSANDRA || driver == "cql" {
			driver = "cql"
			cfg, err := cql.ConfigStringToClusterConfig(dsn)
			if err != nil {
				c.Exit("Can't convert cassandra dsn: %s: err: %s", dsn, err.Error())
			}
			if cfg.Keyspace != "" {
				CassandraKeySpace = cfg.Keyspace
			}
			cfg.Timeout = time.Minute
			cfg.ConnectTimeout = time.Minute
			dsn = cql.ClusterConfigToConfigString(cfg)
		}

		for r := 0; !connected && r < c.RetryAttempts; r++ {
			sess, err = sql.Open(driver, dsn)

			c.lock.Lock()
			c.dbSess = sess
			c.lock.Unlock()

			if err == nil {
				err = c.Ping()
				if err == nil {
					connected = true
				}
			}
			if !connected {
				c.Log(LogDebug, "DB connection attempt #%d failed, error: %v", r+1, err)
				time.Sleep(1 * time.Millisecond)
			}
		}

		if !connected {
			c.Exit("DB connection error: %v", err)
		}

		c.Log(LogTrace, "connected to DB")

		c.dbSess.SetMaxOpenConns(c.DbOpts.MaxOpenConns)
		c.dbSess.SetMaxIdleConns(c.DbOpts.MaxOpenConns)
	}

	connect()

	if c.DbOpts.Driver == CASSANDRA {
		cfg, err := cql.ConfigStringToClusterConfig(dsn)
		if err != nil {
			c.Exit("Can't convert cassandra dsn: %s: err: %s", dsn, err.Error())
		}
		if cfg.Keyspace == "" {
			cqlCreateKeyspaceIfNotExist := fmt.Sprintf(`
					 create keyspace if not exists %s 
					 with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
				`, CassandraKeySpace)
			_, err := c.Exec(cqlCreateKeyspaceIfNotExist)
			if err != nil {
				c.Exit("can't create keyspace: cql=%s: err: %s", cqlCreateKeyspaceIfNotExist, err.Error())
			}
			cfg.Keyspace = CassandraKeySpace
			dsn = cql.ClusterConfigToConfigString(cfg)
			c.Close()
			connect()
		}
	}
}

// GetVersion returns DB version and driver name
func (c *DBConnector) GetVersion() (string, string) {
	var version string
	var query string

	switch c.DbOpts.Driver {
	case POSTGRES:
		query = "SELECT version();"
	case MYSQL, CLICKHOUSE:
		query = "SELECT VERSION();"
	case CASSANDRA:
		query = "SELECT release_version FROM system.local;"
	case MSSQL:
		query = "SELECT @@VERSION;"
	case SQLITE:
		query = "SELECT sqlite_version();"
	default:
		c.Exit("Unsupported driver: %s", c.DbOpts.Driver)
	}

	c.QueryRowAndScan(query, &version)

	if c.DbOpts.Driver == MYSQL {
		var versionComment string
		query = "SELECT @@VERSION_COMMENT;"
		c.QueryRowAndScan(query, &versionComment)

		version = fmt.Sprintf("%s (%s)", version, versionComment)
	}

	return c.DbOpts.Driver, version
}

// GetInfo returns DB info
func (c *DBConnector) GetInfo(version string) (ret []string, dbInfo *DBInfo) {
	dbInfo = NewDBInfo(c, version)

	switch c.DbOpts.Driver {
	case POSTGRES:
		// Execute SHOW ALL command
		rows, err := c.Query("SHOW ALL")
		if err != nil {
			c.Exit(err.Error())
		}
		defer rows.Close()

		header := "|-------------------------------------|--------------------------------------------------------------|------------|"

		ret = append(ret, header)
		ret = append(ret, fmt.Sprintf("| %-35s | %-60s | %-10s |", "Name", "Setting", "Unit"))
		ret = append(ret, header)

		for rows.Next() {
			var name, setting, unit sql.NullString
			if err := rows.Scan(&name, &setting, &unit); err != nil {
				c.Exit(err.Error())
			}

			s := TernaryStr(name.Valid, name.String, "")
			v := TernaryStr(setting.Valid, setting.String, "")

			dbInfo.AddSetting(s, v)
			ret = append(ret, fmt.Sprintf("| %-35s | %-60s | %-10s |", s, v, TernaryStr(unit.Valid, setting.String, "")))
		}
		ret = append(ret, header)

		if err := rows.Err(); err != nil {
			c.Exit(err.Error())
		}
	case MYSQL:
		query := "SHOW VARIABLES;"
		rows, err := c.Query(query)
		if err != nil {
			c.Exit("Failed to execute query: %s, error: %s", query, err)
		}
		defer rows.Close()

		var variableName, value string

		header := "-----------------------------------------|-----------------------------------------------"
		ret = append(ret, header)
		ret = append(ret, fmt.Sprintf("%-40s | %-40s", "Variable_Name", "Value"))
		ret = append(ret, header)

		for rows.Next() {
			err := rows.Scan(&variableName, &value)
			if err != nil {
				c.Exit(err.Error())
			}
			dbInfo.AddSetting(variableName, value)
			ret = append(ret, fmt.Sprintf("%-40s | %-40s", variableName, value))
		}
		ret = append(ret, header)

		if err = rows.Err(); err != nil {
			c.Exit(err.Error())
		}
	case MSSQL:
		query := "SELECT * FROM sys.configurations"
		rows, err := c.Query(query)
		if err != nil {
			c.Exit("Failed to execute query: %s, error: %s", query, err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			c.Exit("Failed to get columns: %s", err)
		}

		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		values := make([]sql.RawBytes, len(cols))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		header := ""
		for range cols {
			header += strings.Repeat("-", 37)
		}
		ret = append(ret, header)

		str := ""
		for _, col := range cols {
			str += fmt.Sprintf("%-35s | ", col)
		}
		ret = append(ret, str)

		// Fetch rows
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				c.Exit(err.Error())
			}

			var value string
			str = ""
			for _, col := range values {
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				str += fmt.Sprintf("%-35s | ", value)
			}
			ret = append(ret, str)
		}
		ret = append(ret, header)

		if err = rows.Err(); err != nil {
			c.Exit(err.Error())
		}
	case CASSANDRA:
		// Execute a CQL query
		rows, err := c.Query("SELECT * FROM system.local") // Replace with your actual query
		if err != nil {
			c.Exit("Failed to execute query: %s", err.Error())
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			c.Exit("failed to get columns: %s", err.Error())
		}

		// Prepare a slice of interface{}'s to hold each value
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Iterate over rows
		for rows.Next() {
			err = rows.Scan(values...)
			if err != nil {
				c.Exit("failed to scan row: %s", err.Error())
			}

			for i, value := range values {
				ret = append(ret, fmt.Sprintf("%s: %v", columns[i], *(value.(*interface{}))))
			}
		}

		// Check for errors after iterating
		if err := rows.Err(); err != nil {
			c.Exit("Error during row iteration: %s", err.Error())
		}
	case SQLITE, CLICKHOUSE:
		//
	default:
		c.Exit("Unsupported driver: %s", c.DbOpts.Driver)
	}

	return ret, dbInfo
}

// rUpdatePlaceholders is a regexp to replace placeholders
var rUpdatePlaceholders = regexp.MustCompile(`\$\d+`)

// updatePlaceholders replaces placeholders
func (c *DBConnector) updatePlaceholders(query string) string {
	if c.DbOpts.Driver == MYSQL || c.DbOpts.Driver == SQLITE || c.DbOpts.Driver == CASSANDRA {
		return rUpdatePlaceholders.ReplaceAllString(query, "?")
	}

	return query
}

// Close closes the DB connection
func (c *DBConnector) Close() {
	if c.dbSess != nil {
		if c.tx != nil {
			err := c.tx.Commit()
			if err != nil {
				c.Exit(err.Error())
			}
		}
		c.dbSess.Close()
		c.Log(LogTrace, "closing 'regular' DB connection")

		c.lock.Lock()
		c.dbSess = nil
		c.lock.Unlock()
	}
	if c.dbrSess != nil {
		c.dbrSess.Close()

		c.lock.Lock()
		c.dbrSess = nil
		c.lock.Unlock()

		c.Log(LogTrace, "closing 'DBR' DB connection")
	}
}

// StatementEnter is called before executing a statement
func (c *DBConnector) StatementEnter(query string, args ...interface{}) time.Time { //nolint:revive
	var startTime time.Time

	if query != "" {
		c.lastQuery = query
	}

	if c.Logger.LogLevel >= c.logLevel {
		startTime = time.Now()
	}

	return startTime
}

// StatementExit is called after executing a statement
func (c *DBConnector) StatementExit(statement string, startTime time.Time, err error, showRowsAffected bool, result sql.Result, format string, args []interface{}, rows *DBRows, dest []interface{}) {
	if c.Logger.LogLevel < c.logLevel && err == nil {
		return
	}

	var msg string
	if c.Logger.LogLevel >= LogTrace {
		if format == "" {
			msg = fmt.Sprintf("%v", args...)
		} else {
			msg = fmt.Sprintf(format, args...)
		}
	} else {
		msg = format
	}

	if err == nil {
		if c.Logger.LogLevel >= LogDebug {
			msg += fmt.Sprintf(" # dur: %.6f", getElapsedTime(startTime))
		}
		if c.Logger.LogLevel >= LogTrace {
			if c.DbOpts.Driver != CLICKHOUSE && showRowsAffected && result != nil {
				affectedRows, err := result.RowsAffected()
				if err != nil {
					c.Exit("DB: %s failed: %s\nError: %s", c.DbOpts.Driver, statement, err.Error())
				}
				msg += fmt.Sprintf(" # affected rows: %d", affectedRows)
			}
			if rows != nil {
				msg += fmt.Sprintf(" # = %d row(s): %s", len(rows.data), rows.Dump())
			}
			if dest != nil {
				var vals []string
				for _, v := range dest {
					vals = append(vals, DumpRecursive(v, ""))
				}
				msg += fmt.Sprintf(" = %v", strings.Join(vals, ", "))
			}
		}
		c.Log(c.logLevel, msg)
	} else {
		c.Log(LogError, fmt.Sprintf("%s: '%s' error:\n%s", statement, msg, err.Error()))
	}
}

// Begin starts a transaction
func (c *DBConnector) Begin() *sql.Tx {
	// CASSANDRA doesn't support transactions
	if c.DbOpts.Driver == CASSANDRA {
		return nil
	}
	if c.DbOpts.DryRun {
		c.Log(LogTrace, "skipping BEGIN request because of 'dry run' mode")

		return nil
	}
	if c.tx != nil {
		c.Exit("internal error: trying to call Begin() while transaction is already open")
	}

	if c.Logger.LogLevel >= LogDebug {
		c.txStart = time.Now()
	}

	var err error
	c.tx, err = c.db().Begin()
	c.Log(LogDebug, "BEGIN")
	if err != nil {
		c.Exit(err.Error())
	}

	return c.tx
}

// Commit commits a transaction
// Note: CASSANDRA doesn't support transactions
func (c *DBConnector) Commit() {
	if c.DbOpts.Driver == CASSANDRA {
		return
	}
	if c.DbOpts.DryRun {
		c.Log(LogTrace, "skipping COMMIT request because of 'dry run' mode")

		return
	}
	if c.tx == nil {
		c.Exit("internal error: trying to call Commit() w/o Begin()")
	}

	err := c.tx.Commit()

	if err == nil {
		if c.Logger.LogLevel >= LogDebug {
			c.Log(LogDebug, fmt.Sprintf("COMMIT # dur: %.6f", getElapsedTime(c.txStart)))
		}
	} else {
		c.Exit("DB commit failed\nError: %s", err.Error())
	}
	c.tx = nil
}

// getElapsedTime returns elapsed time since startTime
func getElapsedTime(prevTime time.Time) float64 {
	return time.Since(prevTime).Seconds()
}

// Exec executes a statement
func (c *DBConnector) Exec(format string, args ...interface{}) (sql.Result, error) {
	var result sql.Result
	var err error

	format = c.updatePlaceholders(format)
	startTime := c.StatementEnter(format, args)

	if c.DbOpts.DryRun {
		c.Log(LogTrace, "skipping the '"+format+"' request because of 'dry run' mode")

		return result, nil
	}

	if c.tx == nil {
		result, err = c.db().Exec(format, args...)
	} else {
		result, err = c.tx.Exec(format, args...)
	}

	if err != nil {
		err = fmt.Errorf("exec failed: %w", err)
	}

	c.StatementExit("Exec()", startTime, err, true, result, format, args, nil, nil)

	return result, err
}

// Query executes a query
func (c *DBConnector) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error

	query = c.updatePlaceholders(query)
	startTime := c.StatementEnter(query, args)

	if c.tx == nil {
		rows, err = c.db().Query(query, args...)
	} else {
		rows, err = c.tx.Query(query, args...)
	}

	if err != nil {
		err = fmt.Errorf("query failed: %w", err)
	}

	c.StatementExit("Query()", startTime, err, false, nil, query, args, nil, nil)

	return rows, err
}

// InsertInto inserts data into a table
func (c *DBConnector) InsertInto(tableName string, data interface{}, columnNames []string) {
	var valuesList []reflect.Value
	v := reflect.ValueOf(data)
	var fields reflect.Type
	// var
	if v.Kind() == reflect.Slice {
		s := reflect.ValueOf(data)
		for i := 0; i < s.Len(); i++ {
			valuesList = append(valuesList, s.Index(i))
			if i == 0 {
				fields = s.Index(i).Type()
			}
		}
	} else {
		valuesList = append(valuesList, reflect.ValueOf(data))
		fields = reflect.TypeOf(data)
	}

	if len(valuesList) == 0 {
		c.Log(LogDebug, "no data to insert")
	}

	numFields := fields.NumField()

	column2val := make(map[string]interface{})

	var columnValues []interface{}
	var valuesPlaceholders []string
	for n, values := range valuesList {
		for i := 0; i < numFields; i++ {
			columnName := fields.Field(i).Tag.Get("db")
			if columnName == "" {
				continue
			}
			column2val[columnName] = values.Field(i).Interface()
		}

		for _, col := range columnNames {
			if _, exists := column2val[col]; !exists {
				c.Exit(fmt.Sprintf("can't find data for column '%s' in object '%v'", col, data))
			}
			columnValues = append(columnValues, column2val[col])
		}
		if c.DbOpts.Driver == CASSANDRA {
			placeholder := GenDBParameterPlaceholdersCassandra(n*len(columnNames), len(columnNames))
			valuesPlaceholders = append(valuesPlaceholders, fmt.Sprintf("(%s)", placeholder))
		} else {
			placeholder := GenDBParameterPlaceholders(n*len(columnNames), len(columnNames))
			valuesPlaceholders = append(valuesPlaceholders, fmt.Sprintf("(%s)", placeholder))
		}
	}

	query := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(valuesPlaceholders, ", "))

	_, err := c.Exec(query, columnValues...)
	if err != nil {
		c.Exit("DB exec failed: %s\nError: %s", query, err.Error())
	}
}

// queryRowAndScan executes a query and scans the result
func (c *DBConnector) queryRowAndScan(query string, allowEmpty bool, dest ...interface{}) {
	var err error

	query = c.updatePlaceholders(query)
	startTime := c.StatementEnter(query, nil)

	if c.tx == nil {
		err = c.db().QueryRow(query).Scan(dest...)
	} else {
		err = c.tx.QueryRow(query).Scan(dest...)
	}

	if err == nil {
		c.StatementExit("QueryRow().Scan()", startTime, err, false, nil, query, nil, nil, dest)
	} else {
		if allowEmpty && errors.Is(sql.ErrNoRows, err) {
			if c.Logger.LogLevel >= c.logLevel {
				c.Log(c.logLevel, fmt.Sprintf("%s # dur: %.6f = empty row", query, getElapsedTime(startTime)))
			}
		} else {
			c.Exit("DB query failed: %s\nError: %s", query, err.Error())
		}
	}
}

// QueryRowAndScan executes a query and scans the result (not allowing empty result)
func (c *DBConnector) QueryRowAndScan(query string, dest ...interface{}) {
	c.queryRowAndScan(query, false, dest...)
}

// QueryRowAndScanAllowEmpty executes a query and scans the result (allowing empty result)
func (c *DBConnector) QueryRowAndScanAllowEmpty(query string, dest ...interface{}) {
	c.queryRowAndScan(query, true, dest...)
}

// addExplainPrefix adds an 'explain' prefix to the query
func (c *DBConnector) addExplainPrefix(query string) string {
	switch c.DbOpts.Driver {
	case MYSQL:
		return "EXPLAIN " + query
	case POSTGRES:
		return "EXPLAIN ANALYZE " + query
	case SQLITE:
		return "EXPLAIN QUERY PLAN " + query
	case CASSANDRA:
		return "TRACING ON; " + query
	default:
		c.Exit("The 'explain' mode is not supported for given database driver: %s", c.DbOpts.Driver)
	}

	return ""
}

// explain executes an 'explain' query
func (c *DBConnector) explain(rows *sql.Rows, query string, args ...interface{}) {
	// Iterate over the result set
	cols, err := rows.Columns()
	if err != nil {
		c.Exit("Explain() error: %s", err)
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	fmt.Printf("\n%s", query)
	if args != nil {
		fmt.Printf(" %v\n", args)
	} else {
		fmt.Printf("\n")
	}

	for rows.Next() {
		switch c.DbOpts.Driver {
		case SQLITE:
			var id, parent, notUsed int
			var detail string
			if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
				c.Exit("DB query result scan failed: %s\nError: %s", query, err.Error())

				return
			}
			fmt.Printf("ID: %d, Parent: %d, Not Used: %d, Detail: %s\n", id, parent, notUsed, detail)
		case MYSQL:
			if err := rows.Scan(scanArgs...); err != nil {
				c.Exit("DB query result scan failed: %s\nError: %s", query, err.Error())

				return
			}
			// Print each column as a string.
			for i, col := range values {
				fmt.Printf("  %-15s: %s\n", cols[i], string(col))
			}
			fmt.Println()
		case POSTGRES:
			var explainOutput string
			if err := rows.Scan(&explainOutput); err != nil {
				c.Exit("DB query result scan failed: %s\nError: %s", query, err.Error())

				return
			}
			fmt.Println("  ", explainOutput)
		case CASSANDRA:
			var explainOutput string
			if err := rows.Scan(&explainOutput); err != nil {
				c.Exit("DB query result scan failed: %s\nError: %s", query, err.Error())

				return
			}
			fmt.Println("  ", explainOutput)
		default:
			c.Exit("The 'explain' mode is not supported for given database driver: %s", c.DbOpts.Driver)
		}
	}
}

// fetchRows fetches rows from the result set and returns them as a slice of maps
func (c *DBConnector) fetchRows(rows *sql.Rows, query string, args ...interface{}) *DBRows { //nolint:revive,unparam
	if rows == nil {
		return &DBRows{}
	}

	cols, err := rows.Columns()
	if err != nil {
		c.Exit("DB query failed: %s\nError: %s", query, err.Error())
	}

	colsCnt := len(cols)
	var rawRows []dbRow

	for rows.Next() {
		rawData := make([]interface{}, colsCnt)
		ptrs := make([]interface{}, colsCnt)
		for i := range cols {
			ptrs[i] = &rawData[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			c.Exit("DB query result scan failed: %s\nError: %s", query, err.Error())

			return nil
		}

		rawRows = append(rawRows, rawData)
	}

	ret := DBRows{data: rawRows}

	return &ret
}

// SelectRaw executes a query and returns the result set as a slice of maps
func (c *DBConnector) SelectRaw(explain bool, query string, args ...interface{}) *DBRows {
	var rows *sql.Rows
	var err error
	startTime := c.StatementEnter(query, args)

	if explain {
		query = c.addExplainPrefix(query)
	}

	if c.tx == nil {
		rows, err = c.db().Query(query, args...)
	} else {
		rows, err = c.tx.Query(query, args...)
	}

	defer rows.Close()

	if explain {
		c.explain(rows, query, args...)

		return nil
	}

	ret := c.fetchRows(rows, query, args...)

	if err != nil {
		c.Exit("DB query failed: %s\nError: %s", query, err.Error())
	}

	c.StatementExit("Query()", startTime, err, false, nil, query, args, ret, nil)

	return ret
}

// Select executes a query and returns the result set as a slice of maps
func (c *DBConnector) Select(from string, what string, where string, orderBy string, limit int, explain bool, args ...interface{}) *DBRows {
	var query string

	switch c.DbOpts.Driver {
	case MSSQL:
		query = fmt.Sprintf("SELECT {LIMIT} %s FROM %s {WHERE} {ORDERBY}", what, from)
	default:
		query = fmt.Sprintf("SELECT %s FROM %s {WHERE} {ORDERBY} {LIMIT}", what, from)
	}

	if where == "" {
		query = strings.Replace(query, "{WHERE}", "", -1)
	} else {
		query = strings.Replace(query, "{WHERE}", fmt.Sprintf("WHERE %s", where), -1) //nolint:perfsprint
	}

	if limit == 0 {
		query = strings.Replace(query, "{LIMIT}", "", -1)
	} else {
		switch c.DbOpts.Driver {
		case MSSQL:
			query = strings.Replace(query, "{LIMIT}", fmt.Sprintf("TOP %d", limit), -1)
		default:
			query = strings.Replace(query, "{LIMIT}", fmt.Sprintf("LIMIT %d", limit), -1)
		}
	}

	if orderBy == "" {
		query = strings.Replace(query, "{ORDERBY}", "", -1)
	} else {
		query = strings.Replace(query, "{ORDERBY}", fmt.Sprintf("ORDER BY %s", orderBy), -1) //nolint:perfsprint
	}

	query = c.updatePlaceholders(query)

	return c.SelectRaw(explain, query, args...)
}

// ExecOrExit executes a statement or exits
func (c *DBConnector) ExecOrExit(format string, args ...interface{}) {
	_, err := c.Exec(format, args...)
	if err != nil {
		c.Exit("DB exec failed: %s\nError: %s", format, err.Error())
	}
}

// QueryOrExit executes a query or exits
func (c *DBConnector) QueryOrExit(format string, args ...interface{}) {
	rows := c.QueryOrExitWithResult(format, args...)
	rows.Close() //nolint:sqlclosecheck
}

// QueryOrExitWithResult executes a query or exits and returns the result set
func (c *DBConnector) QueryOrExitWithResult(format string, args ...interface{}) *sql.Rows {
	rows, err := c.Query(format, args...)
	if err != nil {
		c.Exit("DB query failed: %s\nError: %s", format, err.Error())

		return nil
	}
	if rows.Err() != nil {
		c.Exit("DB query failed: %s\nError: %s", format, rows.Err())

		return nil
	}

	return rows
}

// QueryAndReturnString executes a query and returns the result as a string
func (c *DBConnector) QueryAndReturnString(format string, args ...interface{}) string {
	rows := c.QueryOrExitWithResult(format, args...)
	defer rows.Close()

	var result string
	for rows.Next() {
		if err := rows.Scan(&result); err != nil {
			c.Exit("Error: an error occurred when during query and getting string from result %s: %s", format, err.Error())
		}
	}

	return result
}

// cassandraTableExists checks if a table exists in cassandra
func cassandraTableExists(db *sql.DB, keyspace, tableName string) (bool, error) {
	// Query to check the existence of the table
	query := `SELECT count(*) FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`
	var count int

	// Execute the query
	err := db.QueryRow(query, keyspace, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("error querying table existence: %v", err)
	}

	return count > 0, nil
}

// TableExists checks if a table exists
func (c *DBConnector) TableExists(tableName string) bool {
	var query string

	if c.DbOpts.Driver == SQLITE {
		if tableName == "sqlite_master" {
			return true
		}
		rows := c.GetRowsCount("sqlite_master", fmt.Sprintf("type='table' AND name='%s'", tableName))

		return rows == 1
	}
	if c.DbOpts.Driver == CASSANDRA {
		exists, err := cassandraTableExists(c.db(), CassandraKeySpace, tableName)
		if err != nil {
			c.Exit("Can't check cassandra table existing: err: %s", err.Error())
		}

		return exists
	}

	switch c.DbOpts.Driver {
	case MYSQL:
		query = fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '%s')", tableName)
	case POSTGRES:
		query = fmt.Sprintf("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='%s')", tableName)
	case MSSQL:
		query = fmt.Sprintf("SELECT CASE WHEN EXISTS ( SELECT 1 FROM sys.objects WHERE name = '%s' AND type = 'U') THEN 1 ELSE 0 END AS TableExists", tableName)
	case CLICKHOUSE:
		query = fmt.Sprintf("SELECT count() > 0 FROM system.tables WHERE name = '%s'", tableName)
	default:
		c.Exit("Unsupported driver: %s", c.DbOpts.Driver)
	}

	var exists bool
	c.QueryRowAndScan(query, &exists)

	return exists
}

// DbConstraint represents a database constraint
type DbConstraint struct {
	Name       string `json:"name"`
	TableName  string `json:"table_name"`
	Definition string `json:"definition"`
}

// ReadConstraints reads constraints from the database
func (c *DBConnector) ReadConstraints() []DbConstraint {
	if c.DbOpts.Driver != POSTGRES {
		return nil
	}

	query := `
	SELECT conname, conrelid::regclass AS table_name, pg_get_constraintdef(oid) AS condef
	FROM pg_constraint
	WHERE contype IN ('f', 'p', 'u');
	`

	rows := c.QueryOrExitWithResult(query)
	defer rows.Close()

	var constraints []DbConstraint

	for rows.Next() {
		var constraint DbConstraint
		if err := rows.Scan(&constraint.Name, &constraint.TableName, &constraint.Definition); err != nil {
			return nil
		}
		if !strings.HasPrefix(constraint.TableName, "acronis_db_bench") {
			continue
		}
		if strings.Contains(strings.ToLower(constraint.Definition), "foreign key") {
			constraints = append(constraints, constraint)
		}
	}

	return constraints
}

// RemoveConstraints removes constraints from the database
func (c *DBConnector) RemoveConstraints(constraints []DbConstraint) {
	if c.DbOpts.Driver != POSTGRES || constraints == nil {
		return
	}
	for _, constraint := range constraints {
		query := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", constraint.TableName, constraint.Name)
		c.Log(LogDebug, fmt.Sprintf("removing constraint '%s', '%s' on table '%s'", constraint.Name, constraint.Definition, constraint.TableName))
		c.ExecOrExit(query)
	}
}

// RestoreConstraints restores constraints in the database
func (c *DBConnector) RestoreConstraints(constraints []DbConstraint) {
	if c.DbOpts.Driver != POSTGRES || constraints == nil {
		return
	}
	for _, constraint := range constraints {
		query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", constraint.TableName, constraint.Name, constraint.Definition)
		c.Log(LogDebug, fmt.Sprintf("restoring constraint '%s', '%s' on table '%s'", constraint.Name, constraint.Definition, constraint.TableName))
		c.ExecOrExit(query)
	}
}

// DropTable drops a table if it exists
func (c *DBConnector) DropTable(tableName string) {
	if c.DbOpts.UseTruncate {
		if c.TableExists(tableName) {
			c.Log(LogDebug, fmt.Sprintf("truncate table '%s'", tableName))
			switch c.DbOpts.Driver {
			case POSTGRES:
				c.ExecOrExit("TRUNCATE TABLE " + tableName + " CASCADE")
			default:
				c.ExecOrExit("TRUNCATE TABLE " + tableName)
			}
		}
	} else {
		c.Log(LogDebug, fmt.Sprintf("drop table '%s'", tableName))
		c.ExecOrExit("DROP TABLE IF EXISTS " + tableName)
	}
}

// DropIndex drops an index if it exists
func (c *DBConnector) DropIndex(indexName string) {
	switch c.DbOpts.Driver {
	case CLICKHOUSE:
		//
	default:
		c.ExecOrExit("DROP INDEX " + indexName)
	}
}

// CreateSequence creates a sequence if it doesn't exist
func (c *DBConnector) CreateSequence(sequenceName string) {
	switch c.DbOpts.Driver {
	case POSTGRES, MYSQL:
		c.ExecOrExit("CREATE SEQUENCE IF NOT EXISTS " + sequenceName)
	case SQLITE:
		if !c.TableExists(sequenceName) {
			c.CreateTable(sequenceName, fmt.Sprintf("CREATE TABLE %s (value BIGINT NOT NULL, sequence_id INT NOT NULL); ALTER TABLE %s ADD INDEX %s_value (value);",
				sequenceName, sequenceName, sequenceName))
			c.ExecOrExit(fmt.Sprintf("INSERT INTO %s (value, sequence_id) VALUES (1, 1)", sequenceName))
		}
	case MSSQL:
		c.ExecOrExit(fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = '%[1]s') BEGIN CREATE SEQUENCE %[1]s AS BIGINT START WITH 1 INCREMENT BY 1; END;",
			sequenceName))
	case CLICKHOUSE, CASSANDRA:
		// CLICKHOUSE and CASSANDRA can't manage sequences
	default:
		c.Exit("unknown driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", c.DbOpts.Driver)
	}
}

// DropSequence drops a sequence if it exists
func (c *DBConnector) DropSequence(sequenceName string) {
	switch c.DbOpts.Driver {
	case POSTGRES, MSSQL:
		c.ExecOrExit("DROP SEQUENCE IF EXISTS " + sequenceName)
	case MYSQL, SQLITE:
		c.DropTable(sequenceName)
	case CLICKHOUSE, CASSANDRA:
		//
	default:
		c.Exit("unknown driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", c.DbOpts.Driver)
	}
}

// GetRowsCount returns the number of rows in a table
func (c *DBConnector) GetRowsCount(tableName string, where string) (rowNum uint64) {
	rows := c.dbQueryIfExist("COUNT(*)", tableName, where)
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&rowNum); err != nil {
			c.Exit("Error: an error occurred when counting row number in table %s: %s", tableName, err.Error())
		}
	}

	return rowNum
}

// GetTableSizeMB returns the size of a table in MB
func (c *DBConnector) GetTableSizeMB(tableName string) (sizeMB int64) {
	switch c.DbOpts.Driver {
	case POSTGRES:
		c.QueryRowAndScanAllowEmpty(fmt.Sprintf("SELECT pg_total_relation_size('%s') / (1024 * 1024)", tableName), &sizeMB)
	case MYSQL:
		c.QueryRowAndScanAllowEmpty(fmt.Sprintf("SELECT Data_length FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s'",
			tableName), &sizeMB)
		sizeMB /= 1024 * 1024
	default:
		sizeMB = -1
	}

	return sizeMB
}

// GetIndexesSizeMB returns the size of indexes of a table in MB
func (c *DBConnector) GetIndexesSizeMB(tableName string) (sizeMB int64) {
	switch c.DbOpts.Driver {
	case POSTGRES:
		c.QueryRowAndScanAllowEmpty(fmt.Sprintf("SELECT pg_indexes_size('%s') / (1024 * 1024)", tableName), &sizeMB)
	case MYSQL:
		c.QueryRowAndScanAllowEmpty(fmt.Sprintf("SELECT Index_length FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s'",
			tableName), &sizeMB)
		sizeMB /= 1024 * 1024
	default:
		//
		sizeMB = -1
	}

	return sizeMB
}

// GetUUIDs returns UUIDs from a table
func (c *DBConnector) GetUUIDs(tableName, where string) (uuids []string) {
	rows := c.dbQueryIfExist("uuid", tableName, where)
	defer rows.Close()

	uuids = make([]string, 0)
	var uuid string
	for rows.Next() {
		if err := rows.Scan(&uuid); err != nil {
			c.Exit("Error: an error occurred when counting row number in table %s: %s", tableName, err.Error())
		}
		uuids = append(uuids, uuid)
	}

	return uuids
}

// dbQueryIfExist executes a query if a table exists
func (c *DBConnector) dbQueryIfExist(fields string, tableName string, where string) *sql.Rows {
	ok := c.TableExists(tableName)
	if !ok {
		c.Exit("Error: table '%s' doesn't exist, aborting", tableName)
	}

	query := "SELECT " + fields + " FROM " + tableName
	if where != "" {
		query = query + " WHERE " + where
	}

	rows, err := c.Query(query)
	if err != nil {
		c.Exit("Error: can't get rows count in table '%s':  %s", tableName, err.Error())
	}

	return rows
}

// GetNextVal returns the next value from a sequence
func (c *DBConnector) GetNextVal(sequenceName string) (nextVal uint64) {
	switch c.DbOpts.Driver {
	case POSTGRES, MSSQL, MYSQL:
		var query string
		if c.DbOpts.Driver == POSTGRES {
			query = "SELECT NEXTVAL('" + sequenceName + "')"
		} else if c.DbOpts.Driver == MYSQL {
			query = "SELECT NEXTVAL(" + sequenceName + ")"
		} else if c.DbOpts.Driver == MSSQL {
			query = "SELECT NEXT VALUE FOR " + sequenceName
		}

		rows, err := c.Query(query)
		if err != nil {
			c.Exit("Error: can't get nextVal for sequence '%s':  %s", sequenceName, err.Error())
		}
		defer rows.Close()

		for rows.Next() {
			if err := rows.Scan(&nextVal); err != nil {
				c.Exit("Error: an error occurred when getting nextVal in sequence %s: %s", sequenceName, err.Error())
			}
		}
		if c.Logger.LogLevel >= c.logLevel {
			c.Log(LogDebug, fmt.Sprintf("%s = %d", query, nextVal))
		}
	case SQLITE:
		var value int64
		c.Begin()
		c.QueryRowAndScan("SELECT value FROM "+sequenceName+" WHERE sequence_id = 1 FOR UPDATE", &value)
		c.ExecOrExit(fmt.Sprintf("UPDATE %s SET value = %d WHERE sequence_id = 1", sequenceName, value+1))
		c.Commit()
	default:
		c.Exit("unknown driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", c.DbOpts.Driver)
	}

	return nextVal
}

// QueryMaxVal returns the max value from a table
func (c *DBConnector) QueryMaxVal(tableName string, column string, where string) (retVal int) {
	ok := c.TableExists(tableName)
	if !ok {
		c.Exit("Error: can't get rows count from table '%s' for querying max value, table doesn't exist, aborting", tableName)
	}

	query := "SELECT COALESCE(max(" + column + "), 0) FROM " + tableName
	if where != "" {
		query = query + " WHERE " + where
	}

	rows, err := c.Query(query)
	if err != nil {
		c.Exit("Error: can't get rows count in table '%s':  %s", tableName, err.Error())
	}
	defer rows.Close()

	for rows.Next() {
		if err := rows.Scan(&retVal); err != nil {
			c.Exit("Error: an error occurred when counting row number in table %s: %s", tableName, err.Error())
		}
	}

	return
}

// SQLRandFunc - return the SQL Random() function equivalent for all drivers
func SQLRandFunc(b *Benchmark, driver string) string {
	switch driver {
	case SQLITE, SQLITE3, POSTGRES:
		return "RANDOM()"
	case MYSQL:
		return "RAND()"
	case MSSQL:
		return "NEWID()"
	default:
		b.Exit("unknown driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", driver)
	}

	return ""
}

// ApplyMigrations applies a set of migrations to a table
func (c *DBConnector) ApplyMigrations(tableName, tableMigrationSQL string) {
	var migrationQueries []string

	tableMigrationSQL, err := DefaultCreateQueryPatchFunc(tableName, tableMigrationSQL, c.DbOpts.Driver, c.DbOpts.MySQLEngine)
	if err != nil {
		c.Exit(err.Error())
	}
	c.Log(LogTrace, tableMigrationSQL)

	switch c.DbOpts.Driver {
	case MYSQL:
		// Percona (or MySQL?) fails to create all the steps within single transaction
		migrationQueries = strings.Split(tableMigrationSQL, ";")
	case CASSANDRA:
		migrationQueries = strings.Split(tableMigrationSQL, ";")
	default:
		migrationQueries = []string{tableMigrationSQL}
	}

	for i := range migrationQueries {
		q := strings.TrimSpace(migrationQueries[i])
		if q != "" {
			_, err := c.Exec(q)
			if err != nil {
				c.Exit("DB migration failed: %s\nError: %s", q, err.Error())
			}
		}
	}
}

// CreateTable creates a table if it doesn't exist
func (c *DBConnector) CreateTable(tableName string, tableMigrationSQL string) {
	if tableName == "" || c.TableExists(tableName) {
		return
	}

	if tableMigrationSQL == "" {
		c.Exit("internal error: table %s needs to be created, but migration query has not been provided", tableName)
	}

	c.ApplyMigrations(tableName, tableMigrationSQL)
	c.Log(LogDebug, fmt.Sprintf("created table: %s", tableName)) //nolint:perfsprint
}

// getTableMigrationSQL returns a table migration query for a given driver
func getTableMigrationSQL(tableMigrationSQL string, b *Benchmark, dbOpts DatabaseOpts) string { //nolint:unused
	driver := dbOpts.Driver

	switch driver {
	case MYSQL:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$id}", "id bigint not null AUTO_INCREMENT PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "MEDIUMBLOB")
		if dbOpts.MySQLEngine == "xpand-allnodes" {
			tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$engine}", "engine = xpand")
		} else {
			tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$engine}", "engine = "+dbOpts.MySQLEngine)
		}
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_type}", "json")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_index}",
			"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0')) STORED;"+
				"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0.field0')) STORED;"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0 ON acronis_db_bench_json(_data_f0f0);"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0f0 ON acronis_db_bench_json(_data_f0f0f0);")
	case SQLITE, SQLITE3:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$id}", "id INTEGER PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "MEDIUMBLOB")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$engine}", "")
	case MSSQL:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$id}", "id bigint IDENTITY(1,1) PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "varbinary(max)")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$engine}", "")
	case POSTGRES:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$id}", "id bigserial not null PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "BYTEA")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$engine}", "")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_type}", "jsonb")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_index}",
			"CREATE INDEX acronis_db_bench_json_idx_data ON acronis_db_bench_json USING GIN (json_data jsonb_path_ops)")
	default:
		b.Exit("unknown driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", driver)
	}

	return tableMigrationSQL
}

// makeIndexName returns an index name for a given table and columns
func makeIndexName(tableName string, columns string, id int) string {
	name := strings.Split(columns, " ")[0]
	name = strings.ReplaceAll(name, ",", "")

	return fmt.Sprintf("%s_idx_%s_%d", tableName, name, id)
}

// CreateIndex creates an index if it doesn't exist for a given table and columns
func (c *DBConnector) CreateIndex(tableName string, columns string, id int) {
	var checkIndexExistsQuery string
	indexExists := false
	indexName := makeIndexName(tableName, columns, id)

	if c.DbOpts.Driver == CLICKHOUSE {
		// CLICKHOUSE don't require to create indexes
		return
	} else if c.DbOpts.Driver == SQLITE {
		rows := c.GetRowsCount("sqlite_master", fmt.Sprintf("type='index' AND name='%s' AND tbl_name='%s'", indexName, tableName))
		if rows == 1 {
			indexExists = true
		}
	} else if c.DbOpts.Driver == CASSANDRA {
		query := "CREATE INDEX IF NOT EXISTS %s ON %s.%s (%s);"
		query = fmt.Sprintf(query, indexName, CassandraKeySpace, tableName, columns)
		_, err := c.Exec(query)
		if err != nil {
			c.Exit("DB exec failed: %s\nError: %s", query, err.Error())
		}
		c.Log(LogDebug, fmt.Sprintf("created index: %s", indexName)) //nolint:perfsprint

		return
	} else {
		// Choose the query based on the database type
		switch c.DbOpts.Driver {
		case POSTGRES:
			checkIndexExistsQuery = "SELECT EXISTS (SELECT * FROM pg_indexes WHERE indexname = '" + indexName + "')"
		case MYSQL:
			checkIndexExistsQuery = "SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_NAME = '" + tableName + "' AND INDEX_NAME = '" + indexName + "')"
		case MSSQL:
			checkIndexExistsQuery = "SELECT CASE WHEN EXISTS ( SELECT 1 FROM sys.indexes WHERE name = '" + indexName + "') THEN 1 ELSE 0 END AS IndexExists"
		default:
			c.Exit("unsupported database type: %s", c.DbOpts.Driver)
		}

		c.QueryRowAndScan(checkIndexExistsQuery, &indexExists)
	}

	// If the index does not exist, create it
	if !indexExists {
		query := "CREATE INDEX " + indexName + " ON " + tableName + "(" + columns + ")"
		_, err := c.Exec(query)
		if err != nil {
			c.Exit("DB exec failed: %s\nError: %s", query, err.Error())
		}
		c.Log(LogDebug, fmt.Sprintf("created index: %s", indexName)) //nolint:perfsprint
	}
}

// GetTablesVolumeInfo returns the volume info for a given set of tables
func (c *DBConnector) GetTablesVolumeInfo(tableNames []string) (ret []string) {
	ret = append(ret, fmt.Sprintf("%-55s %15s %17s %17s", "TABLE NAME", "ROWS", "DATA SIZE (MB)", "IDX SIZE (MB)"))
	ret = append(ret, fmt.Sprintf("%-55s %15s %17s %17s", strings.Repeat("-", 55), strings.Repeat("-", 15), strings.Repeat("-", 17), strings.Repeat("-", 17)))

	for _, tableName := range tableNames {
		str := ""
		str += fmt.Sprintf("%-55s ", tableName)
		if c.TableExists(tableName) {
			rows := c.GetRowsCount(tableName, "")
			tableSizeMB := c.GetTableSizeMB(tableName)
			idxSizeMB := c.GetIndexesSizeMB(tableName)
			str += fmt.Sprintf("%15d %17s %17s", rows,
				TernaryStr(tableSizeMB >= 0, strconv.FormatInt(tableSizeMB, 10), "?"),
				TernaryStr(idxSizeMB >= 0, strconv.FormatInt(idxSizeMB, 10), "?"))
		} else {
			str += fmt.Sprintf("%15s %17s %17s", "-", "-", "-")
		}
		ret = append(ret, str)
	}

	return ret
}

// GetTablesSchemaInfo returns the schema info for a given set of tables
func (c *DBConnector) GetTablesSchemaInfo(tableNames []string) (ret []string) {
	for _, table := range tableNames {
		if !c.TableExists(table) {
			continue
		}

		ret = append(ret, fmt.Sprintf("TABLE: %s", table)) //nolint:perfsprint

		// Query to list columns

		var listColumnsQuery string
		switch c.DbOpts.Driver {
		case POSTGRES, MYSQL, MSSQL:
			listColumnsQuery = fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'", table)
		case CLICKHOUSE:
			listColumnsQuery = fmt.Sprintf("SELECT name AS column_name, type AS data_type FROM system.columns WHERE table = '%s'", table)
		case CASSANDRA:
			listColumnsQuery = fmt.Sprintf("SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s'", CassandraKeySpace, table)
		case SQLITE:
			listColumnsQuery = fmt.Sprintf("PRAGMA table_info('%s')", table)
		default:
			c.Exit("unsupported database type: %s", c.DbOpts.Driver)
		}

		columns, err := c.Query(listColumnsQuery)
		if err != nil {
			c.Exit("error: %s", err)
		}

		ret = append(ret, "  Columns:")

		switch c.DbOpts.Driver {
		case SQLITE:
			for columns.Next() {
				var columnName, dataType string
				var unusedInt int
				var unusedBool bool
				var unusedVoid interface{}
				if err := columns.Scan(&unusedInt, &columnName, &dataType, &unusedBool, &unusedVoid, &unusedInt); err != nil {
					c.Exit("error: %s\nquery: %s", err, listColumnsQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s: %s", columnName, dataType))
			}
		default:
			for columns.Next() {
				var columnName, dataType string
				if err := columns.Scan(&columnName, &dataType); err != nil {
					c.Exit("error: %s\nquery: %s", err, listColumnsQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s: %s", columnName, dataType))
			}
		}
		err = columns.Close() //nolint:sqlclosecheck
		if err != nil {
			c.Exit("close error: %s\nquery: %s", err, listColumnsQuery)
		}

		// Query to list indexes

		var listIndexesQuery string
		switch c.DbOpts.Driver {
		case POSTGRES:
			listIndexesQuery = fmt.Sprintf("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '%s'", table)
		case MYSQL:
			listIndexesQuery = fmt.Sprintf("SELECT TABLE_NAME, NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION, "+
				"    CARDINALITY, SUB_PART, NULLABLE, INDEX_TYPE, COMMENT "+
				"FROM "+
				"    information_schema.STATISTICS "+
				"WHERE "+
				"    TABLE_NAME = '%s';", table)
		case MSSQL:
			listIndexesQuery = fmt.Sprintf("SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('%s')", table)
		case SQLITE:
			listIndexesQuery = fmt.Sprintf("PRAGMA index_list('%s')", table)
		case CLICKHOUSE:
			listIndexesQuery = fmt.Sprintf("SHOW CREATE TABLE %s", table) //nolint:perfsprint
		case CASSANDRA:
			listIndexesQuery = fmt.Sprintf("select index_name, kind, options from system_schema.indexes where keyspace_name = '%s' and table_name = '%s'", CassandraKeySpace, table)
		default:
			c.Exit("unsupported database type: %s", c.DbOpts.Driver)
		}

		indexes, err := c.Query(listIndexesQuery)
		if err != nil {
			c.Exit("error: %s", err)
		}

		ret = append(ret, "  Indexes:")
		for indexes.Next() {
			var indexName, indexDef string
			switch c.DbOpts.Driver {
			case POSTGRES:
				if err := indexes.Scan(&indexName, &indexDef); err != nil {
					c.Exit("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s: %s", indexName, indexDef))
			case MYSQL:
				var nonUnique bool
				var seqInIndex int
				var columnName, collation, indexType, comment string
				var cardinality, subPart interface{}
				var nullable string
				if err := indexes.Scan(&table, &nonUnique, &indexName, &seqInIndex, &columnName, &collation, &cardinality,
					&subPart, &nullable, &indexType, &comment); err != nil {
					c.Exit("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s", indexName)) //nolint:perfsprint
			case MSSQL:
				if err := indexes.Scan(&indexName); err != nil {
					c.Exit("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s", indexName)) //nolint:perfsprint
			case SQLITE:
				var seq int
				var unique, partial bool
				var origin string
				if err := indexes.Scan(&seq, &indexName, &unique, &origin, &partial); err != nil {
					c.Exit("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s", indexName)) //nolint:perfsprint
			case CLICKHOUSE:
				var createStatement string
				if err := indexes.Scan(&createStatement); err != nil {
					c.Exit("error: %s\nquery: %s", err, listIndexesQuery)
				}

				// Regular expression to find ORDER BY clause
				re := regexp.MustCompile(`ORDER BY (.*?)\n`)
				matches := re.FindStringSubmatch(createStatement)
				if len(matches) < 2 {
					c.Exit("The 'ORDER BY' clause not found in the output of '%s':\n%s", listIndexesQuery, createStatement)
				}

				// Extracting columns listed in ORDER BY
				pkName := matches[1]

				ret = append(ret, fmt.Sprintf("   - %s (primary key)", pkName))
			case CASSANDRA:
				var idxName string
				var kind string
				var options map[string]string
				if err := indexes.Scan(&idxName, &kind, &options); err != nil {
					c.Exit("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s:%s - %v", idxName, kind, options))
			default:
				c.Exit("unsupported database type: %s", c.DbOpts.Driver)
			}
		}
		err = indexes.Close() //nolint:sqlclosecheck
		if err != nil {
			c.Exit("close error: %s\nquery: %s", err, listIndexesQuery)
		}

		ret = append(ret, "")
	}

	return
}

// DBType - database type
type DBType struct {
	Driver string // driver name (used in the code)
	Symbol string // short symbol for the driver (used in the command line)
	Name   string // full name of the driver (used in the command line)
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

	return ret
}
