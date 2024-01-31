package benchmark

import (
	"fmt"
	"time"

	"github.com/gocraft/dbr"
)

/*
 * SQL queries logging
 */

// DBRQuery is a struct for storing query and its duration
type DBRQuery struct {
	query    string
	duration float64
}

// DBREventReceiver is a wrapper for dbr.EventReceiver interface
type DBREventReceiver struct {
	connector   *DBConnector
	exitOnError bool
	queries     []DBRQuery
}

// Event logs query into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *DBREventReceiver) Event(eventName string) {
	if eventName == "dbr.begin" || eventName == "dbr.commit" {
		return
	}
	fmt.Printf("DBREventReceiver::Event occured: %s", eventName)
}

// EventKv logs query and its key-value pairs into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *DBREventReceiver) EventKv(eventName string, kvs map[string]string) {
	fmt.Printf("DBREventReceiver::EventKv occured: %s: kvs: %v", eventName, kvs)
}

// EventErr logs query and its error into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *DBREventReceiver) EventErr(eventName string, err error) error { //nolint:revive
	fmt.Printf("DBREventReceiver::EventErr occured: %s", eventName)

	return nil
}

// Timing logs query and its duration into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *DBREventReceiver) Timing(eventName string, nanoseconds int64) {
	fmt.Printf("DBREventReceiver::Timing occured: %s: ns: %d", eventName, nanoseconds)
}

// EventErrKv logs query and its error into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *DBREventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	if err != nil {
		c := r.connector
		c.Log(LogError, fmt.Sprintf("eventName: %s: %s # %s", eventName, kvs["sql"], err))
		if r.exitOnError {
			c.Exit("Aborting")
		}
	}

	return nil
}

// TimingKv adds query and its duration into DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (r *DBREventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) { //nolint:revive
	c := r.connector
	if c.logLevel >= LogInfo {
		r.queries = append(r.queries, DBRQuery{query: kvs["sql"], duration: float64(nanoseconds) / 1000000000.0})
	}
}

// DBRLogQuery logs all queries from DBREventReceiver.queries slice (if logLevel >= LogInfo)
func (c *DBConnector) DBRLogQuery(result interface{}) {
	var r = c.DbrSess().EventReceiver

	er := r.(*DBREventReceiver)

	if c.logLevel >= LogInfo {
		for _, q := range er.queries {
			ret := fmt.Sprintf("%s # dur: %.6f", q.query, q.duration)

			if result != nil && c.Logger.LogLevel >= LogTrace {
				ret += " # = "
				ret += DumpRecursive(result, "  ")
			}
			c.Log(c.logLevel, ret)
		}
	}
	er.queries = []DBRQuery{}
}

// DbrSess returns dbr.Session object (creates it if needed)
func (c *DBConnector) DbrSess() *dbr.Session {
	if c.dbrSess == nil {
		c.DBRConnect()
	}

	return c.dbrSess
}

// DBRConnect connects to DB via DBR (gocraft/dbr) library and sets c.dbrSess
func (c *DBConnector) DBRConnect() {
	if c.dbrSess != nil {
		return
	}

	switch c.DbOpts.Driver {
	case SQLITE, POSTGRES, MYSQL, MSSQL:
		break
	default:
		c.Exit("unsupported driver: '%v', supported drivers are: %s|%s|%s|%s", c.DbOpts.Driver, SQLITE, POSTGRES, MYSQL, MSSQL)
	}

	c.Log(LogTrace, "connecting to DB (via DBR) ... ")

	connected := false
	var conn *dbr.Connection
	var err error

	driver := c.DbOpts.Driver
	if driver == SQLITE {
		driver = "sqlite3"
	}

	for r := 0; !connected && r < c.RetryAttempts; r++ {
		conn, err = dbr.Open(driver, c.DbOpts.Dsn, &DBREventReceiver{connector: c, exitOnError: true, queries: []DBRQuery{}})

		if err == nil {
			err = c.Ping()
			if err == nil {
				connected = true
			}
		}

		if !connected {
			c.Log(LogDebug, "DB connection attempt #%d failed, error: %v", r+1, err)
			time.Sleep(1 * time.Millisecond)

			continue
		}

		c.dbrSess = conn.NewSession(nil)
	}

	if !connected {
		c.Exit("DB connection error: %v", err)
	}

	c.Log(LogTrace, "connected to DB")

	c.dbrSess.Connection.DB.SetMaxOpenConns(c.DbOpts.MaxOpenConns)
	c.dbrSess.Connection.DB.SetMaxIdleConns(c.DbOpts.MaxOpenConns)
}

// DBRSelect executes SELECT query and loads result into rows variable (pointer to slice)
func (c *DBConnector) DBRSelect(from string, what string, where string, orderBy string, limit int, rows interface{}) {
	q := c.DbrSess().Select("*").From(from).Limit(uint64(limit))

	if orderBy != "" {
		q = q.OrderBy(orderBy)
	}

	if where != "" {
		q = q.Where(where)
	}
	_, err := q.Load(rows)
	if err != nil {
		c.Exit("DBRSelect load error: %v: from: %s, what: %s, where: %s, orderBy: %s, limit: %d", err, from, what, where, orderBy, limit)
	}

	c.DBRLogQuery(rows)
}
