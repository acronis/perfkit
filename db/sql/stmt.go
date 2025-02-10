package sql

import (
	"database/sql"
	"time"

	"github.com/acronis/perfkit/db"
)

// StatementEnter is called before executing a statement
func (g *sqlGateway) StatementEnter(query string, args ...interface{}) time.Time { //nolint:revive
	var startTime time.Time

	/*
		if query != "" {
			a.lastQuery = query
		}

		if a.log.LogLevel >= c.logLevel {
			startTime = time.Now()
		}

	*/

	return startTime
}

// StatementExit is called after executing a statement
func (g *sqlGateway) StatementExit(statement string, startTime time.Time, err error, showRowsAffected bool, result db.Result, format string, args []interface{}, rows db.Rows, dest []interface{}) {
	/*
		if a.Logger.LogLevel < c.logLevel && err == nil {
			return
		}

		var msg string
		if a.Logger.LogLevel >= LogTrace {
			if format == "" {
				msg = fmt.Sprintf("%v", args...)
			} else {
				msg = fmt.Sprintf(format, args...)
			}
		} else {
			msg = format
		}

		if err == nil {
			if a.Logger.LogLevel >= LogDebug {
				msg += fmt.Sprintf(" # dur: %.6f", getElapsedTime(startTime))
			}
			if a.Logger.LogLevel >= LogTrace {
				if c.dialect.Name() != db.CLICKHOUSE && showRowsAffected && result != nil {
					affectedRows, err := result.RowsAffected()
					if err != nil {
						c.Exit("DB: %s failed: %s\nError: %s", c.dialect.Name(), statement, err.Error())
					}
					msg += fmt.Sprintf(" # affected rows: %d", affectedRows)
				}
				if rows != nil {
					msg += fmt.Sprintf(" # = %d row(s): %s", len(rows.Data), rows.Dump())
				}
				if dest != nil {
					var vals []string
					for _, v := range dest {
						vals = append(vals, db.DumpRecursive(v, ""))
					}
					msg += fmt.Sprintf(" = %v", strings.Join(vals, ", "))
				}
			}
			c.Log(c.logLevel, msg)
		} else {
			c.Log(LogError, fmt.Sprintf("%s: '%s' error:\n%s", statement, msg, err.Error()))
		}

	*/
}

type sqlStatement interface {
	Exec(args ...any) (sql.Result, error)
	Close() error
}

type sqlStmt struct {
	stmt *sql.Stmt
}

func (s *sqlStmt) Exec(args ...any) (sql.Result, error) {
	return s.stmt.Exec(args...)
}

func (s *sqlStmt) Close() error {
	return s.stmt.Close()
}

// wrapperStmt is a wrapper for sqlStmt that adds additional features:
// - logging of queries
// - dry-run mode
type wrapperStmt struct {
	stmt sqlStatement

	dryRun      bool
	queryLogger db.Logger
}

func (ws *wrapperStmt) Exec(args ...any) (db.Result, error) {
	if ws.queryLogger != nil {
		if ws.dryRun {
			ws.queryLogger.Log("-- EXECUTE stmt -- skip because of 'dry-run' mode")
		} else {
			ws.queryLogger.Log("EXECUTE stmt;")
		}
	}

	if ws.dryRun {
		return &sqlSurrogateResult{}, nil
	}

	return ws.stmt.Exec(args...)
}

func (ws *wrapperStmt) Close() error {
	if ws.queryLogger != nil {
		ws.queryLogger.Log("DEALLOCATE PREPARE stmt;")
	}

	return ws.stmt.Close()
}

func (g *sqlGateway) Prepare(query string) (db.Stmt, error) {
	var stmt, err = g.rw.prepareContext(g.ctx, query)
	if err != nil {
		return nil, err
	}

	return &wrapperStmt{
		stmt:        &sqlStmt{stmt: stmt},
		dryRun:      g.dryRun,
		queryLogger: g.queryLogger,
	}, err
}

// getElapsedTime returns elapsed time since startTime
func getElapsedTime(prevTime time.Time) float64 {
	return time.Since(prevTime).Seconds()
}
