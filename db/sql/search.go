package sql

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/acronis/perfkit/db"
)

// rUpdatePlaceholders is a regexp to replace placeholders
var rUpdatePlaceholders = regexp.MustCompile(`\$\d+`)

// updatePlaceholders replaces placeholders
func (a *gateway) updatePlaceholders(query string) string {
	if a.dialect.name() == db.MYSQL || a.dialect.name() == db.SQLITE || a.dialect.name() == db.CASSANDRA {
		return rUpdatePlaceholders.ReplaceAllString(query, "?")
	}

	return query
}

// Search executes a query and returns the result set as a slice of maps
func (a *gateway) Search(from string, what string, where string, orderBy string, limit int, explain bool, args ...interface{}) (db.Rows, error) {
	var query string

	switch a.dialect.name() {
	case db.MSSQL:
		query = fmt.Sprintf("SELECT {LIMIT} %s FROM %s {WHERE} {ORDERBY}", what, a.dialect.table(from))
	default:
		query = fmt.Sprintf("SELECT %s FROM %s {WHERE} {ORDERBY} {LIMIT}", what, a.dialect.table(from))
	}

	if where == "" {
		query = strings.Replace(query, "{WHERE}", "", -1)
	} else {
		query = strings.Replace(query, "{WHERE}", fmt.Sprintf("WHERE %s", where), -1) //nolint:perfsprint
	}

	if limit == 0 {
		query = strings.Replace(query, "{LIMIT}", "", -1)
	} else {
		switch a.dialect.name() {
		case db.MSSQL:
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

	query = a.updatePlaceholders(query)

	var rows *sql.Rows
	var err error
	startTime := a.StatementEnter(query, args)

	if explain {
		query, err = a.addExplainPrefix(query)
	}
	if err != nil {
		return nil, err
	}

	rows, err = a.rw.QueryContext(a.ctx, query, args...)

	if explain {
		return nil, a.explain(rows, query, args...)
	}

	if err != nil {
		return nil, fmt.Errorf("DB query failed: %w", err)
	}

	a.StatementExit("Query()", startTime, err, false, nil, query, args, nil, nil)

	return &sqlRows{rows: rows}, nil
}

// addExplainPrefix adds an 'explain' prefix to the query
func (a *gateway) addExplainPrefix(query string) (string, error) {
	switch a.dialect.name() {
	case db.MYSQL:
		return "EXPLAIN " + query, nil
	case db.POSTGRES:
		return "EXPLAIN ANALYZE " + query, nil
	case db.SQLITE:
		return "EXPLAIN QUERY PLAN " + query, nil
	case db.CASSANDRA:
		return "TRACING ON; " + query, nil
	default:
		return "", fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", a.dialect.name())
	}
}

// explain executes an 'explain' query
func (a *gateway) explain(rows *sql.Rows, query string, args ...interface{}) error {
	// Iterate over the result set
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("DB query failed: %s\nError: %s", query, err)
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	a.queryLogger.Log("\n%s", query)
	if args != nil {
		a.queryLogger.Log(" %v\n", args)
	} else {
		a.queryLogger.Log("\n")
	}

	for rows.Next() {
		switch a.dialect.name() {
		case db.SQLITE:
			var id, parent, notUsed int
			var detail string
			if err = rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			a.queryLogger.Log("ID: %d, Parent: %d, Not Used: %d, Detail: %s\n", id, parent, notUsed, detail)
		case db.MYSQL:
			if err = rows.Scan(scanArgs...); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			// Print each column as a string.
			for i, col := range values {
				a.queryLogger.Log("  %-15s: %s\n", cols[i], string(col))
			}
			a.queryLogger.Log("\n")
		case db.POSTGRES:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			a.queryLogger.Log("  ", explainOutput)
		case db.CASSANDRA:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			a.queryLogger.Log("  ", explainOutput)
		default:
			return fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", a.dialect.name())
		}
	}

	return nil
}

func (a *gateway) Aggregate(from string, what string, where string, groupBy string, orderBy string, limit int, explain bool, args ...interface{}) (db.Rows, error) {
	return nil, nil
}
