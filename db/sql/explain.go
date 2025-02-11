package sql

import (
	"database/sql"
	"fmt"

	"github.com/acronis/perfkit/db"
)

// addExplainPrefix adds an 'explain' prefix to the query
func (g *sqlGateway) addExplainPrefix(query string) (string, error) {
	switch g.dialect.name() {
	case db.MYSQL:
		return "EXPLAIN " + query, nil
	case db.POSTGRES:
		return "EXPLAIN ANALYZE " + query, nil
	case db.SQLITE:
		return "EXPLAIN QUERY PLAN " + query, nil
	case db.CASSANDRA:
		return "TRACING ON; " + query, nil
	default:
		return "", fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", g.dialect.name())
	}
}

// explain executes an 'explain' query
func (g *sqlGateway) explain(rows *sql.Rows, query string, args ...interface{}) error {
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

	g.queryLogger.Log("\n%s", query)
	if args != nil {
		g.queryLogger.Log(" %v\n", args)
	} else {
		g.queryLogger.Log("\n")
	}

	for rows.Next() {
		switch g.dialect.name() {
		case db.SQLITE:
			var id, parent, notUsed int
			var detail string
			if err = rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			g.queryLogger.Log("ID: %d, Parent: %d, Not Used: %d, Detail: %s\n", id, parent, notUsed, detail)
		case db.MYSQL:
			if err = rows.Scan(scanArgs...); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			// Print each column as a string.
			for i, col := range values {
				g.queryLogger.Log("  %-15s: %s\n", cols[i], string(col))
			}
			g.queryLogger.Log("\n")
		case db.POSTGRES:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			g.queryLogger.Log("  ", explainOutput)
		case db.CASSANDRA:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			g.queryLogger.Log("  ", explainOutput)
		default:
			return fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", g.dialect.name())
		}
	}

	return nil
}
