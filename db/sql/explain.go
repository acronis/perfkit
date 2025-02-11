package sql

import (
	"database/sql"
	"fmt"

	"github.com/acronis/perfkit/db"
)

// addExplainPrefix adds an 'explain' prefix to the query
func addExplainPrefix(dialectName db.DialectName, query string) (string, error) {
	switch dialectName {
	case db.MYSQL:
		return "EXPLAIN " + query, nil
	case db.POSTGRES:
		return "EXPLAIN ANALYZE " + query, nil
	case db.SQLITE:
		return "EXPLAIN QUERY PLAN " + query, nil
	case db.CASSANDRA:
		return "TRACING ON; " + query, nil
	default:
		return "", fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", dialectName)
	}
}

// logExplainResults logs the results of the 'explain' query
func logExplainResults(logger db.Logger, dialectName db.DialectName, rows *sql.Rows, query string, args ...interface{}) error {
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

	logger.Log("\n%s", query)
	if args != nil {
		logger.Log(" %v\n", args)
	} else {
		logger.Log("\n")
	}

	for rows.Next() {
		switch dialectName {
		case db.SQLITE:
			var id, parent, notUsed int
			var detail string
			if err = rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			logger.Log("ID: %d, Parent: %d, Not Used: %d, Detail: %s\n", id, parent, notUsed, detail)
		case db.MYSQL:
			if err = rows.Scan(scanArgs...); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			// Print each column as a string.
			for i, col := range values {
				logger.Log("  %-15s: %s\n", cols[i], string(col))
			}
			logger.Log("\n")
		case db.POSTGRES:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			logger.Log("  ", explainOutput)
		case db.CASSANDRA:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			logger.Log("  ", explainOutput)
		default:
			return fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", dialectName)
		}
	}

	return nil
}
