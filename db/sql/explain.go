package sql

import (
	"database/sql"
	"fmt"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"
)

// addExplainPrefix adds an 'explain' prefix to the query
// Parameters:
//   - dialectName: type of database (MySQL, Postgres, SQLite, Cassandra)
//   - query: the SQL query to be explained
//
// Returns:
//   - string: modified query with appropriate EXPLAIN syntax
//   - error: if dialect is not supported
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
// Parameters:
//   - logger: interface for logging output
//   - dialectName: type of database
//   - rows: result set from the executed query
//   - query: the original SQL query
//   - args: optional query parameters
//
// Returns:
//   - error: if any operation fails
func logExplainResults(logger logger.Logger, dialectName db.DialectName, rows *sql.Rows, query string, args ...interface{}) error {
	// Get column names from the result set
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("DB query failed: %s\nError: %s", query, err)
	}

	// Prepare slices for scanning row data (used for MySQL)
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Log the original query and its parameters
	logger.Info("\n%s", query)
	if args != nil {
		logger.Info(" %v\n", args)
	} else {
		logger.Info("\n")
	}

	// Iterate through result rows
	for rows.Next() {
		switch dialectName {
		case db.SQLITE:
			// SQLite specific scanning with fixed columns
			var id, parent, notUsed int
			var detail string
			if err = rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			logger.Info("ID: %d, Parent: %d, Not Used: %d, Detail: %s\n", id, parent, notUsed, detail)
		case db.MYSQL:
			// MySQL scanning into dynamic column array
			if err = rows.Scan(scanArgs...); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			// Print each column with its name
			for i, col := range values {
				logger.Info("  %-15s: %s", cols[i], string(col))
			}
			logger.Info("\n")
		case db.POSTGRES, db.CASSANDRA:
			// Postgres and Cassandra return explain output as a single text column
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			logger.Info("  %s", explainOutput)
		default:
			return fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", dialectName)
		}
	}

	return nil
}
