package sql

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/db"
)

// bulkInsertParameterized implements part of the databaseInserter interface by performing
// a bulk insert operation using parameterized queries for SQL injection protection.
//
// Interface relationships:
// - Uses databaseQuerier.Exec through g.rw.execContext
// - Uses Dialect interface for database-specific formatting
// - Part of the Database.BulkInsert implementation chain
//
// Parameters:
// - tableName: name of the table to insert into
// - rows: slice of rows, where each row is a slice of values matching columnNames
// - columnNames: names of the columns in the order they appear in rows
func (g *sqlGateway) bulkInsertParameterized(tableName string, rows [][]interface{}, columnNames []string) error {
	if len(rows) == 0 {
		return nil
	}

	// Validate row lengths and flatten values for parameterized query
	var values []interface{}
	for _, row := range rows {
		if len(row) != len(columnNames) {
			return fmt.Errorf("row length doesn't match column names length")
		}
		values = append(values, row...)
	}

	// Generate parameter placeholders based on dialect
	var valuesReference []string
	if g.dialect.name() == db.POSTGRES {
		// PostgreSQL uses $1, $2, etc. for parameter placeholders
		var i = 0
		for j := 0; j < len(rows); j++ {
			var ret = make([]string, len(columnNames))
			for k := 0; k < len(columnNames); k++ {
				ret[k] = fmt.Sprintf("$%d", i+1)
				i++
			}
			var parametersPlaceholder = strings.Join(ret, ",")
			valuesReference = append(valuesReference, fmt.Sprintf("(%s)", parametersPlaceholder))
		}
	} else if g.dialect.name() == db.MSSQL {
		// SQL Server uses @p1, @p2, etc. for parameter placeholders
		var i = 0
		for j := 0; j < len(rows); j++ {
			var ret = make([]string, len(columnNames))
			for k := 0; k < len(columnNames); k++ {
				ret[k] = fmt.Sprintf("@p%d", i+1)
				i++
			}
			var parametersPlaceholder = strings.Join(ret, ",")
			valuesReference = append(valuesReference, fmt.Sprintf("(%s)", parametersPlaceholder))
		}
	} else {
		// Other SQL databases use ? for parameter placeholders
		for j := 0; j < len(rows); j++ {
			var ret = make([]string, len(columnNames))
			for k := 0; k < len(columnNames); k++ {
				ret[k] = "?"
			}
			var parametersPlaceholder = strings.Join(ret, ",")
			valuesReference = append(valuesReference, fmt.Sprintf("(%s)", parametersPlaceholder))
		}
	}

	// Construct the final query based on dialect
	var query string
	if g.dialect.name() == db.CASSANDRA && len(rows) > 1 {
		// Cassandra requires BATCH for multiple inserts
		var insertQueries []string
		for _, valRef := range valuesReference {
			insertQueries = append(insertQueries,
				fmt.Sprintf("\tINSERT INTO %s(%s) VALUES %s;",
					g.dialect.table(tableName), // Uses Dialect interface to format table name
					strings.Join(columnNames, ", "),
					valRef))
		}
		query = fmt.Sprintf("BEGIN BATCH\n%s\nAPPLY BATCH;", strings.Join(insertQueries, "\n"))
	} else {
		// Standard SQL INSERT with multiple VALUES clauses
		query = fmt.Sprintf("INSERT INTO %s(%s) VALUES %s;",
			g.dialect.table(tableName), // Uses Dialect interface to format table name
			strings.Join(columnNames, ", "),
			strings.Join(valuesReference, ", "))
	}

	// Execute the query using databaseQuerier.Exec interface
	// Returns Result interface for LastInsertId and RowsAffected
	var _, err = g.rw.execContext(g.ctx, query, values...)

	if err != nil {
		return fmt.Errorf("DB exec failed: %w", err)
	}

	return nil
}

// bulkInsertLiteral implements part of the databaseInserter interface by performing
// a bulk insert operation using string interpolation for values.
//
// Interface relationships:
// - Uses databaseQuerier.Exec through g.rw.execContext
// - Uses Dialect interface for database-specific formatting and value escaping
// - Part of the Database.BulkInsert implementation chain
//
// Parameters:
// - tableName: name of the table to insert into
// - rows: slice of rows, where each row is a slice of values matching columnNames
// - columnNames: names of the columns in the order they appear in rows
//
// Security considerations:
// - Less secure than parameterized queries as it uses string interpolation
// - Values are still escaped according to dialect rules
// - Should only be used when required by specific database features
func (g *sqlGateway) bulkInsertLiteral(tableName string, rows [][]interface{}, columnNames []string) error {
	if len(rows) == 0 {
		return nil
	}

	var values []string
	for _, row := range rows {
		if len(row) != len(columnNames) {
			return fmt.Errorf("row length doesn't match column names length")
		}
		var valuesInRow []string
		for _, col := range row {
			// Uses Dialect interface to format and escape values according to database rules
			valuesInRow = append(valuesInRow, sqlf(g.dialect, "%v", col))
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(valuesInRow, ", ")))
	}

	var query string
	if g.dialect.name() == db.CASSANDRA && len(rows) > 1 {
		// Cassandra requires BATCH for multiple inserts
		var insertQueries []string
		for _, val := range values {
			insertQueries = append(insertQueries,
				fmt.Sprintf("\tINSERT INTO %s(%s) VALUES %s;",
					g.dialect.table(tableName), // Uses Dialect interface to format table name
					strings.Join(columnNames, ", "),
					val))
		}
		query = fmt.Sprintf("BEGIN BATCH\n%s\nAPPLY BATCH;", strings.Join(insertQueries, "\n"))
	} else {
		// Standard SQL INSERT with multiple VALUES clauses
		query = fmt.Sprintf("INSERT INTO %s(%s) VALUES %s;",
			g.dialect.table(tableName), // Uses Dialect interface to format table name
			strings.Join(columnNames, ", "),
			strings.Join(values, ", "))
	}

	// Execute the query using databaseQuerier.Exec interface
	// Returns Result interface for LastInsertId and RowsAffected
	var _, err = g.rw.execContext(g.ctx, query)

	if err != nil {
		return fmt.Errorf("DB exec failed: %w", err)
	}

	return nil
}

// BulkInsert implements the databaseInserter interface by inserting multiple rows
// into a table in a single operation.
//
// Interface relationships:
// - Implements databaseInserter.BulkInsert
// - Delegates to either bulkInsertParameterized or bulkInsertLiteral based on configuration
//
// Parameters:
// - tableName: name of the table to insert into
// - rows: slice of rows, where each row is a slice of values matching columnNames
// - columnNames: names of the columns in the order they appear in rows
//
// Implementation details:
// - When QueryStringInterpolation is false (default):
//   - Uses parameterized queries with placeholders
//   - Values are passed separately from the query string
//   - Provides better SQL injection protection
//   - Different placeholder syntax per dialect:
//   - PostgreSQL: Uses $1, $2, $3, etc.
//   - Other SQL databases: Uses ?
//
// - When QueryStringInterpolation is true:
//   - Uses string interpolation
//   - Values are converted to strings and embedded directly in the query
//   - Each value is formatted according to its type and dialect
//   - Less secure but may be needed for specific database requirements
func (g *sqlGateway) BulkInsert(tableName string, rows [][]interface{}, columnNames []string) error {
	if g.QueryStringInterpolation {
		return g.bulkInsertLiteral(tableName, rows, columnNames)
	} else {
		return g.bulkInsertParameterized(tableName, rows, columnNames)
	}
}
