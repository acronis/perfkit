package sql

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/db"
)

func (g *sqlGateway) bulkInsertParameterized(tableName string, rows [][]interface{}, columnNames []string) error {
	if len(rows) == 0 {
		return nil
	}

	var values []interface{}
	for _, row := range rows {
		if len(row) != len(columnNames) {
			return fmt.Errorf("row length doesn't match column names length")
		}
		values = append(values, row...)
	}

	var valuesReference []string
	if g.dialect.name() == db.POSTGRES {
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
	} else {
		for j := 0; j < len(rows); j++ {
			var ret = make([]string, len(columnNames))
			for k := 0; k < len(columnNames); k++ {
				ret[k] = "?"
			}
			var parametersPlaceholder = strings.Join(ret, ",")
			valuesReference = append(valuesReference, fmt.Sprintf("(%s)", parametersPlaceholder))
		}
	}

	var query string
	if g.dialect.name() == db.CASSANDRA && len(rows) > 1 {
		var insertQueries []string
		for _, valRef := range valuesReference {
			insertQueries = append(insertQueries,
				fmt.Sprintf("\tINSERT INTO %s(%s) VALUES %s;",
					g.dialect.table(tableName),
					strings.Join(columnNames, ", "),
					valRef))
		}
		query = fmt.Sprintf("BEGIN BATCH\n%s\nAPPLY BATCH;", strings.Join(insertQueries, "\n"))
	} else {
		query = fmt.Sprintf("INSERT INTO %s(%s) VALUES %s;",
			g.dialect.table(tableName),
			strings.Join(columnNames, ", "),
			strings.Join(valuesReference, ", "))
	}

	var _, err = g.rw.execContext(g.ctx, query, values...)

	if err != nil {
		return fmt.Errorf("DB exec failed: %w", err)
	}

	return nil
}

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
			valuesInRow = append(valuesInRow, sqlf(g.dialect, "%v", col))
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(valuesInRow, ", ")))
	}

	var query string
	if g.dialect.name() == db.CASSANDRA && len(rows) > 1 {
		var insertQueries []string
		for _, val := range values {
			insertQueries = append(insertQueries,
				fmt.Sprintf("\tINSERT INTO %s(%s) VALUES %s;",
					g.dialect.table(tableName),
					strings.Join(columnNames, ", "),
					val))
		}
		query = fmt.Sprintf("BEGIN BATCH\n%s\nAPPLY BATCH;", strings.Join(insertQueries, "\n"))
	} else {
		query = fmt.Sprintf("INSERT INTO %s(%s) VALUES %s;",
			g.dialect.table(tableName),
			strings.Join(columnNames, ", "),
			strings.Join(values, ", "))
	}

	var _, err = g.rw.execContext(g.ctx, query)

	if err != nil {
		return fmt.Errorf("DB exec failed: %w", err)
	}

	return nil
}

// BulkInsert inserts rows into a table
func (g *sqlGateway) BulkInsert(tableName string, rows [][]interface{}, columnNames []string) error {
	if g.QueryStringInterpolation {
		return g.bulkInsertLiteral(tableName, rows, columnNames)
	} else {
		return g.bulkInsertParameterized(tableName, rows, columnNames)
	}
}
