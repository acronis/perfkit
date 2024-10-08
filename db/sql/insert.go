package sql

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/db"
)

// BulkInsert inserts rows into a table
func (g *sqlGateway) BulkInsert(tableName string, rows [][]interface{}, columnNames []string) error {
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
